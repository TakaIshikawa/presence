"""Audit generated content source attribution chains."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
STALE_METADATA_DAYS = 180


@dataclass(frozen=True)
class SourceAttributionChainBreak:
    issue_type: str
    content_id: int | None
    source_id: int | None
    knowledge_id: int | None
    source_url: str | None
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceAttributionChainBreakReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    issues: tuple[SourceAttributionChainBreak, ...]
    empty_state: dict[str, Any]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "source_attribution_chain_breaks",
            "empty_state": dict(self.empty_state),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_source_attribution_chain_breaks_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    content_type: str | None = None,
    issue_type: str | None = None,
    now: datetime | None = None,
) -> SourceAttributionChainBreakReport:
    if days <= 0:
        raise ValueError("days must be positive")
    valid = {"missing_knowledge_row", "missing_source_url", "stale_link_metadata", "uncited_curated_reference"}
    if issue_type and issue_type not in valid:
        raise ValueError(f"invalid issue_type: {issue_type}")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "content_type": content_type, "issue_type": issue_type}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = ("generated_content", "content_knowledge_links", "knowledge")
    missing = tuple(table for table in required if table not in schema)
    if missing:
        return _report(generated_at, filters, (), 0, missing)
    content_ids = _content_ids(conn, schema["generated_content"], cutoff.isoformat(), content_type)
    issues: list[SourceAttributionChainBreak] = []
    links = _links(conn, schema["content_knowledge_links"], content_ids)
    knowledge = _knowledge(conn, schema["knowledge"])
    linked_knowledge_ids = {int(link["knowledge_id"]) for link in links if link.get("knowledge_id") is not None}
    for link in links:
        kid = _int_or_none(link.get("knowledge_id"))
        content_id = _int_or_none(link.get("content_id"))
        if kid is None or kid not in knowledge:
            issues.append(_issue("missing_knowledge_row", content_id, None, kid, None))
            continue
        row = knowledge[kid]
        source_url = _clean(row.get("source_url"))
        source_id = _int_or_none(row.get("source_id"))
        if not source_url:
            issues.append(_issue("missing_source_url", content_id, source_id, kid, None))
        metadata_at = row.get("metadata_checked_at")
        if metadata_at and (_age_days(generated_at, _parse_ts(metadata_at)) or 0) > STALE_METADATA_DAYS:
            issues.append(_issue("stale_link_metadata", content_id, source_id, kid, source_url))
    for row in _curated_sources(conn, schema):
        source_id = _int_or_none(row.get("id"))
        kid = _int_or_none(row.get("knowledge_id"))
        if kid is not None and kid not in linked_knowledge_ids:
            issues.append(_issue("uncited_curated_reference", None, source_id, kid, _clean(row.get("source_url"))))
    if issue_type:
        issues = [item for item in issues if item.issue_type == issue_type]
    issues.sort(key=lambda item: (item.issue_type, item.content_id or 0, item.knowledge_id or 0))
    return _report(generated_at, filters, tuple(issues), len(links), ())


def format_source_attribution_chain_breaks_json(report: SourceAttributionChainBreakReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_attribution_chain_breaks_text(report: SourceAttributionChainBreakReport) -> str:
    lines = [
        "Source Attribution Chain Breaks",
        f"Window={report.filters['days']} days; content_type={report.filters.get('content_type') or 'all'}; issue_type={report.filters.get('issue_type') or 'all'}",
        f"Links scanned={report.totals['links_scanned']}; issues={report.totals['issue_count']}",
        "",
    ]
    if not report.issues:
        lines.append(report.empty_state["message"])
        return "\n".join(lines)
    for issue in report.issues:
        lines.append(f"- {issue.issue_type} content={issue.content_id or '-'} knowledge={issue.knowledge_id or '-'} source={issue.source_id or '-'} url={issue.source_url or '-'}")
        lines.append(f"  action={issue.recommended_action}")
    return "\n".join(lines)


def _content_ids(conn: sqlite3.Connection, cols: set[str], cutoff: str, content_type: str | None) -> list[int]:
    if "id" not in cols:
        return []
    created_col = _first(cols, ("created_at", "generated_at"))
    type_col = _first(cols, ("content_type", "type", "format"))
    where = []
    params: list[Any] = []
    if created_col:
        where.append(f"{created_col} >= ?")
        params.append(cutoff)
    if content_type and type_col:
        where.append(f"{type_col} = ?")
        params.append(content_type)
    sql = "SELECT id FROM generated_content"
    if where:
        sql += " WHERE " + " AND ".join(where)
    return [int(row["id"]) for row in conn.execute(sql, params).fetchall()]


def _links(conn: sqlite3.Connection, cols: set[str], content_ids: list[int]) -> list[dict[str, Any]]:
    content_col = _first(cols, ("content_id", "generated_content_id"))
    knowledge_col = _first(cols, ("knowledge_id", "source_id"))
    if not content_col or not knowledge_col:
        return []
    if not content_ids:
        return []
    placeholders = ",".join("?" for _ in content_ids)
    sql = f"SELECT {content_col} AS content_id, {knowledge_col} AS knowledge_id FROM content_knowledge_links WHERE {content_col} IN ({placeholders})"
    return [dict(row) for row in conn.execute(sql, content_ids).fetchall()]


def _knowledge(conn: sqlite3.Connection, cols: set[str]) -> dict[int, dict[str, Any]]:
    if "id" not in cols:
        return {}
    source_id_col = _first(cols, ("source_id", "curated_source_id"))
    url_col = _first(cols, ("source_url", "url", "canonical_url"))
    checked_col = _first(cols, ("metadata_checked_at", "link_metadata_checked_at", "last_checked_at"))
    sql = f"""SELECT id,
                     {source_id_col if source_id_col else 'NULL'} AS source_id,
                     {url_col if url_col else 'NULL'} AS source_url,
                     {checked_col if checked_col else 'NULL'} AS metadata_checked_at
              FROM knowledge"""
    return {int(row["id"]): dict(row) for row in conn.execute(sql).fetchall()}


def _curated_sources(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "curated_sources" not in schema:
        return []
    cols = schema["curated_sources"]
    if "id" not in cols:
        return []
    kid_col = _first(cols, ("knowledge_id", "default_knowledge_id"))
    url_col = _first(cols, ("source_url", "url", "canonical_url", "feed_url"))
    curated_col = _first(cols, ("curated", "is_curated"))
    where = f" WHERE {curated_col} = 1" if curated_col else ""
    sql = f"SELECT id, {kid_col if kid_col else 'NULL'} AS knowledge_id, {url_col if url_col else 'NULL'} AS source_url FROM curated_sources{where}"
    return [dict(row) for row in conn.execute(sql).fetchall()]


def _issue(kind: str, content_id: int | None, source_id: int | None, knowledge_id: int | None, source_url: str | None) -> SourceAttributionChainBreak:
    actions = {
        "missing_knowledge_row": "Restore or remove the broken content_knowledge_links reference.",
        "missing_source_url": "Backfill the canonical source URL before relying on this citation.",
        "stale_link_metadata": "Refresh link metadata and verify the source still resolves.",
        "uncited_curated_reference": "Attach this curated reference to generated content or retire it from the citation set.",
    }
    return SourceAttributionChainBreak(kind, content_id, source_id, knowledge_id, source_url, actions[kind])


def _report(generated_at: datetime, filters: dict[str, Any], issues: tuple[SourceAttributionChainBreak, ...], scanned: int, missing: tuple[str, ...]) -> SourceAttributionChainBreakReport:
    return SourceAttributionChainBreakReport(generated_at.isoformat(), filters, {"links_scanned": scanned, "issue_count": len(issues)}, issues, {"is_empty": not issues, "message": "No source attribution chain breaks found." if not missing else "Source attribution schema is unavailable."}, missing)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _first(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _age_days(now: datetime, then: datetime | None) -> float | None:
    return None if then is None else (now - then).total_seconds() / 86400


def _clean(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
