"""Report cited knowledge sources with weak author credential metadata."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


ARTIFACT_TYPE = "knowledge_source_author_credential_gaps"
DEFAULT_STALE_DAYS = 365


def build_knowledge_source_author_credential_gaps_report(
    sources: list[dict[str, Any]],
    *,
    include_uncited: bool = False,
    stale_days: int = DEFAULT_STALE_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    rows = []
    for source in sources:
        citation_count = int(source.get("citation_count") or 0)
        if citation_count <= 0 and not include_uncited:
            continue
        missing = _missing_fields(source, generated_at=generated_at, stale_days=stale_days)
        if not missing:
            continue
        last_cited_at = _dt(source.get("last_cited_at"))
        rows.append(
            {
                "source_id": source.get("source_id") or source.get("id"),
                "canonical_url": source.get("canonical_url") or source.get("url"),
                "author_name": _clean(source.get("author_name") or source.get("author")),
                "missing_fields": missing,
                "citation_count": citation_count,
                "last_cited_at": last_cited_at.isoformat() if last_cited_at else None,
                "priority_score": _priority(citation_count, last_cited_at, generated_at, len(missing)),
            }
        )
    rows.sort(key=lambda row: (-row["priority_score"], str(row["source_id"])))
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"include_uncited": include_uncited, "stale_days": stale_days},
        "totals": {"source_count": len(sources), "row_count": len(rows)},
        "rows": rows,
    }


def build_knowledge_source_author_credential_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "knowledge_sources" not in schema:
        return build_knowledge_source_author_credential_gaps_report([], **kwargs) | {"missing_tables": ["knowledge_sources"]}
    cols = schema["knowledge_sources"]
    select = [
        _expr(cols, ("id", "source_id"), "rowid") + " AS source_id",
        _expr(cols, ("canonical_url", "url"), "NULL") + " AS canonical_url",
        _expr(cols, ("author_name", "author"), "NULL") + " AS author_name",
        _expr(cols, ("author_affiliation", "affiliation"), "NULL") + " AS author_affiliation",
        _expr(cols, ("author_credential", "credential", "role"), "NULL") + " AS author_credential",
        _expr(cols, ("credential_updated_at", "author_updated_at"), "NULL") + " AS credential_updated_at",
        _expr(cols, ("citation_count",), "0") + " AS citation_count",
        _expr(cols, ("last_cited_at",), "NULL") + " AS last_cited_at",
    ]
    rows = [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM knowledge_sources ORDER BY rowid ASC")]
    return build_knowledge_source_author_credential_gaps_report(rows, **kwargs) | {"missing_tables": []}


def format_knowledge_source_author_credential_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_source_author_credential_gaps_text(report: dict[str, Any]) -> str:
    lines = ["Knowledge Source Author Credential Gaps", f"Generated: {report['generated_at']}", f"Rows: {report['totals']['row_count']}"]
    for row in report["rows"]:
        lines.append(f"{row['source_id']} | {row['canonical_url']} | {row['author_name'] or '-'} | {','.join(row['missing_fields'])} | {row['priority_score']}")
    return "\n".join(lines)


def _missing_fields(source: dict[str, Any], *, generated_at: datetime, stale_days: int) -> list[str]:
    missing = []
    if not _clean(source.get("author_name") or source.get("author")):
        missing.append("author_name")
    if not _clean(source.get("author_affiliation") or source.get("affiliation")):
        missing.append("author_affiliation")
    if not _clean(source.get("author_credential") or source.get("credential") or source.get("role")):
        missing.append("author_credential")
    updated = _dt(source.get("credential_updated_at") or source.get("author_updated_at"))
    if updated and updated < generated_at - timedelta(days=stale_days):
        missing.append("stale_credential")
    return missing


def _priority(citations: int, last_cited_at: datetime | None, now: datetime, gap_count: int) -> int:
    recency = 0 if not last_cited_at else max(0, 30 - (now - last_cited_at).days)
    return citations * 10 + recency + gap_count * 5


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    if isinstance(db_or_conn, sqlite3.Connection):
        db_or_conn.row_factory = sqlite3.Row
        return db_or_conn
    conn = sqlite3.connect(db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _expr(columns: set[str], names: tuple[str, ...], fallback: str) -> str:
    return next((name for name in names if name in columns), fallback)
