"""Find generated content whose cited source chain dead-ends."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
VALID_CONTENT_TYPES = {"blog_post", "linkedin_post", "newsletter", "x_post", "x_thread", "x_visual"}


@dataclass(frozen=True)
class ContentSourceDeadendFinding:
    content_id: int
    content_type: str
    source_reference: str
    reason_labels: tuple[str, ...]
    review_priority: str
    message: str
    knowledge_id: int | None = None
    curated_source_id: int | None = None
    publication_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reason_labels"] = list(self.reason_labels)
        return payload


@dataclass(frozen=True)
class ContentSourceDeadendGroup:
    content_id: int
    content_type: str
    generated_at: str | None
    source_reference: str
    review_priority: str
    reason_labels: tuple[str, ...]
    findings: tuple[ContentSourceDeadendFinding, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reason_labels"] = list(self.reason_labels)
        payload["findings"] = [finding.to_dict() for finding in self.findings]
        return payload


@dataclass(frozen=True)
class ContentSourceDeadendsReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    groups: tuple[ContentSourceDeadendGroup, ...]
    schema_warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "content_source_deadends",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "groups": [group.to_dict() for group in self.groups],
            "schema_warnings": list(self.schema_warnings),
            "totals": dict(sorted(self.totals.items())),
        }


def build_content_source_deadends_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    content_type: str | None = None,
    now: datetime | None = None,
) -> ContentSourceDeadendsReport:
    """Return recent generated content source-chain deadends."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    normalized_type = _normalize_content_type(content_type)

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "content_type": normalized_type,
        "cutoff": cutoff.isoformat(),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    warnings = _schema_warnings(schema)
    if warnings:
        return _report(generated_at, filters, (), warnings, content_count=0)

    content_rows = _load_content_rows(
        conn,
        schema,
        cutoff=cutoff,
        content_type=normalized_type,
    )
    findings = _knowledge_findings(conn, schema, content_rows)
    findings.extend(_publication_findings(conn, schema, content_rows))
    groups = _groups(findings, content_rows)[:limit]
    return _report(generated_at, filters, tuple(groups), (), content_count=len(content_rows))


def format_content_source_deadends_json(report: ContentSourceDeadendsReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_source_deadends_text(report: ContentSourceDeadendsReport) -> str:
    """Render the report for command-line review."""
    lines = [
        "Content Source Deadends",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        (
            "Totals: "
            f"content={report.totals['content_count']} "
            f"groups={report.totals['group_count']} "
            f"findings={report.totals['finding_count']}"
        ),
    ]
    if report.filters.get("content_type"):
        lines.append(f"Content type: {report.filters['content_type']}")
    if report.schema_warnings:
        lines.append("Schema warnings: " + "; ".join(report.schema_warnings))
    if not report.groups:
        lines.append("No content source deadends found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Deadends:")
    for group in report.groups:
        labels = ",".join(group.reason_labels)
        lines.append(
            f"- content={group.content_id} type={group.content_type} "
            f"source={group.source_reference} priority={group.review_priority} "
            f"reasons={labels}"
        )
        for finding in group.findings:
            lines.append(f"  - {finding.message}")
    return "\n".join(lines)


def _load_content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    content_type: str | None,
) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    created_at = _column_expr(columns, "created_at", "NULL", alias="gc")
    where = [f"({created_at} IS NULL OR datetime({created_at}) >= datetime(?))"]
    params: list[Any] = [cutoff.isoformat()]
    if content_type:
        where.append("gc.content_type = ?")
        params.append(content_type)
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   gc.id,
                   gc.content_type,
                   {created_at} AS created_at,
                   {_column_expr(columns, "published_url", "NULL", alias="gc")} AS published_url
               FROM generated_content gc
               WHERE {' AND '.join(where)}
               ORDER BY {created_at} DESC, gc.id DESC""",
            params,
        ).fetchall()
    ]


def _knowledge_findings(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_rows: list[dict[str, Any]],
) -> list[ContentSourceDeadendFinding]:
    if not content_rows:
        return []
    ids = [int(row["id"]) for row in content_rows]
    placeholders = ",".join("?" for _ in ids)
    knowledge_columns = schema.get("knowledge", set())
    curated_columns = schema.get("curated_sources", set())
    source_url = _column_expr(knowledge_columns, "source_url", "NULL", alias="k")
    source_id = _column_expr(knowledge_columns, "source_id", "NULL", alias="k")
    author = _column_expr(knowledge_columns, "author", "NULL", alias="k")
    active = _column_expr(curated_columns, "active", "1", alias="cs")
    status = _column_expr(curated_columns, "status", "'active'", alias="cs")
    rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   ckl.id AS link_id,
                   ckl.content_id,
                   ckl.knowledge_id,
                   k.id AS resolved_knowledge_id,
                   {_column_expr(knowledge_columns, "source_type", "NULL", alias="k")} AS source_type,
                   {source_id} AS source_id,
                   {source_url} AS source_url,
                   {author} AS author,
                   cs.id AS curated_source_id,
                   {active} AS curated_active,
                   {status} AS curated_status
               FROM content_knowledge_links ckl
               LEFT JOIN knowledge k ON k.id = ckl.knowledge_id
               LEFT JOIN curated_sources cs
                 ON cs.identifier IN ({source_id}, {author}, lower(replace({author}, '@', '')), lower({source_url}))
               WHERE ckl.content_id IN ({placeholders})
               ORDER BY ckl.content_id ASC, ckl.id ASC""",
            ids,
        ).fetchall()
    ]
    content_by_id = {int(row["id"]): row for row in content_rows}
    findings: list[ContentSourceDeadendFinding] = []
    linked_content_ids = {int(row["content_id"]) for row in rows}
    for content in content_rows:
        content_id = int(content["id"])
        if content_id not in linked_content_ids:
            findings.append(
                _finding(
                    content,
                    "no_knowledge_chunks",
                    "content has no linked knowledge chunks",
                    source_reference="knowledge:none",
                    priority="high",
                )
            )
    for row in rows:
        content = content_by_id[int(row["content_id"])]
        if row.get("resolved_knowledge_id") is None:
            findings.append(
                _finding(
                    content,
                    "missing_knowledge_chunk",
                    f"content_knowledge_links.knowledge_id {row.get('knowledge_id')} does not resolve",
                    source_reference=f"knowledge:{row.get('knowledge_id')}",
                    priority="high",
                    knowledge_id=_int_or_none(row.get("knowledge_id")),
                )
            )
            continue
        source_ref = _source_reference(row)
        if _is_curated(row.get("source_type")) and row.get("curated_source_id") is None:
            findings.append(
                _finding(
                    content,
                    "missing_source_row",
                    f"knowledge {row['resolved_knowledge_id']} has no matching curated source",
                    source_reference=source_ref,
                    priority="medium",
                    knowledge_id=int(row["resolved_knowledge_id"]),
                )
            )
        elif row.get("curated_source_id") is not None and not _active_curated(row):
            findings.append(
                _finding(
                    content,
                    "inactive_curated_source",
                    f"curated source {row['curated_source_id']} is not active",
                    source_reference=source_ref,
                    priority="high",
                    knowledge_id=int(row["resolved_knowledge_id"]),
                    curated_source_id=int(row["curated_source_id"]),
                )
            )
    return findings


def _publication_findings(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_rows: list[dict[str, Any]],
) -> list[ContentSourceDeadendFinding]:
    if "content_publications" not in schema or not content_rows:
        return []
    columns = schema["content_publications"]
    if "content_id" not in columns:
        return []
    ids = [int(row["id"]) for row in content_rows]
    placeholders = ",".join("?" for _ in ids)
    platform_url = _column_expr(columns, "platform_url", "NULL", alias="cp")
    status = _column_expr(columns, "status", "NULL", alias="cp")
    rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   cp.id,
                   cp.content_id,
                   {_column_expr(columns, "platform", "''", alias="cp")} AS platform,
                   {status} AS status,
                   {platform_url} AS platform_url
               FROM content_publications cp
               WHERE cp.content_id IN ({placeholders})
               ORDER BY cp.content_id ASC, cp.id ASC""",
            ids,
        ).fetchall()
    ]
    content_by_id = {int(row["id"]): row for row in content_rows}
    findings: list[ContentSourceDeadendFinding] = []
    for row in rows:
        content = content_by_id[int(row["content_id"])]
        if str(row.get("status") or "").lower() == "published" and not (
            _clean(row.get("platform_url")) or _clean(content.get("published_url"))
        ):
            findings.append(
                _finding(
                    content,
                    "broken_publication_source_join",
                    f"published {row.get('platform') or 'publication'} row lacks a traceable URL",
                    source_reference=f"publication:{row['id']}",
                    priority="medium",
                    publication_id=int(row["id"]),
                )
            )
    return findings


def _groups(
    findings: list[ContentSourceDeadendFinding],
    content_rows: list[dict[str, Any]],
) -> tuple[ContentSourceDeadendGroup, ...]:
    content_by_id = {int(row["id"]): row for row in content_rows}
    bucket: dict[tuple[int, str], list[ContentSourceDeadendFinding]] = defaultdict(list)
    for finding in findings:
        bucket[(finding.content_id, finding.source_reference)].append(finding)
    groups: list[ContentSourceDeadendGroup] = []
    for (content_id, source_reference), items in bucket.items():
        content = content_by_id[content_id]
        labels = tuple(sorted({label for item in items for label in item.reason_labels}))
        priority = _highest_priority(item.review_priority for item in items)
        groups.append(
            ContentSourceDeadendGroup(
                content_id=content_id,
                content_type=str(content.get("content_type") or ""),
                generated_at=content.get("created_at"),
                source_reference=source_reference,
                review_priority=priority,
                reason_labels=labels,
                findings=tuple(sorted(items, key=lambda item: item.message)),
            )
        )
    groups.sort(key=_group_sort_key)
    return tuple(groups)


def _finding(
    content: dict[str, Any],
    reason: str,
    message: str,
    *,
    source_reference: str,
    priority: str,
    knowledge_id: int | None = None,
    curated_source_id: int | None = None,
    publication_id: int | None = None,
) -> ContentSourceDeadendFinding:
    return ContentSourceDeadendFinding(
        content_id=int(content["id"]),
        content_type=str(content.get("content_type") or ""),
        source_reference=source_reference,
        reason_labels=(reason,),
        review_priority=priority,
        message=message,
        knowledge_id=knowledge_id,
        curated_source_id=curated_source_id,
        publication_id=publication_id,
    )


def _report(
    generated_at: datetime,
    filters: dict[str, Any],
    groups: tuple[ContentSourceDeadendGroup, ...],
    warnings: tuple[str, ...],
    *,
    content_count: int,
) -> ContentSourceDeadendsReport:
    finding_count = sum(len(group.findings) for group in groups)
    reason_counts: dict[str, int] = defaultdict(int)
    for group in groups:
        for label in group.reason_labels:
            reason_counts[label] += 1
    return ContentSourceDeadendsReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "content_count": content_count,
            "group_count": len(groups),
            "finding_count": finding_count,
            "by_reason": dict(sorted(reason_counts.items())),
        },
        groups=groups,
        schema_warnings=warnings,
    )


def _schema_warnings(schema: dict[str, set[str]]) -> tuple[str, ...]:
    required = {
        "generated_content": {"id", "content_type"},
        "content_knowledge_links": {"content_id", "knowledge_id"},
        "knowledge": {"id"},
        "curated_sources": {"id", "identifier"},
    }
    warnings: list[str] = []
    for table, columns in required.items():
        if table not in schema:
            warnings.append(f"missing table: {table}")
            continue
        missing = sorted(columns - schema[table])
        if missing:
            warnings.append(f"missing columns: {table}({', '.join(missing)})")
    return tuple(warnings)


def _normalize_content_type(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        raise ValueError("content-type must not be empty")
    if normalized not in VALID_CONTENT_TYPES:
        allowed = ", ".join(sorted(VALID_CONTENT_TYPES))
        raise ValueError(f"content-type must be one of: {allowed}")
    return normalized


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _column_expr(columns: set[str], column: str, fallback: str, *, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _source_reference(row: dict[str, Any]) -> str:
    for key in ("source_url", "source_id", "author"):
        value = _clean(row.get(key))
        if value:
            return value
    return f"knowledge:{row.get('resolved_knowledge_id')}"


def _is_curated(source_type: Any) -> bool:
    return str(source_type or "").startswith("curated_")


def _active_curated(row: dict[str, Any]) -> bool:
    status = str(row.get("curated_status") or "active").lower()
    active = _int_or_none(row.get("curated_active"))
    return status == "active" and active != 0


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _highest_priority(priorities: Any) -> str:
    rank = {"high": 0, "medium": 1, "low": 2}
    return min(priorities, key=lambda item: rank.get(item, 9), default="low")


def _group_sort_key(group: ContentSourceDeadendGroup) -> tuple[Any, ...]:
    rank = {"high": 0, "medium": 1, "low": 2}
    return (
        rank.get(group.review_priority, 9),
        group.content_id,
        group.source_reference,
    )


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
