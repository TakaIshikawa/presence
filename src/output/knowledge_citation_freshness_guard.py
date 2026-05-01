"""Read-only guard for freshness and traceability of cited knowledge."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


STATUS_PASSED = "passed"
STATUS_WARNING = "warning"
STATUS_BLOCKED = "blocked"

BLOCK_SEVERITY = "block"
WARNING_SEVERITY = "warning"

CURATED_SOURCE_TYPES = {
    "curated_x": "x_account",
    "curated_article": "blog",
    "curated_newsletter": "newsletter",
}


@dataclass(frozen=True)
class KnowledgeCitationFreshnessFinding:
    code: str
    severity: str
    message: str
    content_id: int
    knowledge_id: int | None
    source_type: str | None
    source_id: str | None
    source_url: str | None
    canonical_url: str | None
    age_days: float | None
    source_status: str | None = None


@dataclass(frozen=True)
class KnowledgeCitationFreshnessItem:
    content_id: int
    content_type: str | None
    generated_at: str | None
    link_id: int | None
    knowledge_id: int | None
    relevance_score: float | None
    source_type: str | None
    source_id: str | None
    source_url: str | None
    canonical_url: str | None
    source_timestamp: str | None
    age_days: float | None
    status: str
    reason_codes: list[str]
    findings: list[KnowledgeCitationFreshnessFinding]


@dataclass(frozen=True)
class KnowledgeCitationFreshnessReport:
    artifact_type: str
    content_id: int | None
    days: int
    require_canonical: bool
    generated_at: str
    checked_content_count: int
    linked_knowledge_count: int
    blocked_count: int
    warning_count: int
    passed_count: int
    items: list[KnowledgeCitationFreshnessItem]
    missing_required_tables: list[str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_knowledge_citation_freshness_report(
    db: Any,
    content_id: int | None = None,
    days: int = 90,
    require_canonical: bool = True,
    now: datetime | None = None,
) -> KnowledgeCitationFreshnessReport:
    """Flag generated content linked to stale or poorly traceable knowledge."""

    if days < 1:
        raise ValueError("days must be at least 1")
    if content_id is not None and content_id < 1:
        raise ValueError("content_id must be positive")

    generated_at = _normalize_datetime(now or datetime.now(timezone.utc))
    conn = _connection(db)
    schema = _schema(conn)
    missing = [
        table
        for table in ("generated_content", "content_knowledge_links", "knowledge")
        if table not in schema
    ]
    if missing:
        return _empty_report(
            content_id=content_id,
            days=days,
            require_canonical=require_canonical,
            generated_at=generated_at,
            missing=missing,
        )

    inactive_sources = _load_unhealthy_sources(conn, schema)
    rows = _load_linked_rows(conn, schema, content_id=content_id)
    items = [
        _classify_row(
            row,
            inactive_sources=inactive_sources,
            days=days,
            require_canonical=require_canonical,
            now=generated_at,
        )
        for row in rows
    ]
    blocked_count = sum(1 for item in items if item.status == STATUS_BLOCKED)
    warning_count = sum(1 for item in items if item.status == STATUS_WARNING)
    passed_count = sum(1 for item in items if item.status == STATUS_PASSED)
    return KnowledgeCitationFreshnessReport(
        artifact_type="knowledge_citation_freshness",
        content_id=content_id,
        days=days,
        require_canonical=require_canonical,
        generated_at=generated_at.isoformat(),
        checked_content_count=len({item.content_id for item in items}),
        linked_knowledge_count=len(items),
        blocked_count=blocked_count,
        warning_count=warning_count,
        passed_count=passed_count,
        items=items,
        missing_required_tables=[],
    )


def export_to_json(report: KnowledgeCitationFreshnessReport) -> str:
    """Serialize a freshness guard report as stable JSON."""

    return json.dumps(report.as_dict(), indent=2, sort_keys=True)


def format_text_report(report: KnowledgeCitationFreshnessReport) -> str:
    """Render a freshness guard report for terminal review."""

    lines = [
        "Knowledge Citation Freshness",
        (
            "Filters: "
            f"content_id={report.content_id or 'all'} "
            f"days={report.days} "
            f"require_canonical={_yes_no(report.require_canonical)}"
        ),
        (
            "Counts: "
            f"content={report.checked_content_count} "
            f"links={report.linked_knowledge_count} "
            f"blocked={report.blocked_count} "
            f"warnings={report.warning_count} "
            f"passed={report.passed_count}"
        ),
    ]
    if report.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(report.missing_required_tables))
    if not report.items:
        lines.append("")
        lines.append("No linked knowledge citations found.")
        return "\n".join(lines)

    current_content_id: int | None = None
    for item in report.items:
        if item.content_id != current_content_id:
            current_content_id = item.content_id
            lines.append("")
            lines.append(
                f"Content #{item.content_id} "
                f"[{item.content_type or '-'}] @ {item.generated_at or '-'}"
            )
        url = item.canonical_url or item.source_url or "-"
        reasons = ",".join(item.reason_codes) or "-"
        lines.append(
            f"  - {item.status}: link #{item.link_id or '-'} "
            f"knowledge #{item.knowledge_id or '-'} source={item.source_type or '-'} "
            f"age_days={_format_age(item.age_days)} url={url} reasons={reasons}"
        )
        for finding in item.findings:
            lines.append(f"    {finding.severity}: {finding.code} - {finding.message}")
    return "\n".join(lines)


def _load_linked_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    content_id: int | None,
) -> list[dict[str, Any]]:
    knowledge_columns = schema["knowledge"]
    link_columns = schema["content_knowledge_links"]
    content_columns = schema["generated_content"]
    metadata_expr = _column_expr(knowledge_columns, "metadata")
    filters = []
    params: list[Any] = []
    if content_id is not None:
        filters.append("gc.id = ?")
        params.append(content_id)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = conn.execute(
        f"""SELECT gc.id AS content_id,
                  {_column_expr(content_columns, "content_type", "gc")} AS content_type,
                  {_column_expr(content_columns, "created_at", "gc")} AS generated_at,
                  {_column_expr(link_columns, "id", "ckl")} AS link_id,
                  ckl.knowledge_id,
                  {_column_expr(link_columns, "relevance_score", "ckl")} AS relevance_score,
                  k.id AS matched_knowledge_id,
                  {_column_expr(knowledge_columns, "source_type", "k")} AS source_type,
                  {_column_expr(knowledge_columns, "source_id", "k")} AS source_id,
                  {_column_expr(knowledge_columns, "source_url", "k")} AS source_url,
                  {_column_expr(knowledge_columns, "author", "k")} AS author,
                  {_column_expr(knowledge_columns, "approved", "k")} AS approved,
                  {_column_expr(knowledge_columns, "published_at", "k")} AS published_at,
                  {_column_expr(knowledge_columns, "ingested_at", "k")} AS ingested_at,
                  {_column_expr(knowledge_columns, "created_at", "k")} AS knowledge_created_at,
                  {metadata_expr if metadata_expr == "NULL" else "k.metadata"} AS metadata
           FROM generated_content gc
           INNER JOIN content_knowledge_links ckl ON ckl.content_id = gc.id
           LEFT JOIN knowledge k ON k.id = ckl.knowledge_id
           {where}
           ORDER BY gc.created_at DESC, gc.id DESC,
                    ckl.relevance_score DESC, ckl.knowledge_id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _classify_row(
    row: dict[str, Any],
    *,
    inactive_sources: list[dict[str, Any]],
    days: int,
    require_canonical: bool,
    now: datetime,
) -> KnowledgeCitationFreshnessItem:
    canonical_url = _canonical_url(row.get("metadata"))
    source_url = _clean_string(row.get("source_url"))
    source_timestamp = (
        _clean_string(row.get("published_at"))
        or _clean_string(row.get("ingested_at"))
        or _clean_string(row.get("knowledge_created_at"))
    )
    source_age = _age_days(source_timestamp, now)
    findings: list[KnowledgeCitationFreshnessFinding] = []

    def add(
        code: str,
        severity: str,
        message: str,
        *,
        source_status: str | None = None,
    ) -> None:
        findings.append(
            KnowledgeCitationFreshnessFinding(
                code=code,
                severity=severity,
                message=message,
                content_id=int(row["content_id"]),
                knowledge_id=_int_or_none(row.get("knowledge_id")),
                source_type=_clean_string(row.get("source_type")),
                source_id=_clean_string(row.get("source_id")),
                source_url=source_url,
                canonical_url=canonical_url,
                age_days=_round_days(source_age),
                source_status=source_status,
            )
        )

    if row.get("matched_knowledge_id") is None:
        add("missing_knowledge", BLOCK_SEVERITY, "Linked knowledge row no longer exists.")
    else:
        approved = _bool_or_none(row.get("approved"))
        if approved is False:
            add("retired_knowledge", BLOCK_SEVERITY, "Linked knowledge is no longer approved.")
        if source_age is None:
            add(
                "missing_source_timestamp",
                WARNING_SEVERITY,
                "Linked knowledge has no usable source timestamp.",
            )
        elif source_age > days:
            add(
                "stale_knowledge",
                BLOCK_SEVERITY,
                f"Linked knowledge is older than {days} days.",
            )
        if require_canonical and not canonical_url:
            add(
                "missing_canonical_url",
                WARNING_SEVERITY,
                "Linked knowledge is missing canonical URL metadata.",
            )
        if not (canonical_url or source_url):
            add(
                "untraceable_knowledge",
                BLOCK_SEVERITY,
                "Linked knowledge has no traceable source URL.",
            )
        unhealthy_source = _match_unhealthy_source(row, inactive_sources)
        if unhealthy_source is not None:
            status = _source_status(unhealthy_source)
            add(
                "unhealthy_source",
                BLOCK_SEVERITY,
                "Linked knowledge comes from an unhealthy curated source.",
                source_status=status,
            )

    severities = {finding.severity for finding in findings}
    if BLOCK_SEVERITY in severities:
        status = STATUS_BLOCKED
    elif WARNING_SEVERITY in severities:
        status = STATUS_WARNING
    else:
        status = STATUS_PASSED
    return KnowledgeCitationFreshnessItem(
        content_id=int(row["content_id"]),
        content_type=_clean_string(row.get("content_type")),
        generated_at=_clean_string(row.get("generated_at")),
        link_id=_int_or_none(row.get("link_id")),
        knowledge_id=_int_or_none(row.get("knowledge_id")),
        relevance_score=_float_or_none(row.get("relevance_score")),
        source_type=_clean_string(row.get("source_type")),
        source_id=_clean_string(row.get("source_id")),
        source_url=source_url,
        canonical_url=canonical_url,
        source_timestamp=source_timestamp,
        age_days=_round_days(source_age),
        status=status,
        reason_codes=[finding.code for finding in findings],
        findings=findings,
    )


def _load_unhealthy_sources(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema.get("curated_sources")
    if not columns:
        return []
    select = [
        _column_expr(columns, "id"),
        _column_expr(columns, "source_type"),
        _column_expr(columns, "identifier"),
        _column_expr(columns, "active"),
        _column_expr(columns, "status"),
        _column_expr(columns, "last_fetch_status"),
        _column_expr(columns, "consecutive_failures"),
        _column_expr(columns, "last_failure_at"),
        _column_expr(columns, "last_error"),
    ]
    unhealthy_filters = []
    if "active" in columns:
        unhealthy_filters.append("COALESCE(active, 1) = 0")
    if "status" in columns:
        unhealthy_filters.append("COALESCE(status, 'active') != 'active'")
    if "last_fetch_status" in columns:
        unhealthy_filters.append("last_fetch_status IN ('failure', 'quarantined')")
    if "consecutive_failures" in columns:
        unhealthy_filters.append("COALESCE(consecutive_failures, 0) > 0")
    if not unhealthy_filters:
        return []
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM curated_sources
            WHERE {' OR '.join(unhealthy_filters)}"""
    ).fetchall()
    return [dict(row) for row in rows]


def _match_unhealthy_source(
    row: dict[str, Any],
    unhealthy_sources: list[dict[str, Any]],
) -> dict[str, Any] | None:
    curated_type = CURATED_SOURCE_TYPES.get(_clean_string(row.get("source_type")) or "")
    if not curated_type:
        return None
    candidate_values = {
        _normalize_identifier(_clean_string(row.get("author"))),
        _normalize_identifier(_clean_string(row.get("source_id"))),
        _normalize_identifier(_host(_clean_string(row.get("source_url")))),
        _normalize_identifier(_host(_clean_string(row.get("source_id")))),
    }
    candidate_values.discard("")
    for source in unhealthy_sources:
        if source.get("source_type") != curated_type:
            continue
        identifier = _normalize_identifier(_clean_string(source.get("identifier")))
        if identifier and identifier in candidate_values:
            return source
    return None


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[table] = {
            column[1] for column in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _connection(db: Any) -> sqlite3.Connection:
    conn = getattr(db, "conn", db)
    conn.row_factory = sqlite3.Row
    return conn


def _column_expr(columns: set[str], column: str, alias: str | None = None) -> str:
    if column not in columns:
        return "NULL"
    prefix = f"{alias}." if alias else ""
    return f"{prefix}{column}"


def _empty_report(
    *,
    content_id: int | None,
    days: int,
    require_canonical: bool,
    generated_at: datetime,
    missing: list[str],
) -> KnowledgeCitationFreshnessReport:
    return KnowledgeCitationFreshnessReport(
        artifact_type="knowledge_citation_freshness",
        content_id=content_id,
        days=days,
        require_canonical=require_canonical,
        generated_at=generated_at.isoformat(),
        checked_content_count=0,
        linked_knowledge_count=0,
        blocked_count=0,
        warning_count=0,
        passed_count=0,
        items=[],
        missing_required_tables=missing,
    )


def _canonical_url(metadata: Any) -> str | None:
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata or "{}")
        except json.JSONDecodeError:
            metadata = {}
    if not isinstance(metadata, dict):
        return None
    link_metadata = metadata.get("link_metadata")
    if not isinstance(link_metadata, dict):
        return None
    return _clean_string(link_metadata.get("canonical_url"))


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    text = _clean_string(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _normalize_datetime(datetime.fromisoformat(text))
    except ValueError:
        return None


def _age_days(value: Any, now: datetime) -> float | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds(), 0.0) / 86400.0


def _round_days(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean_string(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_identifier(value: str | None) -> str:
    text = (value or "").strip().lower()
    if text.startswith("@"):
        text = text[1:]
    if text.startswith("www."):
        text = text[4:]
    return text.rstrip("/")


def _host(value: str | None) -> str:
    if not value:
        return ""
    parsed = urlparse(value if "://" in value else f"https://{value}")
    return parsed.netloc


def _source_status(source: dict[str, Any]) -> str:
    parts = [
        f"active={_yes_no(source.get('active', 1))}",
        f"status={source.get('status') or '-'}",
        f"last_fetch_status={source.get('last_fetch_status') or '-'}",
    ]
    failures = source.get("consecutive_failures")
    if failures is not None:
        parts.append(f"failures={failures}")
    return " ".join(parts)


def _format_age(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def _yes_no(value: Any) -> str:
    return "yes" if bool(value) else "no"
