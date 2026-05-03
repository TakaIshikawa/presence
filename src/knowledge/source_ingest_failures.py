"""Report curated sources with failed or missing knowledge ingestion."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

from knowledge.curated_ingestion_failure_digest import (
    normalize_curated_ingestion_error_category,
)


DEFAULT_DAYS = 30
SOURCE_TYPE_TO_KNOWLEDGE_TYPE = {
    "x_account": "curated_x",
    "blog": "curated_article",
    "newsletter": "curated_newsletter",
}
FAILURE_STATUSES = {"failure", "failed", "error", "quarantined"}


@dataclass(frozen=True)
class KnowledgeSourceIngestFailureRow:
    """One curated source and its ingestion health classification."""

    id: int | None
    identifier: str
    name: str | None
    url: str | None
    source_type: str
    status: str | None
    failure_bucket: str
    failure_reason: str
    recommended_action: str
    consecutive_failures: int
    last_fetch_status: str | None
    last_success_at: str | None
    last_success_age_days: int | None
    last_failure_at: str | None
    last_error: str | None
    recent_knowledge_count: int
    total_knowledge_count: int
    malformed_metadata: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class KnowledgeSourceIngestFailureReport:
    """Curated source ingest failure report plus summary totals."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[KnowledgeSourceIngestFailureRow, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "knowledge_source_ingest_failures",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": self.totals,
        }


def build_knowledge_source_ingest_failure_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    source_type: str | None = None,
    include_healthy: bool = False,
    now: datetime | None = None,
) -> KnowledgeSourceIngestFailureReport:
    """Build a report for curated sources that need ingest repair."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}

    if _is_sqlite_source(db_or_rows):
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        missing_tables = tuple(
            table for table in ("curated_sources", "knowledge") if table not in schema
        )
        missing_columns = _missing_columns(schema)
        sources = [] if "curated_sources" in missing_tables else _load_sources(
            conn,
            schema["curated_sources"],
            source_type=source_type,
        )
        knowledge_rows = [] if "knowledge" in missing_tables else _load_knowledge_rows(
            conn,
            schema["knowledge"],
        )
    else:
        sources, knowledge_rows = _iterable_inputs(db_or_rows, source_type=source_type)

    rows = _build_rows(
        sources,
        knowledge_rows,
        cutoff=cutoff,
        generated_at=generated_at,
        include_healthy=include_healthy,
    )
    return KnowledgeSourceIngestFailureReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "include_healthy": include_healthy,
            "lookback_start": cutoff.isoformat(),
            "source_type": source_type,
        },
        totals=_totals(rows, sources),
        rows=tuple(rows),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_knowledge_source_ingest_failures_json(
    report: KnowledgeSourceIngestFailureReport,
) -> str:
    """Serialize a source ingest failure report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_knowledge_source_ingest_failures_text(
    report: KnowledgeSourceIngestFailureReport,
) -> str:
    """Render a source ingest failure report for operators."""
    lines = [
        "KNOWLEDGE SOURCE INGEST FAILURES",
        (
            f"Window: {report.filters['days']} days; "
            f"source_type={report.filters.get('source_type') or 'all'}; "
            f"include_healthy={str(report.filters['include_healthy']).lower()}"
        ),
        (
            f"Sources scanned: {report.totals['sources_scanned']}; "
            f"reported: {report.totals['sources_reported']}; "
            f"recent knowledge rows: {report.totals['recent_knowledge_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.totals["malformed_metadata_count"]:
        lines.append(f"Malformed metadata rows: {report.totals['malformed_metadata_count']}")
    lines.append("")

    if not report.rows:
        lines.append("No sources matched.")
        return "\n".join(lines)

    header = (
        f"{'ID':<5} {'Source':<24} {'Type':<12} {'Age':<8} "
        f"{'Recent':<6} {'Bucket':<22} Reason"
    )
    lines.append(header)
    lines.append("-" * 118)
    for row in report.rows:
        age = "-" if row.last_success_age_days is None else f"{row.last_success_age_days}d"
        lines.append(
            f"{str(row.id or '-'):<5} {_truncate(row.name or row.identifier, 24):<24} "
            f"{row.source_type:<12} {age:<8} {row.recent_knowledge_count!s:<6} "
            f"{row.failure_bucket:<22} {_truncate(row.failure_reason, 48)}"
        )
        lines.append(f"{'':<5} url={row.url or '-'}")
        lines.append(f"{'':<5} action={row.recommended_action}")
    return "\n".join(lines)


def _build_rows(
    sources: list[dict[str, Any]],
    knowledge_rows: list[dict[str, Any]],
    *,
    cutoff: datetime,
    generated_at: datetime,
    include_healthy: bool,
) -> list[KnowledgeSourceIngestFailureRow]:
    rows: list[KnowledgeSourceIngestFailureRow] = []
    for source in sources:
        source_metadata, malformed = _source_metadata(source)
        knowledge = [
            row for row in knowledge_rows if _knowledge_matches_source(row, source)
        ]
        recent_count = sum(
            1
            for row in knowledge
            if (timestamp := _parse_datetime(_knowledge_timestamp(row))) is not None
            and timestamp >= cutoff
        )
        total_count = len(knowledge)
        last_success = _parse_datetime(
            _first_value(source, source_metadata, ("last_success_at", "last_success"))
        )
        last_success_at = last_success.isoformat() if last_success else _clean(
            _first_value(source, source_metadata, ("last_success_at", "last_success"))
        )
        age_days = _age_days(generated_at, last_success)
        failures = _int(_first_value(source, source_metadata, ("consecutive_failures",)))
        fetch_status = _clean(_first_value(source, source_metadata, ("last_fetch_status",)))
        last_error = _clean(_first_value(source, source_metadata, ("last_error", "error")))
        bucket, reason, action = _classify(
            source=source,
            age_days=age_days,
            days=(generated_at - cutoff).days,
            failures=failures,
            fetch_status=fetch_status,
            last_error=last_error,
            malformed_metadata=malformed,
            recent_knowledge_count=recent_count,
        )
        if bucket == "healthy" and not include_healthy:
            continue
        rows.append(
            KnowledgeSourceIngestFailureRow(
                id=_int_or_none(source.get("id")),
                identifier=_clean(source.get("identifier")) or "",
                name=_clean(source.get("name")),
                url=_source_url(source),
                source_type=_clean(source.get("source_type")) or "unknown",
                status=_clean(source.get("status")),
                failure_bucket=bucket,
                failure_reason=reason,
                recommended_action=action,
                consecutive_failures=failures,
                last_fetch_status=fetch_status,
                last_success_at=last_success_at,
                last_success_age_days=age_days,
                last_failure_at=_clean(
                    _first_value(source, source_metadata, ("last_failure_at",))
                ),
                last_error=last_error,
                recent_knowledge_count=recent_count,
                total_knowledge_count=total_count,
                malformed_metadata=malformed,
            )
        )
    rows.sort(key=_row_sort_key)
    return rows


def _classify(
    *,
    source: Mapping[str, Any],
    age_days: int | None,
    days: int,
    failures: int,
    fetch_status: str | None,
    last_error: str | None,
    malformed_metadata: bool,
    recent_knowledge_count: int,
) -> tuple[str, str, str]:
    status_text = str(fetch_status or "").casefold()
    if last_error or failures > 0 or status_text in FAILURE_STATUSES:
        category = normalize_curated_ingestion_error_category(last_error)
        reason = last_error or f"last fetch status is {fetch_status or 'failure'}"
        return (
            f"failure:{category}",
            reason,
            _failure_action(category),
        )
    if malformed_metadata:
        return (
            "malformed_metadata",
            "curated source metadata is not valid JSON",
            "Repair curated_sources metadata so ingest state can be read.",
        )
    if age_days is None:
        return (
            "missing_last_success",
            "no successful ingest timestamp recorded",
            "Run or inspect ingestion for this source and backfill last_success_at on success.",
        )
    if age_days >= days:
        return (
            "stale_last_success",
            f"last successful ingest is {age_days} days old",
            "Recrawl this source and inspect fetch logs if it remains stale.",
        )
    if recent_knowledge_count == 0:
        return (
            "no_recent_knowledge",
            f"no knowledge rows created in the last {days} days",
            "Inspect extraction filters and source content; ingest may be succeeding without storing knowledge.",
        )
    if str(source.get("status") or "active").casefold() != "active":
        return (
            "inactive_source",
            f"source status is {source.get('status')}",
            "Review whether this source should be resumed or removed from active monitoring.",
        )
    return ("healthy", "recent successful ingestion found", "No action needed.")


def _failure_action(category: str) -> str:
    return {
        "auth": "Refresh credentials or remove the source if access is no longer allowed.",
        "not_found": "Verify the URL or account identifier; remove or replace the source if gone.",
        "network": "Retry ingestion and inspect network or feed availability.",
        "parse": "Inspect feed/content parsing and update extraction rules.",
        "rate_limit": "Reduce crawl frequency or wait for rate limits to reset.",
        "unavailable": "Retry later and verify upstream availability.",
        "validation": "Inspect source content against knowledge validation rules.",
    }.get("unknown" if not category else category, "Inspect ingest logs and repair the source.")


def _totals(
    rows: list[KnowledgeSourceIngestFailureRow],
    sources: list[dict[str, Any]],
) -> dict[str, Any]:
    by_source_type: dict[str, dict[str, int]] = {}
    by_bucket: dict[str, int] = {}
    for row in rows:
        by_bucket[row.failure_bucket] = by_bucket.get(row.failure_bucket, 0) + 1
        source_counts = by_source_type.setdefault(row.source_type, {})
        source_counts[row.failure_bucket] = source_counts.get(row.failure_bucket, 0) + 1
    return {
        "by_failure_bucket": dict(sorted(by_bucket.items())),
        "by_source_type_and_failure_bucket": {
            source_type: dict(sorted(counts.items()))
            for source_type, counts in sorted(by_source_type.items())
        },
        "malformed_metadata_count": sum(1 for row in rows if row.malformed_metadata),
        "recent_knowledge_count": sum(row.recent_knowledge_count for row in rows),
        "sources_reported": len(rows),
        "sources_scanned": len(sources),
    }


def _load_sources(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    source_type: str | None,
) -> list[dict[str, Any]]:
    wanted = (
        "id",
        "source_type",
        "identifier",
        "name",
        "feed_url",
        "canonical_url",
        "homepage_url",
        "status",
        "last_fetch_status",
        "consecutive_failures",
        "last_success_at",
        "last_failure_at",
        "last_error",
        "metadata",
    )
    where = []
    params: list[Any] = []
    if source_type:
        where.append("source_type = ?")
        params.append(source_type)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT {', '.join(_column_expr(columns, column) for column in wanted)}
            FROM curated_sources
            {where_sql}
            ORDER BY source_type ASC, identifier ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_knowledge_rows(
    conn: sqlite3.Connection,
    columns: set[str],
) -> list[dict[str, Any]]:
    wanted = (
        "id",
        "source_type",
        "source_id",
        "source_url",
        "author",
        "published_at",
        "ingested_at",
        "created_at",
        "approved",
    )
    where = "WHERE COALESCE(approved, 0) = 1" if "approved" in columns else ""
    rows = conn.execute(
        f"""SELECT {', '.join(_column_expr(columns, column) for column in wanted)}
            FROM knowledge
            {where}"""
    ).fetchall()
    return [dict(row) for row in rows]


def _knowledge_matches_source(
    knowledge: Mapping[str, Any],
    source: Mapping[str, Any],
) -> bool:
    source_type = _clean(source.get("source_type")) or ""
    expected_knowledge_type = SOURCE_TYPE_TO_KNOWLEDGE_TYPE.get(source_type)
    if expected_knowledge_type and knowledge.get("source_type") != expected_knowledge_type:
        return False

    identifier = (_clean(source.get("identifier")) or "").lstrip("@").casefold()
    name = (_clean(source.get("name")) or "").lstrip("@").casefold()
    source_id = (_clean(knowledge.get("source_id")) or "").lstrip("@").casefold()
    author = (_clean(knowledge.get("author")) or "").lstrip("@").casefold()
    if identifier and identifier in {source_id, author}:
        return True
    if name and name == author:
        return True

    source_hosts = {
        host
        for host in (
            _host(source.get("identifier")),
            _host(source.get("feed_url")),
            _host(source.get("canonical_url")),
            _host(source.get("homepage_url")),
        )
        if host
    }
    knowledge_host = _host(knowledge.get("source_url"))
    return bool(knowledge_host and knowledge_host in source_hosts)


def _iterable_inputs(
    value: Any,
    *,
    source_type: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if isinstance(value, Mapping):
        sources = [dict(row) for row in value.get("sources", [])]
        knowledge = [dict(row) for row in value.get("knowledge", [])]
    else:
        sources = [dict(row) for row in value]
        knowledge = []
    if source_type:
        sources = [row for row in sources if row.get("source_type") == source_type]
    return sources, knowledge


def _source_metadata(source: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    raw = source.get("metadata")
    if not raw:
        return {}, False
    if isinstance(raw, Mapping):
        return dict(raw), False
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return {}, True
    return (dict(parsed), False) if isinstance(parsed, Mapping) else ({}, True)


def _first_value(
    source: Mapping[str, Any],
    metadata: Mapping[str, Any],
    keys: tuple[str, ...],
) -> Any:
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return value
    for key in keys:
        value = metadata.get(key)
        if value not in (None, ""):
            return value
    ingest = metadata.get("ingest") if isinstance(metadata.get("ingest"), Mapping) else {}
    for key in keys:
        value = ingest.get(key)
        if value not in (None, ""):
            return value
    return None


def _source_url(source: Mapping[str, Any]) -> str | None:
    for key in ("canonical_url", "feed_url", "homepage_url"):
        value = _clean(source.get(key))
        if value:
            return value
    identifier = _clean(source.get("identifier"))
    if not identifier:
        return None
    if str(source.get("source_type")) == "x_account":
        return f"https://x.com/{identifier.lstrip('@')}"
    return identifier if "://" in identifier else f"https://{identifier}"


def _knowledge_timestamp(row: Mapping[str, Any]) -> Any:
    return row.get("published_at") or row.get("ingested_at") or row.get("created_at")


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "curated_sources": (
            "id",
            "source_type",
            "identifier",
            "name",
            "feed_url",
            "canonical_url",
            "status",
            "last_fetch_status",
            "consecutive_failures",
            "last_success_at",
            "last_failure_at",
            "last_error",
        ),
        "knowledge": (
            "id",
            "source_type",
            "source_id",
            "source_url",
            "author",
            "published_at",
            "ingested_at",
            "created_at",
            "approved",
        ),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _is_sqlite_source(value: Any) -> bool:
    return isinstance(value, sqlite3.Connection) or hasattr(value, "conn")


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _column_expr(columns: set[str], column: str) -> str:
    return column if column in columns else f"NULL AS {column}"


def _host(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = (parsed.netloc or parsed.path.split("/", 1)[0]).casefold()
    return host[4:] if host.startswith("www.") else host


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if not value:
        return None
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_days(now: datetime, timestamp: datetime | None) -> int | None:
    if timestamp is None:
        return None
    return max(0, (now - timestamp).days)


def _row_sort_key(row: KnowledgeSourceIngestFailureRow) -> tuple[int, int, str, str]:
    return (
        _bucket_rank(row.failure_bucket),
        -(row.consecutive_failures or 0),
        row.source_type,
        row.identifier.casefold(),
    )


def _bucket_rank(bucket: str) -> int:
    if bucket.startswith("failure:"):
        return 0
    return {
        "malformed_metadata": 1,
        "missing_last_success": 2,
        "stale_last_success": 3,
        "no_recent_knowledge": 4,
        "inactive_source": 5,
        "healthy": 9,
    }.get(bucket, 8)


def _truncate(value: str | None, width: int) -> str:
    text = (value or "").replace("\n", " ")
    return text if len(text) <= width else text[: width - 3] + "..."
