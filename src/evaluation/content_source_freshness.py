"""Report content source freshness and identify stale ingestion streams."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_THRESHOLD_DAYS = 7


@dataclass(frozen=True)
class ContentSourceFreshnessRow:
    """One content source with freshness metrics."""

    source_type: str
    source_identifier: str
    last_ingestion_at: str | None
    days_since_ingestion: int | None
    record_count: int
    is_stale: bool
    status: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContentSourceFreshnessReport:
    """Content source freshness report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ContentSourceFreshnessRow, ...]
    grouped_by_source_type: dict[str, int]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "content_source_freshness",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "grouped_by_source_type": dict(sorted(self.grouped_by_source_type.items())),
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_content_source_freshness_report(
    db_or_conn: Any,
    *,
    stale_threshold_days: int = DEFAULT_STALE_THRESHOLD_DAYS,
    source_type: str | None = None,
    now: datetime | None = None,
) -> ContentSourceFreshnessReport:
    """Return content sources with freshness metrics and stale detection."""
    if stale_threshold_days < 0:
        raise ValueError("stale_threshold_days must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "stale_threshold_days": stale_threshold_days,
        "source_type": source_type,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = _missing_tables(schema)
    if missing_tables:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
        )

    rows_data = _collect_all_sources(conn, schema, now=generated_at)

    # Filter by source type if specified
    if source_type:
        rows_data = [row for row in rows_data if row["source_type"] == source_type]

    # Build rows with freshness calculation
    rows = tuple(
        sorted(
            (_build_row(row, stale_threshold_days=stale_threshold_days, now=generated_at) for row in rows_data),
            key=_sort_key,
            reverse=True,
        )
    )

    grouped_by_source_type: dict[str, int] = {}
    for row in rows:
        grouped_by_source_type[row.source_type] = grouped_by_source_type.get(row.source_type, 0) + 1

    return ContentSourceFreshnessReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "source_count": len(rows),
            "stale_count": sum(1 for row in rows if row.is_stale),
            "active_count": sum(1 for row in rows if not row.is_stale),
        },
        rows=rows,
        grouped_by_source_type=grouped_by_source_type,
        missing_tables=(),
    )


def format_content_source_freshness_json(
    report: ContentSourceFreshnessReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_source_freshness_csv(
    report: ContentSourceFreshnessReport,
) -> str:
    """Render the source freshness report as CSV."""
    lines = [
        "source_type,source_identifier,last_ingestion_at,days_since_ingestion,record_count,is_stale,status"
    ]
    for row in report.rows:
        lines.append(
            f"{_csv_escape(row.source_type)},"
            f"{_csv_escape(row.source_identifier)},"
            f"{_csv_escape(row.last_ingestion_at or '')},"
            f"{row.days_since_ingestion if row.days_since_ingestion is not None else ''},"
            f"{row.record_count},"
            f"{row.is_stale},"
            f"{_csv_escape(row.status or '')}"
        )
    return "\n".join(lines)


def _collect_all_sources(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    """Collect freshness data from all available source tables."""
    sources = []

    # Claude messages by session
    if "claude_messages" in schema:
        sources.extend(_load_claude_message_sources(conn, now=now))

    # GitHub commits by repo
    if "github_commits" in schema:
        sources.extend(_load_github_commit_sources(conn, now=now))

    # GitHub activity by repo and type
    if "github_activity" in schema:
        sources.extend(_load_github_activity_sources(conn, now=now))

    # Knowledge by source type
    if "knowledge" in schema:
        sources.extend(_load_knowledge_sources(conn, now=now))

    # Curated sources (RSS feeds, blogs, newsletters)
    if "curated_sources" in schema:
        sources.extend(_load_curated_sources(conn, now=now))

    return sources


def _load_claude_message_sources(
    conn: sqlite3.Connection,
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    """Load Claude message sources grouped by session."""
    rows = conn.execute("""
        SELECT
            'claude_messages' as source_type,
            COALESCE(session_id, '(unknown)') as source_identifier,
            MAX(timestamp) as last_ingestion_at,
            COUNT(*) as record_count
        FROM claude_messages
        GROUP BY session_id
    """).fetchall()

    return [dict(row) for row in rows]


def _load_github_commit_sources(
    conn: sqlite3.Connection,
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    """Load GitHub commit sources grouped by repo."""
    rows = conn.execute("""
        SELECT
            'github_commits' as source_type,
            repo_name as source_identifier,
            MAX(timestamp) as last_ingestion_at,
            COUNT(*) as record_count
        FROM github_commits
        GROUP BY repo_name
    """).fetchall()

    return [dict(row) for row in rows]


def _load_github_activity_sources(
    conn: sqlite3.Connection,
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    """Load GitHub activity sources grouped by repo and activity type."""
    rows = conn.execute("""
        SELECT
            'github_activity' as source_type,
            repo_name || ':' || activity_type as source_identifier,
            MAX(ingested_at) as last_ingestion_at,
            COUNT(*) as record_count
        FROM github_activity
        GROUP BY repo_name, activity_type
    """).fetchall()

    return [dict(row) for row in rows]


def _load_knowledge_sources(
    conn: sqlite3.Connection,
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    """Load knowledge sources grouped by source type."""
    rows = conn.execute("""
        SELECT
            'knowledge' as source_type,
            source_type as source_identifier,
            MAX(ingested_at) as last_ingestion_at,
            COUNT(*) as record_count
        FROM knowledge
        GROUP BY source_type
    """).fetchall()

    return [dict(row) for row in rows]


def _load_curated_sources(
    conn: sqlite3.Connection,
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    """Load curated sources with their fetch status."""
    columns = _get_columns(conn, "curated_sources")

    # Build SELECT based on available columns
    status_col = "status" if "status" in columns else "NULL"
    last_success_col = "last_success_at" if "last_success_at" in columns else "NULL"

    rows = conn.execute(f"""
        SELECT
            'curated_source' as source_type,
            source_type || ':' || identifier as source_identifier,
            {last_success_col} as last_ingestion_at,
            1 as record_count,
            {status_col} as status
        FROM curated_sources
    """).fetchall()

    return [dict(row) for row in rows]


def _build_row(
    row: dict[str, Any],
    *,
    stale_threshold_days: int,
    now: datetime,
) -> ContentSourceFreshnessRow:
    """Build a freshness row from raw data."""
    last_ingestion = row.get("last_ingestion_at")

    if last_ingestion:
        last_ingestion_dt = _parse_datetime(last_ingestion)
        # Calculate days since ingestion relative to report generation time
        days_since = max(0, int((now - last_ingestion_dt).total_seconds() // 86400))
        is_stale = days_since >= stale_threshold_days
    else:
        days_since = None
        is_stale = True  # No data = stale

    return ContentSourceFreshnessRow(
        source_type=str(row["source_type"]),
        source_identifier=str(row["source_identifier"]),
        last_ingestion_at=last_ingestion,
        days_since_ingestion=days_since,
        record_count=int(row.get("record_count", 0)),
        is_stale=is_stale,
        status=_optional_value(row.get("status")),
    )


def _sort_key(row: ContentSourceFreshnessRow) -> tuple[Any, ...]:
    """Sort by staleness (stale first), then days descending, then type and identifier."""
    # With reverse=True:
    # - is_stale: True (stale) comes before False (active)
    # - days: higher values come first (descending)
    # - type/identifier: reverse alphabetical order
    return (
        row.is_stale,  # True (stale) comes first with reverse=True
        row.days_since_ingestion if row.days_since_ingestion is not None else -1,  # Nulls last
        row.source_type,
        row.source_identifier,
    )


def _missing_tables(schema: dict[str, set[str]]) -> tuple[str, ...]:
    """Check for any missing source tables (we'll operate on whatever exists)."""
    # We don't require any specific table - report works with whatever is available
    # Return empty if at least one ingestion table exists
    available_tables = {
        "claude_messages", "github_commits", "github_activity",
        "knowledge", "curated_sources"
    }
    if not any(table in schema for table in available_tables):
        return ("at least one of: claude_messages, github_commits, github_activity, knowledge, curated_sources",)
    return ()


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...] = (),
) -> ContentSourceFreshnessReport:
    return ContentSourceFreshnessReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "source_count": 0,
            "stale_count": 0,
            "active_count": 0,
        },
        rows=(),
        grouped_by_source_type={},
        missing_tables=missing_tables,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema


def _get_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Get column names for a specific table."""
    return {
        str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")
    }


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if value is None:
        raise ValueError("Cannot parse None as datetime")
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return _ensure_utc(datetime.fromisoformat(text))


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _optional_value(value: Any) -> str | None:
    text = str(value or "").strip()
    return text if text else None


def _csv_escape(value: str) -> str:
    """Escape CSV field values."""
    if not value:
        return ""
    if "," in value or '"' in value or "\n" in value:
        escaped = value.replace('"', '""')
        return f'"{escaped}"'
    return value
