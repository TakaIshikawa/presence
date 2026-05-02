"""Summarize publish queue platform backlog skew."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_SKEW_THRESHOLD = 3
OPEN_STATUSES = ("queued", "failed", "held")
ALL_PLATFORM_TARGETS = ("bluesky", "x")
SCHEDULED_BUCKETS = ("unscheduled", "overdue", "today", "within_days", "later")


@dataclass(frozen=True)
class PublishQueuePlatformSummary:
    """Open queue backlog summary for one target platform."""

    platform: str
    total: int
    all_platform_count: int
    scheduled_count: int
    unscheduled_count: int
    oldest_queued_age_days: float | None
    oldest_queue_id: int | None
    by_status: dict[str, int]
    by_content_type: dict[str, int]
    scheduled_buckets: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "total": self.total,
            "all_platform_count": self.all_platform_count,
            "scheduled_count": self.scheduled_count,
            "unscheduled_count": self.unscheduled_count,
            "oldest_queued_age_days": self.oldest_queued_age_days,
            "oldest_queue_id": self.oldest_queue_id,
            "by_status": dict(sorted(self.by_status.items())),
            "by_content_type": dict(sorted(self.by_content_type.items())),
            "scheduled_buckets": {
                bucket: self.scheduled_buckets.get(bucket, 0)
                for bucket in SCHEDULED_BUCKETS
            },
        }


@dataclass(frozen=True)
class PublishQueueSkewWarning:
    """A platform pair whose queue count difference exceeds the threshold."""

    high_platform: str
    low_platform: str
    high_count: int
    low_count: int
    difference: int
    threshold: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "high_platform": self.high_platform,
            "low_platform": self.low_platform,
            "high_count": self.high_count,
            "low_count": self.low_count,
            "difference": self.difference,
            "threshold": self.threshold,
        }


@dataclass(frozen=True)
class PublishQueuePlatformSkewReport:
    """Publish queue platform imbalance report."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    platforms: tuple[PublishQueuePlatformSummary, ...]
    warnings: tuple[PublishQueueSkewWarning, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": self.totals,
            "platforms": [platform.to_dict() for platform in self.platforms],
            "warnings": [warning.to_dict() for warning in self.warnings],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_publish_queue_platform_skew_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    skew_threshold: int = DEFAULT_SKEW_THRESHOLD,
    now: datetime | None = None,
) -> PublishQueuePlatformSkewReport:
    """Build a read-only report of open queue skew by target platform."""
    if days <= 0:
        raise ValueError("days must be positive")
    if skew_threshold < 0:
        raise ValueError("skew_threshold must be non-negative")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    horizon_end = generated_at + timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}
    rows = _open_queue_rows(conn, schema, missing_tables, missing_columns)

    platforms = _platform_summaries(rows, generated_at, horizon_end)
    warnings = _skew_warnings(platforms, skew_threshold)
    raw_open_count = len(rows)
    expanded_open_count = sum(platform.total for platform in platforms)

    return PublishQueuePlatformSkewReport(
        artifact_type="publish_queue_platform_skew",
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "horizon_end": horizon_end.isoformat(),
            "open_statuses": list(OPEN_STATUSES),
            "skew_threshold": skew_threshold,
        },
        totals={
            "raw_open_count": raw_open_count,
            "expanded_open_count": expanded_open_count,
            "platform_count": len(platforms),
            "warning_count": len(warnings),
            "scheduled_count": sum(platform.scheduled_count for platform in platforms),
            "unscheduled_count": sum(platform.unscheduled_count for platform in platforms),
        },
        platforms=platforms,
        warnings=warnings,
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_publish_queue_platform_skew_json(
    report: PublishQueuePlatformSkewReport,
) -> str:
    """Serialize the platform skew report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publish_queue_platform_skew_text(
    report: PublishQueuePlatformSkewReport,
) -> str:
    """Format the platform skew report for terminal review."""
    lines = [
        "Publish Queue Platform Skew",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"(through {report.filters['horizon_end']})"
        ),
        f"Open statuses: {', '.join(report.filters['open_statuses'])}",
        f"Skew threshold: {report.filters['skew_threshold']}",
        (
            f"Open queue: {report.totals['raw_open_count']} rows, "
            f"{report.totals['expanded_open_count']} target assignments"
        ),
        f"Warnings: {report.totals['warning_count']}",
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)

    if not report.platforms:
        lines.append("No open publish queue items found.")
        return "\n".join(lines)

    lines.append("Platforms:")
    for platform in report.platforms:
        age = (
            "-"
            if platform.oldest_queued_age_days is None
            else f"{platform.oldest_queued_age_days}d"
        )
        statuses = ", ".join(
            f"{status}={count}" for status, count in sorted(platform.by_status.items())
        )
        types = ", ".join(
            f"{content_type}={count}"
            for content_type, count in sorted(platform.by_content_type.items())
        )
        lines.append(
            f"- {platform.platform}: total={platform.total} all={platform.all_platform_count} "
            f"scheduled={platform.scheduled_count} unscheduled={platform.unscheduled_count} "
            f"oldest={age} queue={platform.oldest_queue_id or '-'}"
        )
        lines.append(f"  status: {statuses or '-'}")
        lines.append(f"  content_type: {types or '-'}")

    if report.warnings:
        lines.append("Skew warnings:")
        for warning in report.warnings:
            lines.append(
                f"- {warning.high_platform} has {warning.difference} more open items "
                f"than {warning.low_platform} "
                f"({warning.high_count} vs {warning.low_count}; "
                f"threshold={warning.threshold})"
            )
    return "\n".join(lines)


def _open_queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    if "publish_queue" not in schema:
        missing_tables.add("publish_queue")
        return []
    required = ("id", "content_id", "status")
    missing = tuple(column for column in required if column not in schema["publish_queue"])
    if missing:
        missing_columns["publish_queue"] = missing
        return []

    pq = schema["publish_queue"]
    joins = ""
    content_type_expr = "'unknown'"
    if "generated_content" in schema and "content_type" in schema["generated_content"]:
        joins = "LEFT JOIN generated_content gc ON gc.id = pq.content_id"
        content_type_expr = "gc.content_type"
    elif "generated_content" not in schema:
        missing_tables.add("generated_content")
    else:
        missing_columns["generated_content"] = ("content_type",)

    rows = conn.execute(
        f"""SELECT
               pq.id AS queue_id,
               pq.content_id AS content_id,
               {_column_expr(pq, "platform", "'all'", alias="pq")} AS platform,
               pq.status AS status,
               {_column_expr(pq, "scheduled_at", "NULL", alias="pq")} AS scheduled_at,
               {_column_expr(pq, "created_at", "NULL", alias="pq")} AS created_at,
               {content_type_expr} AS content_type
           FROM publish_queue pq
           {joins}
           WHERE LOWER(pq.status) IN ({", ".join("?" for _ in OPEN_STATUSES)})
           ORDER BY platform ASC, scheduled_at ASC, pq.id ASC""",
        OPEN_STATUSES,
    ).fetchall()
    return [dict(row) for row in rows]


def _platform_summaries(
    rows: list[dict[str, Any]],
    now: datetime,
    horizon_end: datetime,
) -> tuple[PublishQueuePlatformSummary, ...]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        for platform in _target_platforms(row.get("platform")):
            grouped[platform].append(row)

    summaries: list[PublishQueuePlatformSummary] = []
    for platform in sorted(grouped):
        platform_rows = grouped[platform]
        by_status = Counter(_normalize(row.get("status")) for row in platform_rows)
        by_content_type = Counter(_normalize(row.get("content_type")) for row in platform_rows)
        scheduled_buckets = Counter(
            _scheduled_bucket(row.get("scheduled_at"), now, horizon_end)
            for row in platform_rows
        )
        oldest = _oldest_row(platform_rows, now)
        summaries.append(
            PublishQueuePlatformSummary(
                platform=platform,
                total=len(platform_rows),
                all_platform_count=sum(
                    1 for row in platform_rows if _normalize(row.get("platform")) == "all"
                ),
                scheduled_count=sum(
                    1 for row in platform_rows if _has_text(row.get("scheduled_at"))
                ),
                unscheduled_count=sum(
                    1 for row in platform_rows if not _has_text(row.get("scheduled_at"))
                ),
                oldest_queued_age_days=oldest[0],
                oldest_queue_id=oldest[1],
                by_status=dict(by_status),
                by_content_type=dict(by_content_type),
                scheduled_buckets=dict(scheduled_buckets),
            )
        )
    return tuple(summaries)


def _skew_warnings(
    platforms: tuple[PublishQueuePlatformSummary, ...],
    threshold: int,
) -> tuple[PublishQueueSkewWarning, ...]:
    warnings: list[PublishQueueSkewWarning] = []
    for index, high in enumerate(platforms):
        for low in platforms[index + 1 :]:
            if high.total < low.total:
                high, low = low, high
            difference = high.total - low.total
            if difference > threshold:
                warnings.append(
                    PublishQueueSkewWarning(
                        high_platform=high.platform,
                        low_platform=low.platform,
                        high_count=high.total,
                        low_count=low.total,
                        difference=difference,
                        threshold=threshold,
                    )
                )
    return tuple(sorted(warnings, key=lambda item: (-item.difference, item.high_platform)))


def _oldest_row(rows: list[dict[str, Any]], now: datetime) -> tuple[float | None, int | None]:
    oldest_age: float | None = None
    oldest_id: int | None = None
    for row in rows:
        queued_at = _parse_timestamp(row.get("created_at")) or _parse_timestamp(
            row.get("scheduled_at")
        )
        if queued_at is None:
            continue
        age = round((now - queued_at).total_seconds() / 86400, 2)
        if oldest_age is None or age > oldest_age:
            oldest_age = age
            oldest_id = int(row["queue_id"])
    return oldest_age, oldest_id


def _scheduled_bucket(value: Any, now: datetime, horizon_end: datetime) -> str:
    scheduled_at = _parse_timestamp(value)
    if scheduled_at is None:
        return "unscheduled"
    if scheduled_at < now:
        return "overdue"
    if scheduled_at.date() == now.date():
        return "today"
    if scheduled_at <= horizon_end:
        return "within_days"
    return "later"


def _target_platforms(value: Any) -> tuple[str, ...]:
    platform = _normalize(value)
    if platform == "all":
        return ALL_PLATFORM_TARGETS
    return (platform,)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str | None = None,
) -> str:
    if column not in columns:
        return fallback
    return f"{alias}.{column}" if alias else column


def _parse_timestamp(value: Any) -> datetime | None:
    if not _has_text(value):
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize(value: Any) -> str:
    return str(value or "unknown").strip().lower() or "unknown"


def _has_text(value: Any) -> bool:
    return bool(str(value or "").strip())
