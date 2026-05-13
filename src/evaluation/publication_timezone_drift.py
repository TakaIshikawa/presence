"""Report publication timezone and local-day drift."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_TIMEZONE_OFFSET_HOURS = 0
VALID_PLATFORMS = {"all", "x", "bluesky", "linkedin", "mastodon"}


@dataclass(frozen=True)
class PublicationTimezoneDriftItem:
    content_id: int
    platform: str
    source: str
    source_id: int | None
    scheduled_at: str | None
    created_at: str | None
    published_at: str
    drift_types: tuple[str, ...]
    local_scheduled_day: str | None
    local_published_day: str | None
    whole_hour_offset: int | None
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["drift_types"] = list(self.drift_types)
        return data


@dataclass(frozen=True)
class PublicationTimezoneDriftReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    drift_items: tuple[PublicationTimezoneDriftItem, ...]
    empty_state: dict[str, Any]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "publication_timezone_drift",
            "drift_items": [item.to_dict() for item in self.drift_items],
            "empty_state": dict(self.empty_state),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_publication_timezone_drift_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    timezone_offset_hours: int = DEFAULT_TIMEZONE_OFFSET_HOURS,
    now: datetime | None = None,
) -> PublicationTimezoneDriftReport:
    """Build a read-only report for suspicious publication timestamp drift."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")
    if timezone_offset_hours < -23 or timezone_offset_hours > 23:
        raise ValueError("timezone_offset_hours must be between -23 and 23")

    generated_at = _aware(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "lookback_start": cutoff.isoformat(),
        "platform": platform,
        "timezone_offset_hours": timezone_offset_hours,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(table for table in ("publish_queue",) if table not in schema)
    missing_columns = _missing_columns(schema)
    if missing_tables:
        return _report(
            generated_at=generated_at,
            filters=filters,
            rows=(),
            scanned_count=0,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_publication_rows(conn, schema, cutoff=cutoff.isoformat(), platform=platform)
    items = tuple(
        item
        for row in rows
        if (
            item := _classify_row(
                row,
                timezone_offset_hours=timezone_offset_hours,
            )
        )
        is not None
    )
    items = tuple(sorted(items, key=_item_sort_key))
    return _report(
        generated_at=generated_at,
        filters=filters,
        rows=items,
        scanned_count=len(rows),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_publication_timezone_drift_json(report: PublicationTimezoneDriftReport) -> str:
    """Render publication timezone drift as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publication_timezone_drift_text(report: PublicationTimezoneDriftReport) -> str:
    """Render publication timezone drift for terminal review."""
    lines = [
        "Publication Timezone Drift",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days; "
            f"platform={report.filters['platform']}; "
            f"offset={report.filters['timezone_offset_hours']}h"
        ),
        (
            f"Scanned: {report.totals['records_scanned']}; "
            f"drift_items={report.totals['drift_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        )
        if missing:
            lines.append("Missing columns: " + missing)
    lines.append("")

    if not report.drift_items:
        lines.append(report.empty_state["message"])
        return "\n".join(lines)

    for item in report.drift_items:
        lines.append(
            "- "
            f"{item.platform} content={item.content_id} source={item.source}:{item.source_id or '-'} "
            f"types={','.join(item.drift_types)} "
            f"scheduled={item.scheduled_at or '-'} published={item.published_at} "
            f"local_day={item.local_scheduled_day or '-'}->{item.local_published_day or '-'} "
            f"hour_offset={item.whole_hour_offset if item.whole_hour_offset is not None else '-'}"
        )
        lines.append(f"  action={item.recommended_action}")
    return "\n".join(lines)


def _load_publication_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: str,
    platform: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rows.extend(_queue_rows(conn, schema, cutoff=cutoff, platform=platform))
    rows.extend(_content_publication_rows(conn, schema, cutoff=cutoff, platform=platform))
    rows.extend(_attempt_rows(conn, schema, cutoff=cutoff, platform=platform))
    seen: set[tuple[Any, ...]] = set()
    unique: list[dict[str, Any]] = []
    for row in rows:
        key = (row["source"], row["source_id"], row["content_id"], row["platform"], row["published_at"])
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


def _queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: str,
    platform: str,
) -> list[dict[str, Any]]:
    columns = schema.get("publish_queue", set())
    required = {"id", "content_id", "published_at"}
    if not required.issubset(columns):
        return []
    select = {
        "id": _column_expr("pq", columns, "id"),
        "content_id": _column_expr("pq", columns, "content_id"),
        "platform": _column_expr("pq", columns, "platform", "'all'"),
        "scheduled_at": _column_expr("pq", columns, "scheduled_at"),
        "created_at": _column_expr("pq", columns, "created_at"),
        "published_at": _column_expr("pq", columns, "published_at"),
    }
    filters = ["pq.published_at IS NOT NULL", "pq.published_at >= ?"]
    params: list[Any] = [cutoff]
    if platform != "all" and "platform" in columns:
        filters.append("(pq.platform = ? OR pq.platform = 'all')")
        params.append(platform)
    rows = conn.execute(
        f"""SELECT {select['id']} AS source_id,
                  {select['content_id']} AS content_id,
                  {select['platform']} AS platform,
                  {select['scheduled_at']} AS scheduled_at,
                  {select['created_at']} AS created_at,
                  {select['published_at']} AS published_at
           FROM publish_queue pq
           WHERE {' AND '.join(filters)}""",
        params,
    ).fetchall()
    return [_expand_platforms(dict(row), platform, "publish_queue") for row in rows]


def _content_publication_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: str,
    platform: str,
) -> list[dict[str, Any]]:
    cp_columns = schema.get("content_publications", set())
    pq_columns = schema.get("publish_queue", set())
    if not {"id", "content_id", "platform", "published_at"}.issubset(cp_columns):
        return []
    select_created = _column_expr("pq", pq_columns, "created_at")
    select_scheduled = _column_expr("pq", pq_columns, "scheduled_at")
    filters = ["cp.published_at IS NOT NULL", "cp.published_at >= ?"]
    params: list[Any] = [cutoff]
    if platform != "all":
        filters.append("cp.platform = ?")
        params.append(platform)
    rows = conn.execute(
        f"""SELECT cp.id AS source_id,
                  cp.content_id AS content_id,
                  cp.platform AS platform,
                  {select_scheduled} AS scheduled_at,
                  {select_created} AS created_at,
                  cp.published_at AS published_at
           FROM content_publications cp
           LEFT JOIN publish_queue pq
             ON pq.content_id = cp.content_id
            AND (pq.platform = cp.platform OR pq.platform = 'all')
           WHERE {' AND '.join(filters)}
           ORDER BY cp.id ASC, pq.id ASC""",
        params,
    ).fetchall()
    return [{**dict(row), "source": "content_publications"} for row in rows]


def _attempt_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: str,
    platform: str,
) -> list[dict[str, Any]]:
    pa_columns = schema.get("publication_attempts", set())
    pq_columns = schema.get("publish_queue", set())
    if not {"id", "content_id", "platform", "attempted_at", "success"}.issubset(pa_columns):
        return []
    select_created = _column_expr("pq", pq_columns, "created_at")
    select_scheduled = _column_expr("pq", pq_columns, "scheduled_at")
    join_on = "pq.id = pa.queue_id" if "queue_id" in pa_columns else "pq.content_id = pa.content_id"
    filters = ["pa.success = 1", "pa.attempted_at >= ?"]
    params: list[Any] = [cutoff]
    if platform != "all":
        filters.append("pa.platform = ?")
        params.append(platform)
    rows = conn.execute(
        f"""SELECT pa.id AS source_id,
                  pa.content_id AS content_id,
                  pa.platform AS platform,
                  {select_scheduled} AS scheduled_at,
                  {select_created} AS created_at,
                  pa.attempted_at AS published_at
           FROM publication_attempts pa
           LEFT JOIN publish_queue pq ON {join_on}
           WHERE {' AND '.join(filters)}
           ORDER BY pa.id ASC, pq.id ASC""",
        params,
    ).fetchall()
    return [{**dict(row), "source": "publication_attempts"} for row in rows]


def _expand_platforms(row: dict[str, Any], selected_platform: str, source: str) -> dict[str, Any]:
    row["source"] = source
    if row.get("platform") == "all" and selected_platform != "all":
        row["platform"] = selected_platform
    return row


def _classify_row(
    row: dict[str, Any],
    *,
    timezone_offset_hours: int,
) -> PublicationTimezoneDriftItem | None:
    published = _parse_timestamp(row.get("published_at"))
    if published is None:
        return None
    scheduled = _parse_timestamp(row.get("scheduled_at"))
    created = _parse_timestamp(row.get("created_at"))
    local_scheduled = _local_day(scheduled, timezone_offset_hours)
    local_published = _local_day(published, timezone_offset_hours)

    drift_types: list[str] = []
    if scheduled is not None and published < scheduled:
        drift_types.append("before_scheduled")
    if local_scheduled and local_published and local_scheduled != local_published:
        drift_types.append("local_day_mismatch")
    whole_hour_offset = _whole_hour_offset(scheduled, published)
    if whole_hour_offset is not None and abs(whole_hour_offset) >= 1:
        drift_types.append("whole_hour_offset")
    if not drift_types:
        return None

    return PublicationTimezoneDriftItem(
        content_id=_int(row.get("content_id")),
        platform=str(row.get("platform") or "unknown"),
        source=str(row.get("source") or "unknown"),
        source_id=_int_or_none(row.get("source_id")),
        scheduled_at=_clean(row.get("scheduled_at")),
        created_at=_clean(row.get("created_at")),
        published_at=str(row.get("published_at")),
        drift_types=tuple(dict.fromkeys(drift_types)),
        local_scheduled_day=local_scheduled,
        local_published_day=local_published,
        whole_hour_offset=whole_hour_offset,
        recommended_action=_recommended_action(drift_types),
    )


def _report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    rows: tuple[PublicationTimezoneDriftItem, ...],
    scanned_count: int,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> PublicationTimezoneDriftReport:
    by_type: dict[str, int] = {}
    for row in rows:
        for drift_type in row.drift_types:
            by_type[drift_type] = by_type.get(drift_type, 0) + 1
    empty = not rows
    return PublicationTimezoneDriftReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "by_drift_type": dict(sorted(by_type.items())),
            "drift_count": len(rows),
            "records_scanned": scanned_count,
        },
        drift_items=rows,
        empty_state={
            "is_empty": empty,
            "message": (
                "No publication timezone drift found."
                if not missing_tables
                else "Publication queue schema is unavailable."
            ),
        },
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _recommended_action(drift_types: list[str]) -> str:
    if "before_scheduled" in drift_types:
        return "Inspect scheduler timezone conversion and prevent publishing before scheduled_at."
    if "local_day_mismatch" in drift_types:
        return "Review local publish-day calculation for this platform and reschedule affected content."
    return "Compare scheduled_at and published_at timezone handling for whole-hour conversion drift."


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "publish_queue": ("id", "content_id", "platform", "scheduled_at", "created_at", "published_at"),
        "content_publications": ("id", "content_id", "platform", "published_at"),
        "publication_attempts": ("id", "content_id", "platform", "attempted_at", "success"),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _column_expr(alias: str, columns: set[str], column: str, default: str = "NULL") -> str:
    return f"{alias}.{column}" if column in columns else default


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _aware(value)
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return _aware(datetime.fromisoformat(text))
    except ValueError:
        try:
            return _aware(datetime.strptime(text, "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            return None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _local_day(value: datetime | None, offset_hours: int) -> str | None:
    if value is None:
        return None
    return (value + timedelta(hours=offset_hours)).date().isoformat()


def _whole_hour_offset(scheduled: datetime | None, published: datetime) -> int | None:
    if scheduled is None:
        return None
    delta = published - scheduled
    seconds = int(delta.total_seconds())
    if seconds == 0 or seconds % 3600 != 0:
        return None
    return seconds // 3600


def _item_sort_key(item: PublicationTimezoneDriftItem) -> tuple[int, str, int]:
    rank = {"before_scheduled": 0, "local_day_mismatch": 1, "whole_hour_offset": 2}
    return (min(rank.get(kind, 9) for kind in item.drift_types), item.platform, item.content_id)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
