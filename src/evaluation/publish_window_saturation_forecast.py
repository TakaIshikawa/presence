"""Forecast queued publish-window saturation by channel, weekday, and hour."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_CAPACITY = 2
OPEN_STATUSES = ("queued", "scheduled", "held")
ALL_CHANNEL_TARGETS = ("bluesky", "x")
DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")


def build_publish_window_saturation_forecast_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    capacity: int = DEFAULT_CAPACITY,
    preferred_windows: list[dict[str, Any]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a row-based forecast of overloaded and empty preferred publish windows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if capacity <= 0:
        raise ValueError("capacity must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    horizon_end = generated_at + timedelta(days=days)
    scheduled = [
        row
        for row in (_normalize_scheduled_row(row, generated_at, horizon_end) for row in rows)
        if row is not None
    ]
    groups: dict[tuple[str, int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in scheduled:
        for channel in _target_channels(row["channel"]):
            key = (channel, row["weekday"], row["hour"])
            groups[key].append({**row, "channel": channel})

    preferred_keys = {
        key
        for row in preferred_windows or []
        for key in _preferred_keys(row)
    }
    preferred_keys.update(
        (row["channel"], row["weekday"], row["hour"])
        for row in scheduled
        if row["is_preferred"]
    )

    overloaded_windows = [
        _window_payload(key, group, capacity)
        for key, group in groups.items()
        if len(group) > capacity
    ]
    overloaded_windows.sort(key=lambda item: (item["channel"], item["weekday"], item["hour"]))

    empty_preferred_windows = [
        _empty_window_payload(key, capacity)
        for key in preferred_keys
        if key not in groups
    ]
    empty_preferred_windows.sort(key=lambda item: (item["channel"], item["weekday"], item["hour"]))

    summary_channels = sorted({key[0] for key in groups} | {key[0] for key in preferred_keys})
    channel_summary = {
        channel: {
            "scheduled_count": sum(len(group) for key, group in groups.items() if key[0] == channel),
            "window_count": sum(1 for key in groups if key[0] == channel),
            "overloaded_window_count": sum(1 for row in overloaded_windows if row["channel"] == channel),
            "empty_preferred_window_count": sum(1 for row in empty_preferred_windows if row["channel"] == channel),
            "capacity": capacity,
        }
        for channel in summary_channels
    }

    return {
        "artifact_type": "publish_window_saturation_forecast",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "capacity": capacity,
            "horizon_start": generated_at.isoformat(),
            "horizon_end": horizon_end.isoformat(),
            "open_statuses": list(OPEN_STATUSES),
        },
        "totals": {
            "scheduled_count": len(scheduled),
            "expanded_scheduled_count": sum(len(group) for group in groups.values()),
            "scheduled_window_count": len(groups),
            "overloaded_window_count": len(overloaded_windows),
            "empty_preferred_window_count": len(empty_preferred_windows),
            "channel_count": len(channel_summary),
        },
        "overloaded_windows": overloaded_windows,
        "empty_preferred_windows": empty_preferred_windows,
        "channel_summary": channel_summary,
    }


def build_publish_window_saturation_forecast_report_from_db(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    capacity: int = DEFAULT_CAPACITY,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load queued/scheduled publication rows from SQLite and build the forecast."""
    if days <= 0:
        raise ValueError("days must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    horizon_end = generated_at + timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_scheduled_rows(conn, schema, generated_at, horizon_end)
    report = build_publish_window_saturation_forecast_report(
        rows,
        days=days,
        capacity=capacity,
        now=generated_at,
    )
    report["missing_tables"] = [] if rows or "publish_queue" in schema else ["publish_queue"]
    return report


def format_publish_window_saturation_forecast_json(report: dict[str, Any]) -> str:
    """Serialize the saturation forecast as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_window_saturation_forecast_text(report: dict[str, Any]) -> str:
    """Render the saturation forecast for terminal review."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Publish Window Saturation Forecast",
        f"Generated: {report['generated_at']}",
        f"Window: {filters['days']}d capacity={filters['capacity']} through {filters['horizon_end']}",
        (
            f"Totals: scheduled={totals['scheduled_count']} expanded={totals['expanded_scheduled_count']} "
            f"windows={totals['scheduled_window_count']} overloaded={totals['overloaded_window_count']} "
            f"empty_preferred={totals['empty_preferred_window_count']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["channel_summary"]:
        lines.append("Channels:")
        for channel, summary in sorted(report["channel_summary"].items()):
            lines.append(
                f"- {channel}: scheduled={summary['scheduled_count']} windows={summary['window_count']} "
                f"overloaded={summary['overloaded_window_count']} "
                f"empty_preferred={summary['empty_preferred_window_count']}"
            )
    if report["overloaded_windows"]:
        lines.append("Overloaded windows:")
        for row in report["overloaded_windows"]:
            lines.append(
                f"- {row['channel']} {row['day_name']} {row['hour']:02d}:00 "
                f"scheduled={row['scheduled_count']} capacity={row['capacity']} "
                f"excess={row['excess_count']} ids={_format_ids(row['scheduled_content_ids'])}"
            )
    if report["empty_preferred_windows"]:
        lines.append("Empty preferred windows:")
        for row in report["empty_preferred_windows"]:
            lines.append(
                f"- {row['channel']} {row['day_name']} {row['hour']:02d}:00 capacity={row['capacity']}"
            )
    if not report["overloaded_windows"] and not report["empty_preferred_windows"]:
        lines.append("No publish window saturation issues found.")
    return "\n".join(lines)


def _load_scheduled_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    now: datetime,
    horizon_end: datetime,
) -> list[dict[str, Any]]:
    table = _first_table(schema, "publish_queue", "publication_queue", "scheduled_publications")
    if table is None:
        return []
    columns = schema[table]
    scheduled_col = _first_column(columns, "scheduled_at", "publish_at", "planned_at", "run_at")
    if scheduled_col is None:
        return []
    id_col = _first_column(columns, "id", "queue_id", "publication_id")
    content_col = _first_column(columns, "content_id", "generated_content_id", "item_id")
    channel_col = _first_column(columns, "channel", "platform", "target_channel", "network")
    status_col = _first_column(columns, "status", "queue_status", "state")
    preferred_col = _first_column(columns, "is_preferred", "preferred", "preferred_window")
    selected = [
        _select_expr(table, id_col, "queue_id"),
        _select_expr(table, content_col, "content_id"),
        _select_expr(table, scheduled_col, "scheduled_at"),
        _select_expr(table, channel_col, "channel", "'all'"),
        _select_expr(table, status_col, "status", "'queued'"),
        _select_expr(table, preferred_col, "is_preferred", "0"),
    ]
    where = [f"{table}.{scheduled_col} >= ?", f"{table}.{scheduled_col} < ?"]
    params: list[Any] = [now.isoformat(), horizon_end.isoformat()]
    if status_col:
        where.append(f"LOWER(COALESCE({table}.{status_col}, 'queued')) IN ({', '.join('?' for _ in OPEN_STATUSES)})")
        params.extend(OPEN_STATUSES)
    rows = conn.execute(
        f"""SELECT {", ".join(selected)}
            FROM {table}
            WHERE {" AND ".join(where)}
            ORDER BY {table}.{scheduled_col} ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _normalize_scheduled_row(
    row: dict[str, Any],
    now: datetime,
    horizon_end: datetime,
) -> dict[str, Any] | None:
    scheduled_at = _parse_datetime(_first(row, "scheduled_at", "publish_at", "planned_at", "run_at"))
    if scheduled_at is None or scheduled_at < now or scheduled_at >= horizon_end:
        return None
    status = _text(_first(row, "status", "queue_status", "state") or "queued").lower()
    if status not in OPEN_STATUSES:
        return None
    return {
        "queue_id": _text(_first(row, "queue_id", "id", "publication_id")),
        "content_id": _text(_first(row, "content_id", "generated_content_id", "item_id")),
        "scheduled_at": scheduled_at.isoformat(),
        "channel": _text(_first(row, "channel", "platform", "target_channel", "network") or "all").lower(),
        "status": status,
        "weekday": scheduled_at.weekday(),
        "hour": scheduled_at.hour,
        "is_preferred": _truthy(_first(row, "is_preferred", "preferred", "preferred_window")),
    }


def _preferred_keys(row: dict[str, Any]) -> list[tuple[str, int, int]]:
    weekday = _weekday(row)
    hour = _int(_first(row, "hour", "hour_utc", "publish_hour"))
    if weekday is None or hour is None or hour < 0 or hour > 23:
        return []
    channel = _text(_first(row, "channel", "platform", "target_channel", "network") or "all").lower()
    return [(target, weekday, hour) for target in _target_channels(channel)]


def _window_payload(
    key: tuple[str, int, int],
    rows: list[dict[str, Any]],
    capacity: int,
) -> dict[str, Any]:
    channel, weekday, hour = key
    scheduled = sorted(rows, key=lambda row: (row["scheduled_at"], row["queue_id"], row["content_id"]))
    return {
        "channel": channel,
        "weekday": weekday,
        "day_name": DAY_NAMES[weekday],
        "hour": hour,
        "scheduled_count": len(scheduled),
        "capacity": capacity,
        "excess_count": len(scheduled) - capacity,
        "scheduled_content_ids": [row["content_id"] or row["queue_id"] for row in scheduled],
        "scheduled_queue_ids": [row["queue_id"] for row in scheduled if row["queue_id"]],
        "scheduled_at": [row["scheduled_at"] for row in scheduled],
    }


def _empty_window_payload(key: tuple[str, int, int], capacity: int) -> dict[str, Any]:
    channel, weekday, hour = key
    return {
        "channel": channel,
        "weekday": weekday,
        "day_name": DAY_NAMES[weekday],
        "hour": hour,
        "scheduled_count": 0,
        "capacity": capacity,
    }


def _target_channels(value: Any) -> tuple[str, ...]:
    channel = _text(value).lower() or "unknown"
    if channel == "all":
        return ALL_CHANNEL_TARGETS
    return (channel,)


def _weekday(row: dict[str, Any]) -> int | None:
    value = _first(row, "weekday", "day_of_week")
    parsed = _int(value)
    if parsed is not None and 0 <= parsed <= 6:
        return parsed
    day_name = _text(_first(row, "day_name", "weekday_name")).lower()
    return next((index for index, name in enumerate(DAY_NAMES) if name.lower() == day_name), None)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {
        row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _first_table(schema: dict[str, set[str]], *names: str) -> str | None:
    return next((name for name in names if name in schema), None)


def _first_column(columns: set[str], *names: str) -> str | None:
    return next((name for name in names if name in columns), None)


def _select_expr(table: str, column: str | None, output: str, fallback: str = "NULL") -> str:
    if column is None:
        return f"{fallback} AS {output}"
    return f"{table}.{column} AS {output}"


def _parse_datetime(value: Any) -> datetime | None:
    if not _text(value):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *names: str) -> Any:
    return next((row[name] for name in names if name in row and row[name] is not None), None)


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).lower() in {"1", "true", "yes", "y"}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _format_ids(values: list[str]) -> str:
    return ",".join(str(value) for value in values if value) or "-"
