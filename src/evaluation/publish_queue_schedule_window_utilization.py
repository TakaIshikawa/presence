"""Measure publish queue utilization against configured schedule windows."""

from __future__ import annotations

from collections import defaultdict
from typing import Any
from zoneinfo import ZoneInfo

from ._batch_report_common import (
    bounded_share,
    clean,
    connection,
    dt,
    empty_state,
    flatten_missing,
    json_dumps,
    lower,
    now_iso,
    positive,
    schema,
    to_int,
)


ARTIFACT_TYPE = "publish_queue_schedule_window_utilization"
DEFAULT_LIMIT = 50
DEFAULT_UNDERUSED = 0.5
DEFAULT_OVERFILLED = 1.0


def build_publish_queue_schedule_window_utilization_report(
    queue_items: list[dict[str, Any]],
    publish_windows: list[dict[str, Any]] | None = None,
    *,
    timezone: str | None = None,
    timezone_name: str | None = None,
    underused_threshold: float = DEFAULT_UNDERUSED,
    overfilled_threshold: float = DEFAULT_OVERFILLED,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now: Any = None,
) -> dict[str, Any]:
    """Build a deterministic utilization report for scheduled publish windows."""

    tz_name = timezone_name or timezone or "UTC"
    tz = ZoneInfo(tz_name)
    bounded_share("underused_threshold", underused_threshold)
    positive("overfilled_threshold", overfilled_threshold)
    positive("limit", limit)

    windows = [_normalise_window(row, index) for index, row in enumerate(publish_windows or [])]
    utilization: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
        lambda: {"scheduled_count": 0, "capacity": 0, "window": None, "items": []}
    )
    for window in windows:
        key = _window_key(window)
        utilization[key]["capacity"] += window["capacity"]
        utilization[key]["window"] = window

    outside_window: list[dict[str, Any]] = []
    for index, item in enumerate(queue_items):
        scheduled = _local_dt(item.get("scheduled_at") or item.get("publish_at") or item.get("published_at"), tz)
        platform = lower(item.get("platform"), "unknown")
        matched = _matching_window(scheduled, platform, windows)
        if matched is None:
            outside_window.append(
                {
                    "type": "outside_window",
                    "reason": "outside_window",
                    "queue_id": item.get("queue_id") or item.get("id") or index + 1,
                    "platform": platform,
                    "scheduled_at": scheduled.isoformat() if scheduled else clean(item.get("scheduled_at")) or None,
                    "severity": 50,
                }
            )
            continue

        key = _window_key(matched)
        utilization[key]["scheduled_count"] += 1
        utilization[key]["items"].append(item.get("queue_id") or item.get("id") or index + 1)

    utilization_rows = []
    findings = list(outside_window)
    for (platform, day, window_label), row in sorted(utilization.items()):
        capacity = max(int(row["capacity"]), 1)
        rate = round(row["scheduled_count"] / capacity, 4)
        utilization_rows.append(
            {
                "platform": platform,
                "day": day,
                "window": window_label,
                "window_id": row["window"]["window_id"] if row["window"] else None,
                "scheduled_count": row["scheduled_count"],
                "capacity": capacity,
                "utilization": rate,
            }
        )
        if rate < underused_threshold:
            findings.append(
                {
                    "type": "underused",
                    "platform": platform,
                    "day": day,
                    "window": window_label,
                    "window_id": row["window"]["window_id"] if row["window"] else None,
                    "scheduled_count": row["scheduled_count"],
                    "capacity": capacity,
                    "utilization": rate,
                    "severity": round((underused_threshold - rate) * 100, 2),
                }
            )
        if rate > overfilled_threshold:
            findings.append(
                {
                    "type": "overfilled",
                    "platform": platform,
                    "day": day,
                    "window": window_label,
                    "window_id": row["window"]["window_id"] if row["window"] else None,
                    "scheduled_count": row["scheduled_count"],
                    "capacity": capacity,
                    "utilization": rate,
                    "severity": round((rate - overfilled_threshold) * 100, 2),
                }
            )

    findings.sort(
        key=lambda item: (
            {"overfilled": 0, "outside_window": 1, "underused": 2}.get(item["type"], 9),
            -float(item.get("severity", 0)),
            item.get("platform", ""),
            item.get("day", ""),
            str(item.get("window_id", "")),
        )
    )
    shown_findings = findings[:limit]
    schema_gap = bool(missing_tables or missing_columns)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": now_iso(now),
        "filters": {
            "timezone": tz_name,
            "underused_threshold": underused_threshold,
            "overfilled_threshold": overfilled_threshold,
            "limit": limit,
        },
        "totals": {
            "queue_items": len(queue_items),
            "windows": len(windows),
            "findings": len(findings),
            "outside_window": len(outside_window),
            "shown_findings": len(shown_findings),
        },
        "utilization": utilization_rows,
        "window_utilization": utilization_rows,
        "findings": shown_findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {key: sorted(value) for key, value in sorted((missing_columns or {}).items())},
        "empty_state": empty_state(
            findings,
            "No publish queue schedule window utilization issues found.",
            schema_gap=schema_gap,
        ),
    }


def build_publish_queue_schedule_window_utilization_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load publish queue and optional publish window metadata from SQLite."""

    conn = connection(db_or_conn)
    db_schema = schema(conn)
    missing_tables = [] if "publish_queue" in db_schema else ["publish_queue"]
    missing_columns: dict[str, list[str]] = {}
    queue_rows: list[dict[str, Any]] = []
    window_rows: list[dict[str, Any]] = []

    if "publish_queue" in db_schema:
        queue_rows = _load_publish_queue(conn, db_schema["publish_queue"])

    for table in ("posting_windows", "publish_windows"):
        if table in db_schema:
            window_rows.extend(_load_windows(conn, table, db_schema[table]))

    if "publish_queue" in db_schema and not window_rows:
        missing_tables.append("posting_windows|publish_windows")

    return build_publish_queue_schedule_window_utilization_report(
        queue_rows,
        window_rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_publish_queue_schedule_window_utilization_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_publish_queue_schedule_window_utilization_text(report: dict[str, Any]) -> str:
    lines = [
        "Publish Queue Schedule Window Utilization",
        f"Generated: {report['generated_at']}",
        f"Timezone: {report['filters']['timezone']}",
        (
            "Totals: "
            f"queue_items={report['totals']['queue_items']} "
            f"windows={report['totals']['windows']} "
            f"findings={report['totals']['findings']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "type | platform | day | window | scheduled | capacity | utilization"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['type']} | {finding.get('platform', '-')} | "
            f"{finding.get('day', '-')} | {finding.get('window', '-')} | "
            f"{finding.get('scheduled_count', '-')} | {finding.get('capacity', '-')} | "
            f"{finding.get('utilization', '-')}"
        )
    return "\n".join(lines)


def _load_publish_queue(conn: Any, cols: set[str]) -> list[dict[str, Any]]:
    selected = [
        _pick(cols, "queue_id", "id", out="queue_id"),
        _pick(cols, "platform", out="platform"),
        _pick(cols, "scheduled_at", "publish_at", "published_at", out="scheduled_at"),
        _pick(cols, "status", out="status"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM publish_queue ORDER BY rowid")]


def _load_windows(conn: Any, table: str, cols: set[str]) -> list[dict[str, Any]]:
    selected = [
        _pick(cols, "window_id", "id", out="window_id"),
        _pick(cols, "platform", out="platform"),
        _pick(cols, "day_of_week", "weekday", out="day_of_week"),
        _pick(cols, "start_time", out="start_time"),
        _pick(cols, "end_time", out="end_time"),
        _pick(cols, "capacity", "max_items", "slot_count", default="1", out="capacity"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table} ORDER BY rowid")]


def _pick(cols: set[str], *names: str, default: str = "NULL", out: str) -> str:
    for name in names:
        if name in cols:
            return f"{name} AS {out}"
    return f"{default} AS {out}"


def _normalise_window(row: dict[str, Any], index: int) -> dict[str, Any]:
    start = clean(row.get("start_time"), "00:00")
    end = clean(row.get("end_time"), "23:59")
    return {
        "window_id": row.get("window_id") or row.get("id") or index + 1,
        "platform": lower(row.get("platform"), "unknown"),
        "day_of_week": lower(row.get("day_of_week") or row.get("weekday"), "all")[:3],
        "start_time": start,
        "end_time": end,
        "capacity": to_int(row.get("capacity") or row.get("max_items") or row.get("slot_count"), 1),
    }


def _matching_window(scheduled: Any, platform: str, windows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if scheduled is None:
        return None
    minute = scheduled.hour * 60 + scheduled.minute
    day = scheduled.strftime("%a").lower()[:3]
    for window in windows:
        if window["platform"] not in {platform, "all"}:
            continue
        if window["day_of_week"] not in {day, "all"}:
            continue
        if _minute(window["start_time"]) <= minute < _minute(window["end_time"]):
            return window
    return None


def _window_key(window: dict[str, Any]) -> tuple[str, str, str]:
    return (
        window["platform"],
        window["day_of_week"],
        f"{window['start_time']}-{window['end_time']}",
    )


def _local_dt(value: Any, tz: ZoneInfo) -> Any:
    parsed = dt(value)
    return parsed.astimezone(tz) if parsed else None


def _minute(value: Any) -> int:
    parts = clean(value, "00:00").split(":")
    if len(parts) < 2:
        return 0
    return to_int(parts[0]) * 60 + to_int(parts[1])
