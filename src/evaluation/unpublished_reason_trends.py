"""Trend unpublished or rejected content reasons over time."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_WINDOW_DAYS = 7
DEFAULT_LIMIT = 100
UNPUBLISHED_STATUSES = {"rejected", "unpublished", "failed", "blocked", "not_published", "review_rejected"}


def build_unpublished_reason_trends_report(
    reason_rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    window_days: int = DEFAULT_WINDOW_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return reason totals and time-window trend rows."""
    if days <= 0 or window_days <= 0 or limit <= 0:
        raise ValueError("days, window_days, and limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    records = []
    skipped = Counter({"outside_window": 0, "published_or_accepted": 0, "missing_timestamp": 0})
    for row in reason_rows:
        if not _is_unpublished(row):
            skipped["published_or_accepted"] += 1
            continue
        occurred_at = _parse_dt(row.get("occurred_at") or row.get("rejected_at") or row.get("unpublished_at") or row.get("created_at") or row.get("updated_at"))
        if not occurred_at:
            skipped["missing_timestamp"] += 1
            continue
        if occurred_at < cutoff or occurred_at > generated_at:
            skipped["outside_window"] += 1
            continue
        records.append(_record(row, occurred_at=occurred_at, cutoff=cutoff, window_days=window_days))

    trend_rows = _trend_rows(records, limit=limit)
    reason_totals = Counter(record["reason"] for record in records)
    channel_totals = Counter(record["channel"] for record in records)
    stage_totals = Counter(record["pipeline_stage"] for record in records)
    return {
        "artifact_type": "unpublished_reason_trends",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "window_days": window_days,
            "limit": limit,
            "window_start": cutoff.isoformat(),
            "window_end": generated_at.isoformat(),
        },
        "totals": {
            "record_count": len(records),
            "reason_counts": dict(sorted(reason_totals.items())),
            "channel_counts": dict(sorted(channel_totals.items())),
            "pipeline_stage_counts": dict(sorted(stage_totals.items())),
            **dict(skipped),
        },
        "trend_rows": trend_rows,
        "empty_state": {
            "is_empty": not records,
            "message": "No unpublished or rejected content reasons found." if not records else None,
        },
    }


def build_unpublished_reason_trends_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_rows(conn, schema)
    report = build_unpublished_reason_trends_report(rows, **kwargs)
    report["missing_tables"] = [] if rows or _has_reason_shape(schema) else ["pipeline_runs"]
    return report


def format_unpublished_reason_trends_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_unpublished_reason_trends_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Unpublished Reason Trends",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} window_days={report['filters']['window_days']} limit={report['filters']['limit']}",
        f"Totals: records={totals['record_count']} reasons={len(totals['reason_counts'])}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["trend_rows"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "Trends:"])
    for row in report["trend_rows"]:
        lines.append(
            f"- {row['window_start']}..{row['window_end']} reason={row['reason']} "
            f"count={row['count']} top_channel={_top(row['channels'])} top_stage={_top(row['pipeline_stages'])}"
        )
    return "\n".join(lines)


format_unpublished_reason_trends_table = format_unpublished_reason_trends_text


def _record(row: dict[str, Any], *, occurred_at: datetime, cutoff: datetime, window_days: int) -> dict[str, Any]:
    offset_days = max(0, int((occurred_at - cutoff).total_seconds() // 86400))
    window_index = offset_days // window_days
    window_start = cutoff + timedelta(days=window_index * window_days)
    window_end = min(window_start + timedelta(days=window_days), cutoff + timedelta(days=10_000))
    return {
        "content_id": _text(row.get("content_id") or row.get("item_id") or row.get("id")),
        "reason": _reason(row),
        "channel": _clean(row.get("channel") or row.get("platform")) or "unknown",
        "pipeline_stage": _clean(row.get("pipeline_stage") or row.get("stage") or row.get("gate")) or "unknown",
        "status": _clean(row.get("status") or row.get("outcome") or row.get("state")) or "unpublished",
        "occurred_at": occurred_at.isoformat(),
        "window_index": window_index,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }


def _trend_rows(records: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[(record["window_index"], record["reason"])].append(record)
    rows = []
    for (_window_index, reason), items in grouped.items():
        channels = Counter(item["channel"] for item in items)
        stages = Counter(item["pipeline_stage"] for item in items)
        rows.append(
            {
                "window_start": items[0]["window_start"],
                "window_end": items[0]["window_end"],
                "reason": reason,
                "count": len(items),
                "channels": dict(sorted(channels.items())),
                "pipeline_stages": dict(sorted(stages.items())),
                "examples": [
                    {"content_id": item["content_id"], "channel": item["channel"], "pipeline_stage": item["pipeline_stage"], "occurred_at": item["occurred_at"]}
                    for item in sorted(items, key=lambda item: (item["occurred_at"], item["content_id"]))[:5]
                ],
            }
        )
    rows.sort(key=lambda row: (row["window_start"], -row["count"], row["reason"]))
    return rows[:limit]


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for table in ("unpublished_content", "rejected_content", "publication_failures", "pipeline_runs"):
        if table in schema:
            rows.extend(_rows_from_table(conn, schema[table], table))
    return rows


def _rows_from_table(conn: sqlite3.Connection, columns: set[str], table: str) -> list[dict[str, Any]]:
    selected = [
        _select(columns, ("id", "content_id", "item_id"), "id"),
        _select(columns, ("content_id", "item_id"), "content_id"),
        _select(columns, ("reason", "rejection_reason", "unpublished_reason", "failure_reason", "error"), "reason"),
        _select(columns, ("reason_code", "code"), "reason_code"),
        _select(columns, ("channel", "platform"), "channel"),
        _select(columns, ("pipeline_stage", "stage", "gate"), "pipeline_stage"),
        _select(columns, ("status", "outcome", "state"), "status"),
        _select(columns, ("occurred_at", "rejected_at", "unpublished_at", "created_at", "updated_at"), "occurred_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]


def _is_unpublished(row: dict[str, Any]) -> bool:
    status = _clean(row.get("status") or row.get("outcome") or row.get("state")).lower()
    if status:
        return status in UNPUBLISHED_STATUSES or "reject" in status or "unpublish" in status or "fail" in status
    return bool(_clean(row.get("reason") or row.get("rejection_reason") or row.get("unpublished_reason") or row.get("failure_reason")))


def _reason(row: dict[str, Any]) -> str:
    code = _clean(row.get("reason_code") or row.get("code")).lower().replace(" ", "_")
    if code:
        return code
    text = _clean(row.get("reason") or row.get("rejection_reason") or row.get("unpublished_reason") or row.get("failure_reason") or row.get("error"))
    return text.lower().replace(" ", "_") if text else "unknown"


def _has_reason_shape(schema: dict[str, set[str]]) -> bool:
    return any(table in schema for table in ("unpublished_content", "rejected_content", "publication_failures", "pipeline_runs"))


def _select(columns: set[str], names: tuple[str, ...], alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{name} AS {alias}"
    return f"NULL AS {alias}"


def _top(counts: dict[str, int]) -> str:
    if not counts:
        return "-"
    return sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))[0][0]


def _parse_dt(value: Any) -> datetime | None:
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
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _clean(value: Any) -> str:
    return str(value).strip() if value not in (None, "") else ""


def _text(value: Any) -> str:
    return "" if value is None else str(value)
