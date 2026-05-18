"""Identify publication queue SLA breaches by channel and state."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_THRESHOLD_HOURS = {
    "queued": 24.0,
    "review": 48.0,
    "failed": 12.0,
    "retryable": 6.0,
}
STATE_ALIASES = {
    "pending_review": "review",
    "needs_review": "review",
    "held": "review",
    "retry": "retryable",
    "retrying": "retryable",
}


def build_publication_channel_sla_breach_report(
    rows: list[dict[str, Any]],
    *,
    threshold_hours: dict[str, float] | None = None,
    channel_threshold_hours: dict[str, dict[str, float]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build an SLA breach report from publication queue rows."""
    generated_at = _utc(now or datetime.now(timezone.utc))
    thresholds = _thresholds(threshold_hours)
    channel_overrides = {
        channel.lower(): _thresholds(values)
        for channel, values in (channel_threshold_hours or {}).items()
    }
    items = [_item(row, generated_at) for row in rows]
    breach_items = []
    for item in items:
        state = item["state"]
        if state not in thresholds:
            continue
        threshold = channel_overrides.get(item["channel"], thresholds).get(state, thresholds[state])
        if item["age_hours"] is not None and item["age_hours"] > threshold:
            breach_items.append({**item, "threshold_hours": threshold, "breach_hours": round(item["age_hours"] - threshold, 2)})
    breach_items.sort(key=lambda row: (-row["breach_hours"], row["channel"], row["state"], row["item_id"]))

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in breach_items:
        grouped[(item["channel"], item["state"])].append(item)
    channel_summary = []
    for (channel, state), group in sorted(grouped.items()):
        oldest = max(group, key=lambda row: row["age_hours"] or 0)
        channel_summary.append(
            {
                "channel": channel,
                "state": state,
                "breach_count": len(group),
                "oldest_item_id": oldest["item_id"],
                "oldest_age_hours": oldest["age_hours"],
                "threshold_hours": oldest["threshold_hours"],
            }
        )

    return {
        "artifact_type": "publication_channel_sla_breach",
        "generated_at": generated_at.isoformat(),
        "threshold_hours": thresholds,
        "channel_threshold_hours": channel_overrides,
        "total_breaches": len(breach_items),
        "breached_items": breach_items,
        "channel_summary": channel_summary,
    }


def build_publication_channel_sla_breach_report_from_db(
    db_or_conn: Any,
    *,
    threshold_hours: dict[str, float] | None = None,
    channel_threshold_hours: dict[str, dict[str, float]] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load publication queue rows from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_db_rows(conn, schema)
    report = build_publication_channel_sla_breach_report(
        rows,
        threshold_hours=threshold_hours,
        channel_threshold_hours=channel_threshold_hours,
        now=now,
    )
    report["missing_tables"] = [] if "publish_queue" in schema else ["publish_queue"]
    return report


def format_publication_channel_sla_breach_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_channel_sla_breach_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication Channel SLA Breach",
        f"Generated: {report['generated_at']}",
        f"Total breaches: {report['total_breaches']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["channel_summary"]:
        lines.append("Summary:")
        for row in report["channel_summary"]:
            lines.append(
                f"- {row['channel']}/{row['state']}: breaches={row['breach_count']} "
                f"oldest={row['oldest_item_id']} age={row['oldest_age_hours']}h "
                f"threshold={row['threshold_hours']}h"
            )
    if report["breached_items"]:
        lines.append("Breached items:")
        for row in report["breached_items"]:
            lines.append(
                f"- {row['item_id']}: {row['channel']}/{row['state']} age={row['age_hours']}h "
                f"threshold={row['threshold_hours']}h"
            )
    if not report["breached_items"]:
        lines.append("No publication channel SLA breaches found.")
    return "\n".join(lines)


def _load_db_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "publish_queue" if "publish_queue" in schema else None
    if table is None:
        return []
    columns = schema[table]
    selected = [
        _select_expr(table, _first_column(columns, "id", "queue_id"), "item_id"),
        _select_expr(table, _first_column(columns, "content_id"), "content_id"),
        _select_expr(table, _first_column(columns, "platform", "channel", "target_channel"), "channel", "'unknown'"),
        _select_expr(table, _first_column(columns, "status", "state"), "state", "'queued'"),
        _select_expr(table, _first_column(columns, "created_at", "queued_at", "scheduled_at"), "created_at"),
        _select_expr(table, _first_column(columns, "scheduled_at", "publish_at"), "scheduled_at"),
        _select_expr(table, _first_column(columns, "last_retry_at", "updated_at"), "last_retry_at"),
        _select_expr(table, _first_column(columns, "error", "hold_reason"), "reason"),
    ]
    rows = conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()
    return [dict(row) for row in rows]


def _item(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    state = _state(_first(row, "state", "status", "queue_status"))
    basis = _basis_time(row, state) or now
    age_hours = round((now - basis).total_seconds() / 3600, 2)
    return {
        "item_id": _text(_first(row, "item_id", "queue_id", "id", "publication_id")),
        "content_id": _text(_first(row, "content_id", "generated_content_id")),
        "channel": _text(_first(row, "channel", "platform", "target_channel") or "unknown").lower(),
        "state": state,
        "age_basis_at": basis.isoformat(),
        "age_hours": age_hours,
        "scheduled_at": _iso(_parse_datetime(_first(row, "scheduled_at", "publish_at"))),
        "reason": _text(_first(row, "reason", "error", "hold_reason")),
    }


def _basis_time(row: dict[str, Any], state: str) -> datetime | None:
    if state == "retryable":
        return _parse_datetime(_first(row, "last_retry_at", "updated_at", "created_at", "scheduled_at"))
    if state == "queued":
        return _parse_datetime(_first(row, "scheduled_at", "queued_at", "created_at"))
    return _parse_datetime(_first(row, "updated_at", "created_at", "scheduled_at", "last_retry_at"))


def _state(value: Any) -> str:
    state = _text(value).lower() or "queued"
    return STATE_ALIASES.get(state, state)


def _thresholds(values: dict[str, float] | None) -> dict[str, float]:
    merged = dict(DEFAULT_THRESHOLD_HOURS)
    for key, value in (values or {}).items():
        parsed = float(value)
        if parsed <= 0:
            raise ValueError("threshold hours must be positive")
        merged[_state(key)] = parsed
    return merged


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _first_column(columns: set[str], *names: str) -> str | None:
    return next((name for name in names if name in columns), None)


def _select_expr(table: str, column: str | None, output: str, fallback: str = "NULL") -> str:
    return f"{table}.{column} AS {output}" if column else f"{fallback} AS {output}"


def _parse_datetime(value: Any) -> datetime | None:
    if not _text(value):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _first(row: dict[str, Any], *names: str) -> Any:
    return next((row[name] for name in names if name in row and row[name] is not None), None)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()
