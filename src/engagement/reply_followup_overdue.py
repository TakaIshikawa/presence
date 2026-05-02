"""Escalate overdue pending reply follow-up reminders."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 7
DEFAULT_HIGH_PRIORITY_HOURS = 24
DEFAULT_LIMIT = 10
SEVERITIES = ("urgent", "stale", "watch")
REQUIRED_REMINDER_COLUMNS = {
    "id",
    "target_handle",
    "source_type",
    "source_id",
    "due_at",
    "status",
    "reason",
}


def build_reply_followup_overdue_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    high_priority_hours: int = DEFAULT_HIGH_PRIORITY_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return overdue pending reply follow-up reminders grouped by severity."""

    if days <= 0:
        raise ValueError("days must be positive")
    if high_priority_hours <= 0:
        raise ValueError("high_priority_hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "high_priority_hours": high_priority_hours,
        "limit": limit,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)

    if "reply_followup_reminders" not in schema:
        return _empty_report(generated_at, filters, ["reply_followup_reminders"], {})

    missing = sorted(REQUIRED_REMINDER_COLUMNS - schema["reply_followup_reminders"])
    if missing:
        return _empty_report(
            generated_at,
            filters,
            [],
            {"reply_followup_reminders": missing},
        )

    rows = _reminder_rows(conn, schema, cutoff=cutoff, now=generated_at)
    reminders = [
        _build_reminder(row, now=generated_at, high_priority_hours=high_priority_hours)
        for row in rows
    ]
    reminders = [item for item in reminders if item is not None]
    reminders.sort(key=_reminder_sort_key)

    buckets = {severity: [] for severity in SEVERITIES}
    for reminder in reminders:
        buckets[reminder["severity"]].append(reminder)

    return {
        "artifact_type": "reply_followup_overdue",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(reminders),
        "overdue_buckets": [
            {
                "severity": severity,
                "count": len(buckets[severity]),
                "reminders": buckets[severity],
            }
            for severity in SEVERITIES
        ],
        "representative_reminders": reminders[:limit],
        "reminders": reminders,
        "target_handles": sorted({item["target_handle"] for item in reminders}),
        "missing_tables": [],
        "missing_columns": {},
    }


def format_reply_followup_overdue_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_followup_overdue_text(report: dict[str, Any]) -> str:
    """Render a concise escalation view for operators."""

    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Reply Follow-up Overdue Escalations",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} "
            f"high_priority_hours={filters['high_priority_hours']} "
            f"limit={filters['limit']}"
        ),
        (
            f"Totals: overdue={totals['total']} urgent={totals['by_severity']['urgent']} "
            f"stale={totals['by_severity']['stale']} watch={totals['by_severity']['watch']} "
            f"targets={totals['target_handles']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        formatted = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append("Missing columns: " + "; ".join(formatted))
    if not report["representative_reminders"]:
        lines.append("No overdue pending reply follow-ups.")
        return "\n".join(lines)

    for bucket in report["overdue_buckets"]:
        if not bucket["reminders"]:
            continue
        lines.append("")
        lines.append(f"{bucket['severity'].title()} ({bucket['count']}):")
        for item in bucket["reminders"][: filters["limit"]]:
            source = f"{item['source_type']}:{item['source_id']}"
            age = f"{item['overdue_hours']:.1f}h"
            lines.append(
                f"- #{item['id']} @{item['target_handle']} due={item['due_at']} "
                f"overdue={age} source={source} reason={item['reason'] or '-'}"
            )
            lines.append(f"  recommendation={item['recommendation']}")
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "reply_followup_overdue",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals([]),
        "overdue_buckets": [
            {"severity": severity, "count": 0, "reminders": []}
            for severity in SEVERITIES
        ],
        "representative_reminders": [],
        "reminders": [],
        "target_handles": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _reminder_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    reminder_columns = schema["reply_followup_reminders"]
    select_columns = [
        _column_expr("r", reminder_columns, "id"),
        _column_expr("r", reminder_columns, "target_handle"),
        _column_expr("r", reminder_columns, "source_type"),
        _column_expr("r", reminder_columns, "source_id"),
        _column_expr("r", reminder_columns, "source_reply_id"),
        _column_expr("r", reminder_columns, "due_at"),
        _column_expr("r", reminder_columns, "status"),
        _column_expr("r", reminder_columns, "reason"),
        _column_expr("r", reminder_columns, "notes"),
        _column_expr("r", reminder_columns, "created_at"),
    ]

    reply_columns = schema.get("reply_queue", set())
    join = ""
    if reply_columns:
        if "source_reply_id" in reminder_columns:
            join = "LEFT JOIN reply_queue q ON q.id = r.source_reply_id"
        else:
            join = (
                "LEFT JOIN reply_queue q "
                "ON q.id = r.source_id AND r.source_type = 'reply_queue'"
            )
        select_columns.extend(
            [
                _column_expr("q", reply_columns, "platform", "'unknown'", "reply_platform"),
                _column_expr("q", reply_columns, "inbound_tweet_id"),
                _column_expr("q", reply_columns, "inbound_author_handle"),
                _column_expr("q", reply_columns, "inbound_author_id"),
                _column_expr("q", reply_columns, "inbound_text"),
                _column_expr("q", reply_columns, "inbound_url"),
                _column_expr("q", reply_columns, "draft_text"),
                _column_expr("q", reply_columns, "status", "NULL", "reply_status"),
            ]
        )
    else:
        select_columns.extend(
            [
                "'unknown' AS reply_platform",
                "NULL AS inbound_tweet_id",
                "NULL AS inbound_author_handle",
                "NULL AS inbound_author_id",
                "NULL AS inbound_text",
                "NULL AS inbound_url",
                "NULL AS draft_text",
                "NULL AS reply_status",
            ]
        )

    query = (
        f"SELECT {', '.join(select_columns)} "
        f"FROM reply_followup_reminders r {join} "
        "WHERE r.status = ? "
        "AND datetime(r.due_at) < datetime(?) "
        "AND datetime(r.due_at) >= datetime(?) "
        "ORDER BY datetime(r.due_at) ASC, r.id ASC"
    )
    cursor = conn.execute(query, ("pending", now.isoformat(), cutoff.isoformat()))
    names = [description[0] for description in cursor.description]
    return [_row_to_dict(row, names) for row in cursor.fetchall()]


def _build_reminder(
    row: dict[str, Any],
    *,
    now: datetime,
    high_priority_hours: int,
) -> dict[str, Any] | None:
    due_at = _parse_timestamp(row.get("due_at"))
    if due_at is None or due_at >= now:
        return None

    overdue_hours = round((now - due_at).total_seconds() / 3600, 2)
    source_type = str(row.get("source_type") or "unknown")
    source_id = _int_or_none(row.get("source_id"))
    source_reply_id = _int_or_none(row.get("source_reply_id"))
    if source_reply_id is None and source_type == "reply_queue":
        source_reply_id = source_id
    target_handle = _clean_handle(row.get("inbound_author_handle") or row.get("target_handle"))
    platform = str(row.get("reply_platform") or "unknown").strip().lower() or "unknown"
    severity = _severity(overdue_hours, high_priority_hours)

    reply_context = None
    if row.get("inbound_tweet_id") is not None:
        reply_context = {
            "reply_queue_id": source_reply_id,
            "platform": platform,
            "inbound_id": row.get("inbound_tweet_id"),
            "inbound_url": row.get("inbound_url"),
            "author_handle": _clean_handle(row.get("inbound_author_handle")),
            "author_id": row.get("inbound_author_id"),
            "inbound_text": row.get("inbound_text"),
            "draft_text": row.get("draft_text"),
            "status": row.get("reply_status"),
        }

    item = {
        "id": _int_or_none(row.get("id")),
        "severity": severity,
        "target_handle": target_handle,
        "due_at": due_at.isoformat(),
        "overdue_hours": overdue_hours,
        "overdue_age": f"{overdue_hours:.1f}h",
        "source_type": source_type,
        "source_id": source_id,
        "source_reply_id": source_reply_id,
        "reason": row.get("reason"),
        "notes": row.get("notes"),
        "platform": platform,
        "reply_context": reply_context,
    }
    item["recommendation"] = _recommendation(item)
    return item


def _totals(reminders: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total": len(reminders),
        "by_severity": {
            severity: sum(1 for item in reminders if item["severity"] == severity)
            for severity in SEVERITIES
        },
        "target_handles": len({item["target_handle"] for item in reminders}),
    }


def _severity(overdue_hours: float, high_priority_hours: int) -> str:
    if overdue_hours >= high_priority_hours:
        return "urgent"
    if overdue_hours >= max(1.0, high_priority_hours / 2):
        return "stale"
    return "watch"


def _recommendation(item: dict[str, Any]) -> str:
    context = "" if item["reply_context"] else " after checking the source context"
    if item["severity"] == "urgent":
        return f"Escalate and send or dismiss the {item['platform']} follow-up now{context}."
    if item["severity"] == "stale":
        return f"Review and resolve the {item['platform']} follow-up today{context}."
    return f"Monitor and clear the {item['platform']} follow-up before it becomes stale{context}."


def _reminder_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    severity_rank = {severity: index for index, severity in enumerate(SEVERITIES)}
    return (
        severity_rank.get(item["severity"], 99),
        -float(item["overdue_hours"]),
        item["id"] or 0,
    )


def _column_expr(
    alias: str,
    columns: set[str],
    column: str,
    default: str = "NULL",
    output: str | None = None,
) -> str:
    output = output or column
    if column in columns:
        return f"{alias}.{column} AS {output}"
    return f"{default} AS {output}"


def _row_to_dict(row: Any, names: list[str]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return dict(zip(names, row))


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean_handle(value: Any) -> str:
    handle = str(value or "unknown").strip().lstrip("@")
    return handle or "unknown"


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
