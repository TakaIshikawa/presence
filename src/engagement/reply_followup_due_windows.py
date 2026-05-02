"""Build due-window reports for reply follow-up reminders."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_HORIZON_HOURS = 24
BUCKETS = ("overdue", "due_soon", "missing_target", "blocked_source")
ACTIVE_REMINDER_STATUS = "pending"
BLOCKED_SOURCE_STATUSES = {
    "cancelled",
    "canceled",
    "dismissed",
    "expired",
    "failed",
    "rejected",
}
REQUIRED_REMINDER_COLUMNS = {
    "id",
    "target_handle",
    "source_type",
    "source_id",
    "due_at",
    "status",
    "reason",
}


def build_reply_followup_due_windows_report(
    db_or_conn: Any,
    *,
    horizon_hours: int = DEFAULT_HORIZON_HOURS,
    target_handle: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return pending follow-up reminders that are due soon, overdue, or blocked."""

    if horizon_hours <= 0:
        raise ValueError("horizon_hours must be positive")
    normalized_target = _clean_handle(target_handle)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    horizon_end = generated_at + timedelta(hours=horizon_hours)
    filters = {
        "horizon_end": horizon_end.isoformat(),
        "horizon_hours": horizon_hours,
        "target_handle": normalized_target,
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

    rows = _reminder_rows(conn, schema)
    findings: list[dict[str, Any]] = []
    for row in rows:
        findings.extend(
            _findings_for_row(
                row,
                now=generated_at,
                horizon_end=horizon_end,
                target_handle=normalized_target,
            )
        )
    findings.sort(key=_finding_sort_key)

    buckets = {bucket: [] for bucket in BUCKETS}
    for finding in findings:
        buckets[finding["bucket"]].append(finding)

    return {
        "artifact_type": "reply_followup_due_windows",
        "buckets": buckets,
        "findings": findings,
        "generated_at": generated_at.isoformat(),
        "horizon_hours": horizon_hours,
        "target_handle": normalized_target,
        "filters": filters,
        "totals": {
            "total": len(findings),
            **{bucket: len(buckets[bucket]) for bucket in BUCKETS},
        },
        "missing_tables": [],
        "missing_columns": {},
    }


def format_reply_followup_due_windows_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_followup_due_windows_text(report: dict[str, Any]) -> str:
    """Render a compact due-window view for operators."""

    totals = report["totals"]
    lines = [
        "Reply Follow-up Due Windows",
        f"Generated: {report['generated_at']}",
        (
            f"Horizon: {report['horizon_hours']}h "
            f"target={report['target_handle'] or 'all'}"
        ),
        (
            f"Totals: overdue={totals['overdue']} due_soon={totals['due_soon']} "
            f"missing_target={totals['missing_target']} "
            f"blocked_source={totals['blocked_source']} total={totals['total']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    if not report["findings"]:
        lines.append("No pending reply follow-ups matched the due window.")
        return "\n".join(lines)

    for bucket in BUCKETS:
        items = report["buckets"][bucket]
        if not items:
            continue
        lines.append("")
        lines.append(bucket.replace("_", " ").title() + f" ({len(items)}):")
        for item in items:
            source = f"{item['source_type']}:{item['source_id']}"
            target = item["target_handle"] or "-"
            source_status = item["source_status"] or "-"
            lines.append(
                f"- #{item['id']} @{target} due={item['due_at']} "
                f"hours={item['hours_until_due']:.1f} source={source} "
                f"source_status={source_status}"
            )
            lines.append(f"  recommended_action={item['recommended_action']}")
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        table: _table_columns(conn, table)
        for table in ("reply_followup_reminders", "reply_queue")
        if _table_columns(conn, table)
    }


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
    buckets: dict[str, list[dict[str, Any]]] = {bucket: [] for bucket in BUCKETS}
    return {
        "artifact_type": "reply_followup_due_windows",
        "buckets": buckets,
        "findings": [],
        "generated_at": generated_at.isoformat(),
        "horizon_hours": filters["horizon_hours"],
        "target_handle": filters["target_handle"],
        "filters": filters,
        "totals": {"total": 0, **{bucket: 0 for bucket in BUCKETS}},
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _reminder_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
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
                _column_expr("q", reply_columns, "inbound_author_handle"),
                _column_expr("q", reply_columns, "inbound_author_id"),
                _column_expr("q", reply_columns, "inbound_tweet_id"),
                _column_expr("q", reply_columns, "inbound_url"),
                _column_expr("q", reply_columns, "status", "NULL", "source_status"),
            ]
        )
    else:
        select_columns.extend(
            [
                "'unknown' AS reply_platform",
                "NULL AS inbound_author_handle",
                "NULL AS inbound_author_id",
                "NULL AS inbound_tweet_id",
                "NULL AS inbound_url",
                "NULL AS source_status",
            ]
        )

    query = (
        f"SELECT {', '.join(select_columns)} "
        f"FROM reply_followup_reminders r {join} "
        "WHERE r.status = ? "
        "ORDER BY datetime(r.due_at) ASC, r.id ASC"
    )
    return [dict(row) for row in conn.execute(query, (ACTIVE_REMINDER_STATUS,)).fetchall()]


def _findings_for_row(
    row: dict[str, Any],
    *,
    now: datetime,
    horizon_end: datetime,
    target_handle: str | None,
) -> list[dict[str, Any]]:
    due_at = _parse_timestamp(row.get("due_at"))
    if due_at is None:
        return []

    effective_target = _clean_handle(
        row.get("inbound_author_handle") or row.get("target_handle")
    )
    if target_handle and effective_target != target_handle:
        return []

    hours_until_due = round((due_at - now).total_seconds() / 3600, 2)
    source_status = _clean_status(row.get("source_status"))
    base = {
        "id": _int_or_none(row.get("id")),
        "target_handle": effective_target,
        "stored_target_handle": _clean_handle(row.get("target_handle")),
        "due_at": due_at.isoformat(),
        "hours_until_due": hours_until_due,
        "source_type": str(row.get("source_type") or "unknown"),
        "source_id": _int_or_none(row.get("source_id")),
        "source_reply_id": _source_reply_id(row),
        "source_status": source_status,
        "platform": str(row.get("reply_platform") or "unknown").strip().lower() or "unknown",
        "reason": row.get("reason"),
        "notes": row.get("notes"),
        "source_context": _source_context(row),
    }

    findings: list[dict[str, Any]] = []
    if due_at < now:
        findings.append(_finding(base, "overdue"))
    elif due_at <= horizon_end:
        findings.append(_finding(base, "due_soon"))

    if not effective_target:
        findings.append(_finding(base, "missing_target"))
    if source_status in BLOCKED_SOURCE_STATUSES:
        findings.append(_finding(base, "blocked_source"))
    return findings


def _finding(base: dict[str, Any], bucket: str) -> dict[str, Any]:
    item = dict(base)
    item["bucket"] = bucket
    item["recommended_action"] = _recommended_action(item)
    return item


def _recommended_action(item: dict[str, Any]) -> str:
    bucket = item["bucket"]
    platform = item["platform"]
    if bucket == "missing_target":
        return "Add a target_handle before sending or dismissing this follow-up."
    if bucket == "blocked_source":
        return "Dismiss or reassign this follow-up because the source reply is no longer actionable."
    if bucket == "overdue":
        return f"Send or dismiss the {platform} follow-up now."
    return f"Prepare the {platform} follow-up before the due window closes."


def _source_context(row: dict[str, Any]) -> dict[str, Any] | None:
    if row.get("inbound_tweet_id") is None:
        return None
    return {
        "reply_queue_id": _source_reply_id(row),
        "author_handle": _clean_handle(row.get("inbound_author_handle")),
        "author_id": row.get("inbound_author_id"),
        "inbound_id": row.get("inbound_tweet_id"),
        "inbound_url": row.get("inbound_url"),
        "status": _clean_status(row.get("source_status")),
    }


def _source_reply_id(row: dict[str, Any]) -> int | None:
    source_reply_id = _int_or_none(row.get("source_reply_id"))
    if source_reply_id is None and row.get("source_type") == "reply_queue":
        source_reply_id = _int_or_none(row.get("source_id"))
    return source_reply_id


def _finding_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    bucket_rank = {bucket: index for index, bucket in enumerate(BUCKETS)}
    return (
        bucket_rank.get(item["bucket"], 99),
        item["due_at"],
        item["target_handle"] or "",
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


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean_handle(value: Any) -> str | None:
    if value is None:
        return None
    handle = str(value).strip().lstrip("@").lower()
    return handle or None


def _clean_status(value: Any) -> str | None:
    if value is None:
        return None
    status = str(value).strip().lower()
    return status or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
