"""Build a review digest for reply follow-up reminders."""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS_AHEAD = 7
DUE_BUCKETS = ("overdue", "due_today", "upcoming", "completed")


def build_reply_followup_digest_report(
    db_or_conn: Any,
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    include_completed: bool = False,
    platform: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return pending and optionally completed follow-up reminders grouped by due bucket."""

    if days_ahead < 0:
        raise ValueError("days_ahead must be non-negative")
    if platform is not None and not platform.strip():
        raise ValueError("platform must not be blank")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _reminder_rows(conn, schema, include_completed=include_completed)
    findings = [
        finding
        for row in rows
        if (
            finding := _build_finding(
                row,
                now=generated_at,
                days_ahead=days_ahead,
                reply_columns=schema.get("reply_queue", set()),
            )
        )
        is not None
    ]

    if platform:
        normalized_platform = platform.strip().lower()
        findings = [
            finding
            for finding in findings
            if str(finding["platform"] or "").strip().lower() == normalized_platform
        ]

    findings.sort(key=_finding_sort_key)
    buckets = {bucket: [] for bucket in DUE_BUCKETS}
    for finding in findings:
        buckets[finding["due_bucket"]].append(finding)

    return {
        "artifact_type": "reply_followup_digest",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days_ahead": days_ahead,
            "include_completed": include_completed,
            "platform": platform,
        },
        "counts": {
            **{bucket: len(items) for bucket, items in buckets.items()},
            "total": len(findings),
        },
        "buckets": buckets,
        "findings": findings,
        "missing_tables": [
            table for table in ("reply_followup_reminders",) if table not in schema
        ],
    }


def format_reply_followup_digest_json(report: dict[str, Any]) -> str:
    """Render a reply follow-up digest as deterministic JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_followup_digest_text(report: dict[str, Any]) -> str:
    """Render a concise terminal digest for reply follow-up reminders."""

    filters = report["filters"]
    counts = report["counts"]
    lines = [
        "Reply Follow-up Digest",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days_ahead={filters['days_ahead']} "
            f"platform={filters['platform'] or 'all'} "
            f"include_completed={filters['include_completed']}"
        ),
        (
            f"Reminders: overdue={counts['overdue']} due_today={counts['due_today']} "
            f"upcoming={counts['upcoming']} completed={counts['completed']} "
            f"total={counts['total']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No follow-up reminders matched.")
        return "\n".join(lines)

    for bucket in DUE_BUCKETS:
        items = report["buckets"][bucket]
        if not items:
            continue
        lines.append("")
        lines.append(bucket.replace("_", " ").title() + ":")
        for item in items:
            reply_ref = (
                f"reply={item['original_reply_id']}"
                if item["original_reply_id"] is not None
                else f"source={item['source_type']}:{item['source_id']}"
            )
            age = "-" if item["age_hours"] is None else f"{item['age_hours']:.1f}h"
            lines.append(
                f"- #{item['id']} @{item['target_author']} {item['platform']} "
                f"{reply_ref} due={item['due_at']} age={age} "
                f"action={item['suggested_action']}"
            )
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


def _reminder_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    include_completed: bool,
) -> list[dict[str, Any]]:
    reminder_columns = schema.get("reply_followup_reminders")
    if not reminder_columns:
        return []

    select_columns = [
        _column_expr("r", reminder_columns, "id"),
        _column_expr("r", reminder_columns, "target_handle"),
        _column_expr("r", reminder_columns, "source_type"),
        _column_expr("r", reminder_columns, "source_id"),
        _column_expr("r", reminder_columns, "source_reply_id"),
        _column_expr("r", reminder_columns, "due_at"),
        _column_expr("r", reminder_columns, "status", "'pending'"),
        _column_expr("r", reminder_columns, "reason"),
        _column_expr("r", reminder_columns, "notes"),
        _column_expr("r", reminder_columns, "created_at"),
        _column_expr("r", reminder_columns, "completed_at"),
    ]

    reply_columns = schema.get("reply_queue", set())
    join = ""
    if reply_columns:
        join = "LEFT JOIN reply_queue q ON q.id = r.source_reply_id"
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

    statuses = ["pending"]
    if include_completed:
        statuses.append("done")
    placeholders = ", ".join("?" for _ in statuses)
    query = (
        f"SELECT {', '.join(select_columns)} "
        f"FROM reply_followup_reminders r {join} "
        f"WHERE r.status IN ({placeholders}) "
        "ORDER BY datetime(r.due_at) ASC, r.id ASC"
    )
    return [dict(row) for row in conn.execute(query, statuses).fetchall()]


def _build_finding(
    row: dict[str, Any],
    *,
    now: datetime,
    days_ahead: int,
    reply_columns: set[str],
) -> dict[str, Any] | None:
    due_at = _parse_timestamp(row.get("due_at"))
    if due_at is None:
        return None
    status = str(row.get("status") or "pending").strip().lower()
    bucket = _due_bucket(status, due_at, now, days_ahead)
    if bucket is None:
        return None

    created_at = _parse_timestamp(row.get("created_at"))
    age_hours = None if created_at is None else round((now - created_at).total_seconds() / 3600, 2)
    due_in_hours = round((due_at - now).total_seconds() / 3600, 2)
    original_reply_id = _int_or_none(row.get("source_reply_id"))
    if original_reply_id is None and row.get("source_type") == "reply_queue":
        original_reply_id = _int_or_none(row.get("source_id"))

    target_author = _clean_handle(row.get("inbound_author_handle") or row.get("target_handle"))
    platform = str(row.get("reply_platform") or "unknown").strip().lower() or "unknown"
    original_reply = None
    if reply_columns and row.get("inbound_tweet_id") is not None:
        original_reply = {
            "reply_queue_id": original_reply_id,
            "inbound_id": row.get("inbound_tweet_id"),
            "inbound_url": row.get("inbound_url"),
            "author_handle": _clean_handle(row.get("inbound_author_handle")),
            "author_id": row.get("inbound_author_id"),
            "inbound_text": row.get("inbound_text"),
            "draft_text": row.get("draft_text"),
            "status": row.get("reply_status"),
        }

    return {
        "id": _int_or_none(row.get("id")),
        "status": status,
        "due_bucket": bucket,
        "target_author": target_author,
        "target_handle": target_author,
        "platform": platform,
        "original_reply_id": original_reply_id,
        "source_type": row.get("source_type"),
        "source_id": _int_or_none(row.get("source_id")),
        "due_at": due_at.isoformat(),
        "age_hours": age_hours,
        "age": None if age_hours is None else f"{age_hours:.1f}h",
        "due_in_hours": due_in_hours,
        "reason": row.get("reason"),
        "notes": row.get("notes"),
        "original_reply": original_reply,
        "suggested_action": _suggested_action(bucket, platform, original_reply is not None),
    }


def _due_bucket(
    status: str,
    due_at: datetime,
    now: datetime,
    days_ahead: int,
) -> str | None:
    if status == "done":
        return "completed"
    if status != "pending":
        return None

    today_start = datetime.combine(now.date(), time.min, tzinfo=now.tzinfo)
    tomorrow_start = today_start + timedelta(days=1)
    if due_at < today_start:
        return "overdue"
    if due_at < tomorrow_start:
        return "due_today"
    if due_at <= now + timedelta(days=days_ahead):
        return "upcoming"
    return None


def _suggested_action(bucket: str, platform: str, has_reply_context: bool) -> str:
    if bucket == "completed":
        return "Review completed follow-up notes."
    context_note = "" if has_reply_context else " after checking the source context"
    if bucket == "overdue":
        return f"Send or dismiss the {platform} follow-up now{context_note}."
    if bucket == "due_today":
        return f"Review and send the {platform} follow-up today{context_note}."
    return f"Prepare the {platform} follow-up before it becomes due{context_note}."


def _finding_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    bucket_rank = {bucket: index for index, bucket in enumerate(DUE_BUCKETS)}
    return (
        bucket_rank.get(item["due_bucket"], 99),
        item["due_at"],
        item["platform"],
        item["target_author"].lower(),
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
