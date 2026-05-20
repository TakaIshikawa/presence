"""Report reply review event timeliness from queue and event rows."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_REVIEW_THRESHOLD_HOURS = 24
DEFAULT_POST_THRESHOLD_HOURS = 6
ARTIFACT_TYPE = "reply_review_event_timeliness"
ISSUE_TYPES = (
    "status_mismatch",
    "missing_posted_event",
    "overdue_unreviewed_reply",
    "delayed_reviewed_to_posted_transition",
)
SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}
REVIEW_EVENT_TYPES = {"approved", "edited", "rejected", "dismissed", "failed", "posted"}
POST_OUTCOME_STATUSES = {"posted", "failed"}


def build_reply_review_event_timeliness_report(
    reply_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
    *,
    review_threshold_hours: int = DEFAULT_REVIEW_THRESHOLD_HOURS,
    post_threshold_hours: int = DEFAULT_POST_THRESHOLD_HOURS,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic timeliness report from in-memory rows."""
    if review_threshold_hours <= 0:
        raise ValueError("review_threshold_hours must be positive")
    if post_threshold_hours <= 0:
        raise ValueError("post_threshold_hours must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    replies = [_normalize_reply(row) for row in reply_rows]
    events = [_normalize_event(row) for row in event_rows]
    events_by_reply: dict[int, list[dict[str, Any]]] = {}
    for event in events:
        if event["reply_queue_id"] is not None:
            events_by_reply.setdefault(event["reply_queue_id"], []).append(event)
    for grouped in events_by_reply.values():
        grouped.sort(key=lambda item: (item["created_at_dt"] or datetime.min.replace(tzinfo=timezone.utc), item["event_id"] or 0))

    findings: list[dict[str, Any]] = []
    for reply in replies:
        grouped = events_by_reply.get(reply["reply_queue_id"] or -1, [])
        findings.extend(_reply_findings(reply, grouped, generated_at, review_threshold_hours, post_threshold_hours))

    findings.sort(key=_sort_key)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {
            "review_threshold_hours": review_threshold_hours,
            "post_threshold_hours": post_threshold_hours,
        },
        "totals": {
            "reply_count": len(replies),
            "event_count": len(events),
            "finding_count": len(findings),
            "by_issue_type": {issue_type: Counter(item["issue_type"] for item in findings)[issue_type] for issue_type in ISSUE_TYPES},
            "by_severity": dict(sorted(Counter(item["severity"] for item in findings).items())),
        },
        "findings": findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No reply review event timeliness issues found." if not findings else None,
        },
    }


def build_reply_review_event_timeliness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load reply_queue and reply_review_events rows from SQLite."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("reply_queue", "reply_review_events") if table not in schema]
    missing_columns = _missing_columns(schema)
    if "reply_queue" not in schema:
        return build_reply_review_event_timeliness_report(
            [],
            [],
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            **kwargs,
        )
    replies = _load_replies(conn, schema["reply_queue"])
    events = _load_events(conn, schema.get("reply_review_events", set())) if "reply_review_events" in schema else []
    return build_reply_review_event_timeliness_report(
        replies,
        events,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_reply_review_event_timeliness_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_review_event_timeliness_text(report: dict[str, Any]) -> str:
    """Render operator-readable text."""
    totals = report["totals"]
    filters = report["filters"]
    lines = [
        "Reply Review Event Timeliness",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"review_threshold_hours={filters['review_threshold_hours']} "
            f"post_threshold_hours={filters['post_threshold_hours']}"
        ),
        f"Totals: replies={totals['reply_count']} events={totals['event_count']} findings={totals['finding_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "severity | issue_type | reply_queue_id | status | age_hours | event_id | detail"])
    for item in report["findings"]:
        lines.append(
            f"{item['severity']} | {item['issue_type']} | {item['reply_queue_id']} | "
            f"{item['status']} | {_display(item.get('age_hours'))} | {_display(item.get('event_id'))} | "
            f"{item['detail']}"
        )
    return "\n".join(lines)


def _reply_findings(
    reply: dict[str, Any],
    events: list[dict[str, Any]],
    now: datetime,
    review_threshold_hours: int,
    post_threshold_hours: int,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    first_review = next((event for event in events if _is_review_event(event)), None)
    latest = events[-1] if events else None
    posted_event = next((event for event in events if _event_reaches(event, "posted")), None)

    if reply["status"] == "pending" and first_review is None:
        age = _elapsed_hours(reply["detected_at_dt"], now)
        if age is not None and age >= review_threshold_hours:
            findings.append(
                _finding(
                    "overdue_unreviewed_reply",
                    "warning",
                    reply,
                    age,
                    None,
                    f"pending reply has no review event after {review_threshold_hours}h",
                )
            )

    if latest and latest["new_status"] and latest["new_status"] != reply["status"]:
        findings.append(
            _finding(
                "status_mismatch",
                "critical",
                reply,
                _elapsed_hours(latest["created_at_dt"], now),
                latest,
                f"latest event new_status={latest['new_status']} differs from reply_queue.status={reply['status']}",
            )
        )

    if reply["status"] == "posted" and posted_event is None:
        findings.append(
            _finding(
                "missing_posted_event",
                "critical",
                reply,
                _elapsed_hours(reply["posted_at_dt"] or reply["reviewed_at_dt"] or reply["detected_at_dt"], now),
                latest,
                "posted reply has no matching posted review event",
            )
        )

    review_start = next((event for event in events if _is_post_start_event(event)), None)
    if review_start:
        outcome = next(
            (event for event in events if event["created_at_dt"] and review_start["created_at_dt"] and event["created_at_dt"] >= review_start["created_at_dt"] and _is_post_outcome_event(event)),
            None,
        )
        age = _elapsed_hours(review_start["created_at_dt"], outcome["created_at_dt"] if outcome else now)
        if age is not None and age >= post_threshold_hours:
            findings.append(
                _finding(
                    "delayed_reviewed_to_posted_transition",
                    "warning",
                    reply,
                    age,
                    outcome or review_start,
                    f"reviewed reply did not reach posted/failed within {post_threshold_hours}h",
                )
            )
    return findings


def _load_replies(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    wanted = ["id", "status", "detected_at", "reviewed_at", "posted_at", "inbound_author_handle", "inbound_tweet_id"]
    select = [_expr(columns, column, column, "NULL") for column in wanted]
    order = "id" if "id" in columns else "rowid"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM reply_queue ORDER BY {order} ASC")]


def _load_events(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    wanted = ["id", "reply_queue_id", "event_type", "old_status", "new_status", "created_at"]
    select = [_expr(columns, column, column, "NULL") for column in wanted]
    order = "id" if "id" in columns else "rowid"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM reply_review_events ORDER BY {order} ASC")]


def _normalize_reply(row: dict[str, Any]) -> dict[str, Any]:
    detected = _parse_timestamp(row.get("detected_at"))
    reviewed = _parse_timestamp(row.get("reviewed_at"))
    posted = _parse_timestamp(row.get("posted_at"))
    return {
        "reply_queue_id": _int_or_none(row.get("reply_queue_id") or row.get("id")),
        "status": _clean_status(row.get("status")) or "pending",
        "detected_at": _iso(detected),
        "detected_at_dt": detected,
        "reviewed_at": _iso(reviewed),
        "reviewed_at_dt": reviewed,
        "posted_at": _iso(posted),
        "posted_at_dt": posted,
        "inbound_author_handle": _clean(row.get("inbound_author_handle") or row.get("author_handle")) or None,
        "inbound_tweet_id": _clean(row.get("inbound_tweet_id")) or None,
    }


def _normalize_event(row: dict[str, Any]) -> dict[str, Any]:
    created = _parse_timestamp(row.get("created_at"))
    return {
        "event_id": _int_or_none(row.get("event_id") or row.get("id")),
        "reply_queue_id": _int_or_none(row.get("reply_queue_id") or row.get("reply_id")),
        "event_type": _clean_status(row.get("event_type")),
        "old_status": _clean_status(row.get("old_status")),
        "new_status": _clean_status(row.get("new_status")),
        "created_at": _iso(created),
        "created_at_dt": created,
    }


def _finding(
    issue_type: str,
    severity: str,
    reply: dict[str, Any],
    age_hours: float | None,
    event: dict[str, Any] | None,
    detail: str,
) -> dict[str, Any]:
    return {
        "issue_type": issue_type,
        "severity": severity,
        "reply_queue_id": reply["reply_queue_id"],
        "status": reply["status"],
        "age_hours": _round_or_none(age_hours),
        "detected_at": reply["detected_at"],
        "reviewed_at": reply["reviewed_at"],
        "posted_at": reply["posted_at"],
        "event_id": event.get("event_id") if event else None,
        "event_type": event.get("event_type") if event else None,
        "old_status": event.get("old_status") if event else None,
        "new_status": event.get("new_status") if event else None,
        "event_created_at": event.get("created_at") if event else None,
        "detail": detail,
    }


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    expected = {
        "reply_queue": {"id", "status", "detected_at"},
        "reply_review_events": {"reply_queue_id", "event_type", "new_status", "created_at"},
    }
    return {
        table: sorted(required - schema.get(table, set()))
        for table, required in expected.items()
        if table in schema and required - schema.get(table, set())
    }


def _is_review_event(event: dict[str, Any]) -> bool:
    return bool(event["created_at_dt"] and (event["event_type"] in REVIEW_EVENT_TYPES or event["new_status"] in {"approved", "dismissed", "posted", "failed"}))


def _is_post_start_event(event: dict[str, Any]) -> bool:
    return bool(event["created_at_dt"] and (event["event_type"] in {"approved", "edited"} or event["new_status"] in {"approved"}))


def _is_post_outcome_event(event: dict[str, Any]) -> bool:
    return bool(event["created_at_dt"] and (event["event_type"] in POST_OUTCOME_STATUSES or event["new_status"] in POST_OUTCOME_STATUSES))


def _event_reaches(event: dict[str, Any], status: str) -> bool:
    return event["event_type"] == status or event["new_status"] == status


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        SEVERITY_ORDER.get(item["severity"], 99),
        -(item["age_hours"] or 0.0),
        ISSUE_TYPES.index(item["issue_type"]),
        item["reply_queue_id"] or 0,
        item["event_id"] or 0,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{_quote_identifier(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    text = _clean(value)
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        try:
            return _utc(datetime.strptime(text, "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            return None


def _elapsed_hours(start: datetime | None, end: datetime | None) -> float | None:
    if not start or not end:
        return None
    return max((_utc(end) - _utc(start)).total_seconds() / 3600, 0.0)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _clean_status(value: Any) -> str | None:
    text = _clean(value).lower()
    return text or None


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _round_or_none(value: float | None) -> float | None:
    return round(value, 2) if value is not None else None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
