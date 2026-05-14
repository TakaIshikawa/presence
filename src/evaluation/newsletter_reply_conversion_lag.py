"""Measure lag from newsletter sends to reply-driven conversations."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_AFTER_DAYS = 14
DEFAULT_LIMIT = 50


def build_newsletter_reply_conversion_lag_report(
    newsletter_send_rows: list[dict[str, Any]],
    reply_rows: list[dict[str, Any]],
    *,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return issue-level reply conversion lag records from in-memory rows."""
    if stale_after_days <= 0:
        raise ValueError("stale_after_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    replies = [_normalize_reply(row) for row in reply_rows]
    records = []
    for send in newsletter_send_rows:
        sent_at = _parse_dt(_first(send, "sent_at", "created_at"))
        if sent_at is None:
            continue
        send_id = _optional_int(_first(send, "id", "newsletter_send_id"))
        issue_id = _text(_first(send, "issue_id", "newsletter_issue_id"))
        source_content_ids = set(_parse_int_list(_first(send, "source_content_ids", "content_ids")))
        matches = [
            reply
            for reply in replies
            if reply["detected_at"] is not None
            and reply["detected_at"] >= sent_at
            and _reply_matches_send(reply, send_id, issue_id, source_content_ids)
        ]
        first_reply = min(matches, key=lambda item: (item["detected_at"], item["id"] or 0)) if matches else None
        age_days = max((generated_at - sent_at).total_seconds() / 86400, 0)
        if first_reply:
            lag_days = max((first_reply["detected_at"] - sent_at).total_seconds() / 86400, 0)
            bucket = _lag_bucket(lag_days, stale_after_days)
            status = "converted"
        else:
            lag_days = None
            bucket = "no_conversion"
            status = "no_conversion"
        records.append(
            {
                "newsletter_send_id": send_id,
                "issue_id": issue_id,
                "subject": _text(send.get("subject")),
                "sent_at": sent_at.isoformat(),
                "age_days": round(age_days, 2),
                "conversion_status": status,
                "lag_bucket": bucket,
                "lag_days": round(lag_days, 2) if lag_days is not None else None,
                "reply_count": len(matches),
                "first_reply_id": first_reply["id"] if first_reply else None,
                "first_reply_detected_at": first_reply["detected_at"].isoformat() if first_reply else None,
                "first_reply_author": first_reply["author"] if first_reply else None,
            }
        )

    records.sort(key=_risk_sort_key)
    records = records[:limit]
    totals = {
        "issue_count": len(newsletter_send_rows),
        "record_count": len(records),
        "converted_count": sum(1 for item in records if item["conversion_status"] == "converted"),
        "no_conversion_count": sum(1 for item in records if item["conversion_status"] == "no_conversion"),
        "same_day_count": sum(1 for item in records if item["lag_bucket"] == "same_day"),
        "week_1_count": sum(1 for item in records if item["lag_bucket"] == "week_1"),
        "week_2_count": sum(1 for item in records if item["lag_bucket"] == "week_2"),
        "stale_count": sum(1 for item in records if item["lag_bucket"] == "stale"),
    }
    totals["conversion_rate"] = round(totals["converted_count"] / totals["record_count"], 4) if records else 0.0
    return {
        "artifact_type": "newsletter_reply_conversion_lag",
        "generated_at": generated_at.isoformat(),
        "filters": {"stale_after_days": stale_after_days, "limit": limit},
        "totals": totals,
        "issues": records,
        "empty_state": {
            "is_empty": not records,
            "message": "No newsletter sends found." if not records else None,
        },
    }


def build_newsletter_reply_conversion_lag_report_from_db(
    db_or_conn: Any,
    *,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load local SQLite rows and build the reply conversion lag report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_newsletter_reply_conversion_lag_report(
        _load_newsletter_sends(conn, schema),
        _load_reply_rows(conn, schema),
        stale_after_days=stale_after_days,
        limit=limit,
        now=now,
    )


def format_newsletter_reply_conversion_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_reply_conversion_lag_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Reply Conversion Lag",
        f"Generated: {report['generated_at']}",
        f"Filters: stale_after_days={report['filters']['stale_after_days']} limit={report['filters']['limit']}",
        (
            "Totals: "
            f"issues={report['totals']['record_count']} "
            f"converted={report['totals']['converted_count']} "
            f"no_conversion={report['totals']['no_conversion_count']} "
            f"stale={report['totals']['stale_count']} "
            f"rate={report['totals']['conversion_rate']:.2%}"
        ),
    ]
    if not report["issues"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Issues:", "send  issue        bucket         age_d  lag_d  replies  subject"])
    for item in report["issues"]:
        lag = "-" if item["lag_days"] is None else f"{item['lag_days']:.2f}"
        lines.append(
            f"{item['newsletter_send_id'] or '-':<5} "
            f"{(item['issue_id'] or '-')[:12]:<12} "
            f"{item['lag_bucket']:<13} "
            f"{item['age_days']:<6.2f} "
            f"{lag:<6} "
            f"{item['reply_count']:<7} "
            f"{item['subject']}"
        )
    return "\n".join(lines)


def _load_newsletter_sends(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "newsletter_sends" not in schema or not {"id", "sent_at"}.issubset(schema["newsletter_sends"]):
        return []
    columns = schema["newsletter_sends"]
    status_filter = "LOWER(COALESCE(status, 'sent')) = 'sent'" if "status" in columns else "1 = 1"
    selected = [
        "id",
        "issue_id" if "issue_id" in columns else "NULL AS issue_id",
        "subject" if "subject" in columns else "'' AS subject",
        "source_content_ids" if "source_content_ids" in columns else "NULL AS source_content_ids",
        "sent_at",
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
           FROM newsletter_sends
           WHERE {status_filter}
           ORDER BY sent_at ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_reply_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "reply_queue" not in schema or not {"id", "detected_at"}.issubset(schema["reply_queue"]):
        return []
    columns = schema["reply_queue"]
    selected = [
        "id",
        "detected_at",
        "our_content_id" if "our_content_id" in columns else "NULL AS our_content_id",
        "inbound_author_handle" if "inbound_author_handle" in columns else "NULL AS inbound_author_handle",
        "platform_metadata" if "platform_metadata" in columns else "NULL AS platform_metadata",
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
           FROM reply_queue
           ORDER BY detected_at ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _normalize_reply(row: dict[str, Any]) -> dict[str, Any]:
    metadata = _json_obj(row.get("platform_metadata") or row.get("metadata"))
    return {
        "id": _optional_int(row.get("id") or row.get("reply_id")),
        "detected_at": _parse_dt(_first(row, "detected_at", "created_at", "inbound_at")),
        "our_content_id": _optional_int(_first(row, "our_content_id", "content_id")),
        "newsletter_send_id": _optional_int(
            _first(row, "newsletter_send_id", "send_id") or metadata.get("newsletter_send_id")
        ),
        "issue_id": _text(
            _first(row, "issue_id", "newsletter_issue_id") or metadata.get("issue_id") or metadata.get("newsletter_issue_id")
        ),
        "author": _text(_first(row, "inbound_author_handle", "author", "handle")),
    }


def _reply_matches_send(
    reply: dict[str, Any],
    send_id: int | None,
    issue_id: str,
    source_content_ids: set[int],
) -> bool:
    if send_id is not None and reply["newsletter_send_id"] == send_id:
        return True
    if issue_id and reply["issue_id"] == issue_id:
        return True
    return reply["our_content_id"] is not None and reply["our_content_id"] in source_content_ids


def _lag_bucket(lag_days: float, stale_after_days: int) -> str:
    if lag_days <= 1:
        return "same_day"
    if lag_days <= 7:
        return "week_1"
    if lag_days <= stale_after_days:
        return "week_2"
    return "stale"


def _risk_sort_key(item: dict[str, Any]) -> tuple[int, float, float, int]:
    bucket_risk = {"no_conversion": 4, "stale": 3, "week_2": 2, "week_1": 1, "same_day": 0}
    return (
        -bucket_risk[item["lag_bucket"]],
        -(item["age_days"] or 0),
        -(item["lag_days"] or 0),
        item["newsletter_send_id"] or 0,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row["name"] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _text(value: Any) -> str:
    return str(value).strip() if value not in (None, "") else ""


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_int_list(value: Any) -> list[int]:
    if not value:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = [part.strip() for part in value.split(",")]
    if not isinstance(value, list):
        return []
    return [parsed for item in value if (parsed := _optional_int(item)) is not None]


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}
