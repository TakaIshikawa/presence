"""Report reply review lifecycle sequence gaps."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_STALE_TOLERANCE_HOURS = 1
DEFAULT_LIMIT = 100


def build_reply_review_sequence_gaps_report(
    reply_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
    *,
    status: str | Iterable[str] = "all",
    stale_tolerance_hours: int = DEFAULT_STALE_TOLERANCE_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if stale_tolerance_hours < 0:
        raise ValueError("stale_tolerance_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    statuses = _normalize_filter(status)
    events_by_reply: dict[int, list[dict[str, Any]]] = {}
    for event in event_rows:
        reply_id = _int_or_none(event.get("reply_queue_id") or event.get("reply_id"))
        if reply_id is not None:
            events_by_reply.setdefault(reply_id, []).append(event)

    findings: list[dict[str, Any]] = []
    for row in reply_rows:
        row_status = _clean(row.get("status"), "unknown").lower()
        if statuses and "all" not in statuses and row_status not in statuses:
            continue
        reply_id = _int_or_none(row.get("reply_queue_id") or row.get("id"))
        events = sorted(events_by_reply.get(reply_id or -1, []), key=lambda event: _parse_dt(event.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc))
        latest = events[-1] if events else {}
        latest_at = _parse_dt(latest.get("created_at"))
        latest_type = _clean(latest.get("event_type"), None)
        gap_type = _gap_type(row_status, row, events, latest_at, stale_tolerance_hours)
        if not gap_type:
            continue
        findings.append(
            {
                "reply_queue_id": reply_id,
                "gap_type": gap_type,
                "status": row_status,
                "latest_event_type": latest_type,
                "latest_event_at": _iso(latest_at),
                "reviewed_at": _iso(_parse_dt(row.get("reviewed_at"))),
                "posted_at": _iso(_parse_dt(row.get("posted_at"))),
                "inbound_author_handle": _clean(row.get("inbound_author_handle") or row.get("author_handle")) or None,
            }
        )
    findings.sort(key=lambda item: (item["reply_queue_id"] or 0, item["gap_type"]))
    shown = findings[:limit]
    return {
        "artifact_type": "reply_review_sequence_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"status": list(statuses), "stale_tolerance_hours": stale_tolerance_hours, "limit": limit},
        "summary": {"reply_count": len(reply_rows), "event_count": len(event_rows), "gap_count": len(findings), "shown_count": len(shown)},
        "gaps": shown,
        "missing_tables": sorted(missing_tables or []),
    }


def build_reply_review_sequence_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing = [table for table in ("reply_queue", "reply_review_events") if table not in schema]
    replies = _load_replies(conn, schema["reply_queue"]) if "reply_queue" in schema else []
    events = _load_events(conn, schema["reply_review_events"]) if "reply_review_events" in schema else []
    return build_reply_review_sequence_gaps_report(replies, events, missing_tables=missing, **kwargs)


def format_reply_review_sequence_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_review_sequence_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Reply Review Sequence Gaps",
        f"Generated: {report['generated_at']}",
        f"Stale tolerance hours: {report['filters']['stale_tolerance_hours']}",
        f"Totals: replies={summary['reply_count']} events={summary['event_count']} gaps={summary['gap_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["gaps"]:
        lines.append("No reply review sequence gaps found.")
        return "\n".join(lines)
    lines.extend(["", "reply_queue_id | gap_type | status | latest_event_type | latest_event_at | reviewed_at | posted_at | inbound_author_handle"])
    for item in report["gaps"]:
        lines.append(
            f"{item['reply_queue_id'] or '-'} | {item['gap_type']} | {item['status']} | {item['latest_event_type'] or '-'} | "
            f"{item['latest_event_at'] or '-'} | {item['reviewed_at'] or '-'} | {item['posted_at'] or '-'} | {item['inbound_author_handle'] or '-'}"
        )
    return "\n".join(lines)


def _gap_type(
    status: str,
    row: dict[str, Any],
    events: list[dict[str, Any]],
    latest_at: datetime | None,
    tolerance_hours: int,
) -> str | None:
    if status in {"approved", "posted"} and not events:
        return "missing_review_event"
    event_types = {_clean(event.get("event_type")).lower() for event in events}
    if status in {"dismissed", "rejected"} and not (event_types & {"dismissed", "dismissal", "rejected"}):
        return "missing_dismissal_event"
    if status == "posted" and latest_at:
        tolerance = timedelta(hours=tolerance_hours)
        for key in ("reviewed_at", "posted_at"):
            marker = _parse_dt(row.get(key))
            if marker and latest_at + tolerance < marker:
                return "stale_latest_event"
    return None


def _load_replies(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    wanted = ["id", "status", "reviewed_at", "posted_at", "inbound_author_handle", "author_handle"]
    select = [_expr(columns, col, "reply_queue_id" if col == "id" else col, "NULL") for col in wanted]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM reply_queue ORDER BY rowid ASC")]


def _load_events(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    wanted = ["id", "reply_queue_id", "reply_id", "event_type", "created_at"]
    select = [_expr(columns, col, col, "NULL") for col in wanted]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM reply_review_events ORDER BY rowid ASC")]


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _normalize_filter(value: str | Iterable[str]) -> tuple[str, ...]:
    values = value.split(",") if isinstance(value, str) else list(value)
    normalized = tuple(item for item in (_clean(part).lower() for part in values) if item)
    return normalized or ("all",)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            return _utc(datetime.fromisoformat(candidate))
        except ValueError:
            continue
    return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
