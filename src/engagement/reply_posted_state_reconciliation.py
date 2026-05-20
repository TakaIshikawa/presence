"""Reconcile reply posted state across queue rows and review events."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "reply_posted_state_reconciliation"
POSTED_FIELDS = ("posted_tweet_id", "posted_platform_id", "posted_at")
POSTED_IDENTIFIER_FIELDS = ("posted_tweet_id", "posted_platform_id")
ISSUE_TYPES = (
    "posted_missing_posted_tweet_id",
    "posted_missing_posted_platform_id",
    "posted_missing_posted_at",
    "non_posted_with_posted_identifiers",
    "review_event_status_disagreement",
)


def build_reply_posted_state_reconciliation_report(
    reply_rows: list[dict[str, Any]],
    review_event_rows: list[dict[str, Any]] | None = None,
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic posted-state reconciliation report from rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    missing_columns = missing_columns or {}
    unavailable_reply_columns = set(missing_columns.get("reply_queue", []))
    replies = [_normalize_reply(row) for row in reply_rows]
    latest_events = _latest_review_events(review_event_rows or [])

    findings: list[dict[str, Any]] = []
    for row in replies:
        if row["status"] == "posted":
            for field in POSTED_FIELDS:
                if field not in unavailable_reply_columns and not _clean(row.get(field)):
                    findings.append(_finding(row, f"posted_missing_{field}", missing_field=field))
        elif any(_clean(row.get(field)) for field in POSTED_IDENTIFIER_FIELDS if field not in unavailable_reply_columns):
            findings.append(
                _finding(
                    row,
                    "non_posted_with_posted_identifiers",
                    present_fields=[field for field in POSTED_IDENTIFIER_FIELDS if _clean(row.get(field))],
                )
            )

        latest = latest_events.get(row["reply_queue_id"])
        if latest and latest["event_status"] in {"posted", "failed"} and row["status"] != latest["event_status"]:
            findings.append(
                _finding(
                    row,
                    "review_event_status_disagreement",
                    event_id=latest["event_id"],
                    event_status=latest["event_status"],
                    event_created_at=latest["created_at"],
                )
            )

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "summary": {
            "reply_count": len(reply_rows),
            "latest_review_event_count": len(latest_events),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: Counter(item["issue_type"] for item in findings)[issue_type] for issue_type in ISSUE_TYPES},
        },
        "findings": _group_findings(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted(missing_columns.items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No reply posted state reconciliation issues found." if not findings else None,
        },
    }


def build_reply_posted_state_reconciliation_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load reply queue and latest review event state from SQLite."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}

    if "reply_queue" not in schema:
        return build_reply_posted_state_reconciliation_report([], [], missing_tables=["reply_queue"], **kwargs)

    reply_columns = schema["reply_queue"]
    required_reply = {"id", "status"}
    reply_missing = sorted(required_reply - reply_columns)
    optional_reply_missing = [field for field in POSTED_FIELDS if field not in reply_columns]
    if reply_missing or optional_reply_missing:
        missing_columns["reply_queue"] = reply_missing + optional_reply_missing
    reply_rows = [] if reply_missing else _load_reply_rows(conn, reply_columns)

    event_rows: list[dict[str, Any]] = []
    if "reply_review_events" not in schema:
        missing_tables.append("reply_review_events")
    else:
        event_columns = schema["reply_review_events"]
        event_missing = sorted({"reply_queue_id", "created_at"} - event_columns)
        if "new_status" not in event_columns and "event_type" not in event_columns:
            event_missing.append("new_status")
        if event_missing:
            missing_columns["reply_review_events"] = event_missing
        else:
            event_rows = _load_event_rows(conn, event_columns)

    return build_reply_posted_state_reconciliation_report(
        reply_rows,
        event_rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_reply_posted_state_reconciliation_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_posted_state_reconciliation_text(report: dict[str, Any]) -> str:
    """Render the report as terminal-friendly text."""
    summary = report["summary"]
    lines = [
        "Reply Posted State Reconciliation",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['filters']['limit']}",
        (
            "Totals: "
            f"replies={summary['reply_count']} "
            f"latest_review_events={summary['latest_review_event_count']} "
            f"findings={summary['finding_count']} "
            f"shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "reply_queue_id | status | issue_type | details"])
    for group in report["findings"]:
        for item in group["items"]:
            lines.append(
                f"{item['reply_queue_id']} | {item['status']} | {item['issue_type']} | "
                f"{_details(item)}"
            )
    return "\n".join(lines)


def _load_reply_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        '"id" AS reply_queue_id',
        '"status" AS status',
        _expr(columns, "posted_tweet_id", "posted_tweet_id", "NULL"),
        _expr(columns, "posted_platform_id", "posted_platform_id", "NULL"),
        _expr(columns, "posted_at", "posted_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM reply_queue ORDER BY id ASC")]


def _load_event_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "event_id", "NULL"),
        '"reply_queue_id" AS reply_queue_id',
        _expr(columns, "new_status", "new_status", "NULL"),
        _expr(columns, "event_type", "event_type", "NULL"),
        '"created_at" AS created_at',
    ]
    order = "id" if "id" in columns else "rowid"
    return [
        dict(row)
        for row in conn.execute(
            f"SELECT {', '.join(select)} FROM reply_review_events ORDER BY reply_queue_id ASC, datetime(created_at) ASC, {order} ASC"
        )
    ]


def _normalize_reply(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "reply_queue_id": _int_or_none(row.get("reply_queue_id") or row.get("id")),
        "status": _clean(row.get("status")).lower() or "unknown",
        "posted_tweet_id": _clean(row.get("posted_tweet_id")),
        "posted_platform_id": _clean(row.get("posted_platform_id")),
        "posted_at": _clean(row.get("posted_at")),
    }


def _latest_review_events(rows: list[dict[str, Any]]) -> dict[int | None, dict[str, Any]]:
    latest: dict[int | None, dict[str, Any]] = {}
    for raw in rows:
        reply_id = _int_or_none(raw.get("reply_queue_id"))
        event = {
            "event_id": _int_or_none(raw.get("event_id") or raw.get("id")),
            "reply_queue_id": reply_id,
            "event_status": _event_status(raw),
            "created_at": _clean(raw.get("created_at")) or None,
            "created_at_dt": _parse_ts(raw.get("created_at")),
        }
        current = latest.get(reply_id)
        if current is None or _event_sort_key(event) >= _event_sort_key(current):
            latest[reply_id] = event
    return latest


def _event_status(row: dict[str, Any]) -> str:
    value = _clean(row.get("new_status")).lower() or _clean(row.get("event_type")).lower()
    return value.removeprefix("status_")


def _finding(row: dict[str, Any], issue_type: str, **extra: Any) -> dict[str, Any]:
    return {
        "reply_queue_id": row["reply_queue_id"],
        "status": row["status"],
        "posted_tweet_id": row["posted_tweet_id"] or None,
        "posted_platform_id": row["posted_platform_id"] or None,
        "posted_at": row["posted_at"] or None,
        "issue_type": issue_type,
        **extra,
    }


def _group_findings(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[item["issue_type"]].append(item)
    return [
        {"issue_type": issue_type, "count": len(grouped[issue_type]), "items": grouped[issue_type]}
        for issue_type in ISSUE_TYPES
        if issue_type in grouped
    ]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (ISSUE_TYPES.index(item["issue_type"]), item["reply_queue_id"] or 0, item.get("missing_field") or "")


def _event_sort_key(event: dict[str, Any]) -> tuple[Any, ...]:
    return (event["created_at_dt"] or datetime.min.replace(tzinfo=timezone.utc), event["event_id"] or 0)


def _parse_ts(value: Any) -> datetime | None:
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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote(str(row[0]))})")} for row in rows}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{_quote(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _details(item: dict[str, Any]) -> str:
    if item["issue_type"].startswith("posted_missing_"):
        return f"missing={item['missing_field']}"
    if item["issue_type"] == "non_posted_with_posted_identifiers":
        return "present=" + ",".join(item["present_fields"])
    return f"event_status={item.get('event_status')} event_id={item.get('event_id')}"


def _quote(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
