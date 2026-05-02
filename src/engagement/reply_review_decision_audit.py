"""Audit reply review decision trails for inconsistent durable events."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 50

VALID_TRANSITIONS = {
    ("pending", "pending"),
    ("pending", "approved"),
    ("pending", "dismissed"),
    ("approved", "approved"),
    ("approved", "dismissed"),
    ("approved", "posted"),
    ("posted", "posted"),
    ("dismissed", "dismissed"),
}


def build_reply_review_decision_audit(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only audit for reply_queue/reply_review_events consistency."""

    if days < 1:
        raise ValueError("days must be at least 1")
    if limit < 1:
        raise ValueError("limit must be at least 1")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
    }
    missing_tables = tuple(
        table for table in ("reply_queue", "reply_review_events") if table not in schema
    )
    missing_columns = _missing_columns(schema)
    if "reply_queue" not in schema:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = _load_reply_rows(
        conn,
        schema["reply_queue"],
        cutoff=cutoff,
        now=generated_at,
    )
    events = _load_events(
        conn,
        schema.get("reply_review_events", set()),
        [row["reply_queue_id"] for row in rows],
    )
    findings: list[dict[str, Any]] = []
    for row in rows:
        findings.extend(_audit_reply(row, events.get(row["reply_queue_id"], [])))
    findings.sort(key=_finding_sort_key)
    issue_totals = dict(sorted(Counter(item["issue_code"] for item in findings).items()))

    return {
        "artifact_type": "reply_review_decision_audit",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "reply_count": len(rows),
            "issue_count": len(findings),
            "finding_count": min(len(findings), limit),
            "issue_totals": issue_totals,
        },
        "findings": findings[:limit],
        "missing_tables": list(missing_tables),
        "missing_columns": {
            table: list(columns) for table, columns in sorted(missing_columns.items())
        },
        "has_issues": bool(findings),
    }


def format_reply_review_decision_audit_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_review_decision_audit_text(report: dict[str, Any]) -> str:
    """Render a concise operator-readable audit."""

    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Reply Review Decision Audit",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"lookback_start={filters['lookback_start']} "
            f"lookback_end={filters['lookback_end']}"
        ),
        (
            "Totals: "
            f"replies={totals['reply_count']} "
            f"issues={totals['issue_count']} "
            f"findings={totals['finding_count']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    missing_columns = [
        f"{table}({', '.join(columns)})"
        for table, columns in report.get("missing_columns", {}).items()
        if columns
    ]
    if missing_columns:
        lines.append("Missing optional columns: " + "; ".join(missing_columns))

    lines.extend(["", "Issue totals:"])
    if totals["issue_totals"]:
        for code, count in totals["issue_totals"].items():
            lines.append(f"- {code}: {count}")
    else:
        lines.append("- none")

    if report["findings"]:
        lines.extend(["", "Representative findings:"])
        for item in report["findings"]:
            latest = item["latest_event"]
            latest_summary = "-"
            if latest:
                latest_summary = (
                    f"{latest.get('event_type') or '-'} "
                    f"{latest.get('old_status') or '-'}->{latest.get('new_status') or '-'} "
                    f"at={latest.get('created_at') or '-'}"
                )
            lines.append(
                f"- reply_queue:{item['reply_queue_id']} @{item['author_handle']} "
                f"inbound={item['inbound_tweet_id'] or '-'} "
                f"status={item['current_status']} issue={item['issue_code']} "
                f"latest={latest_summary} action={item['suggested_action']}"
            )
    elif not report.get("missing_tables"):
        lines.extend(["", "No reply review decision issues matched."])

    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> dict[str, Any]:
    return {
        "artifact_type": "reply_review_decision_audit",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "reply_count": 0,
            "issue_count": 0,
            "finding_count": 0,
            "issue_totals": {},
        },
        "findings": [],
        "missing_tables": list(missing_tables),
        "missing_columns": {
            table: list(columns) for table, columns in sorted(missing_columns.items())
        },
        "has_issues": False,
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "reply_queue": (
            "id",
            "inbound_tweet_id",
            "inbound_author_handle",
            "status",
            "detected_at",
            "reviewed_at",
            "posted_at",
        ),
        "reply_review_events": (
            "id",
            "reply_queue_id",
            "event_type",
            "old_status",
            "new_status",
            "created_at",
        ),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _load_reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "inbound_tweet_id"),
        _column_expr(columns, "inbound_author_handle"),
        _column_expr(columns, "status", "'pending'"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "posted_at"),
    ]
    raw_rows = conn.execute(
        f"SELECT {', '.join(select_columns)} FROM reply_queue ORDER BY id ASC"
    ).fetchall()

    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        row = dict(raw)
        reply_id = _int_or_none(row.get("id"))
        if reply_id is None:
            continue
        timestamp = (
            _parse_timestamp(row.get("posted_at"))
            or _parse_timestamp(row.get("reviewed_at"))
            or _parse_timestamp(row.get("detected_at"))
            or now
        )
        if not cutoff <= timestamp <= now:
            continue
        rows.append(
            {
                "reply_queue_id": reply_id,
                "inbound_tweet_id": _clean_text(row.get("inbound_tweet_id")),
                "author_handle": _normalize_handle(row.get("inbound_author_handle")),
                "current_status": _clean_status(row.get("status")) or "pending",
                "row_timestamp": timestamp.isoformat(),
            }
        )
    return rows


def _load_events(
    conn: sqlite3.Connection,
    columns: set[str],
    reply_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    required = {"reply_queue_id"}
    if not reply_ids or not required.issubset(columns):
        return {}
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "reply_queue_id"),
        _column_expr(columns, "event_type"),
        _column_expr(columns, "old_status"),
        _column_expr(columns, "new_status"),
        _column_expr(columns, "created_at"),
    ]
    placeholders = ", ".join("?" for _ in reply_ids)
    query = (
        f"SELECT {', '.join(select_columns)} FROM reply_review_events "
        f"WHERE reply_queue_id IN ({placeholders}) "
        "ORDER BY reply_queue_id ASC"
    )
    if "created_at" in columns:
        query += ", datetime(created_at) ASC"
    if "id" in columns:
        query += ", id ASC"
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for raw in conn.execute(query, reply_ids).fetchall():
        event = _event_dict(dict(raw))
        grouped[event["reply_queue_id"]].append(event)
    return grouped


def _audit_reply(row: dict[str, Any], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    latest = events[-1] if events else None
    current_status = row["current_status"]
    has_approved = any(_event_reaches(event, "approved") for event in events)
    has_posted = any(_event_reaches(event, "posted") for event in events)

    if latest and latest.get("new_status") and latest["new_status"] != current_status:
        findings.append(
            _finding(
                row,
                latest,
                "latest_event_status_mismatch",
                "reconcile reply_queue.status with the latest review event",
            )
        )
    if current_status == "posted" and not has_posted:
        findings.append(
            _finding(
                row,
                latest,
                "posted_without_posted_event",
                "insert or repair the missing posted review event",
            )
        )
    if current_status in {"approved", "posted"} and not has_approved:
        findings.append(
            _finding(
                row,
                latest,
                "approved_without_approved_event",
                "insert or repair the missing approval review event",
            )
        )
    for event in events:
        event_type = _clean_status(event.get("event_type"))
        new_status = _clean_status(event.get("new_status"))
        if (event_type == "failed" or new_status == "failed") and current_status != "pending":
            findings.append(
                _finding(
                    row,
                    latest,
                    "failed_event_on_non_pending_reply",
                    "reset the row to pending or remove the stale failed event",
                    event=event,
                )
            )
    findings.extend(_transition_findings(row, events, latest))
    return findings


def _transition_findings(
    row: dict[str, Any],
    events: list[dict[str, Any]],
    latest: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    previous_new_status: str | None = None
    for event in events:
        old_status = _clean_status(event.get("old_status"))
        new_status = _clean_status(event.get("new_status"))
        if old_status and previous_new_status and old_status != previous_new_status:
            findings.append(
                _finding(
                    row,
                    latest,
                    "event_old_status_chain_mismatch",
                    "repair event old_status to match the previous decision state",
                    event=event,
                )
            )
        if old_status and new_status:
            if old_status == "pending" and new_status == "posted":
                findings.append(
                    _finding(
                        row,
                        latest,
                        "skipped_approval_transition",
                        "add the missing approval event before the posted event",
                        event=event,
                    )
                )
            elif (old_status, new_status) not in VALID_TRANSITIONS:
                findings.append(
                    _finding(
                        row,
                        latest,
                        "invalid_status_transition",
                        "review the event trail and restore the expected pending to approved to posted order",
                        event=event,
                    )
                )
        if new_status:
            previous_new_status = new_status
    return findings


def _finding(
    row: dict[str, Any],
    latest: dict[str, Any] | None,
    issue_code: str,
    suggested_action: str,
    *,
    event: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "reply_queue_id": row["reply_queue_id"],
        "inbound_tweet_id": row["inbound_tweet_id"],
        "author_handle": row["author_handle"],
        "current_status": row["current_status"],
        "latest_event": latest,
        "event": event,
        "issue_code": issue_code,
        "suggested_action": suggested_action,
    }


def _event_dict(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _int_or_none(row.get("id")),
        "reply_queue_id": _int_or_none(row.get("reply_queue_id")) or 0,
        "event_type": _clean_status(row.get("event_type")),
        "old_status": _clean_status(row.get("old_status")),
        "new_status": _clean_status(row.get("new_status")),
        "created_at": _clean_text(row.get("created_at")),
    }


def _event_reaches(event: dict[str, Any], status: str) -> bool:
    return event.get("event_type") == status or event.get("new_status") == status


def _finding_sort_key(item: dict[str, Any]) -> tuple[int, str, int]:
    priority = {
        "latest_event_status_mismatch": 0,
        "posted_without_posted_event": 1,
        "approved_without_approved_event": 2,
        "failed_event_on_non_pending_reply": 3,
        "event_old_status_chain_mismatch": 4,
        "invalid_status_transition": 5,
        "skipped_approval_transition": 6,
    }
    return (
        priority.get(item["issue_code"], 99),
        item["issue_code"],
        item["reply_queue_id"],
    )


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return column
    return f"{default} AS {column}"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_timestamp(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return _as_utc(raw)
    text = str(raw).strip()
    if not text:
        return None
    try:
        return _as_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_status(value: Any) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    return text.lower()


def _normalize_handle(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return "unknown"
    return text[1:] if text.startswith("@") else text


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
