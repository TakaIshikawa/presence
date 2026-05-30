"""Report stale reply drafts or sent replies that promise follow-up."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


ARTIFACT_TYPE = "reply_draft_followup_promise_staleness"
DEFAULT_STATUS = "pending,queued,sent"
DEFAULT_STALE_HOURS = 48
DEFAULT_WINDOW_DAYS = 30
DEFAULT_LIMIT = 100
TABLES = ("reply_drafts", "reply_queue", "replies")
TEXT_COLUMNS = ("draft_text", "content", "body", "reply_text", "text")
METADATA_COLUMNS = ("metadata", "draft_metadata", "followup_metadata", "platform_metadata")
PROMISE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("I'll follow up", re.compile(r"\bi(?:'|’)?ll\s+follow\s+up\b|\bi\s+will\s+follow\s+up\b", re.I)),
    ("will circle back", re.compile(r"\bwill\s+circle\s+back\b|\bcircle\s+back\b", re.I)),
    ("I'll check", re.compile(r"\bi(?:'|’)?ll\s+check\b|\bi\s+will\s+check\b", re.I)),
    ("I'll send", re.compile(r"\bi(?:'|’)?ll\s+send\b|\bi\s+will\s+send\b", re.I)),
)
RESOLVED_STATUSES = {"resolved", "completed", "complete", "done", "closed"}


def build_reply_draft_followup_promise_staleness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in TABLES if name in schema), None)
    if not table:
        return build_reply_draft_followup_promise_staleness_report([], missing_tables=["reply_drafts|reply_queue"], **kwargs)
    columns = schema[table]
    missing = sorted({"id"} - columns)
    if not set(TEXT_COLUMNS) & columns:
        missing.append("|".join(TEXT_COLUMNS))
    rows = _load_rows(conn, table, columns) if not missing else []
    resolved_ids = _resolved_followup_ids(conn, schema)
    return build_reply_draft_followup_promise_staleness_report(rows, resolved_reply_ids=resolved_ids, missing_columns={table: missing} if missing else None, **kwargs)


def build_reply_draft_followup_promise_staleness_report(
    rows: list[dict[str, Any]],
    *,
    status: str | None = DEFAULT_STATUS,
    stale_hours: int = DEFAULT_STALE_HOURS,
    window_days: int = DEFAULT_WINDOW_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    resolved_reply_ids: set[Any] | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if stale_hours <= 0:
        raise ValueError("stale_hours must be positive")
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or _bounded_observed_now(_latest_reply_observed_at(rows, stale_hours=stale_hours)))
    cutoff = generated_at - timedelta(days=window_days)
    wanted = _status_set(status)
    resolved = {str(value) for value in (resolved_reply_ids or set())}
    findings: list[dict[str, Any]] = []
    scanned = 0
    for row in rows:
        draft_status = _norm(row.get("status") or row.get("draft_status")) or "unknown"
        if wanted and draft_status not in wanted:
            continue
        timestamp = _parse_time(row.get("created_at") or row.get("updated_at"))
        if timestamp and timestamp < cutoff:
            continue
        scanned += 1
        reply_id = row.get("id") or row.get("reply_draft_id")
        if str(reply_id) in resolved or _metadata_resolved(row.get("metadata")):
            continue
        phrase = _promise_phrase(row.get("text"))
        if not phrase or timestamp is None:
            continue
        age_hours = round(max(0.0, (generated_at - timestamp).total_seconds() / 3600), 2)
        if age_hours < stale_hours:
            continue
        findings.append(
            {
                "reply_draft_id": reply_id,
                "platform": _norm(row.get("platform")) or "unknown",
                "status": draft_status,
                "promised_phrase": phrase,
                "age_hours": age_hours,
                "suggested_action": "create_or_resolve_followup_action",
                "created_at": timestamp.isoformat(),
            }
        )
    findings.sort(key=lambda item: (-item["age_hours"], item["platform"], _sort_id(item["reply_draft_id"])))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"status": status, "stale_hours": stale_hours, "window_days": window_days, "limit": limit},
        "summary": {
            "replies_scanned": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_status": dict(sorted(Counter(item["status"] for item in findings).items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
    }


def format_reply_draft_followup_promise_staleness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_draft_followup_promise_staleness_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Draft Follow-up Promise Staleness",
        f"Generated: {report['generated_at']}",
        f"Filters: status={report['filters']['status'] or '*'} stale_hours={report['filters']['stale_hours']} window_days={report['filters']['window_days']} limit={report['filters']['limit']}",
        f"Totals: replies={report['summary']['replies_scanned']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append("No stale follow-up promises found.")
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(f"  - draft={f['reply_draft_id']} platform={f['platform']} status={f['status']} phrase={f['promised_phrase']} age_hours={f['age_hours']:.1f} action={f['suggested_action']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    aliases = {
        "id": ("id",),
        "platform": ("platform",),
        "status": ("draft_status", "status"),
        "text": TEXT_COLUMNS,
        "metadata": METADATA_COLUMNS,
        "created_at": ("created_at", "sent_at", "posted_at"),
        "updated_at": ("updated_at",),
    }
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases.items())
    return [dict(row) for row in conn.execute(f"SELECT {select} FROM {table} ORDER BY id ASC").fetchall()]


def _resolved_followup_ids(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> set[Any]:
    ids: set[Any] = set()
    for table in ("reply_followups", "reply_followup_reminders"):
        cols = schema.get(table)
        if not cols:
            continue
        id_expr = _coalesce(cols, ("reply_draft_id", "reply_queue_id", "source_reply_id", "source_id"))
        status_expr = _coalesce(cols, ("status", "state"))
        done_expr = _coalesce(cols, ("completed_at", "resolved_at", "closed_at"))
        for row in conn.execute(f"SELECT {id_expr} AS reply_id, {status_expr} AS status, {done_expr} AS done_at FROM {table}").fetchall():
            if row["reply_id"] is not None and (_norm(row["status"]) in RESOLVED_STATUSES or row["done_at"]):
                ids.add(row["reply_id"])
    cols = schema.get("proactive_actions")
    if cols:
        id_expr = _coalesce(cols, ("source_reply_id", "reply_draft_id", "reply_queue_id"))
        status_expr = _coalesce(cols, ("status", "state"))
        for row in conn.execute(f"SELECT {id_expr} AS reply_id, {status_expr} AS status FROM proactive_actions").fetchall():
            if row["reply_id"] is not None and _norm(row["status"]) in RESOLVED_STATUSES:
                ids.add(row["reply_id"])
    return ids


def _promise_phrase(value: Any) -> str | None:
    text = str(value or "")
    for label, pattern in PROMISE_PATTERNS:
        if pattern.search(text):
            return label
    return None


def _latest_reply_observed_at(rows: list[dict[str, Any]], *, stale_hours: int) -> datetime | None:
    timestamps: list[datetime] = []
    for row in rows:
        for key in ("created_at", "updated_at"):
            parsed = _parse_time(row.get(key))
            if parsed is not None:
                timestamps.append(parsed)
    if not timestamps:
        return None
    return max(timestamps) + timedelta(hours=stale_hours)


def _bounded_observed_now(observed_at: datetime | None) -> datetime:
    current = datetime.now(timezone.utc)
    if observed_at is None:
        return current
    observed_at = _utc(observed_at)
    return observed_at if observed_at <= current else current


def _metadata_resolved(value: Any) -> bool:
    data = _json(value)
    if not isinstance(data, dict):
        return False
    if _norm(data.get("followup_status") or data.get("status")) in RESOLVED_STATUSES:
        return True
    return any(data.get(key) for key in ("completed_at", "resolved_at", "followup_completed_at"))


def _json(value: Any) -> Any:
    if not value:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return None


def _coalesce(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _status_set(value: str | None) -> set[str]:
    return {_norm(part) for part in str(value or "").split(",") if _norm(part)}


def _norm(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text or None


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _sort_id(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, str(value or ""))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
