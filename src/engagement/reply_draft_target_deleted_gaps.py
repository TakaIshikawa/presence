"""Report queued reply drafts whose target is missing or deleted."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STATUS = "queued"
DEFAULT_LIMIT = 100
TARGET_COLUMNS = ("target_id", "target_tweet_id", "inbound_tweet_id", "mention_id", "target_url", "inbound_url")
DELETED_COLUMNS = ("target_deleted", "is_deleted", "deleted_at", "tombstoned_at", "target_status", "linkable", "permalink")
REQUIRED_COLUMNS = {"id", "status", "platform"}
BAD_TARGET_STATUSES = {"deleted", "missing", "tombstoned", "unavailable", "not_found", "gone"}


def build_reply_draft_target_deleted_gaps_report(
    rows: list[dict[str, Any]],
    *,
    status: str | None = DEFAULT_STATUS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    wanted = _norm(status) if status else None
    findings = []
    for row in rows:
        draft_status = _norm(row.get("draft_status") or row.get("status")) or "unknown"
        if wanted and draft_status != wanted:
            continue
        reason = _gap_reason(row)
        if reason:
            findings.append(_finding(row, reason))
    findings.sort(key=lambda f: (f["platform"], f["draft_status"], str(f["created_at"] or ""), _int_or_text(f["reply_draft_id"])))
    shown = findings[:limit]
    return {
        "artifact_type": "reply_draft_target_deleted_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"status": status, "limit": limit},
        "summary": {
            "row_count": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_platform_status": [
                {"platform": p, "draft_status": s, "count": c}
                for (p, s), c in sorted(Counter((f["platform"], f["draft_status"]) for f in findings).items())
            ],
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
        "empty_state": {"is_empty": not findings, "message": "No deleted target gaps found." if not findings else None},
    }


def build_reply_draft_target_deleted_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in ("reply_drafts", "reply_queue", "reply_draft_queue") if name in schema), None)
    if table is None:
        return build_reply_draft_target_deleted_gaps_report([], missing_tables=["reply_drafts|reply_queue"], **kwargs)
    columns = schema[table]
    missing = sorted(REQUIRED_COLUMNS - columns)
    if not set(TARGET_COLUMNS) & columns:
        missing.append("|".join(TARGET_COLUMNS))
    if missing:
        return build_reply_draft_target_deleted_gaps_report([], missing_columns={table: missing}, **kwargs)
    return build_reply_draft_target_deleted_gaps_report(_load_rows(conn, table, columns, kwargs.get("status", DEFAULT_STATUS)), **kwargs)


def format_reply_draft_target_deleted_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_draft_target_deleted_gaps_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        "Reply Draft Target Deleted Gaps",
        f"Generated: {report['generated_at']}",
        f"Filters: status={report['filters']['status'] or '*'} limit={report['filters']['limit']}",
        f"Totals: rows={s['row_count']} findings={s['finding_count']} shown={s['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(f"  - draft={f['reply_draft_id']} platform={f['platform']} status={f['draft_status']} target={f['target_identifier'] or '-'} reason={f['gap_reason']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str], status: str | None) -> list[dict[str, Any]]:
    aliases = {
        "id": ("id",),
        "platform": ("platform",),
        "draft_status": ("draft_status", "status"),
        "target_identifier": TARGET_COLUMNS,
        "target_status": ("target_status",),
        "target_deleted": ("target_deleted", "is_deleted"),
        "deleted_at": ("deleted_at", "tombstoned_at"),
        "linkable": ("linkable",),
        "permalink": ("permalink", "target_url", "inbound_url"),
        "created_at": ("created_at",),
        "updated_at": ("updated_at",),
    }
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases.items())
    where = ""
    params: tuple[Any, ...] = ()
    if status:
        where = f"WHERE LOWER(COALESCE({_coalesce(columns, ('draft_status', 'status'))}, '')) = LOWER(?)"
        params = (status,)
    rows = conn.execute(f"SELECT {select} FROM {table} {where} ORDER BY id ASC", params).fetchall()
    return [dict(row) for row in rows]


def _gap_reason(row: dict[str, Any]) -> str | None:
    if not row.get("target_identifier"):
        return "missing_target_identifier"
    if _truthy(row.get("target_deleted")) or row.get("deleted_at"):
        return "target_deleted_or_tombstoned"
    if _norm(row.get("target_status")) in BAD_TARGET_STATUSES:
        return "target_status_not_linkable"
    if row.get("linkable") is not None and not _truthy(row.get("linkable")):
        return "target_no_longer_linkable"
    if row.get("permalink") is None and _norm(row.get("platform")) in {"x", "twitter", "bluesky", "mastodon"}:
        return "missing_target_permalink"
    return None


def _finding(row: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "reply_draft_id": row.get("id") or row.get("reply_draft_id"),
        "platform": _norm(row.get("platform")) or "unknown",
        "draft_status": _norm(row.get("draft_status") or row.get("status")) or "unknown",
        "target_identifier": row.get("target_identifier") or row.get("target_id") or row.get("target_tweet_id") or row.get("inbound_tweet_id"),
        "gap_reason": reason,
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
    }


def _coalesce(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "deleted", "t"}


def _norm(value: Any) -> str | None:
    text = "" if value is None else str(value).strip().lower()
    return text or None


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
