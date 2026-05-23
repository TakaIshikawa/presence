"""Report reply polling cursor integrity gaps."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from ._report_helpers import base_report, clean, connection, json_format, missing, parse_time, schema, text_format, utc


ARTIFACT_TYPE = "reply_platform_cursor_integrity"
DEFAULT_MAX_AGE_HOURS = 24
DEFAULT_LIMIT = 100
REASONS = ("missing_cursor", "stale_platform_cursor", "legacy_cursor_divergence", "missing_platform_state")


def build_reply_platform_cursor_integrity_report(rows: list[dict[str, Any]], *, max_age_hours: int = DEFAULT_MAX_AGE_HOURS, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if max_age_hours < 0:
        raise ValueError("max_age_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc(now)
    cutoff = generated_at - timedelta(hours=max_age_hours)
    findings: list[dict[str, Any]] = []
    platforms = {clean(row.get("platform")).lower() for row in rows if clean(row.get("platform"))}
    if "x" not in platforms:
        findings.append({"reason": "missing_platform_state", "platform": "x", "detail": "missing x platform reply state"})
    for row in rows:
        platform = clean(row.get("platform"), "legacy")
        cursor = clean(row.get("cursor"))
        legacy = clean(row.get("last_mention_id"))
        if not cursor:
            findings.append({"reason": "missing_cursor", "platform": platform, "detail": "platform cursor is missing"})
        updated = parse_time(row.get("updated_at"))
        if updated and updated <= cutoff:
            findings.append({"reason": "stale_platform_cursor", "platform": platform, "updated_at": row.get("updated_at"), "detail": "platform cursor is older than max age"})
        if platform.lower() in {"x", "twitter"} and legacy and cursor and legacy != cursor:
            findings.append({"reason": "legacy_cursor_divergence", "platform": platform, "cursor": cursor, "last_mention_id": legacy, "detail": "legacy reply_state cursor differs from x cursor"})
    findings.sort(key=lambda f: (REASONS.index(f["reason"]), str(f.get("platform"))))
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"max_age_hours": max_age_hours, "limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def build_reply_platform_cursor_integrity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"reply_state": {"last_mention_id"}, "platform_reply_state": {"platform", "cursor"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_reply_platform_cursor_integrity_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    updated = "prs.updated_at" if "updated_at" in db_schema["platform_reply_state"] else "NULL"
    rows = [dict(r) for r in conn.execute(f"SELECT prs.platform, prs.cursor, {updated} AS updated_at, rs.last_mention_id FROM platform_reply_state prs LEFT JOIN reply_state rs ON rs.id = 1 ORDER BY prs.platform").fetchall()]
    return build_reply_platform_cursor_integrity_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_reply_platform_cursor_integrity_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_reply_platform_cursor_integrity_text(report: dict[str, Any]) -> str:
    return text_format("Reply Platform Cursor Integrity", report)
