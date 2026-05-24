"""Report reply drafts whose tone metadata drifts from guard or review signals."""

from __future__ import annotations

from datetime import datetime, timedelta
import sqlite3
from typing import Any

from ._batch_report_utils import connection, dump_json, first_table, json_load, parse_time, pick, schema, text, utc_now

DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 100


def build_reply_draft_tone_drift_report_from_db(db_or_conn: Any, *, lookback_days: int = DEFAULT_LOOKBACK_DAYS, limit: int = DEFAULT_LIMIT, now: datetime | None = None) -> dict[str, Any]:
    if lookback_days <= 0 or limit <= 0:
        raise ValueError("lookback_days and limit must be positive")
    generated_at = utc_now(now)
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = first_table(sch, ("reply_drafts", "reply_queue"))
    missing_tables = [] if table else ["reply_drafts|reply_queue"]
    missing_columns: dict[str, list[str]] = {}
    rows: list[dict[str, Any]] = []
    if table:
        cols = sch[table]
        if "id" not in cols:
            missing_columns[table] = ["id"]
        else:
            rows = _load(conn, table, cols, generated_at - timedelta(days=lookback_days))
    findings = []
    for row in rows:
        meta = json_load(row.get("metadata"))
        tone = text(row.get("tone")) or text(meta.get("tone") if isinstance(meta, dict) else "")
        persona = text(row.get("persona")) or text(meta.get("persona") if isinstance(meta, dict) else "")
        platform = text(row.get("platform")) or "unknown"
        guard_tone = text(row.get("guard_tone")) or text(meta.get("guard_tone") if isinstance(meta, dict) else "")
        source_tone = text(row.get("source_tone")) or text(meta.get("source_tone") if isinstance(meta, dict) else "")
        review_status = text(row.get("review_status")).lower()
        if not tone:
            findings.append(_finding("missing_tone_metadata", row, platform, persona, tone, guard_tone, review_status, "Draft has no explicit tone metadata."))
        if tone and guard_tone and tone.lower() != guard_tone.lower():
            findings.append(_finding("persona_guard_tone_mismatch", row, platform, persona, tone, guard_tone, review_status, "Persona guard tone disagrees with draft tone."))
        if review_status in {"rejected", "needs_tone_revision", "tone_rejected"}:
            findings.append(_finding("reviewer_tone_rejection", row, platform, persona, tone, guard_tone, review_status, "Reviewer rejected or requested tone revision."))
        expected = {"linkedin": "professional", "x": "concise", "twitter": "concise", "mastodon": "conversational"}.get(platform.lower())
        if expected and tone and tone.lower() != expected:
            findings.append(_finding("platform_tone_mismatch", row, platform, persona, tone, expected, review_status, "Tone does not match platform expectation."))
        if source_tone and tone and source_tone.lower() != tone.lower():
            findings.append(_finding("source_tone_mismatch", row, platform, persona, tone, source_tone, review_status, "Source content tone differs from reply tone."))
    findings.sort(key=lambda f: (f["finding_type"], str(f["draft_id"])))
    findings = findings[:limit]
    return {"artifact_type": "reply_draft_tone_drift", "generated_at": generated_at.isoformat(), "filters": {"lookback_days": lookback_days, "limit": limit}, "summary": {"draft_count": len(rows), "finding_count": len(findings)}, "findings": findings, "missing_tables": missing_tables, "missing_columns": missing_columns, "empty_state": {"is_empty": not rows or not findings, "message": "No reply draft tone drift findings found." if rows and not findings else "No reply drafts found." if not rows and not (missing_tables or missing_columns) else None}}


def build_reply_draft_tone_drift_report(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return build_reply_draft_tone_drift_report_from_db(*args, **kwargs)


def format_reply_draft_tone_drift_json(report: dict[str, Any]) -> str:
    return dump_json(report)


def format_reply_draft_tone_drift_text(report: dict[str, Any]) -> str:
    lines = ["Reply Draft Tone Drift", f"Generated: {report['generated_at']}", f"Totals: drafts={report['summary']['draft_count']} findings={report['summary']['finding_count']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"] and report["empty_state"]["message"]:
        lines.append(report["empty_state"]["message"])
    lines.extend(f"  - {f['finding_type']} draft={f['draft_id']} platform={f['platform']} tone={f['tone_label'] or '-'}" for f in report["findings"])
    return "\n".join(lines)


def _load(conn: sqlite3.Connection, table: str, cols: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    ts = pick(cols, "created_at", "updated_at", default="'1970-01-01T00:00:00+00:00'")
    select = [f"{pick(cols, 'id', default='rowid')} AS draft_id", f"{pick(cols, 'platform', default='NULL')} AS platform", f"{pick(cols, 'persona', 'persona_id', default='NULL')} AS persona", f"{pick(cols, 'tone', 'tone_label', default='NULL')} AS tone", f"{pick(cols, 'guard_tone', 'persona_guard_tone', default='NULL')} AS guard_tone", f"{pick(cols, 'source_tone', default='NULL')} AS source_tone", f"{pick(cols, 'review_status', 'status', default='NULL')} AS review_status", f"{pick(cols, 'metadata', default='NULL')} AS metadata", f"{ts} AS created_at"]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} WHERE datetime({ts}) >= datetime(?) ORDER BY draft_id ASC", (cutoff.isoformat(),))]


def _finding(kind: str, row: dict[str, Any], platform: str, persona: str, tone: str, expected: str, review_status: str, detail: str) -> dict[str, Any]:
    return {"finding_type": kind, "draft_id": row.get("draft_id"), "platform": platform, "persona": persona or None, "tone_label": tone or None, "expected_tone_label": expected or None, "review_status": review_status or None, "detail": detail}
