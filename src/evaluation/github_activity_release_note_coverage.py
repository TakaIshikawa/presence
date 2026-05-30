"""Compare release-worthy GitHub activity against release notes."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


ARTIFACT_TYPE = "github_activity_release_note_coverage"


def build_github_activity_release_note_coverage_report(
    activities: list[dict[str, Any]],
    release_notes: list[dict[str, Any]],
    *,
    significance_threshold: int = 5,
    late_days: int = 7,
    now: datetime | None = None,
) -> dict[str, Any]:
    if significance_threshold < 0:
        raise ValueError("significance_threshold must be non-negative")
    if late_days < 0:
        raise ValueError("late_days must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    notes = [_normalize_note(note) for note in release_notes]
    rows = []
    scoped = [item for item in activities if int(item.get("significance") or item.get("significance_score") or 0) >= significance_threshold and _bool(item.get("release_worthy"), True)]
    for activity in scoped:
        merged_at = _dt(activity.get("merged_at"))
        match = _match_note(activity, notes)
        if not match:
            status, lag, reason, tag = "missing", None, "no_release_note_match", None
        else:
            released_at = match["released_at"]
            lag = None if not (merged_at and released_at) else (released_at.date() - merged_at.date()).days
            low_detail = len(str(match.get("summary") or "").split()) < 4
            status = "low_detail" if low_detail else "late" if lag is not None and lag > late_days else "covered"
            reason = "summary_too_short" if low_detail else "release_note_late" if status == "late" else ""
            tag = match.get("release_tag")
        if status == "covered":
            continue
        rows.append(
            {
                "repo": activity.get("repo"),
                "activity_id": activity.get("activity_id") or activity.get("id"),
                "merged_at": merged_at.isoformat() if merged_at else None,
                "release_tag": tag,
                "coverage_status": status,
                "lag_days": lag,
                "missing_summary_reason": reason,
            }
        )
    rows.sort(key=lambda row: (row["repo"] or "", row["merged_at"] or "", str(row["activity_id"])))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": generated_at.isoformat(), "filters": {"significance_threshold": significance_threshold, "late_days": late_days}, "totals": {"activity_count": len(activities), "scoped_activity_count": len(scoped), "row_count": len(rows)}, "rows": rows}


def build_github_activity_release_note_coverage_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "github_activities" not in schema or "github_release_notes" not in schema:
        return build_github_activity_release_note_coverage_report([], [], **kwargs) | {"missing_tables": sorted({"github_activities", "github_release_notes"} - set(schema))}
    ac, nc = schema["github_activities"], schema["github_release_notes"]
    activities = [dict(row) for row in conn.execute(f"SELECT {_expr(ac, ('repo',), 'NULL')} AS repo, {_expr(ac, ('id','activity_id'), 'rowid')} AS activity_id, {_expr(ac, ('merged_at',), 'NULL')} AS merged_at, {_expr(ac, ('significance','significance_score'), '0')} AS significance, {_expr(ac, ('release_worthy',), '1')} AS release_worthy FROM github_activities ORDER BY rowid")]
    notes = [dict(row) for row in conn.execute(f"SELECT {_expr(nc, ('repo',), 'NULL')} AS repo, {_expr(nc, ('activity_id',), 'NULL')} AS activity_id, {_expr(nc, ('release_tag','tag'), 'NULL')} AS release_tag, {_expr(nc, ('released_at','created_at'), 'NULL')} AS released_at, {_expr(nc, ('summary','body'), 'NULL')} AS summary FROM github_release_notes ORDER BY rowid")]
    return build_github_activity_release_note_coverage_report(activities, notes, **kwargs) | {"missing_tables": []}


def format_github_activity_release_note_coverage_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_github_activity_release_note_coverage_text(report: dict[str, Any]) -> str:
    lines = ["GitHub Activity Release Note Coverage", f"Generated: {report['generated_at']}", f"Rows: {report['totals']['row_count']}"]
    lines.extend(f"{row['repo']} | {row['activity_id']} | {row['coverage_status']} | {row['release_tag'] or '-'} | {row['missing_summary_reason']}" for row in report["rows"])
    return "\n".join(lines)


def _normalize_note(note: dict[str, Any]) -> dict[str, Any]:
    return {**note, "released_at": _dt(note.get("released_at"))}


def _match_note(activity: dict[str, Any], notes: list[dict[str, Any]]) -> dict[str, Any] | None:
    aid = str(activity.get("activity_id") or activity.get("id"))
    repo = activity.get("repo")
    for note in notes:
        if note.get("repo") == repo and str(note.get("activity_id")) == aid:
            return note
    return None


def _bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, str):
        return value.lower() not in {"0", "false", "no"}
    return bool(value)


def _dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _utc(value)
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    if isinstance(db_or_conn, sqlite3.Connection):
        db_or_conn.row_factory = sqlite3.Row
        return db_or_conn
    conn = sqlite3.connect(db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _expr(columns: set[str], names: tuple[str, ...], fallback: str) -> str:
    return next((name for name in names if name in columns), fallback)
