"""Compare drafted replies with approved or published reply tone baselines."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import sqlite3
from statistics import mean
from typing import Any


DEFAULT_BASELINE_LIMIT = 50
DEFAULT_DRAFT_LIMIT = 50
BASELINE_STATUSES = {"approved", "posted", "published"}
DRAFT_STATUSES = {"draft", "pending", "queued"}
GENERIC_PHRASES = ("thanks for sharing", "great point", "good point", "interesting", "appreciate")
EFFUSIVE_WORDS = {"amazing", "awesome", "brilliant", "excellent", "fantastic", "love", "wonderful"}
WORD_RE = re.compile(r"[a-z0-9]+(?:'[a-z0-9]+)?", re.IGNORECASE)


def build_reply_tone_consistency_report(
    rows: list[dict[str, Any]],
    *,
    baseline_limit: int = DEFAULT_BASELINE_LIMIT,
    draft_limit: int = DEFAULT_DRAFT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if baseline_limit <= 0:
        raise ValueError("baseline_limit must be positive")
    if draft_limit <= 0:
        raise ValueError("draft_limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    normalized = [_normalize_row(row) for row in rows if _text(_first(row, "draft_text", "reply_text", "text", "content"))]
    baseline_rows = [row for row in normalized if row["status"] in BASELINE_STATUSES][:baseline_limit]
    draft_rows = [row for row in normalized if row["status"] in DRAFT_STATUSES][:draft_limit]
    baseline = _baseline([row["features"] for row in baseline_rows])
    drafts = [_score_draft(row, baseline) for row in draft_rows]
    flagged = [row for row in drafts if row["drift_reasons"]]
    return {
        "artifact_type": "reply_tone_consistency",
        "generated_at": generated_at.isoformat(),
        "filters": {"baseline_limit": baseline_limit, "draft_limit": draft_limit},
        "baseline": baseline,
        "totals": {
            "rows_scanned": len(rows),
            "baseline_count": len(baseline_rows),
            "draft_count": len(drafts),
            "flagged_draft_count": len(flagged),
        },
        "drafts": drafts,
        "flagged_drafts": flagged,
        "empty_state": {"is_empty": not flagged, "message": "No reply tone consistency drift found." if not flagged else None},
    }


def build_reply_tone_consistency_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_reply_tone_consistency_report(_load_rows(conn, _schema(conn)), **kwargs)


def format_reply_tone_consistency_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_tone_consistency_table(report: dict[str, Any]) -> str:
    lines = [
        "Reply Tone Consistency",
        f"Generated: {report['generated_at']}",
        f"Totals: baseline={report['totals']['baseline_count']} drafts={report['totals']['draft_count']} flagged={report['totals']['flagged_draft_count']}",
    ]
    if not report["drafts"]:
        lines.append("No draft replies found.")
        return "\n".join(lines)
    lines.extend(["", "reply_id | severity | reasons | word_delta | question_delta | generic_delta"])
    for draft in report["drafts"]:
        deltas = draft["feature_deltas"]
        lines.append(
            f"{draft['reply_id']} | {draft['severity']} | {', '.join(draft['drift_reasons']) or '-'} | "
            f"{deltas['word_count']} | {deltas['question_count']} | {deltas['generic_phrase_count']}"
        )
    return "\n".join(lines)


format_reply_tone_consistency_text = format_reply_tone_consistency_table


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    text = _text(_first(row, "draft_text", "reply_text", "text", "content"))
    return {
        "reply_id": _text(_first(row, "reply_id", "id", "queue_id")) or "unknown",
        "status": _text(_first(row, "status", "state")).lower() or "draft",
        "platform": _text(_first(row, "platform", "channel")) or None,
        "text": text,
        "features": _features(text),
    }


def _features(text: str) -> dict[str, float]:
    lower = text.lower()
    words = WORD_RE.findall(lower)
    return {
        "word_count": float(len(words)),
        "question_count": float(text.count("?")),
        "exclamation_count": float(text.count("!")),
        "generic_phrase_count": float(sum(1 for phrase in GENERIC_PHRASES if phrase in lower)),
        "effusive_word_count": float(sum(1 for word in words if word in EFFUSIVE_WORDS)),
    }


def _baseline(features: list[dict[str, float]]) -> dict[str, Any]:
    keys = ("word_count", "question_count", "exclamation_count", "generic_phrase_count", "effusive_word_count")
    return {
        "sample_count": len(features),
        "averages": {key: round(mean([item[key] for item in features]), 2) if features else 0.0 for key in keys},
    }


def _score_draft(row: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    averages = baseline["averages"]
    features = row["features"]
    deltas = {key: round(features[key] - averages[key], 2) for key in features}
    reasons = []
    if baseline["sample_count"] == 0:
        reasons.append("missing_baseline")
    elif features["word_count"] < max(4, averages["word_count"] * 0.65):
        reasons.append("unusually_terse")
    if features["effusive_word_count"] >= averages["effusive_word_count"] + 2 or features["exclamation_count"] >= averages["exclamation_count"] + 2:
        reasons.append("unusually_effusive")
    if features["generic_phrase_count"] >= averages["generic_phrase_count"] + 1 and features["generic_phrase_count"] >= 1:
        reasons.append("generic_language")
    if features["question_count"] >= max(2, averages["question_count"] + 2):
        reasons.append("question_heavy")
    severity = "high" if len(reasons) >= 2 or "missing_baseline" in reasons else "medium" if reasons else "low"
    return {
        "reply_id": row["reply_id"],
        "status": row["status"],
        "platform": row["platform"],
        "features": {key: int(value) for key, value in features.items()},
        "feature_deltas": deltas,
        "drift_reasons": reasons,
        "severity": severity,
        "draft_preview": row["text"][:120],
    }


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "reply_queue" not in schema:
        return []
    cols = schema["reply_queue"]
    selected = [
        _col(cols, "id", "reply_id", "queue_id", default="rowid") + " AS reply_id",
        _col(cols, "status", "state", default="'draft'") + " AS status",
        _col(cols, "platform", "channel", default="NULL") + " AS platform",
        _col(cols, "draft_text", "reply_text", "text", "content", default="NULL") + " AS draft_text",
        _col(cols, "created_at", "updated_at", "detected_at", default="NULL") + " AS created_at",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM reply_queue ORDER BY datetime(created_at) DESC").fetchall()]


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
