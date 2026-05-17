"""Track warmth and genericity drift in replies by relationship."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MIN_REPLIES = 3
WARM_TERMS = {"thanks", "thank", "appreciate", "glad", "great", "love", "helpful", "thoughtful"}
GENERIC_TERMS = {"interesting", "nice", "cool", "thanks for sharing", "great point"}
PERSONAL_RE = re.compile(r"\b(you|your|we|our|i)\b", re.IGNORECASE)


def build_reply_relationship_warmth_drift_report(
    rows: list[dict[str, Any]],
    *,
    min_replies: int = DEFAULT_MIN_REPLIES,
    now: datetime | None = None,
) -> dict[str, Any]:
    if min_replies <= 0:
        raise ValueError("min_replies must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        relationship = _text(_first(row, "relationship_id", "author_id", "author", "handle")) or "unknown"
        grouped[relationship].append(_reply_item(row))
    flagged = []
    for relationship, replies in grouped.items():
        replies.sort(key=lambda item: item["created_at"] or "")
        if len(replies) < min_replies:
            continue
        first_half = replies[: max(1, len(replies) // 2)]
        second_half = replies[max(1, len(replies) // 2) :]
        earlier = sum(item["warmth_score"] for item in first_half) / len(first_half)
        recent = sum(item["warmth_score"] for item in second_half) / len(second_half)
        drift = round(recent - earlier, 3)
        generic_recent = sum(item["generic_score"] for item in second_half) / len(second_half)
        direction = _direction(drift, generic_recent)
        if direction == "stable":
            continue
        flagged.append(
            {
                "relationship_id": relationship,
                "reply_count": len(replies),
                "drift_direction": direction,
                "confidence": round(min(abs(drift) + generic_recent, 1), 3),
                "earlier_warmth": round(earlier, 3),
                "recent_warmth": round(recent, 3),
                "recent_examples": replies[-3:],
                "reason": _reason(direction, drift, generic_recent),
            }
        )
    flagged.sort(key=lambda item: (-item["confidence"], item["relationship_id"]))
    return {
        "artifact_type": "reply_relationship_warmth_drift",
        "generated_at": generated_at.isoformat(),
        "filters": {"min_replies": min_replies},
        "totals": {"rows_scanned": len(rows), "relationship_count": len(grouped), "flagged_relationship_count": len(flagged)},
        "relationships": flagged,
        "empty_state": {"is_empty": not flagged, "message": "No relationship warmth drift found." if not flagged else None},
    }


def build_reply_relationship_warmth_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_reply_relationship_warmth_drift_report(_load_rows(conn, schema), **kwargs)


def format_reply_relationship_warmth_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_relationship_warmth_drift_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Relationship Warmth Drift",
        f"Generated: {report['generated_at']}",
        f"Min replies: {report['filters']['min_replies']}",
        f"Totals: relationships={report['totals']['relationship_count']} flagged={report['totals']['flagged_relationship_count']}",
    ]
    if not report["relationships"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "Flagged relationships:"])
    for item in report["relationships"]:
        lines.append(
            f"- {item['relationship_id']} direction={item['drift_direction']} "
            f"confidence={item['confidence']} reason={item['reason']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "reply_drafts" if "reply_drafts" in schema else "replies" if "replies" in schema else None
    if table is None:
        return []
    columns = schema[table]
    selected = [
        _col(columns, "relationship_id", "author_id", "author", "handle", default="'unknown'") + " AS relationship_id",
        _col(columns, "id", "reply_id", default="NULL") + " AS reply_id",
        _col(columns, "draft", "reply_text", "text", "content", default="''") + " AS reply_text",
        _col(columns, "created_at", "sent_at", "updated_at", default="NULL") + " AS created_at",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]


def _reply_item(row: dict[str, Any]) -> dict[str, Any]:
    text = _text(_first(row, "reply_text", "draft", "text", "content"))
    return {
        "reply_id": _text(_first(row, "reply_id", "id")) or "unknown",
        "created_at": _text(_first(row, "created_at", "sent_at", "updated_at")) or None,
        "excerpt": text[:120],
        "warmth_score": _warmth_score(text),
        "generic_score": _generic_score(text),
    }


def _warmth_score(text: str) -> float:
    lowered = text.lower()
    score = sum(0.15 for term in WARM_TERMS if term in lowered)
    if PERSONAL_RE.search(text):
        score += 0.2
    if "!" in text:
        score += 0.1
    return round(min(score, 1), 3)


def _generic_score(text: str) -> float:
    lowered = text.lower()
    score = sum(0.2 for term in GENERIC_TERMS if term in lowered)
    if len(lowered.split()) < 8:
        score += 0.2
    return round(min(score, 1), 3)


def _direction(drift: float, generic_recent: float) -> str:
    if drift <= -0.15:
        return "colder"
    if drift >= 0.15:
        return "warmer"
    if generic_recent >= 0.35:
        return "more_generic"
    return "stable"


def _reason(direction: str, drift: float, generic_recent: float) -> str:
    if direction == "more_generic":
        return f"Recent replies use generic phrasing score {generic_recent:.2f}."
    return f"Recent warmth changed by {drift:+.2f} compared with earlier replies."


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
