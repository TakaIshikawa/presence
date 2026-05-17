"""Compare reply drafts and reviewed replies against relationship context signals."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_STALE_DAYS = 90
_TOKEN_RE = re.compile(r"[a-z0-9_@#]+", re.I)
_STOPWORDS = {
    "about",
    "after",
    "also",
    "and",
    "are",
    "because",
    "been",
    "for",
    "from",
    "have",
    "into",
    "our",
    "that",
    "the",
    "their",
    "this",
    "with",
    "you",
    "your",
}
_STANCE_TERMS = {
    "supportive": {"support", "supportive", "encourage", "appreciate", "thanks", "agree", "helpful"},
    "curious": {"curious", "question", "wonder", "learn", "ask", "explore"},
    "challenging": {"challenge", "push", "disagree", "concern", "risk", "however"},
    "neutral": {"noted", "thanks", "understood"},
}


def build_reply_relationship_signal_drift_report(
    reply_rows: list[dict[str, Any]],
    relationship_context_rows: list[dict[str, Any]] | None = None,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    contexts = _context_index(relationship_context_rows or [], generated_at=generated_at, stale_days=stale_days)
    records = []
    for row in reply_rows:
        record_contexts = _contexts_for_row(row, contexts, generated_at=generated_at, stale_days=stale_days)
        records.append(_record(row, record_contexts))

    records.sort(key=lambda item: (-len(item["flags"]), item["classification"], item["reply_id"]))
    classification_counts = Counter(item["classification"] for item in records)
    flag_counts = Counter(flag for item in records for flag in item["flags"])
    return {
        "artifact_type": "reply_relationship_signal_drift",
        "generated_at": generated_at.isoformat(),
        "filters": {"stale_days": stale_days, "limit": limit},
        "totals": {
            "reply_count": len(records),
            "flagged_count": sum(1 for item in records if item["flags"]),
            "classification_counts": dict(sorted(classification_counts.items())),
            "flag_counts": dict(sorted(flag_counts.items())),
        },
        "replies": records[:limit],
        "aggregates": {
            "by_relationship_id": _aggregate(records, "relationship_id"),
            "by_intended_stance": _aggregate(records, "intended_stance"),
        },
        "empty_state": {"is_empty": not records, "message": "No reply relationship signal drift rows found." if not records else None},
    }


def build_reply_relationship_signal_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    reply_rows = _load_reply_rows(conn, schema)
    context_rows = _load_context_rows(conn, schema)
    return build_reply_relationship_signal_drift_report(reply_rows, context_rows, **kwargs)


def format_reply_relationship_signal_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_relationship_signal_drift_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Reply Relationship Signal Drift",
        f"Generated: {report['generated_at']}",
        f"Filters: stale_days={report['filters']['stale_days']} limit={report['filters']['limit']}",
        f"Totals: replies={totals['reply_count']} flagged={totals['flagged_count']}",
    ]
    if not report["replies"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "Replies:"])
    for item in report["replies"]:
        lines.append(
            f"- reply={item['reply_id']} relationship={item['relationship_id']} "
            f"classification={item['classification']} flags={','.join(item['flags']) or '-'} "
            f"coverage={item['final_context_coverage']}"
        )
    return "\n".join(lines)


def _record(row: dict[str, Any], contexts: list[dict[str, Any]]) -> dict[str, Any]:
    draft_text = _text(_first(row, "draft_text", "draft", "reply_text"))
    final_text = _text(_first(row, "final_reply_text", "reviewed_reply_text", "final_text", "posted_text", "reply_text", "draft_text", "draft"))
    intended_stance = _normalize_stance(_first(row, "intended_stance", "relationship_stance", "stance"))
    final_stance = _normalize_stance(_first(row, "final_stance", "reviewed_stance")) or _infer_stance(final_text)
    available = [context for context in contexts if context["summary"]]
    used = [context for context in available if _token_overlap(context["summary"], final_text)]
    stale_used = [context for context in used if context["is_stale"]]
    coverage = round(len(used) / len(available), 4) if available else 1.0
    flags = []
    if available and not used:
        flags.append("ignored_context")
    if stale_used:
        flags.append("stale_context_used")
    if intended_stance and final_stance and intended_stance != final_stance:
        flags.append("stance_mismatch")
    classification = flags[0] if flags else "healthy"
    return {
        "reply_id": _text(_first(row, "reply_id", "id")) or "unknown",
        "relationship_id": _relationship_id(row),
        "intended_stance": intended_stance or None,
        "final_stance": final_stance or None,
        "classification": classification,
        "flags": flags,
        "available_context_count": len(available),
        "used_context_count": len(used),
        "stale_context_used_count": len(stale_used),
        "final_context_coverage": coverage,
        "draft_excerpt": draft_text[:120],
        "final_excerpt": final_text[:120],
    }


def _contexts_for_row(
    row: dict[str, Any],
    indexed: dict[str, list[dict[str, Any]]],
    *,
    generated_at: datetime,
    stale_days: int,
) -> list[dict[str, Any]]:
    contexts = list(indexed.get(_relationship_id(row), []))
    inline = _parse_json_object(row.get("relationship_context"))
    if inline:
        contexts.append(_context_item(inline, generated_at=generated_at, stale_days=stale_days))
    for key in ("relationship_notes", "prior_interaction_summary", "relationship_summary"):
        if _text(row.get(key)):
            contexts.append(_context_item({"summary": row.get(key), "updated_at": row.get("relationship_context_updated_at")}, generated_at=generated_at, stale_days=stale_days))
    return contexts


def _context_index(
    rows: list[dict[str, Any]],
    *,
    generated_at: datetime,
    stale_days: int,
) -> dict[str, list[dict[str, Any]]]:
    indexed: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        relationship_id = _relationship_id(row)
        indexed[relationship_id].append(_context_item(row, generated_at=generated_at, stale_days=stale_days))
    return indexed


def _context_item(row: dict[str, Any], *, generated_at: datetime, stale_days: int) -> dict[str, Any]:
    updated_at = _parse_dt(_first(row, "updated_at", "context_updated_at", "last_seen_at", "created_at"))
    age_days = (generated_at - updated_at).total_seconds() / 86400 if updated_at else None
    return {
        "summary": _context_summary(row),
        "updated_at": updated_at.isoformat() if updated_at else None,
        "age_days": round(age_days, 2) if age_days is not None else None,
        "is_stale": bool(age_days is not None and age_days > stale_days),
    }


def _context_summary(row: dict[str, Any]) -> str:
    parsed = _parse_json_object(row.get("relationship_context")) if "relationship_context" in row else row
    parts = []
    for key in (
        "summary",
        "relationship_summary",
        "relationship_notes",
        "notes",
        "prior_interaction_summary",
        "recent_interactions",
        "known_context",
        "signal",
    ):
        value = parsed.get(key)
        if isinstance(value, list):
            parts.extend(_text(item) for item in value)
        elif isinstance(value, dict):
            parts.append(" ".join(_text(item) for item in value.values()))
        else:
            parts.append(_text(value))
    return " ".join(part for part in parts if part).strip()


def _load_reply_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("reply_drafts", "reply_reviews", "reply_queue"):
        columns = schema.get(table)
        if not columns:
            continue
        selected = [
            _select(columns, ("id", "reply_id"), "reply_id"),
            _select(columns, ("relationship_id", "inbound_author", "author_handle", "target_handle"), "relationship_id"),
            _select(columns, ("draft_text", "draft", "reply_text"), "draft_text"),
            _select(columns, ("final_reply_text", "reviewed_reply_text", "posted_text", "reply_text", "draft_text"), "final_reply_text"),
            _select(columns, ("relationship_context",), "relationship_context"),
            _select(columns, ("relationship_notes",), "relationship_notes"),
            _select(columns, ("prior_interaction_summary",), "prior_interaction_summary"),
            _select(columns, ("intended_stance", "relationship_stance", "intent"), "intended_stance"),
            _select(columns, ("final_stance", "reviewed_stance"), "final_stance"),
            _select(columns, ("relationship_context_updated_at", "detected_at", "created_at"), "relationship_context_updated_at"),
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    return []


def _load_context_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("relationship_contexts", "relationship_context"):
        columns = schema.get(table)
        if not columns:
            continue
        selected = [
            _select(columns, ("relationship_id", "author_handle", "target_handle"), "relationship_id"),
            _select(columns, ("summary", "relationship_summary", "relationship_notes", "notes"), "summary"),
            _select(columns, ("prior_interaction_summary", "recent_interactions"), "prior_interaction_summary"),
            _select(columns, ("updated_at", "context_updated_at", "last_seen_at", "created_at"), "updated_at"),
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    return []


def _aggregate(records: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[_text(record.get(field)) or "unknown"].append(record)
    rows = []
    for key, items in groups.items():
        rows.append(
            {
                field: key,
                "count": len(items),
                "flagged": sum(1 for item in items if item["flags"]),
                "classification_counts": dict(sorted(Counter(item["classification"] for item in items).items())),
            }
        )
    rows.sort(key=lambda item: (-item["flagged"], -item["count"], item[field]))
    return rows


def _relationship_id(row: dict[str, Any]) -> str:
    return _text(_first(row, "relationship_id", "author_handle", "inbound_author", "target_handle", "author")) or "unknown"


def _token_overlap(value: str, text: str) -> bool:
    needles = _tokens(value)
    if not needles:
        return False
    haystack = _tokens(text)
    required = 1 if len(needles) <= 2 else 2
    return len(needles & haystack) >= required


def _tokens(value: str) -> set[str]:
    return {token for token in (_normalise(raw) for raw in _TOKEN_RE.findall(value.lower())) if len(token) >= 4 and token not in _STOPWORDS}


def _normalise(value: str) -> str:
    return value.strip().lower().replace("-", "_")


def _normalize_stance(value: Any) -> str:
    text = _text(value).lower()
    for stance in _STANCE_TERMS:
        if stance in text:
            return stance
    return text if text in _STANCE_TERMS else ""


def _infer_stance(text: str) -> str:
    tokens = _tokens(text)
    scores = {stance: len(tokens & terms) for stance, terms in _STANCE_TERMS.items()}
    stance, score = max(scores.items(), key=lambda item: (item[1], item[0]))
    return stance if score else ""


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
