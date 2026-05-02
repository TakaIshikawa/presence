"""Summarize reply draft intent mix across review states."""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
GENERIC_SHARE_RECOMMENDATION_THRESHOLD = 0.40

GENERIC_INTENTS = {"generic"}
PENDING_STATUSES = {"pending"}

_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9'-]*")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)

_QUESTION_OPENERS = {
    "can",
    "could",
    "do",
    "does",
    "did",
    "how",
    "is",
    "are",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "will",
    "would",
    "should",
}
_QUESTION_PHRASES = (
    "any advice",
    "any idea",
    "can you help",
    "could you explain",
    "how should i",
    "what should i",
    "would love your take",
)
_THANKS_PHRASES = (
    "appreciate",
    "grateful",
    "thank you",
    "thanks",
    "thx",
)
_CORRECTION_PHRASES = (
    "actually",
    "correction",
    "not quite",
    "that's wrong",
    "that is wrong",
    "typo",
    "you mean",
)
_SUPPORT_PHRASES = (
    "bug",
    "broken",
    "can't",
    "cannot",
    "crash",
    "error",
    "failed",
    "help",
    "issue",
    "problem",
    "stuck",
    "support",
)
_GENERIC_PHRASES = (
    "appreciate you sharing",
    "great point",
    "nice update",
    "thanks for sharing",
)

_INTENT_ALIASES = {
    "appreciation": "thanks",
    "praise": "thanks",
    "thank_you": "thanks",
    "bug-report": "support",
    "bug_report": "support",
    "support-request": "support",
    "support_request": "support",
    "disagreement": "correction",
    "objection": "correction",
    "other": "generic",
    "unknown": "generic",
    "none": "generic",
}


def build_reply_intent_mix_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str | None = None,
    include_reviewed: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic intent mix report for recent reply drafts."""

    if days <= 0:
        raise ValueError("days must be positive")
    if platform is not None and not platform.strip():
        raise ValueError("platform must not be blank")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    columns = _table_columns(conn, "reply_queue")
    rows = _reply_rows(
        conn,
        columns,
        cutoff=cutoff,
        platform=platform,
        include_reviewed=include_reviewed,
    )

    items = [_summarize_row(row, columns) for row in rows]
    groups = _groups(items)
    intent_counts = Counter(item["intent"] for item in items)
    generic_count = sum(count for intent, count in intent_counts.items() if intent in GENERIC_INTENTS)
    total = len(items)
    generic_share = round(generic_count / total, 4) if total else 0.0

    return {
        "artifact_type": "reply_intent_mix",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "platform": platform,
            "include_reviewed": include_reviewed,
        },
        "counts": {
            "rows_scanned": total,
            "pending_replies": sum(1 for item in items if item["review_outcome"] == "pending"),
            "reviewed_replies": sum(1 for item in items if item["review_outcome"] != "pending"),
            "generic_replies": generic_count,
            "generic_share": generic_share,
        },
        "by_intent": _distribution(intent_counts, total, "intent"),
        "groups": groups,
        "recommendations": _recommendations(generic_share, generic_count, total),
    }


def infer_reply_intent(row: dict[str, Any], columns: set[str] | None = None) -> str:
    """Return explicit reply intent when present, otherwise infer a coarse intent."""

    for column in ("intent", "reply_intent", "classification_intent", "classifier_intent"):
        if columns is not None and column not in columns:
            continue
        explicit = _normalize_intent(row.get(column))
        if explicit:
            return explicit

    text = _normalize(" ".join(str(row.get(column) or "") for column in ("inbound_text", "draft_text")))
    tokens = _TOKEN_RE.findall(text)
    if "?" in str(row.get("inbound_text") or ""):
        return "question"
    if tokens and tokens[0] in _QUESTION_OPENERS:
        return "question"
    if len(tokens) > 1 and tokens[0] in {"hey", "hi", "hello"} and tokens[1] in _QUESTION_OPENERS:
        return "question"
    if any(phrase in text for phrase in _QUESTION_PHRASES):
        return "question"
    if any(phrase in text for phrase in _GENERIC_PHRASES):
        return "generic"
    if any(phrase in text for phrase in _THANKS_PHRASES):
        return "thanks"
    if any(phrase in text for phrase in _CORRECTION_PHRASES):
        return "correction"
    if any(phrase in text for phrase in _SUPPORT_PHRASES):
        return "support"
    return "generic"


def format_reply_intent_mix_json(report: dict[str, Any]) -> str:
    """Render a reply intent mix report as deterministic JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_intent_mix_text(report: dict[str, Any]) -> str:
    """Render a concise human-readable intent mix report."""

    counts = report["counts"]
    filters = report["filters"]
    lines = [
        "Reply Intent Mix Report",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} platform={filters['platform'] or 'all'} "
            f"include_reviewed={filters['include_reviewed']}"
        ),
        (
            f"Rows: scanned={counts['rows_scanned']} pending={counts['pending_replies']} "
            f"reviewed={counts['reviewed_replies']} generic={counts['generic_replies']} "
            f"generic_share={counts['generic_share']:.1%}"
        ),
    ]
    if report["by_intent"]:
        lines.append("Intents: " + ", ".join(
            f"{item['intent']}={item['count']} ({item['share']:.1%})"
            for item in report["by_intent"]
        ))
    else:
        lines.append("No reply drafts matched.")

    if report["recommendations"]:
        lines.append("Recommendations: " + " ".join(report["recommendations"]))

    if report["groups"]:
        lines.append("")
        lines.append("Groups:")
        for group in report["groups"][:12]:
            lines.append(
                f"- {group['intent']} {group['platform']} {group['relationship_tier']} "
                f"status={group['status']} outcome={group['review_outcome']} count={group['count']}"
            )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    platform: str | None,
    include_reviewed: bool,
) -> list[dict[str, Any]]:
    if not columns:
        return []

    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "platform", "'x'"),
        _column_expr(columns, "status", "'pending'"),
        _column_expr(columns, "intent"),
        _column_expr(columns, "reply_intent"),
        _column_expr(columns, "classification_intent"),
        _column_expr(columns, "classifier_intent"),
        _column_expr(columns, "relationship_context"),
        _column_expr(columns, "inbound_text"),
        _column_expr(columns, "draft_text"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "posted_at"),
        _column_expr(columns, "posted_tweet_id"),
        _column_expr(columns, "posted_platform_id"),
    ]
    date_filter, date_param_count = _date_filter(columns, include_reviewed)
    filters: list[str] = []
    params: list[Any] = []
    if "status" in columns:
        pending_filter = "LOWER(COALESCE(status, 'pending')) = 'pending'"
        if include_reviewed:
            filters.append(f"({pending_filter} OR {date_filter})")
            params.extend([cutoff.isoformat()] * date_param_count)
        else:
            filters.append(pending_filter)
    else:
        filters.append(date_filter)
        params.extend([cutoff.isoformat()] * date_param_count)
    if platform and "platform" in columns:
        filters.append("LOWER(COALESCE(platform, 'x')) = ?")
        params.append(platform.lower())

    query = f"SELECT {', '.join(select_columns)} FROM reply_queue WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _date_filter(columns: set[str], include_reviewed: bool) -> tuple[str, int]:
    timestamp_columns = [
        column for column in ("detected_at", "reviewed_at", "posted_at") if column in columns
    ]
    if not timestamp_columns:
        return "1 = 1", 0
    clauses = [f"datetime({column}) >= datetime(?)" for column in timestamp_columns]
    if len(timestamp_columns) == 1:
        no_timestamp = f"{timestamp_columns[0]} IS NULL"
    else:
        no_timestamp = "COALESCE(" + ", ".join(timestamp_columns) + ") IS NULL"
    if include_reviewed:
        return "(" + no_timestamp + " OR " + " OR ".join(clauses) + ")", len(clauses)
    return f"({timestamp_columns[0]} IS NULL OR {clauses[0]})", 1


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return column
    return f"{default} AS {column}"


def _order_clause(columns: set[str]) -> str:
    parts = []
    for column in ("detected_at", "reviewed_at", "posted_at"):
        if column in columns:
            parts.append(f"datetime({column}) DESC")
            break
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _summarize_row(row: dict[str, Any], columns: set[str]) -> dict[str, Any]:
    status = _normalize_status(row.get("status"))
    return {
        "intent": infer_reply_intent(row, columns),
        "platform": str(row.get("platform") or "x").strip().lower() or "x",
        "relationship_tier": _relationship_tier(row.get("relationship_context")) or "unknown",
        "status": status,
        "review_outcome": _review_outcome(row, status),
    }


def _groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(
        (
            item["intent"],
            item["platform"],
            item["relationship_tier"],
            item["status"],
            item["review_outcome"],
        )
        for item in items
    )
    return [
        {
            "intent": intent,
            "platform": platform,
            "relationship_tier": relationship_tier,
            "status": status,
            "review_outcome": review_outcome,
            "count": count,
        }
        for (intent, platform, relationship_tier, status, review_outcome), count in sorted(
            counts.items()
        )
    ]


def _distribution(counts: Counter[str], total: int, key: str) -> list[dict[str, Any]]:
    return [
        {key: value, "count": count, "share": round(count / total, 4) if total else 0.0}
        for value, count in sorted(counts.items())
    ]


def _recommendations(generic_share: float, generic_count: int, total: int) -> list[str]:
    if total and generic_share >= GENERIC_SHARE_RECOMMENDATION_THRESHOLD:
        return [
            (
                "Generic replies are high; review prompt/classifier coverage and add more "
                "specific question, thanks, correction, or support handling."
            )
        ]
    if generic_count:
        return []
    return []


def _normalize_intent(value: Any) -> str | None:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    if not normalized:
        return None
    return _INTENT_ALIASES.get(normalized, normalized)


def _normalize_status(value: Any) -> str:
    return str(value or "pending").strip().lower() or "pending"


def _review_outcome(row: dict[str, Any], status: str) -> str:
    if status in PENDING_STATUSES:
        return "pending"
    if status in {"posted", "sent"} or row.get("posted_at") or row.get("posted_tweet_id") or row.get("posted_platform_id"):
        return "sent"
    if status in {"dismissed", "rejected", "failed", "spam"}:
        return "dismissed"
    if status in {"expired"}:
        return "expired"
    if status in {"approved", "reviewed", "ready"} or row.get("reviewed_at"):
        return "reviewed"
    return status


def _relationship_tier(relationship_context: Any) -> str | None:
    context = _parse_json_object(relationship_context)
    if not context:
        return None
    tier_name = context.get("tier_name")
    tier = context.get("dunbar_tier")
    if tier_name and tier is not None:
        return f"{tier_name} (tier {tier})"
    if tier_name:
        return str(tier_name)
    if tier is not None:
        return f"tier {tier}"
    return None


def _parse_json_object(value: Any) -> dict[str, Any] | None:
    if not value:
        return None
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _normalize(text: str) -> str:
    value = _URL_RE.sub(" ", text.lower())
    value = re.sub(r"@\w+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
