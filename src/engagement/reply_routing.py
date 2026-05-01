"""Deterministic lane routing for pending inbound replies."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal


ReplyRoute = Literal[
    "reply",
    "quote_candidate",
    "ignore_spam",
    "escalate",
    "relationship_nurture",
]

ROUTE_ORDER: tuple[ReplyRoute, ...] = (
    "escalate",
    "relationship_nurture",
    "reply",
    "quote_candidate",
    "ignore_spam",
)
PRIORITY_RANK = {"high": 0, "normal": 1, "low": 2}
LOW_QUALITY_FLAGS = {
    "generic",
    "hallucinated",
    "low_quality",
    "low_value",
    "off_topic",
    "spam",
    "sycophantic",
    "unsafe",
}
SPAM_PHRASES = (
    "airdrop",
    "crypto giveaway",
    "dm me",
    "earn money",
    "follow back",
    "forex",
    "investment opportunity",
    "onlyfans",
    "work from home",
)
QUOTE_PHRASES = (
    "hot take",
    "the future of",
    "the real problem",
    "research shows",
    "data shows",
    "i learned",
    "lesson learned",
    "people underestimate",
    "nobody talks about",
)
URGENT_PHRASES = (
    "urgent",
    "asap",
    "security",
    "vulnerability",
    "broken",
    "crash",
    "error",
    "exception",
    "not working",
    "fails",
    "failure",
    "regression",
)


@dataclass(frozen=True)
class ReplyRouteRecommendation:
    """Recommended handling lane for a queued inbound reply."""

    reply_id: int
    route: ReplyRoute
    reason: str
    urgency: int
    review_owner: str
    platform: str
    author: str | None
    intent: str
    priority: str
    age_hours: float
    relationship_tier: str | None = None
    inbound_tweet_id: str | None = None
    inbound_text_preview: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def route_reply(
    row: dict[str, Any],
    *,
    now: datetime | None = None,
) -> ReplyRouteRecommendation:
    """Route one reply_queue-like row without mutating storage or calling an LLM."""

    now = _as_utc(now or datetime.now(timezone.utc))
    intent = str(row.get("intent") or "other").strip().lower() or "other"
    priority = _normalize_priority(row.get("priority"))
    quality_score = _float_or_none(row.get("quality_score"))
    quality_flags = _parse_json_list(row.get("quality_flags"))
    relationship = _parse_json_object(row.get("relationship_context"))
    metadata = _parse_json_object(row.get("platform_metadata"))
    text = str(row.get("inbound_text") or "")
    normalized_text = " ".join(text.lower().split())
    age_hours = round(_age_hours(row.get("detected_at"), now), 2)

    relationship_score = _relationship_score(relationship)
    relationship_rich = relationship_score >= 12
    question_like = intent in {"question", "bug_report"} or "?" in text
    urgent_text = any(phrase in normalized_text for phrase in URGENT_PHRASES)
    spam_signal = intent == "spam" or any(phrase in normalized_text for phrase in SPAM_PHRASES)
    low_quality = _is_low_quality(quality_score, quality_flags)
    high_priority = priority == "high"

    route: ReplyRoute
    reason: str
    owner: str

    if (spam_signal or low_quality) and high_priority:
        route = "escalate"
        reason = "high priority overrides spam or low quality"
        owner = "operator"
    elif spam_signal:
        route = "ignore_spam"
        reason = "spam signal"
        owner = "none"
    elif low_quality:
        route = "ignore_spam"
        reason = "very low quality"
        owner = "none"
    elif intent == "bug_report" or urgent_text or _metadata_flag(metadata, "needs_escalation"):
        route = "escalate"
        reason = "support or risk signal"
        owner = "support"
    elif question_like and relationship_rich and (high_priority or age_hours >= 12):
        route = "reply"
        reason = "relationship question with urgency"
        owner = "relationship"
    elif question_like and relationship_rich:
        route = "relationship_nurture"
        reason = "relationship-rich question"
        owner = "relationship"
    elif _is_quote_candidate(intent, normalized_text, metadata):
        route = "quote_candidate"
        reason = "quote-worthy public hook"
        owner = "editorial"
    else:
        route = "reply"
        reason = _default_reply_reason(intent, priority)
        owner = "community"

    urgency = _urgency(
        route=route,
        priority=priority,
        intent=intent,
        age_hours=age_hours,
        quality_score=quality_score,
        relationship_score=relationship_score,
        question_like=question_like,
        urgent_text=urgent_text,
        metadata=metadata,
    )
    return ReplyRouteRecommendation(
        reply_id=int(row.get("id") or row.get("rowid") or 0),
        route=route,
        reason=reason,
        urgency=urgency,
        review_owner=owner,
        platform=str(row.get("platform") or "x"),
        author=row.get("inbound_author_handle"),
        intent=intent,
        priority=priority,
        age_hours=age_hours,
        relationship_tier=_relationship_tier(relationship),
        inbound_tweet_id=row.get("inbound_tweet_id"),
        inbound_text_preview=_preview(text),
    )


def route_replies(
    rows: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    min_urgency: int | None = None,
    limit: int | None = None,
) -> list[ReplyRouteRecommendation]:
    """Route rows and return deterministic urgency-ordered recommendations."""

    if min_urgency is not None and min_urgency < 0:
        raise ValueError("min_urgency must be non-negative")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    recommendations = [route_reply(row, now=now) for row in rows]
    if min_urgency is not None:
        recommendations = [item for item in recommendations if item.urgency >= min_urgency]
    recommendations.sort(key=_route_sort_key)
    if limit is not None:
        recommendations = recommendations[:limit]
    return recommendations


def build_reply_routing_report(
    db: Any,
    *,
    limit: int | None = None,
    min_urgency: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a stable JSON-serializable routing report for pending reply_queue rows."""

    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    if min_urgency is not None and min_urgency < 0:
        raise ValueError("min_urgency must be non-negative")

    conn = _connection(db)
    now = _as_utc(now or datetime.now(timezone.utc))
    columns = _table_columns(conn, "reply_queue")
    rows = _pending_rows(conn, columns) if columns else []
    recommendations = route_replies(rows, now=now, min_urgency=min_urgency, limit=limit)
    items = [item.to_dict() for item in recommendations]
    return {
        "generated_at": now.isoformat(),
        "filters": {
            "limit": limit,
            "min_urgency": min_urgency,
        },
        "total_pending": len(items),
        "by_route": dict(Counter(item["route"] for item in items)),
        "by_review_owner": dict(Counter(item["review_owner"] for item in items)),
        "items": items,
    }


def apply_reply_routes(db: Any, recommendations: list[ReplyRouteRecommendation | dict[str, Any]]) -> dict[str, Any]:
    """Persist route metadata only when compatible storage already exists."""

    conn = _connection(db)
    if not recommendations:
        return {"applied": 0, "skipped": 0, "storage": None, "message": "no routes to apply"}
    normalized = [_coerce_recommendation(item) for item in recommendations]

    queue_result = _apply_to_reply_queue_columns(conn, normalized)
    if queue_result is not None:
        return queue_result

    table_result = _apply_to_route_table(conn, normalized)
    if table_result is not None:
        return table_result

    return {
        "applied": 0,
        "skipped": len(normalized),
        "storage": None,
        "message": "no compatible route storage found",
    }


def format_json_report(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_text_report(report: dict[str, Any]) -> str:
    lines = [
        "Reply Routing Matrix",
        f"Generated: {report['generated_at']}",
        f"Pending: {report['total_pending']}",
    ]
    filters = report["filters"]
    filter_parts = []
    if filters.get("limit") is not None:
        filter_parts.append(f"limit={filters['limit']}")
    if filters.get("min_urgency") is not None:
        filter_parts.append(f"min_urgency={filters['min_urgency']}")
    lines.append("Filters: " + (", ".join(filter_parts) if filter_parts else "none"))
    lines.append(f"Routes: {_format_counts(report['by_route'])}")
    lines.append(f"Owners: {_format_counts(report['by_review_owner'])}")
    apply_result = report.get("apply")
    if apply_result:
        lines.append(
            "Apply: "
            f"applied={apply_result['applied']} skipped={apply_result['skipped']} "
            f"storage={apply_result['storage'] or 'none'}"
        )
        if apply_result.get("message"):
            lines.append(f"Apply note: {apply_result['message']}")
    lines.append("")

    if not report["items"]:
        lines.append("No pending replies matched.")
        return "\n".join(lines)

    for item in report["items"]:
        author = f"@{item['author']}" if item.get("author") else "@unknown"
        tier = f" {item['relationship_tier']}" if item.get("relationship_tier") else ""
        target = item.get("inbound_tweet_id") or f"reply_queue:{item['reply_id']}"
        lines.append(
            f"#{item['reply_id']} u={item['urgency']:03d} {item['route']} "
            f"{item['review_owner']} {item['platform']} {author}{tier} "
            f"{item['reason']} target={target}"
        )
    return "\n".join(lines)


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _coerce_recommendation(item: ReplyRouteRecommendation | dict[str, Any]) -> ReplyRouteRecommendation:
    if isinstance(item, ReplyRouteRecommendation):
        return item
    return ReplyRouteRecommendation(
        reply_id=int(item.get("reply_id") or 0),
        route=item.get("route") or "reply",
        reason=str(item.get("reason") or ""),
        urgency=int(item.get("urgency") or 0),
        review_owner=str(item.get("review_owner") or "community"),
        platform=str(item.get("platform") or "x"),
        author=item.get("author"),
        intent=str(item.get("intent") or "other"),
        priority=_normalize_priority(item.get("priority")),
        age_hours=float(item.get("age_hours") or 0.0),
        relationship_tier=item.get("relationship_tier"),
        inbound_tweet_id=item.get("inbound_tweet_id"),
        inbound_text_preview=item.get("inbound_text_preview"),
    )


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _pending_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    filters = []
    if "status" in columns:
        filters.append("COALESCE(status, 'pending') = 'pending'")
    query = "SELECT rowid, * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    cursor = conn.execute(query)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "priority" in columns:
        parts.append(
            "CASE priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 WHEN 'low' THEN 2 ELSE 3 END"
        )
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _apply_to_reply_queue_columns(
    conn: sqlite3.Connection,
    recommendations: list[ReplyRouteRecommendation],
) -> dict[str, Any] | None:
    columns = _table_columns(conn, "reply_queue")
    candidate_sets = [
        {
            "route": "route",
            "reason": "route_reason",
            "urgency": "route_urgency",
            "review_owner": "review_owner",
        },
        {
            "route": "routing_route",
            "reason": "routing_reason",
            "urgency": "routing_urgency",
            "review_owner": "routing_review_owner",
        },
    ]
    mapping = next(
        (candidate for candidate in candidate_sets if set(candidate.values()).issubset(columns)),
        None,
    )
    if mapping is None:
        return None

    updated_at_column = next(
        (column for column in ("routed_at", "routing_updated_at", "updated_at") if column in columns),
        None,
    )
    assignments = [
        f"{mapping['route']} = ?",
        f"{mapping['reason']} = ?",
        f"{mapping['urgency']} = ?",
        f"{mapping['review_owner']} = ?",
    ]
    if updated_at_column:
        assignments.append(f"{updated_at_column} = ?")
    params_now = datetime.now(timezone.utc).isoformat()
    applied = 0
    for item in recommendations:
        if not item.reply_id:
            continue
        params: list[Any] = [item.route, item.reason, item.urgency, item.review_owner]
        if updated_at_column:
            params.append(params_now)
        params.append(item.reply_id)
        conn.execute(
            f"UPDATE reply_queue SET {', '.join(assignments)} WHERE id = ?",
            params,
        )
        applied += 1
    conn.commit()
    return {
        "applied": applied,
        "skipped": len(recommendations) - applied,
        "storage": "reply_queue",
        "message": "updated route columns",
    }


def _apply_to_route_table(
    conn: sqlite3.Connection,
    recommendations: list[ReplyRouteRecommendation],
) -> dict[str, Any] | None:
    for table in ("reply_routes", "reply_routing"):
        columns = _table_columns(conn, table)
        required = {"reply_queue_id", "route", "reason", "urgency", "review_owner"}
        if not required.issubset(columns):
            continue

        optional = [column for column in ("created_at", "updated_at", "metadata") if column in columns]
        insert_columns = ["reply_queue_id", "route", "reason", "urgency", "review_owner", *optional]
        placeholders = ", ".join("?" for _ in insert_columns)
        now = datetime.now(timezone.utc).isoformat()
        applied = 0
        for item in recommendations:
            if not item.reply_id:
                continue
            values: list[Any] = [
                item.reply_id,
                item.route,
                item.reason,
                item.urgency,
                item.review_owner,
            ]
            for column in optional:
                if column == "metadata":
                    values.append(json.dumps(item.to_dict(), sort_keys=True))
                else:
                    values.append(now)
            conn.execute(
                f"INSERT INTO {table} ({', '.join(insert_columns)}) VALUES ({placeholders})",
                values,
            )
            applied += 1
        conn.commit()
        return {
            "applied": applied,
            "skipped": len(recommendations) - applied,
            "storage": table,
            "message": "inserted route rows",
        }
    return None


def _route_sort_key(item: ReplyRouteRecommendation) -> tuple[Any, ...]:
    return (
        -item.urgency,
        ROUTE_ORDER.index(item.route),
        PRIORITY_RANK.get(item.priority, 3),
        -item.age_hours,
        item.reply_id,
    )


def _urgency(
    *,
    route: ReplyRoute,
    priority: str,
    intent: str,
    age_hours: float,
    quality_score: float | None,
    relationship_score: int,
    question_like: bool,
    urgent_text: bool,
    metadata: dict[str, Any],
) -> int:
    score = {
        "escalate": 78,
        "relationship_nurture": 62,
        "reply": 55,
        "quote_candidate": 42,
        "ignore_spam": 8,
    }[route]
    score += {"high": 14, "normal": 0, "low": -10}.get(priority, 0)
    if intent == "bug_report":
        score += 8
    elif intent == "question":
        score += 6
    elif intent == "appreciation":
        score -= 8
    if question_like:
        score += 4
    if urgent_text:
        score += 8
    if age_hours >= 48:
        score += 10
    elif age_hours >= 24:
        score += 6
    elif age_hours >= 6:
        score += 3
    score += min(12, relationship_score // 2)
    conversation_depth = _float_or_none(
        metadata.get("conversation_depth") or metadata.get("reply_depth")
    )
    if conversation_depth is not None:
        score += int(min(8, max(0, conversation_depth) * 2))
    if quality_score is not None and quality_score >= 8:
        score += 3
    return max(0, min(100, int(round(score))))


def _relationship_score(context: dict[str, Any]) -> int:
    score = 0
    tier = _float_or_none(context.get("dunbar_tier"))
    if tier is not None:
        if tier <= 1:
            score += 18
        elif tier <= 2:
            score += 14
        elif tier <= 3:
            score += 8
        else:
            score += 3
    strength = _float_or_none(context.get("relationship_strength"))
    if strength is not None:
        score += int(max(0, min(1, strength)) * 12)
    stage = _float_or_none(context.get("engagement_stage"))
    if stage is not None:
        score += int(max(0, min(5, stage)) * 2)
    if context.get("is_known") is True:
        score += 5
    if context.get("tier_name"):
        score += 3
    return score


def _relationship_tier(context: dict[str, Any]) -> str | None:
    tier_name = context.get("tier_name")
    tier = context.get("dunbar_tier")
    if tier_name and tier is not None:
        return f"{tier_name} (tier {tier})"
    if tier_name:
        return str(tier_name)
    if tier is not None:
        return f"tier {tier}"
    return None


def _is_low_quality(quality_score: float | None, flags: list[Any]) -> bool:
    if quality_score is not None and quality_score < 3:
        return True
    normalized_flags = {str(flag).lower() for flag in flags}
    return bool(normalized_flags & LOW_QUALITY_FLAGS)


def _is_quote_candidate(intent: str, text: str, metadata: dict[str, Any]) -> bool:
    if intent == "disagreement":
        return True
    if any(phrase in text for phrase in QUOTE_PHRASES):
        return True
    return _metadata_flag(metadata, "quote_candidate") or _metadata_flag(metadata, "public_hook")


def _metadata_flag(metadata: dict[str, Any], key: str) -> bool:
    value = metadata.get(key)
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes"}
    return value is True


def _default_reply_reason(intent: str, priority: str) -> str:
    if priority == "high":
        return "high priority reply"
    if intent == "question":
        return "direct question"
    if intent == "appreciation":
        return "acknowledge appreciation"
    return "standard reply"


def _normalize_priority(priority: Any) -> str:
    value = str(priority or "normal").strip().lower()
    return value if value in PRIORITY_RANK else "normal"


def _parse_json_object(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_json_list(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _age_hours(detected_at: Any, now: datetime) -> float:
    detected = _parse_datetime(detected_at)
    if detected is None:
        return 0.0
    return max(0.0, (_as_utc(now) - _as_utc(detected)).total_seconds() / 3600)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _preview(value: Any, max_len: int = 96) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "..."


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "{}"
    return ", ".join(f"{key}={counts[key]}" for key in sorted(counts))
