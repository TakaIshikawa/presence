"""Detect repetitive planned-topic angles inside active campaigns."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 20
DEFAULT_SIMILARITY_THRESHOLD = 0.72
DEFAULT_STATUSES = ("planned",)

_STOPWORDS = {
    "a",
    "about",
    "after",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "can",
    "for",
    "from",
    "how",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "our",
    "that",
    "the",
    "their",
    "this",
    "to",
    "use",
    "using",
    "we",
    "when",
    "with",
    "you",
    "your",
}


def build_campaign_angle_diversity_report(
    db_or_conn: Any,
    *,
    campaign_id: int | None = None,
    statuses: list[str] | tuple[str, ...] | None = None,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a report of near-duplicate planned-topic angles."""
    if limit < 0:
        raise ValueError("limit must be non-negative")
    if not 0 <= similarity_threshold <= 1:
        raise ValueError("similarity_threshold must be between 0 and 1")

    selected_statuses = _normalize_statuses(statuses)
    conn = getattr(db_or_conn, "conn", db_or_conn)
    schema = _schema(conn)
    now = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {
        "campaign_id": campaign_id,
        "statuses": list(selected_statuses),
        "similarity_threshold": similarity_threshold,
        "limit": limit,
    }
    missing = [
        table
        for table in ("content_campaigns", "planned_topics")
        if table not in schema
    ]
    if missing:
        return _empty_report(now, filters, missing)

    campaigns = _resolve_campaigns(conn, campaign_id)
    if campaign_id is not None and not campaigns:
        raise ValueError(f"Campaign {campaign_id} does not exist")

    campaign_reports = []
    duplicate_groups: list[dict[str, Any]] = []
    considered_topic_count = 0
    for campaign in campaigns:
        topics = _load_topics(conn, campaign["id"], selected_statuses)
        considered_topic_count += len(topics)
        groups = _duplicate_groups(
            campaign=campaign,
            topics=topics,
            similarity_threshold=similarity_threshold,
        )
        campaign_reports.append(
            {
                "campaign": campaign,
                "considered_topic_count": len(topics),
                "duplicate_group_count": len(groups),
                "duplicate_groups": groups,
                "summary": _campaign_summary(campaign, topics, groups),
            }
        )
        duplicate_groups.extend(groups)

    duplicate_groups.sort(
        key=lambda group: (
            -group["similarity_score"],
            group["campaign_id"],
            group["planned_topic_ids"],
        )
    )
    limited_groups = duplicate_groups[:limit]
    healthy = not limited_groups
    return {
        "generated_at": now.isoformat(),
        "filters": filters,
        "summary": {
            "campaign_count": len(campaigns),
            "considered_topic_count": considered_topic_count,
            "duplicate_group_count": len(limited_groups),
            "healthy": healthy,
            "message": (
                "Campaign angles look healthy; no near-duplicate planned topics found."
                if healthy
                else f"Found {len(limited_groups)} near-duplicate planned-topic group(s)."
            ),
        },
        "campaigns": campaign_reports,
        "duplicate_groups": limited_groups,
        "missing_required_tables": [],
    }


def format_campaign_angle_diversity_json(report: dict[str, Any]) -> str:
    """Render a campaign angle diversity report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_campaign_angle_diversity_text(report: dict[str, Any]) -> str:
    """Render a campaign angle diversity report for terminal review."""
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Campaign angle diversity",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"campaign_id={filters.get('campaign_id') or 'active'} "
            f"statuses={','.join(filters['statuses'])} "
            f"threshold={filters['similarity_threshold']} "
            f"limit={filters['limit']}"
        ),
        (
            "Totals: "
            f"campaigns={summary['campaign_count']} "
            f"topics={summary['considered_topic_count']} "
            f"duplicate_groups={summary['duplicate_group_count']}"
        ),
        f"Summary: {summary['message']}",
        "",
    ]
    if not report["duplicate_groups"]:
        return "\n".join(lines).rstrip() + "\n"

    lines.append("Duplicate Groups")
    for group in report["duplicate_groups"]:
        lines.append(
            f"- Campaign #{group['campaign_id']} {group['campaign_name']}: "
            f"score={group['similarity_score']:.3f} "
            f"action={group['recommended_action']}"
        )
        lines.append(f"  planned_topic_ids: {', '.join(map(str, group['planned_topic_ids']))}")
        lines.append(f"  shared_tokens: {', '.join(group['shared_tokens']) or '-'}")
        lines.append(f"  reason: {group['recommendation_reason']}")
        for topic in group["topics"]:
            lines.append(
                f"  - #{topic['planned_topic_id']} [{topic['status']}] "
                f"{topic['topic']} :: {topic['angle'] or '-'}"
            )
    return "\n".join(lines) + "\n"


def _normalize_statuses(statuses: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    values = statuses or DEFAULT_STATUSES
    result = []
    seen = set()
    for value in values:
        for part in str(value or "").split(","):
            status = part.strip()
            if not status or status in seen:
                continue
            seen.add(status)
            result.append(status)
    if not result:
        raise ValueError("at least one status must be selected")
    return tuple(result)


def _resolve_campaigns(
    conn: sqlite3.Connection,
    campaign_id: int | None,
) -> list[dict[str, Any]]:
    if campaign_id is not None:
        row = conn.execute(
            "SELECT * FROM content_campaigns WHERE id = ?",
            (campaign_id,),
        ).fetchone()
        return [dict(row)] if row else []
    rows = conn.execute(
        """SELECT *
           FROM content_campaigns
           WHERE status = 'active'
           ORDER BY start_date ASC NULLS LAST, created_at ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_topics(
    conn: sqlite3.Connection,
    campaign_id: int,
    statuses: tuple[str, ...],
) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in statuses)
    rows = conn.execute(
        f"""SELECT id, campaign_id, topic, angle, status, target_date, created_at
            FROM planned_topics
            WHERE campaign_id = ?
              AND status IN ({placeholders})
            ORDER BY target_date ASC NULLS LAST, created_at ASC, id ASC""",
        (campaign_id, *statuses),
    ).fetchall()
    return [dict(row) for row in rows]


def _duplicate_groups(
    *,
    campaign: dict[str, Any],
    topics: list[dict[str, Any]],
    similarity_threshold: float,
) -> list[dict[str, Any]]:
    pairs = []
    for left_index, left in enumerate(topics):
        for right in topics[left_index + 1 :]:
            comparison = _compare_topics(left, right)
            if comparison["similarity_score"] >= similarity_threshold:
                pairs.append(comparison)
    if not pairs:
        return []

    by_id = {int(topic["id"]): topic for topic in topics}
    graph: dict[int, set[int]] = defaultdict(set)
    for pair in pairs:
        left_id, right_id = pair["planned_topic_ids"]
        graph[left_id].add(right_id)
        graph[right_id].add(left_id)

    groups = []
    visited: set[int] = set()
    for topic_id in sorted(graph):
        if topic_id in visited:
            continue
        stack = [topic_id]
        component: set[int] = set()
        while stack:
            current = stack.pop()
            if current in component:
                continue
            component.add(current)
            stack.extend(graph[current] - component)
        visited |= component
        component_pairs = [
            pair
            for pair in pairs
            if set(pair["planned_topic_ids"]).issubset(component)
        ]
        groups.append(_group_report(campaign, by_id, component, component_pairs))

    groups.sort(key=lambda group: (-group["similarity_score"], group["planned_topic_ids"]))
    return groups


def _compare_topics(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    left_text = _topic_text(left)
    right_text = _topic_text(right)
    sequence_similarity = SequenceMatcher(None, left_text, right_text).ratio()
    left_tokens = set(_tokens(left_text))
    right_tokens = set(_tokens(right_text))
    shared_tokens = sorted(left_tokens & right_tokens)
    union = left_tokens | right_tokens
    token_overlap = len(shared_tokens) / len(union) if union else 0.0
    similarity_score = round((sequence_similarity * 0.65) + (token_overlap * 0.35), 3)
    return {
        "planned_topic_ids": [int(left["id"]), int(right["id"])],
        "similarity_score": similarity_score,
        "sequence_similarity": round(sequence_similarity, 3),
        "token_overlap": round(token_overlap, 3),
        "shared_tokens": shared_tokens,
    }


def _group_report(
    campaign: dict[str, Any],
    by_id: dict[int, dict[str, Any]],
    component: set[int],
    pairs: list[dict[str, Any]],
) -> dict[str, Any]:
    topic_ids = sorted(component)
    topics = [_topic_summary(by_id[topic_id]) for topic_id in topic_ids]
    shared_tokens = sorted(set().union(*(set(pair["shared_tokens"]) for pair in pairs)))
    max_similarity = max(pair["similarity_score"] for pair in pairs)
    action, reason = _recommend_action([by_id[topic_id] for topic_id in topic_ids], pairs)
    return {
        "campaign_id": int(campaign["id"]),
        "campaign_name": campaign.get("name") or f"Campaign {campaign['id']}",
        "planned_topic_ids": topic_ids,
        "similarity_score": max_similarity,
        "shared_tokens": shared_tokens,
        "recommended_action": action,
        "recommendation_reason": reason,
        "topics": topics,
        "pairs": sorted(pairs, key=lambda pair: (-pair["similarity_score"], pair["planned_topic_ids"])),
    }


def _recommend_action(
    topics: list[dict[str, Any]],
    pairs: list[dict[str, Any]],
) -> tuple[str, str]:
    normalized_topics = {_normalize_text(topic.get("topic")) for topic in topics}
    normalized_angles = {_normalize_text(topic.get("angle")) for topic in topics}
    best_pair = max(pairs, key=lambda pair: pair["similarity_score"])
    target_dates = {str(topic.get("target_date") or "").strip() for topic in topics}

    if len(normalized_topics) == 1 and len(normalized_angles) == 1:
        return (
            "merge_topics",
            "Topic and angle are effectively identical; keep one planned topic and remove or merge the rest.",
        )
    if len(normalized_topics) == 1:
        return (
            "rewrite_angle",
            "Topics match, but the angles are too close; rewrite one angle around a distinct audience, constraint, or example.",
        )
    if len(target_dates) > 1 and best_pair["similarity_score"] < 0.88:
        return (
            "move_later",
            "Angles overlap but are not identical; separate them in the campaign sequence or replace one with a fresher angle.",
        )
    return (
        "keep",
        "Similarity is near the threshold; keep only if the planned executions will use clearly different evidence or formats.",
    )


def _campaign_summary(
    campaign: dict[str, Any],
    topics: list[dict[str, Any]],
    groups: list[dict[str, Any]],
) -> dict[str, Any]:
    healthy = not groups
    return {
        "campaign_id": int(campaign["id"]),
        "campaign_name": campaign.get("name") or f"Campaign {campaign['id']}",
        "considered_topic_count": len(topics),
        "duplicate_group_count": len(groups),
        "healthy": healthy,
        "message": (
            "Campaign angles look healthy; no near-duplicate planned topics found."
            if healthy
            else f"Found {len(groups)} near-duplicate planned-topic group(s)."
        ),
    }


def _topic_summary(topic: dict[str, Any]) -> dict[str, Any]:
    return {
        "planned_topic_id": int(topic["id"]),
        "topic": topic.get("topic"),
        "angle": topic.get("angle"),
        "status": topic.get("status"),
        "target_date": topic.get("target_date"),
    }


def _topic_text(topic: dict[str, Any]) -> str:
    return _normalize_text(f"{topic.get('topic') or ''} {topic.get('angle') or ''}")


def _tokens(text: str) -> list[str]:
    terms = []
    for token in re.findall(r"[a-z][a-z0-9_-]{2,}", text.lower()):
        token = token.replace("_", "-").strip("-")
        if token and token not in _STOPWORDS and not token.isdigit():
            terms.append(_singularize(token))
    return terms


def _singularize(term: str) -> str:
    if len(term) > 4 and term.endswith("ies"):
        return term[:-3] + "y"
    if len(term) > 4 and term.endswith("s") and not term.endswith("ss"):
        return term[:-1]
    return term


def _normalize_text(value: Any) -> str:
    text = str(value or "").strip().lower()
    return re.sub(r"\s+", " ", text)


def _empty_report(
    now: datetime,
    filters: dict[str, Any],
    missing_required_tables: list[str],
) -> dict[str, Any]:
    return {
        "generated_at": now.isoformat(),
        "filters": filters,
        "summary": {
            "campaign_count": 0,
            "considered_topic_count": 0,
            "duplicate_group_count": 0,
            "healthy": True,
            "message": "Campaign angle diversity could not be checked because required tables are missing.",
        },
        "campaigns": [],
        "duplicate_groups": [],
        "missing_required_tables": missing_required_tables,
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
