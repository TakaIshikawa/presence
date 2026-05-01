"""Find planned-topic overlap across active campaigns."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime
from difflib import SequenceMatcher
import json
import re
import sqlite3
from typing import Any


DEFAULT_MIN_SIMILARITY = 0.72

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


@dataclass(frozen=True)
class CampaignCannibalizationTopic:
    planned_topic_id: int
    campaign_id: int
    campaign_name: str
    topic: str
    angle: str | None
    source_material: str | None
    target_date: str | None
    status: str | None
    content_id: int | None
    suggested_action: str


@dataclass(frozen=True)
class CampaignCannibalizationPair:
    planned_topic_ids: list[int]
    campaign_ids: list[int]
    similarity_score: float
    text_similarity: float
    token_overlap: float
    date_proximity: float
    shared_tokens: list[str]


@dataclass(frozen=True)
class CampaignCannibalizationGroup:
    group_id: int
    campaign_ids: list[int]
    campaign_names: list[str]
    planned_topic_ids: list[int]
    similarity_score: float
    shared_tokens: list[str]
    suggested_actions: dict[str, str]
    recommendation_reason: str
    topics: list[CampaignCannibalizationTopic]
    pairs: list[CampaignCannibalizationPair]


@dataclass(frozen=True)
class CampaignCannibalizationReport:
    artifact_type: str
    campaign_id: int | None
    min_similarity: float
    include_generated: bool
    campaign_count: int
    considered_topic_count: int
    overlap_group_count: int
    missing_required_tables: list[str]
    groups: list[CampaignCannibalizationGroup]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_campaign_cannibalization_report(
    db_or_conn: Any,
    campaign_id: int | None = None,
    min_similarity: float = DEFAULT_MIN_SIMILARITY,
    include_generated: bool = False,
) -> CampaignCannibalizationReport:
    """Build a deterministic report of planned-topic overlap across campaigns."""

    if campaign_id is not None and campaign_id <= 0:
        raise ValueError("campaign_id must be positive")
    if not 0 <= min_similarity <= 1:
        raise ValueError("min_similarity must be between 0 and 1")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    schema = _schema(conn)
    missing = [
        table for table in ("content_campaigns", "planned_topics") if table not in schema
    ]
    if missing:
        return _empty_report(campaign_id, min_similarity, include_generated, missing)

    campaigns = _load_campaigns(conn, schema=schema, campaign_id=campaign_id)
    if campaign_id is not None and not any(int(row["id"]) == campaign_id for row in campaigns):
        raise ValueError(f"Campaign {campaign_id} does not exist")

    campaign_ids = [int(campaign["id"]) for campaign in campaigns]
    topics = _load_topics(
        conn,
        schema=schema,
        campaign_ids=campaign_ids,
        include_generated=include_generated,
    )
    groups = _overlap_groups(topics, campaign_id=campaign_id, min_similarity=min_similarity)
    return CampaignCannibalizationReport(
        artifact_type="campaign_cannibalization",
        campaign_id=campaign_id,
        min_similarity=min_similarity,
        include_generated=include_generated,
        campaign_count=len(campaigns),
        considered_topic_count=len(topics),
        overlap_group_count=len(groups),
        missing_required_tables=[],
        groups=groups,
    )


def export_to_json(report: CampaignCannibalizationReport) -> str:
    """Serialize a campaign cannibalization report as stable JSON."""

    return json.dumps(report.as_dict(), indent=2, sort_keys=True)


def format_text_report(report: CampaignCannibalizationReport) -> str:
    """Render a campaign cannibalization report for terminal review."""

    lines = [
        "Campaign Cannibalization",
        (
            "Filters: "
            f"campaign_id={report.campaign_id if report.campaign_id is not None else 'active'} "
            f"min_similarity={report.min_similarity:.2f} "
            f"include_generated={str(report.include_generated).lower()}"
        ),
        (
            "Totals: "
            f"campaigns={report.campaign_count} "
            f"topics={report.considered_topic_count} "
            f"overlap_groups={report.overlap_group_count}"
        ),
    ]
    if report.missing_required_tables:
        lines.append(
            "Missing required tables: " + ", ".join(report.missing_required_tables)
        )
    if not report.groups:
        lines.append("")
        lines.append("No overlapping planned topics found across active campaigns.")
        return "\n".join(lines)

    lines.append("")
    for group in report.groups:
        lines.append(
            f"group #{group.group_id}: score={group.similarity_score:.3f} "
            f"campaigns={', '.join(group.campaign_names)} "
            f"topics={', '.join(map(str, group.planned_topic_ids))}"
        )
        lines.append(f"  shared_tokens: {', '.join(group.shared_tokens) or '-'}")
        lines.append(f"  reason: {group.recommendation_reason}")
        for topic in group.topics:
            lines.append(
                f"  - {topic.suggested_action}: campaign #{topic.campaign_id} "
                f"{topic.campaign_name} topic #{topic.planned_topic_id} "
                f"[{topic.status or '-'}] {topic.topic} :: {topic.angle or '-'}"
            )
    return "\n".join(lines)


def _load_campaigns(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    campaign_id: int | None,
) -> list[dict[str, Any]]:
    campaign_cols = schema.get("content_campaigns", set())
    order_cols = _order_columns(
        campaign_cols,
        [
            ("start_date", "ASC NULLS LAST"),
            ("created_at", "ASC"),
            ("id", "ASC"),
        ],
        alias="content_campaigns",
    )
    if campaign_id is not None:
        row = conn.execute(
            "SELECT * FROM content_campaigns WHERE id = ?",
            (campaign_id,),
        ).fetchone()
        if row is None:
            return []
        active_filter = "status = 'active'" if "status" in campaign_cols else "1"
        active_rows = conn.execute(
            f"""SELECT *
                FROM content_campaigns
                WHERE {active_filter} AND id != ?
                ORDER BY {', '.join(order_cols)}""",
            (campaign_id,),
        ).fetchall()
        return [dict(row), *[dict(active) for active in active_rows]]

    active_filter = "status = 'active'" if "status" in campaign_cols else "1"
    rows = conn.execute(
        f"""SELECT *
            FROM content_campaigns
            WHERE {active_filter}
            ORDER BY {', '.join(order_cols)}"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_topics(
    conn: sqlite3.Connection,
    *,
    schema: dict[str, set[str]],
    campaign_ids: list[int],
    include_generated: bool,
) -> list[dict[str, Any]]:
    if not campaign_ids:
        return []

    planned_cols = schema.get("planned_topics", set())
    if "campaign_id" not in planned_cols:
        return []

    select_cols = [
        "pt.id",
        _column_expr(planned_cols, "campaign_id", "pt", "campaign_id"),
        "pt.topic",
        _column_expr(planned_cols, "angle", "pt", "angle"),
        _column_expr(planned_cols, "source_material", "pt", "source_material"),
        _column_expr(planned_cols, "target_date", "pt", "target_date"),
        _column_expr(planned_cols, "status", "pt", "status"),
        _column_expr(planned_cols, "content_id", "pt", "content_id"),
        _column_expr(planned_cols, "created_at", "pt", "created_at"),
        "cc.name AS campaign_name",
    ]
    filters = [f"pt.campaign_id IN ({','.join('?' for _ in campaign_ids)})"]
    params: list[Any] = list(campaign_ids)
    if not include_generated:
        if "status" in planned_cols:
            filters.append("COALESCE(pt.status, 'planned') != 'generated'")
        if "content_id" in planned_cols:
            filters.append("pt.content_id IS NULL")

    order_cols = _order_columns(
        planned_cols,
        [
            ("target_date", "ASC NULLS LAST"),
            ("created_at", "ASC"),
            ("id", "ASC"),
        ],
        alias="pt",
    )

    rows = conn.execute(
        f"""SELECT {', '.join(select_cols)}
            FROM planned_topics pt
            INNER JOIN content_campaigns cc ON cc.id = pt.campaign_id
            WHERE {' AND '.join(filters)}
            ORDER BY {', '.join(order_cols)}""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _overlap_groups(
    topics: list[dict[str, Any]],
    *,
    campaign_id: int | None,
    min_similarity: float,
) -> list[CampaignCannibalizationGroup]:
    pairs: list[CampaignCannibalizationPair] = []
    for left_index, left in enumerate(topics):
        for right in topics[left_index + 1 :]:
            if int(left["campaign_id"]) == int(right["campaign_id"]):
                continue
            if campaign_id is not None and campaign_id not in {
                int(left["campaign_id"]),
                int(right["campaign_id"]),
            }:
                continue
            pair = _compare_topics(left, right)
            if pair.similarity_score >= min_similarity:
                pairs.append(pair)

    if not pairs:
        return []

    by_id = {int(topic["id"]): topic for topic in topics}
    graph: dict[int, set[int]] = defaultdict(set)
    for pair in pairs:
        left_id, right_id = pair.planned_topic_ids
        graph[left_id].add(right_id)
        graph[right_id].add(left_id)

    groups: list[CampaignCannibalizationGroup] = []
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
            pair for pair in pairs if set(pair.planned_topic_ids).issubset(component)
        ]
        groups.append(_group_report(len(groups) + 1, by_id, component, component_pairs))

    groups.sort(key=lambda group: (-group.similarity_score, group.planned_topic_ids))
    return [replace(group, group_id=index) for index, group in enumerate(groups, start=1)]


def _compare_topics(
    left: dict[str, Any],
    right: dict[str, Any],
) -> CampaignCannibalizationPair:
    left_text = _comparison_text(left)
    right_text = _comparison_text(right)
    text_similarity = SequenceMatcher(None, left_text, right_text).ratio()
    left_tokens = set(_tokens(left_text))
    right_tokens = set(_tokens(right_text))
    shared_tokens = sorted(left_tokens & right_tokens)
    union = left_tokens | right_tokens
    token_overlap = len(shared_tokens) / len(union) if union else 0.0
    date_proximity = _date_proximity(left.get("target_date"), right.get("target_date"))
    similarity = (text_similarity * 0.50) + (token_overlap * 0.35) + (date_proximity * 0.15)
    return CampaignCannibalizationPair(
        planned_topic_ids=sorted([int(left["id"]), int(right["id"])]),
        campaign_ids=sorted([int(left["campaign_id"]), int(right["campaign_id"])]),
        similarity_score=round(similarity, 3),
        text_similarity=round(text_similarity, 3),
        token_overlap=round(token_overlap, 3),
        date_proximity=round(date_proximity, 3),
        shared_tokens=shared_tokens,
    )


def _group_report(
    group_id: int,
    by_id: dict[int, dict[str, Any]],
    component: set[int],
    pairs: list[CampaignCannibalizationPair],
) -> CampaignCannibalizationGroup:
    topic_ids = sorted(component)
    rows = [by_id[topic_id] for topic_id in topic_ids]
    keep_id = _keep_topic_id(rows)
    topic_actions = {
        int(row["id"]): ("keep" if int(row["id"]) == keep_id else "defer")
        for row in rows
    }
    topics = [
        _topic_summary(row, suggested_action=topic_actions[int(row["id"])])
        for row in rows
    ]
    return CampaignCannibalizationGroup(
        group_id=group_id,
        campaign_ids=sorted({int(row["campaign_id"]) for row in rows}),
        campaign_names=sorted({str(row.get("campaign_name") or row["campaign_id"]) for row in rows}),
        planned_topic_ids=topic_ids,
        similarity_score=max(pair.similarity_score for pair in pairs),
        shared_tokens=sorted(set().union(*(set(pair.shared_tokens) for pair in pairs))),
        suggested_actions={str(topic_id): topic_actions[topic_id] for topic_id in topic_ids},
        recommendation_reason=_recommendation_reason(rows, pairs, keep_id),
        topics=topics,
        pairs=sorted(pairs, key=lambda pair: (-pair.similarity_score, pair.planned_topic_ids)),
    )


def _topic_summary(
    row: dict[str, Any],
    *,
    suggested_action: str,
) -> CampaignCannibalizationTopic:
    return CampaignCannibalizationTopic(
        planned_topic_id=int(row["id"]),
        campaign_id=int(row["campaign_id"]),
        campaign_name=str(row.get("campaign_name") or f"Campaign {row['campaign_id']}"),
        topic=str(row.get("topic") or ""),
        angle=row.get("angle"),
        source_material=row.get("source_material"),
        target_date=row.get("target_date"),
        status=row.get("status"),
        content_id=row.get("content_id"),
        suggested_action=suggested_action,
    )


def _keep_topic_id(rows: list[dict[str, Any]]) -> int:
    def sort_key(row: dict[str, Any]) -> tuple[bool, str, str, int]:
        target = str(row.get("target_date") or "")
        created = str(row.get("created_at") or "")
        return (not bool(target), target, created, int(row["id"]))

    return int(sorted(rows, key=sort_key)[0]["id"])


def _recommendation_reason(
    rows: list[dict[str, Any]],
    pairs: list[CampaignCannibalizationPair],
    keep_id: int,
) -> str:
    best_pair = max(pairs, key=lambda pair: pair.similarity_score)
    generated = [row for row in rows if row.get("status") == "generated" or row.get("content_id")]
    if generated:
        return (
            f"Topic #{keep_id} is the earliest slot; defer overlapping generated or later "
            "planned topics unless they can use a clearly different angle."
        )
    if best_pair.date_proximity >= 0.75:
        return (
            f"Topic #{keep_id} is the earliest slot; defer the other campaign slot because "
            "the angle and target dates are close."
        )
    return (
        f"Topic #{keep_id} is the earliest slot; keep it and defer or rewrite the other "
        "campaign topic to avoid repeating the same angle."
    )


def _comparison_text(row: dict[str, Any]) -> str:
    return _normalize_text(
        " ".join(
            str(row.get(key) or "")
            for key in ("topic", "angle", "source_material")
        )
    )


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


def _date_proximity(left: Any, right: Any) -> float:
    left_date = _parse_date(left)
    right_date = _parse_date(right)
    if left_date is None or right_date is None:
        return 0.0
    days = abs((left_date - right_date).days)
    if days == 0:
        return 1.0
    if days >= 30:
        return 0.0
    return max(0.0, 1.0 - (days / 30))


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _column_expr(columns: set[str], column: str, alias: str, output: str) -> str:
    if column in columns:
        return f"{alias}.{column}"
    return f"NULL AS {output}"


def _order_columns(
    columns: set[str],
    candidates: list[tuple[str, str]],
    *,
    alias: str,
) -> list[str]:
    return [
        f"{alias}.{column} {direction}"
        for column, direction in candidates
        if column in columns
    ] or ["1"]


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


def _empty_report(
    campaign_id: int | None,
    min_similarity: float,
    include_generated: bool,
    missing_required_tables: list[str],
) -> CampaignCannibalizationReport:
    return CampaignCannibalizationReport(
        artifact_type="campaign_cannibalization",
        campaign_id=campaign_id,
        min_similarity=min_similarity,
        include_generated=include_generated,
        campaign_count=0,
        considered_topic_count=0,
        overlap_group_count=0,
        missing_required_tables=missing_required_tables,
        groups=[],
    )
