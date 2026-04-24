"""Coverage reporting for planned topics against approved knowledge."""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any

from evaluation.topic_extractor import TOPIC_TAXONOMY
from synthesis.content_gaps import classify_source_topics


TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9+#._-]*")
MIN_MATCH_SCORE = 1.0
TOP_SOURCE_LIMIT = 5
SUGGESTION_LIMIT = 6

STOPWORDS = {
    "about",
    "across",
    "after",
    "against",
    "also",
    "and",
    "are",
    "before",
    "between",
    "build",
    "can",
    "from",
    "has",
    "have",
    "how",
    "into",
    "its",
    "more",
    "our",
    "that",
    "the",
    "their",
    "this",
    "through",
    "topic",
    "what",
    "when",
    "where",
    "which",
    "with",
    "why",
    "will",
}


@dataclass(frozen=True)
class MatchingSource:
    knowledge_id: int
    source_type: str
    author: str | None
    source_id: str | None
    source_url: str | None
    score: float
    matched_terms: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class TopicCoverage:
    planned_topic_id: int
    topic: str
    angle: str | None
    campaign_id: int | None
    campaign_name: str | None
    target_date: str | None
    status: str
    source_count: int
    min_sources: int
    matched_knowledge_ids: list[int]
    source_authors: list[str]
    source_types: list[str]
    top_matching_sources: list[MatchingSource]
    suggested_search_terms: list[str]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["top_matching_sources"] = [
            source.to_dict() for source in self.top_matching_sources
        ]
        return data


@dataclass(frozen=True)
class PlannedTopicCoverageReport:
    campaign_id: int | None
    min_sources: int
    planned_topic_count: int
    covered_topics: list[TopicCoverage]
    weakly_covered_topics: list[TopicCoverage]
    missing_topics: list[TopicCoverage]
    top_matching_sources: list[MatchingSource]

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "min_sources": self.min_sources,
            "planned_topic_count": self.planned_topic_count,
            "covered_topics": [topic.to_dict() for topic in self.covered_topics],
            "weakly_covered_topics": [
                topic.to_dict() for topic in self.weakly_covered_topics
            ],
            "missing_topics": [topic.to_dict() for topic in self.missing_topics],
            "top_matching_sources": [
                source.to_dict() for source in self.top_matching_sources
            ],
        }


def build_planned_topic_coverage_report(
    db,
    *,
    campaign_id: int | None = None,
    min_sources: int = 2,
) -> PlannedTopicCoverageReport:
    """Compare planned topics with approved knowledge snippets."""
    if min_sources < 1:
        raise ValueError("min_sources must be at least 1")
    if campaign_id is not None and db.get_campaign(campaign_id) is None:
        raise ValueError(f"Campaign {campaign_id} does not exist")

    planned_topics = _planned_topics(db, campaign_id)
    knowledge_rows = _approved_knowledge(db)
    coverages = [
        _build_topic_coverage(topic, knowledge_rows, min_sources)
        for topic in planned_topics
    ]

    covered = [topic for topic in coverages if topic.status == "covered"]
    weak = [topic for topic in coverages if topic.status == "weak"]
    missing = [topic for topic in coverages if topic.status == "missing"]

    return PlannedTopicCoverageReport(
        campaign_id=campaign_id,
        min_sources=min_sources,
        planned_topic_count=len(coverages),
        covered_topics=covered,
        weakly_covered_topics=weak,
        missing_topics=missing,
        top_matching_sources=_top_matching_sources(coverages),
    )


def _planned_topics(db, campaign_id: int | None) -> list[dict[str, Any]]:
    params: list[Any] = []
    where = ["pt.status = 'planned'"]
    if campaign_id is not None:
        where.append("pt.campaign_id = ?")
        params.append(campaign_id)
    rows = db.conn.execute(
        f"""SELECT pt.*,
                  cc.name AS campaign_name
           FROM planned_topics pt
           LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
           WHERE {" AND ".join(where)}
           ORDER BY cc.start_date ASC NULLS LAST,
                    pt.campaign_id ASC NULLS LAST,
                    pt.target_date ASC NULLS LAST,
                    pt.created_at ASC,
                    pt.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _approved_knowledge(db) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        """SELECT id, source_type, source_id, source_url, author, content, insight
           FROM knowledge
           WHERE approved = 1
           ORDER BY id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _build_topic_coverage(
    planned_topic: dict[str, Any],
    knowledge_rows: list[dict[str, Any]],
    min_sources: int,
) -> TopicCoverage:
    matches = [
        match
        for row in knowledge_rows
        if (match := _match_source(planned_topic, row)) is not None
    ]
    matches.sort(
        key=lambda source: (
            -source.score,
            source.source_type,
            source.author or "",
            source.knowledge_id,
        )
    )

    matched_ids = [source.knowledge_id for source in matches]
    source_authors = sorted(
        {source.author for source in matches if source.author},
        key=lambda value: value.lower(),
    )
    source_types = sorted({source.source_type for source in matches})
    status = _coverage_status(len(matches), min_sources)

    return TopicCoverage(
        planned_topic_id=int(planned_topic["id"]),
        topic=planned_topic["topic"],
        angle=planned_topic.get("angle"),
        campaign_id=planned_topic.get("campaign_id"),
        campaign_name=planned_topic.get("campaign_name"),
        target_date=planned_topic.get("target_date"),
        status=status,
        source_count=len(matches),
        min_sources=min_sources,
        matched_knowledge_ids=matched_ids,
        source_authors=source_authors,
        source_types=source_types,
        top_matching_sources=matches[:TOP_SOURCE_LIMIT],
        suggested_search_terms=(
            _suggest_search_terms(planned_topic) if len(matches) < min_sources else []
        ),
    )


def _match_source(
    planned_topic: dict[str, Any],
    knowledge_row: dict[str, Any],
) -> MatchingSource | None:
    topic_text = " ".join(
        str(value or "")
        for value in (
            planned_topic.get("topic"),
            planned_topic.get("angle"),
            planned_topic.get("source_material"),
        )
    )
    source_text = " ".join(
        str(value or "")
        for value in (knowledge_row.get("insight"), knowledge_row.get("content"))
    )
    topic_tokens = _tokens(topic_text)
    source_tokens = _tokens(source_text)
    overlap = sorted(topic_tokens & source_tokens)

    score = float(len(overlap))
    taxonomy_matches = _taxonomy_matches(planned_topic, source_text)
    if taxonomy_matches:
        score += 1.5 * len(taxonomy_matches)

    if score < MIN_MATCH_SCORE:
        return None

    return MatchingSource(
        knowledge_id=int(knowledge_row["id"]),
        source_type=knowledge_row["source_type"],
        author=knowledge_row.get("author"),
        source_id=knowledge_row.get("source_id"),
        source_url=knowledge_row.get("source_url"),
        score=round(score, 3),
        matched_terms=sorted(set(overlap + taxonomy_matches)),
    )


def _tokens(text: str | None) -> set[str]:
    return {
        token.strip("._-")
        for token in TOKEN_RE.findall((text or "").lower())
        if len(token.strip("._-")) >= 3 and token.strip("._-") not in STOPWORDS
    }


def _taxonomy_matches(planned_topic: dict[str, Any], source_text: str) -> list[str]:
    planned_taxonomy = [
        topic
        for topic in classify_source_topics(
            f"{planned_topic.get('topic') or ''} {planned_topic.get('angle') or ''}"
        )
        if topic in TOPIC_TAXONOMY
    ]
    if planned_topic.get("topic") in TOPIC_TAXONOMY:
        planned_taxonomy.insert(0, planned_topic["topic"])

    source_taxonomy = set(classify_source_topics(source_text))
    return sorted(set(planned_taxonomy) & source_taxonomy)


def _coverage_status(source_count: int, min_sources: int) -> str:
    if source_count >= min_sources:
        return "covered"
    if source_count > 0:
        return "weak"
    return "missing"


def _suggest_search_terms(planned_topic: dict[str, Any]) -> list[str]:
    tokens = _tokens(
        f"{planned_topic.get('topic') or ''} {planned_topic.get('angle') or ''}"
    )
    counts = Counter(tokens)
    ordered = sorted(counts, key=lambda token: (-counts[token], token))
    return ordered[:SUGGESTION_LIMIT]


def _top_matching_sources(coverages: list[TopicCoverage]) -> list[MatchingSource]:
    by_id: dict[int, MatchingSource] = {}
    for coverage in coverages:
        for source in coverage.top_matching_sources:
            current = by_id.get(source.knowledge_id)
            if current is None or source.score > current.score:
                by_id[source.knowledge_id] = source
    return sorted(
        by_id.values(),
        key=lambda source: (
            -source.score,
            source.source_type,
            source.author or "",
            source.knowledge_id,
        ),
    )[:TOP_SOURCE_LIMIT]

