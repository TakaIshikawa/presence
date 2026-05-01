"""Check upcoming campaign topics for evidence before generation."""

from __future__ import annotations

import json
import re
import string
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS_AHEAD = 14
DEFAULT_MIN_EVIDENCE = 3
DEFAULT_GITHUB_DAYS = 30
_PUNCT_TRANSLATION = str.maketrans({char: " " for char in string.punctuation})
_STOPWORDS = {
    "about",
    "after",
    "and",
    "behind",
    "for",
    "from",
    "into",
    "that",
    "the",
    "this",
    "with",
}


@dataclass(frozen=True)
class CampaignEvidenceReadinessTopic:
    """Readiness classification for one planned topic."""

    planned_topic_id: int
    campaign_id: int | None
    campaign_name: str | None
    topic: str
    angle: str | None
    target_date: str | None
    readiness: str
    evidence_counts: dict[str, int]
    total_evidence: int
    recommendations: tuple[str, ...] = field(default_factory=tuple)
    matched_keywords: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["recommendations"] = list(self.recommendations)
        payload["matched_keywords"] = list(self.matched_keywords)
        return payload


@dataclass(frozen=True)
class CampaignEvidenceReadinessReport:
    """Evidence readiness report plus applied filters."""

    campaign_id: int | None
    days_ahead: int
    min_evidence: int
    window_start: str
    window_end: str
    topic_count: int
    ready_count: int
    thin_count: int
    missing_count: int
    topics: tuple[CampaignEvidenceReadinessTopic, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "days_ahead": self.days_ahead,
            "min_evidence": self.min_evidence,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "topic_count": self.topic_count,
            "ready_count": self.ready_count,
            "thin_count": self.thin_count,
            "missing_count": self.missing_count,
            "topics": [topic.to_dict() for topic in self.topics],
        }


def build_campaign_evidence_readiness_report(
    db,
    *,
    campaign_id: int | None = None,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    min_evidence: int = DEFAULT_MIN_EVIDENCE,
    now: datetime | None = None,
    github_days: int = DEFAULT_GITHUB_DAYS,
) -> CampaignEvidenceReadinessReport:
    """Return readiness for planned topics in the upcoming target-date window."""
    if days_ahead < 0:
        raise ValueError("days_ahead must be non-negative")
    if min_evidence <= 0:
        raise ValueError("min_evidence must be positive")
    if github_days <= 0:
        raise ValueError("github_days must be positive")
    if campaign_id is not None and db.get_campaign(campaign_id) is None:
        raise ValueError(f"Campaign {campaign_id} does not exist")

    current = _ensure_aware(now or datetime.now(timezone.utc))
    window_start = current.date()
    window_end = window_start + timedelta(days=days_ahead)
    planned_topics = _load_planned_topics(
        db,
        campaign_id=campaign_id,
        window_start=window_start,
        window_end=window_end,
    )
    knowledge_rows = _load_knowledge_rows(db)
    github_rows = _load_github_activity_rows(
        db,
        cutoff=current - timedelta(days=github_days),
        now=current,
    )
    prior_content_rows = _load_prior_content_rows(db)

    topics = tuple(
        _readiness_for_topic(
            row,
            knowledge_rows=knowledge_rows,
            github_rows=github_rows,
            prior_content_rows=prior_content_rows,
            min_evidence=min_evidence,
        )
        for row in planned_topics
    )
    counts = {
        status: sum(1 for item in topics if item.readiness == status)
        for status in ("ready", "thin", "missing")
    }
    return CampaignEvidenceReadinessReport(
        campaign_id=campaign_id,
        days_ahead=days_ahead,
        min_evidence=min_evidence,
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        topic_count=len(topics),
        ready_count=counts["ready"],
        thin_count=counts["thin"],
        missing_count=counts["missing"],
        topics=topics,
    )


def format_campaign_evidence_readiness_json(report: CampaignEvidenceReadinessReport) -> str:
    """Format a readiness report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_evidence_readiness_text(report: CampaignEvidenceReadinessReport) -> str:
    """Format a readiness report for terminal review."""
    lines = [
        "",
        "=" * 70,
        "Campaign Evidence Readiness",
        "=" * 70,
        "",
        f"Window: {report.window_start} to {report.window_end}",
        f"Minimum evidence: {report.min_evidence}",
        f"Topics: {report.topic_count} ready={report.ready_count} "
        f"thin={report.thin_count} missing={report.missing_count}",
    ]
    if report.campaign_id is not None:
        lines.append(f"Campaign ID: {report.campaign_id}")
    if not report.topics:
        lines.extend(["", "- none", "", "=" * 70])
        return "\n".join(lines)

    for index, topic in enumerate(report.topics, start=1):
        counts = topic.evidence_counts
        lines.append("")
        lines.append(
            f"{index}. planned topic #{topic.planned_topic_id} [{topic.readiness}] "
            f"{topic.topic} - evidence {topic.total_evidence}/{report.min_evidence}"
        )
        lines.append(f"   Target date: {topic.target_date or 'unscheduled'}")
        if topic.angle:
            lines.append(f"   Angle: {_shorten(topic.angle)}")
        lines.append(
            "   Counts: "
            f"source_material={counts['source_material']}, "
            f"knowledge={counts['knowledge']}, "
            f"github_activity={counts['github_activity']}, "
            f"prior_content={counts['prior_content']}"
        )
        for recommendation in topic.recommendations:
            lines.append(f"   - {recommendation}")

    lines.extend(["", "=" * 70])
    return "\n".join(lines)


def _load_planned_topics(
    db,
    *,
    campaign_id: int | None,
    window_start: date,
    window_end: date,
) -> list[dict[str, Any]]:
    sql = """SELECT pt.*,
                    cc.name AS campaign_name
             FROM planned_topics pt
             LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
             WHERE pt.status = 'planned'
               AND pt.content_id IS NULL
               AND pt.target_date IS NOT NULL
               AND date(pt.target_date) >= date(?)
               AND date(pt.target_date) <= date(?)"""
    params: list[Any] = [window_start.isoformat(), window_end.isoformat()]
    if campaign_id is not None:
        sql += " AND pt.campaign_id = ?"
        params.append(campaign_id)
    sql += " ORDER BY date(pt.target_date) ASC, pt.created_at ASC, pt.id ASC"
    return [dict(row) for row in db.conn.execute(sql, params).fetchall()]


def _load_knowledge_rows(db) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        """SELECT id, source_type, content, insight
           FROM knowledge
           WHERE approved = 1
           ORDER BY COALESCE(published_at, ingested_at, created_at) DESC, id DESC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_github_activity_rows(
    db,
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        """SELECT id, title, body, labels, updated_at
           FROM github_activity
           WHERE datetime(updated_at) >= datetime(?)
             AND datetime(updated_at) <= datetime(?)
           ORDER BY datetime(updated_at) DESC, id DESC""",
        (cutoff.isoformat(), now.isoformat()),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_prior_content_rows(db) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        """SELECT gc.id AS content_id,
                  gc.content,
                  ct.topic
           FROM generated_content gc
           LEFT JOIN content_topics ct ON ct.content_id = gc.id
           WHERE gc.published != -1
           ORDER BY COALESCE(gc.published_at, gc.created_at) DESC, gc.id DESC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _readiness_for_topic(
    topic: dict[str, Any],
    *,
    knowledge_rows: list[dict[str, Any]],
    github_rows: list[dict[str, Any]],
    prior_content_rows: list[dict[str, Any]],
    min_evidence: int,
) -> CampaignEvidenceReadinessTopic:
    keywords = _topic_keywords(topic)
    source_count = len(_source_material_refs(topic.get("source_material")))
    knowledge_count = sum(
        1
        for row in knowledge_rows
        if _matches_keywords(
            " ".join(
                str(value or "")
                for value in [row.get("content"), row.get("insight")]
            ),
            keywords,
        )
    )
    github_count = sum(
        1
        for row in github_rows
        if _matches_keywords(
            " ".join(
                str(value or "")
                for value in [row.get("title"), row.get("body"), row.get("labels")]
            ),
            keywords,
        )
    )
    prior_content_count = len(
        {
            int(row["content_id"])
            for row in prior_content_rows
            if _same_topic(topic, row) or _matches_keywords(row.get("content"), keywords)
        }
    )
    evidence_counts = {
        "source_material": source_count,
        "knowledge": knowledge_count,
        "github_activity": github_count,
        "prior_content": prior_content_count,
    }
    total_evidence = sum(evidence_counts.values())
    if total_evidence >= min_evidence:
        readiness = "ready"
    elif total_evidence > 0:
        readiness = "thin"
    else:
        readiness = "missing"

    return CampaignEvidenceReadinessTopic(
        planned_topic_id=int(topic["id"]),
        campaign_id=topic.get("campaign_id"),
        campaign_name=topic.get("campaign_name"),
        topic=str(topic.get("topic") or ""),
        angle=topic.get("angle"),
        target_date=topic.get("target_date"),
        readiness=readiness,
        evidence_counts=evidence_counts,
        total_evidence=total_evidence,
        recommendations=_recommendations(evidence_counts, readiness, min_evidence),
        matched_keywords=tuple(sorted(keywords)),
    )


def _recommendations(
    counts: dict[str, int],
    readiness: str,
    min_evidence: int,
) -> tuple[str, ...]:
    recommendations: list[str] = []
    if counts["source_material"] == 0:
        recommendations.append("attach explicit source_material references before generation")
    if counts["knowledge"] == 0:
        recommendations.append("add or approve related knowledge rows")
    if counts["github_activity"] == 0:
        recommendations.append("look for recent issue, PR, release, or discussion activity")
    if counts["prior_content"] > 0:
        recommendations.append("review prior content to avoid repeating the same angle")
    if readiness == "missing":
        recommendations.append(
            f"block generation until at least {min_evidence} evidence item(s) are available"
        )
    elif readiness == "thin":
        recommendations.append(f"add evidence before generation; below minimum of {min_evidence}")
    return tuple(recommendations)


def _topic_keywords(topic: dict[str, Any]) -> set[str]:
    text = " ".join(
        str(value)
        for value in [topic.get("topic"), topic.get("angle")]
        if value
    )
    normalized = _normalize_text(text)
    return {
        token
        for token in normalized.split()
        if len(token) >= 3 and token not in _STOPWORDS
    }


def _matches_keywords(value: object | None, keywords: set[str]) -> bool:
    if not keywords:
        return False
    haystack = _normalize_text(value)
    return any(keyword in haystack.split() for keyword in keywords)


def _same_topic(topic: dict[str, Any], row: dict[str, Any]) -> bool:
    return _normalize_text(topic.get("topic")) == _normalize_text(row.get("topic"))


def _source_material_refs(value: str | None) -> set[str]:
    if not value:
        return set()
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return set(re.findall(r"[A-Za-z0-9_:/.-]{4,}", str(value)))
    refs: set[str] = set()
    values: list[Any]
    if isinstance(parsed, dict):
        values = list(parsed.values())
    elif isinstance(parsed, list):
        values = parsed
    else:
        values = [parsed]
    for item in values:
        if isinstance(item, list):
            refs.update(str(value) for value in item if value)
        elif isinstance(item, dict):
            refs.update(str(value) for value in item.values() if value)
        elif item:
            refs.add(str(item))
    return refs


def _normalize_text(value: object | None) -> str:
    text = str(value or "").casefold().translate(_PUNCT_TRANSLATION)
    return re.sub(r"\s+", " ", text).strip()


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _shorten(text: str | None, width: int = 120) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)] + "..."
