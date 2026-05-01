"""Seed content ideas for planned campaign topics with thin evidence."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from synthesis.campaign_evidence_readiness import (
    DEFAULT_DAYS_AHEAD,
    DEFAULT_MIN_EVIDENCE,
    CampaignEvidenceReadinessTopic,
    build_campaign_evidence_readiness_report,
)


SOURCE_NAME = "campaign_evidence_gap"


@dataclass(frozen=True)
class CampaignEvidenceGapSeedResult:
    status: str
    planned_topic_id: int
    campaign_id: int | None
    campaign_name: str | None
    topic: str
    angle: str | None
    target_date: str | None
    readiness: str
    total_evidence: int
    min_evidence: int
    evidence_counts: dict[str, int]
    idea_id: int | None
    reason: str
    note: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CampaignEvidenceGapSeedReport:
    campaign_id: int | None
    days_ahead: int
    min_evidence: int
    dry_run: bool
    limit: int | None
    window_start: str
    window_end: str
    eligible_count: int
    created_count: int
    proposed_count: int
    skipped_count: int
    results: tuple[CampaignEvidenceGapSeedResult, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "campaign_id": self.campaign_id,
            "days_ahead": self.days_ahead,
            "min_evidence": self.min_evidence,
            "dry_run": self.dry_run,
            "limit": self.limit,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "eligible_count": self.eligible_count,
            "created_count": self.created_count,
            "proposed_count": self.proposed_count,
            "skipped_count": self.skipped_count,
            "results": [result.to_dict() for result in self.results],
        }


def seed_campaign_evidence_gaps(
    db,
    *,
    campaign_id: int | None = None,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    min_evidence: int = DEFAULT_MIN_EVIDENCE,
    dry_run: bool = False,
    limit: int | None = 25,
    now: datetime | None = None,
) -> CampaignEvidenceGapSeedReport:
    """Create content ideas for upcoming planned topics below the evidence threshold."""
    if limit is not None and limit <= 0:
        limit = 0

    readiness = build_campaign_evidence_readiness_report(
        db,
        campaign_id=campaign_id,
        days_ahead=days_ahead,
        min_evidence=min_evidence,
        now=now,
    )
    eligible_topics = [
        topic for topic in readiness.topics if topic.total_evidence < min_evidence
    ]
    if limit is not None:
        eligible_topics = eligible_topics[:limit]

    results: list[CampaignEvidenceGapSeedResult] = []
    for topic in eligible_topics:
        candidate = _result_base(topic, min_evidence=min_evidence)
        existing = _find_open_idea_for_planned_topic(db, topic.planned_topic_id)
        if existing:
            results.append(
                CampaignEvidenceGapSeedResult(
                    status="skipped",
                    idea_id=existing["id"],
                    reason="open duplicate",
                    **candidate,
                )
            )
            continue

        if dry_run:
            results.append(
                CampaignEvidenceGapSeedResult(
                    status="proposed",
                    idea_id=None,
                    reason="dry run",
                    **candidate,
                )
            )
            continue

        metadata = _source_metadata(topic, min_evidence=min_evidence)
        idea_id = db.insert_content_idea(
            note=candidate["note"],
            topic=topic.topic,
            priority="high" if topic.readiness == "missing" else "normal",
            source=SOURCE_NAME,
            source_metadata=metadata,
        )
        results.append(
            CampaignEvidenceGapSeedResult(
                status="created",
                idea_id=idea_id,
                reason="created",
                **candidate,
            )
        )

    return CampaignEvidenceGapSeedReport(
        campaign_id=campaign_id,
        days_ahead=days_ahead,
        min_evidence=min_evidence,
        dry_run=dry_run,
        limit=limit,
        window_start=readiness.window_start,
        window_end=readiness.window_end,
        eligible_count=len(eligible_topics),
        created_count=sum(1 for result in results if result.status == "created"),
        proposed_count=sum(1 for result in results if result.status == "proposed"),
        skipped_count=sum(1 for result in results if result.status == "skipped"),
        results=tuple(results),
    )


def format_campaign_evidence_gap_seed_json(report: CampaignEvidenceGapSeedReport) -> str:
    """Format a seed report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_campaign_evidence_gap_seed_text(report: CampaignEvidenceGapSeedReport) -> str:
    """Format a seed report for terminal review."""
    lines = [
        "",
        "=" * 70,
        "Campaign Evidence Gap Seeder",
        "=" * 70,
        "",
        f"Window: {report.window_start} to {report.window_end}",
        f"Minimum evidence: {report.min_evidence}",
        f"Mode: {'dry-run' if report.dry_run else 'apply'}",
        f"Results: eligible={report.eligible_count} created={report.created_count} "
        f"proposed={report.proposed_count} skipped={report.skipped_count}",
    ]
    if report.campaign_id is not None:
        lines.append(f"Campaign ID: {report.campaign_id}")
    if not report.results:
        lines.extend(["", "- none", "", "=" * 70])
        return "\n".join(lines)

    for index, result in enumerate(report.results, start=1):
        idea = f"idea #{result.idea_id}" if result.idea_id is not None else "no idea"
        lines.append("")
        lines.append(
            f"{index}. {result.status} planned topic #{result.planned_topic_id} "
            f"({idea}) [{result.readiness}] {result.topic}"
        )
        lines.append(
            f"   Evidence: {result.total_evidence}/{result.min_evidence}; "
            f"reason: {result.reason}"
        )
        lines.append(f"   Target date: {result.target_date or 'unscheduled'}")
        if result.angle:
            lines.append(f"   Angle: {_shorten(result.angle)}")
        counts = result.evidence_counts
        lines.append(
            "   Counts: "
            f"source_material={counts['source_material']}, "
            f"knowledge={counts['knowledge']}, "
            f"github_activity={counts['github_activity']}, "
            f"prior_content={counts['prior_content']}"
        )

    lines.extend(["", "=" * 70])
    return "\n".join(lines)


def _result_base(
    topic: CampaignEvidenceReadinessTopic,
    *,
    min_evidence: int,
) -> dict[str, Any]:
    return {
        "planned_topic_id": topic.planned_topic_id,
        "campaign_id": topic.campaign_id,
        "campaign_name": topic.campaign_name,
        "topic": topic.topic,
        "angle": topic.angle,
        "target_date": topic.target_date,
        "readiness": topic.readiness,
        "total_evidence": topic.total_evidence,
        "min_evidence": min_evidence,
        "evidence_counts": dict(topic.evidence_counts),
        "note": _idea_note(topic, min_evidence=min_evidence),
    }


def _source_metadata(
    topic: CampaignEvidenceReadinessTopic,
    *,
    min_evidence: int,
) -> dict[str, Any]:
    return {
        "source": SOURCE_NAME,
        "planned_topic_id": topic.planned_topic_id,
        "campaign_id": topic.campaign_id,
        "campaign_name": topic.campaign_name,
        "readiness": topic.readiness,
        "total_evidence": topic.total_evidence,
        "min_evidence": min_evidence,
        "evidence_counts": dict(topic.evidence_counts),
        "target_date": topic.target_date,
        "matched_keywords": list(topic.matched_keywords),
        "recommendations": list(topic.recommendations),
    }


def _idea_note(
    topic: CampaignEvidenceReadinessTopic,
    *,
    min_evidence: int,
) -> str:
    parts = [
        f"Planned campaign topic #{topic.planned_topic_id} has {topic.total_evidence}/{min_evidence} evidence items.",
        f"Topic: {topic.topic}.",
    ]
    if topic.angle:
        parts.append(f"Angle: {topic.angle}.")
    if topic.target_date:
        parts.append(f"Target date: {topic.target_date}.")
    if topic.campaign_name:
        parts.append(f"Campaign: {topic.campaign_name}.")
    if topic.recommendations:
        parts.append("Next steps: " + "; ".join(topic.recommendations) + ".")
    return " ".join(parts)


def _find_open_idea_for_planned_topic(
    db,
    planned_topic_id: int,
) -> dict[str, Any] | None:
    finder = getattr(db, "find_open_content_idea_for_planned_topic", None)
    if finder is not None:
        return finder(planned_topic_id)
    rows = db.conn.execute(
        """SELECT * FROM content_ideas
           WHERE status = 'open'
             AND source_metadata IS NOT NULL
           ORDER BY created_at ASC, id ASC"""
    ).fetchall()
    for row in rows:
        item = dict(row)
        try:
            metadata = json.loads(item.get("source_metadata") or "{}")
        except (TypeError, ValueError):
            continue
        if metadata.get("planned_topic_id") == planned_topic_id:
            return item
    return None


def _shorten(text: str | None, width: int = 120) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)] + "..."
