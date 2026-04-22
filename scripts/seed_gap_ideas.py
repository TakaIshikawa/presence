#!/usr/bin/env python3
"""Seed reviewable content ideas from content gap findings."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.content_gaps import ContentGapDetector, PlannedTopicGap, SourceRichGap


SOURCE_NAME = "content_gap_detector"


@dataclass(frozen=True)
class GapIdeaCandidate:
    kind: str
    topic: str
    note: str
    priority: str
    source_metadata: dict[str, Any]


@dataclass(frozen=True)
class SeedResult:
    status: str
    kind: str
    topic: str
    idea_id: int | None
    reason: str
    note: str


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _source_gap_fingerprint(gap: SourceRichGap) -> str:
    """Build a stable open-idea key for a source-rich uncovered topic."""
    payload = {
        "gap_type": "source_rich",
        "topic": gap.topic,
    }
    raw = json.dumps(payload, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def planned_gap_to_candidate(
    gap: PlannedTopicGap,
    *,
    days: int,
    priority: str,
    report_period_start: str,
    report_period_end: str,
) -> GapIdeaCandidate:
    angle = f": {gap.angle}" if gap.angle else ""
    campaign = f" for {gap.campaign_name}" if gap.campaign_name else ""
    target = gap.target_date or "no target date"
    nearest = gap.nearest_generated_at or "none"
    note = (
        f"Cover planned {gap.topic}{angle}{campaign}. "
        f"Target was {target}; nearest generated content was {nearest}, "
        f"outside the {days}-day gap window."
    )
    return GapIdeaCandidate(
        kind="planned",
        topic=gap.topic,
        note=note,
        priority=priority,
        source_metadata={
            "source": SOURCE_NAME,
            "gap_type": "planned_topic",
            "planned_topic_id": gap.planned_topic_id,
            "topic": gap.topic,
            "angle": gap.angle,
            "target_date": gap.target_date,
            "campaign_id": gap.campaign_id,
            "campaign_name": gap.campaign_name,
            "nearest_generated_at": gap.nearest_generated_at,
            "days_from_target": gap.days_from_target,
            "report_period_start": report_period_start,
            "report_period_end": report_period_end,
        },
    )


def source_gap_to_candidate(
    gap: SourceRichGap,
    *,
    priority: str,
    report_period_start: str,
    report_period_end: str,
) -> GapIdeaCandidate:
    fingerprint = _source_gap_fingerprint(gap)
    example = f" Example: {_shorten(gap.examples[0], 110)}" if gap.examples else ""
    note = (
        f"Turn recent source activity on {gap.topic} into content. "
        f"{gap.source_count} source signals "
        f"({gap.commit_count} commits, {gap.message_count} messages), "
        f"latest {gap.latest_source_at or 'unknown'}.{example}"
    )
    return GapIdeaCandidate(
        kind="source",
        topic=gap.topic,
        note=note,
        priority=priority,
        source_metadata={
            "source": SOURCE_NAME,
            "gap_type": "source_rich",
            "gap_fingerprint": fingerprint,
            "topic": gap.topic,
            "source_count": gap.source_count,
            "commit_count": gap.commit_count,
            "message_count": gap.message_count,
            "latest_source_at": gap.latest_source_at,
            "latest_generated_at": gap.latest_generated_at,
            "examples": gap.examples,
            "report_period_start": report_period_start,
            "report_period_end": report_period_end,
        },
    )


def build_candidates(
    report,
    *,
    priority: str,
) -> list[GapIdeaCandidate]:
    candidates: list[GapIdeaCandidate] = []
    for gap in report.planned_gaps:
        candidates.append(
            planned_gap_to_candidate(
                gap,
                days=report.days,
                priority=priority,
                report_period_start=report.period_start,
                report_period_end=report.period_end,
            )
        )
    for gap in report.source_rich_gaps:
        candidates.append(
            source_gap_to_candidate(
                gap,
                priority=priority,
                report_period_start=report.period_start,
                report_period_end=report.period_end,
            )
        )
    return candidates


def seed_gap_ideas(
    db,
    *,
    days: int = 14,
    campaign_id: int | None = None,
    dry_run: bool = False,
    limit: int | None = None,
    priority: str = "normal",
    target_date: datetime | None = None,
) -> list[SeedResult]:
    if limit is not None and limit <= 0:
        return []

    # Reuse Database validation so script and storage stay in sync.
    priority = db._normalize_content_idea_priority(priority)
    if target_date is None:
        target_date = datetime.now(timezone.utc)

    report = ContentGapDetector(db).detect(
        days=days,
        campaign_id=campaign_id,
        target_date=target_date,
    )
    candidates = build_candidates(report, priority=priority)
    if limit is not None:
        candidates = candidates[:limit]

    results: list[SeedResult] = []
    for candidate in candidates:
        metadata = candidate.source_metadata
        existing = db.find_active_content_idea_for_source_metadata(
            note=candidate.note,
            topic=candidate.topic,
            source=SOURCE_NAME,
            source_metadata=metadata,
        )

        if existing:
            results.append(
                SeedResult(
                    status="skipped",
                    kind=candidate.kind,
                    topic=candidate.topic,
                    idea_id=existing["id"],
                    reason=f"{existing['status']} duplicate",
                    note=candidate.note,
                )
            )
            continue

        if dry_run:
            results.append(
                SeedResult(
                    status="skipped",
                    kind=candidate.kind,
                    topic=candidate.topic,
                    idea_id=None,
                    reason="dry run",
                    note=candidate.note,
                )
            )
            continue

        idea_id = db.add_content_idea(
            note=candidate.note,
            topic=candidate.topic,
            priority=candidate.priority,
            source=SOURCE_NAME,
            source_metadata=metadata,
        )
        results.append(
            SeedResult(
                status="created",
                kind=candidate.kind,
                topic=candidate.topic,
                idea_id=idea_id,
                reason="created",
                note=candidate.note,
            )
        )

    return results


def format_results_table(results: list[SeedResult]) -> str:
    lines = [f"{'Status':8s}  {'ID':>4s}  {'Kind':7s}  {'Topic':18s}  Reason / idea"]
    lines.append(f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 7:7s}  {'-' * 18:18s}  {'-' * 40}")
    if not results:
        lines.append("none      ----  -------  ------------------  no eligible gap ideas")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        detail = f"{result.reason}: {_shorten(result.note, 82)}"
        lines.append(
            f"{result.status:8s}  "
            f"{idea_id:>4s}  "
            f"{result.kind:7s}  "
            f"{_shorten(result.topic, 18):18s}  "
            f"{detail}"
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Lookback window in days for generated content and source activity (default: 14)",
    )
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Only seed planned topic gap ideas for this campaign ID",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show ideas that would be created without writing to the database",
    )
    parser.add_argument("--limit", type=int, help="Maximum gap ideas to process")
    parser.add_argument(
        "--priority",
        choices=("high", "normal", "low"),
        default="normal",
        help="Priority for created ideas (default: normal)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = seed_gap_ideas(
            db,
            days=args.days,
            campaign_id=args.campaign_id,
            dry_run=args.dry_run,
            limit=args.limit,
            priority=args.priority,
        )
    print(format_results_table(results))


if __name__ == "__main__":
    main()
