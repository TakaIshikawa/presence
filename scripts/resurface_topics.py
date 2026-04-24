#!/usr/bin/env python3
"""Report and optionally seed stale high-value topics for resurfacing."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.stale_topic_resurfacer import (
    StaleTopic,
    StaleTopicReport,
    StaleTopicResurfacer,
    format_stale_topic_json,
)


SOURCE_NAME = "stale_topic_resurfacer"


@dataclass(frozen=True)
class SeedResult:
    status: str
    topic: str
    idea_id: int | None
    reason: str
    note: str


def _shorten(text: str | None, width: int = 88) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def stale_topic_to_note(topic: StaleTopic) -> str:
    return (
        f"Resurface {topic.topic}: historically strong but dormant. "
        f"Average engagement {topic.avg_engagement:.1f} across {topic.sample_count} posts; "
        f"last generated {topic.days_since_latest} days ago."
    )


def stale_topic_metadata(topic: StaleTopic, report: StaleTopicReport) -> dict[str, Any]:
    return {
        "source": SOURCE_NAME,
        "source_id": f"stale-topic:{topic.topic}",
        "topic": topic.topic,
        "stale_topic": topic.topic,
        "source_content_ids": topic.source_content_ids,
        "avg_engagement": topic.avg_engagement,
        "max_engagement": topic.max_engagement,
        "sample_count": topic.sample_count,
        "latest_published_at": topic.latest_published_at,
        "days_since_latest": topic.days_since_latest,
        "min_age_days": report.min_age_days,
        "lookback_days": report.lookback_days,
        "reasons": topic.reasons,
    }


def seed_content_ideas(
    db,
    report: StaleTopicReport,
    *,
    priority: str = "high",
) -> list[SeedResult]:
    priority = db._normalize_content_idea_priority(priority)
    results: list[SeedResult] = []

    for topic in report.topics:
        note = stale_topic_to_note(topic)
        metadata = stale_topic_metadata(topic, report)
        existing = db.find_active_content_idea_for_source_metadata(
            note=note,
            topic=topic.topic,
            source=SOURCE_NAME,
            source_metadata=metadata,
        )
        if existing:
            results.append(
                SeedResult(
                    status="skipped",
                    topic=topic.topic,
                    idea_id=existing["id"],
                    reason=f"{existing['status']} duplicate",
                    note=note,
                )
            )
            continue

        idea_id = db.add_content_idea(
            note=note,
            topic=topic.topic,
            priority=priority,
            source=SOURCE_NAME,
            source_metadata=metadata,
        )
        results.append(
            SeedResult(
                status="created",
                topic=topic.topic,
                idea_id=idea_id,
                reason="created",
                note=note,
            )
        )

    return results


def format_text_report(
    report: StaleTopicReport,
    seed_results: list[SeedResult] | None = None,
) -> str:
    lines = [
        "Stale Topic Resurfacing Report",
        "=" * 70,
        f"Min age:  {report.min_age_days} days",
        f"Lookback: last {report.lookback_days} days",
        "",
    ]
    if not report.topics:
        lines.append("No stale high-value topics found.")
    for index, topic in enumerate(report.topics, start=1):
        lines.append(
            f"{index}. {topic.topic}: score {topic.score:.2f}, "
            f"avg engagement {topic.avg_engagement:.2f}, "
            f"n={topic.sample_count}, last {topic.days_since_latest}d ago"
        )
        lines.append(f"   source content: {', '.join(str(i) for i in topic.source_content_ids)}")
        for reason in topic.reasons:
            lines.append(f"   - {reason}")

    if seed_results is not None:
        lines.extend(["", "Seeded Content Ideas", "-" * 70])
        if not seed_results:
            lines.append("none")
        for result in seed_results:
            idea_id = str(result.idea_id) if result.idea_id is not None else "-"
            lines.append(
                f"{result.status:8s}  {idea_id:>4s}  {result.topic:18s}  "
                f"{result.reason}: {_shorten(result.note)}"
            )

    return "\n".join(lines)


def format_json_payload(
    report: StaleTopicReport,
    seed_results: list[SeedResult] | None = None,
) -> str:
    payload = json.loads(format_stale_topic_json(report))
    if seed_results is not None:
        payload["seed_results"] = [
            {
                "status": result.status,
                "topic": result.topic,
                "idea_id": result.idea_id,
                "reason": result.reason,
                "note": result.note,
            }
            for result in seed_results
        ]
    return json.dumps(payload, indent=2, sort_keys=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--min-age-days",
        type=int,
        default=30,
        help="Exclude topics with generated content newer than this many days (default: 30)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=180,
        help="Historical performance window in days (default: 180)",
    )
    parser.add_argument("--limit", type=int, default=10, help="Maximum topics to return")
    parser.add_argument(
        "--seed-ideas",
        action="store_true",
        help="Create deduplicated content ideas for returned stale topics",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        report = StaleTopicResurfacer(db).detect(
            min_age_days=args.min_age_days,
            lookback_days=args.lookback_days,
            limit=args.limit,
            target_date=datetime.now(timezone.utc),
        )
        seed_results = seed_content_ideas(db, report) if args.seed_ideas else None

    if args.json:
        print(format_json_payload(report, seed_results))
    else:
        print(format_text_report(report, seed_results))


if __name__ == "__main__":
    main()
