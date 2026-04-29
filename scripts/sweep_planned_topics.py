#!/usr/bin/env python3
"""Sweep stale planned topics into reports, skips, or content ideas."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.planned_topic_sweeper import PlannedTopicSweepResult, sweep_planned_topics

DEFAULT_OLDER_THAN_DAYS = 14


def format_results_json(results: list[PlannedTopicSweepResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def format_results_table(results: list[PlannedTopicSweepResult]) -> str:
    lines = [f"matched={len(results)}"]
    lines.append(
        f"{'Action':8s}  {'Status':19s}  {'ID':>4s}  "
        f"{'Campaign':>8s}  {'Target':10s}  {'Idea':>6s}  Topic / reason"
    )
    lines.append(
        f"{'-' * 8:8s}  {'-' * 19:19s}  {'-' * 4:>4s}  "
        f"{'-' * 8:>8s}  {'-' * 10:10s}  {'-' * 6:>6s}  {'-' * 44}"
    )
    if not results:
        lines.append(
            f"{'-':8s}  {'none':19s}  {'-':>4s}  "
            f"{'-':>8s}  {'-':10s}  {'-':>6s}  no stale planned topics"
        )
        return "\n".join(lines)

    for result in results:
        campaign = str(result.campaign_id) if result.campaign_id is not None else "-"
        idea = str(result.content_idea_id) if result.content_idea_id is not None else "-"
        lines.append(
            f"{result.action:8s}  {result.status:19s}  {result.topic_id:4d}  "
            f"{campaign:>8s}  {result.target_date[:10]:10s}  {idea:>6s}  "
            f"{result.topic} ({result.reason})"
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=DEFAULT_OLDER_THAN_DAYS,
        help=f"Select target dates older than this many days (default: {DEFAULT_OLDER_THAN_DAYS})",
    )
    parser.add_argument("--campaign-id", type=int, help="Only sweep topics in this campaign")
    parser.add_argument(
        "--action",
        choices=("report", "skip", "idea"),
        default="report",
        help="How to handle stale planned topics (default: report)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show selected topics without updating the database",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = sweep_planned_topics(
            db,
            older_than_days=args.older_than_days,
            campaign_id=args.campaign_id,
            action=args.action,
            dry_run=args.dry_run,
        )
    print(format_results_json(results) if args.json else format_results_table(results))


if __name__ == "__main__":
    main()
