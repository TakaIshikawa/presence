#!/usr/bin/env python3
"""Expire stale planned topics that were never used."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.planned_topic_expiration import (
    PlannedTopicExpirationResult,
    expire_planned_topics,
)

DEFAULT_OLDER_THAN_DAYS = 14


def format_results_json(results: list[PlannedTopicExpirationResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def format_results_table(results: list[PlannedTopicExpirationResult], *, dry_run: bool) -> str:
    status = "eligible" if dry_run else "expired"
    lines = [f"{status}={len(results)}"]
    lines.append(f"{'Status':8s}  {'ID':>4s}  {'Campaign':>8s}  {'Target':10s}  Topic / reason")
    lines.append(f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 8:>8s}  {'-' * 10:10s}  {'-' * 44}")
    if not results:
        lines.append(f"{status:8s}  {'-':>4s}  {'-':>8s}  {'-':10s}  no stale planned topics")
        return "\n".join(lines)

    for result in results:
        campaign = str(result.campaign_id) if result.campaign_id is not None else "-"
        target = result.target_date[:10]
        lines.append(
            f"{result.status:8s}  {result.topic_id:4d}  {campaign:>8s}  "
            f"{target:10s}  {result.topic} ({result.reason})"
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=DEFAULT_OLDER_THAN_DAYS,
        help=f"Expire target dates older than this many days (default: {DEFAULT_OLDER_THAN_DAYS})",
    )
    parser.add_argument("--campaign-id", type=int, help="Only expire topics in this campaign")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List eligible topics without updating the database",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = expire_planned_topics(
            db,
            older_than_days=args.older_than_days,
            campaign_id=args.campaign_id,
            dry_run=args.dry_run,
        )
    print(format_results_json(results) if args.json else format_results_table(results, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
