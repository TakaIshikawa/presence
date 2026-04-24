#!/usr/bin/env python3
"""Seed content ideas from unanswered inbound mentions."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.mention_idea_seeder import MentionIdeaSeeder, SeedResult
from runner import script_context


def _shorten(text: str | None, width: int = 86) -> str:
    value = " ".join(str(text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_results_table(results: list[SeedResult]) -> str:
    lines = [f"{'Status':9s}  {'ID':>4s}  {'Kind':7s}  {'Topic':18s}  Reason / idea"]
    lines.append(f"{'-' * 9:9s}  {'-' * 4:>4s}  {'-' * 7:7s}  {'-' * 18:18s}  {'-' * 40}")
    if not results:
        lines.append("none       ----  -------  ------------------  no eligible mention ideas")
        return "\n".join(lines)
    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        lines.append(
            f"{result.status:9s}  "
            f"{idea_id:>4s}  "
            f"{result.kind:7s}  "
            f"{_shorten(result.topic, 18):18s}  "
            f"{result.reason}: {_shorten(result.note)}"
        )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show ideas that would be created without writing to the database",
    )
    parser.add_argument("--limit", type=int, help="Maximum mention ideas to process")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable JSON instead of a table",
    )
    parser.add_argument(
        "--min-quality-score",
        type=float,
        default=5.0,
        help="Minimum reply quality score to consider when a score exists (default: 5.0)",
    )
    parser.add_argument(
        "--recurring-min-count",
        type=int,
        default=2,
        help="Mentions needed before grouping them as a recurring theme (default: 2)",
    )
    return parser.parse_args(argv)


def seed_mention_ideas(
    db,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    min_quality_score: float = 5.0,
    recurring_min_count: int = 2,
) -> list[SeedResult]:
    return MentionIdeaSeeder(
        db,
        min_quality_score=min_quality_score,
        recurring_min_count=recurring_min_count,
    ).seed(dry_run=dry_run, limit=limit)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = seed_mention_ideas(
            db,
            dry_run=args.dry_run,
            limit=args.limit,
            min_quality_score=args.min_quality_score,
            recurring_min_count=args.recurring_min_count,
        )
    if args.json:
        print(json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True))
    else:
        print(format_results_table(results))


if __name__ == "__main__":
    main()
