#!/usr/bin/env python3
"""Seed content ideas from profile growth milestone crossings."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.profile_milestones import (
    DEFAULT_STEP,
    ProfileMilestoneResult,
    results_to_json,
    seed_profile_milestone_ideas,
)
from runner import script_context


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_results_table(results: list[ProfileMilestoneResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    proposed = sum(1 for result in results if result.status == "proposed")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} proposed={proposed} skipped={skipped}"]
    lines.append(
        f"{'Status':8s}  {'ID':>4s}  {'Platform':8s}  {'Threshold':>9s}  Reason / idea"
    )
    lines.append(
        f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 8:8s}  "
        f"{'-' * 9:>9s}  {'-' * 42}"
    )
    if not results:
        lines.append("none      ----  --------  ---------  no profile milestones")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        detail = f"{result.reason}: {_shorten(result.note, 78)}"
        lines.append(
            f"{result.status:8s}  "
            f"{idea_id:>4s}  "
            f"{_shorten(result.platform, 8):8s}  "
            f"{result.threshold:9d}  "
            f"{detail}"
        )
    return "\n".join(lines)


def format_results_json(results: list[ProfileMilestoneResult]) -> str:
    return results_to_json(results)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--platform",
        default="all",
        help="Platform to process, or 'all' for every platform (default: all)",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=DEFAULT_STEP,
        help=f"Follower threshold interval (default: {DEFAULT_STEP})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show milestone ideas without writing to the database",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = seed_profile_milestone_ideas(
            db,
            platform=args.platform,
            step=args.step,
            dry_run=args.dry_run,
        )
    print(format_results_json(results) if args.json else format_results_table(results))


if __name__ == "__main__":
    main()
