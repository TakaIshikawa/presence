#!/usr/bin/env python3
"""Seed reviewable content ideas from stale open or recently closed GitHub issues."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.issue_idea_seeder import IssueIdeaSeedResult, seed_issue_ideas


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_results_table(results: list[IssueIdeaSeedResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    proposed = sum(1 for result in results if result.status == "proposed")
    skipped = sum(1 for result in results if result.status == "skipped")
    duplicates = sum(1 for result in results if result.status == "duplicate")
    lines = [
        f"created={created} proposed={proposed} skipped={skipped} duplicate={duplicates}"
    ]
    lines.append(
        f"{'Status':9s}  {'ID':>4s}  {'Priority':8s}  {'Issue':18s}  Topic / reason"
    )
    lines.append(
        f"{'-' * 9:9s}  {'-' * 4:>4s}  {'-' * 8:8s}  "
        f"{'-' * 18:18s}  {'-' * 44}"
    )
    if not results:
        lines.append("none       ----  --------  ------------------  no eligible issues")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        issue_ref = f"{_shorten(result.repo_name, 10)}#{result.number}"
        detail = f"{_shorten(result.topic, 44)} ({result.reason})"
        lines.append(
            f"{result.status:9s}  "
            f"{idea_id:>4s}  "
            f"{(result.priority or '-'):8s}  "
            f"{issue_ref:18s}  "
            f"{detail}"
        )
    return "\n".join(lines)


def format_results_json(results: list[IssueIdeaSeedResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("stale-open", "closed"),
        default="stale-open",
        help="Issue selection mode (default: stale-open)",
    )
    parser.add_argument(
        "--days-stale",
        type=int,
        default=30,
        help="Minimum age in days since issue updated_at for stale-open mode",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Lookback window in days for closed issue activity",
    )
    parser.add_argument("--limit", type=int, default=10, help="Maximum issues to process")
    parser.add_argument(
        "--label",
        action="append",
        dest="labels",
        help="Require at least one matching label in stale-open mode; repeat for multiple labels",
    )
    parser.add_argument("--repo", help="Only seed issues for this repo name")
    parser.add_argument(
        "--priority",
        choices=("high", "normal", "low"),
        default="normal",
        help="Priority for created stale-open content ideas (default: normal)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show ideas that would be created without writing to the database",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = seed_issue_ideas(
            db,
            mode="closed" if args.mode == "closed" else "stale_open",
            days_stale=args.days_stale,
            days=args.days,
            limit=args.limit,
            labels=args.labels,
            repo=args.repo,
            priority=args.priority,
            dry_run=args.dry_run,
        )
    print(format_results_json(results) if args.json else format_results_table(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
