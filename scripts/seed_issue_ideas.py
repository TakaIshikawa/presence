#!/usr/bin/env python3
"""Seed reviewable content ideas from stale open GitHub issues."""

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
    lines = [f"created={created} proposed={proposed} skipped={skipped} duplicate={duplicates}"]
    lines.append(f"{'Status':9s}  {'ID':>4s}  {'Repo':20s}  {'Issue':>6s}  Reason")
    lines.append(
        f"{'-' * 9:9s}  {'-' * 4:>4s}  "
        f"{'-' * 20:20s}  {'-' * 6:>6s}  {'-' * 32}"
    )
    if not results:
        lines.append("none       ----  --------------------  ------  no eligible stale issues")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        lines.append(
            f"{result.status:9s}  "
            f"{idea_id:>4s}  "
            f"{_shorten(result.repo_name, 20):20s}  "
            f"{result.number:6d}  "
            f"{_shorten(result.reason, 32)}"
        )
    return "\n".join(lines)


def format_results_json(results: list[IssueIdeaSeedResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days-stale",
        type=int,
        default=30,
        help="Minimum age in days since issue updated_at (default: 30)",
    )
    parser.add_argument("--limit", type=int, default=10, help="Maximum issues to process")
    parser.add_argument(
        "--label",
        action="append",
        dest="labels",
        help="Require at least one matching label; repeat for multiple labels",
    )
    parser.add_argument("--repo", help="Only seed issues for this repo name")
    parser.add_argument(
        "--priority",
        choices=("high", "normal", "low"),
        default="normal",
        help="Priority for created content ideas (default: normal)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show ideas that would be created without writing to the database",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = seed_issue_ideas(
            db,
            days_stale=args.days_stale,
            limit=args.limit,
            labels=args.labels,
            repo=args.repo,
            priority=args.priority,
            dry_run=args.dry_run,
        )
    print(format_results_json(results) if args.json else format_results_table(results))


if __name__ == "__main__":
    main()
