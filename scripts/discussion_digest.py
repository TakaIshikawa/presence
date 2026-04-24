#!/usr/bin/env python3
"""Seed content ideas from recent GitHub Discussions."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.discussion_digest import (
    DEFAULT_MIN_SCORE,
    DiscussionDigestResult,
    seed_discussion_ideas,
)


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_results_table(results: list[DiscussionDigestResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    proposed = sum(1 for result in results if result.status == "proposed")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} proposed={proposed} skipped={skipped}"]
    lines.append(
        f"{'Status':8s}  {'ID':>4s}  {'Score':>5s}  {'Discussion':18s}  Category / reason"
    )
    lines.append(
        f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 5:>5s}  {'-' * 18:18s}  {'-' * 44}"
    )
    if not results:
        lines.append("none      ----  -----  ------------------  no eligible discussions")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        ref = f"{_shorten(result.repo_name, 13)}#{result.number}"
        detail = f"{_shorten(result.category, 18)} ({result.reason})"
        lines.append(
            f"{result.status:8s}  "
            f"{idea_id:>4s}  "
            f"{result.score:5.1f}  "
            f"{ref:18s}  "
            f"{detail}"
        )
    return "\n".join(lines)


def format_results_json(results: list[DiscussionDigestResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=14,
        help="Lookback window in days for discussion activity (default: 14)",
    )
    parser.add_argument("--repo", help="Only digest discussions for this repo name")
    parser.add_argument("--limit", type=int, default=10, help="Maximum ideas to process")
    parser.add_argument(
        "--min-score",
        type=float,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum candidate score to accept (default: {DEFAULT_MIN_SCORE:g})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print candidates without writing content ideas to the database",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = seed_discussion_ideas(
            db,
            days=args.days,
            repo=args.repo,
            limit=args.limit,
            min_score=args.min_score,
            dry_run=args.dry_run,
        )
    print(format_results_json(results) if args.json else format_results_table(results))


if __name__ == "__main__":
    main()
