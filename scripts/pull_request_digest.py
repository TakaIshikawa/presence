#!/usr/bin/env python3
"""Seed content ideas from recent GitHub pull request activity."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.pull_request_digest import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_SCORE,
    PullRequestDigestItem,
    PullRequestSeedResult,
    build_pull_request_digest,
    seed_pull_request_ideas,
)


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_results_table(
    items: list[PullRequestDigestItem],
    seed_results: list[PullRequestSeedResult] | None = None,
) -> str:
    lines = [
        f"pull_requests={len(items)}",
        f"{'Score':>5s}  {'Pull request':24s}  {'Merged':20s}  Summary",
        f"{'-' * 5:>5s}  {'-' * 24:24s}  {'-' * 20:20s}  {'-' * 44}",
    ]
    if not items:
        lines.append(
            "-----  none                      --------------------  no eligible pull requests"
        )
    for item in items:
        ref = f"{_shorten(item.repo_name, 18)}#{item.number}"
        files = (
            ", ".join(item.changed_file_hints[:2])
            if item.changed_file_hints
            else "no file hints"
        )
        lines.append(
            f"{item.score:5.1f}  "
            f"{ref:24s}  "
            f"{_shorten(item.merged_at, 20):20s}  "
            f"{_shorten(item.title, 52)} ({files})"
        )

    if seed_results is not None:
        created = sum(1 for result in seed_results if result.status == "created")
        proposed = sum(1 for result in seed_results if result.status == "proposed")
        skipped = sum(1 for result in seed_results if result.status == "skipped")
        lines.append("")
        lines.append(f"seed_results created={created} proposed={proposed} skipped={skipped}")
        for result in seed_results:
            idea_id = str(result.idea_id) if result.idea_id is not None else "-"
            lines.append(
                f"  {result.status:8s} id={idea_id:>4s} "
                f"score={result.score:4.1f} {_shorten(result.repo_name, 18)}#{result.number}: "
                f"{result.reason}"
            )
    return "\n".join(lines)


def format_results_json(
    items: list[PullRequestDigestItem],
    seed_results: list[PullRequestSeedResult] | None = None,
) -> str:
    payload: dict[str, object] = {"pull_requests": [item.to_dict() for item in items]}
    if seed_results is not None:
        payload["seed_results"] = [result.to_dict() for result in seed_results]
    return json.dumps(payload, indent=2, sort_keys=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for pull request activity (default: {DEFAULT_DAYS})",
    )
    parser.add_argument("--repo", help="Only include pull requests from this repo_name")
    parser.add_argument(
        "--min-score",
        type=float,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum candidate score to accept (default: {DEFAULT_MIN_SCORE:g})",
    )
    parser.add_argument(
        "--seed-ideas",
        action="store_true",
        help="Create content ideas for eligible pull requests",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report seed candidates without writing content ideas to the database",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with script_context() as (_config, db):
        if args.seed_ideas or args.dry_run:
            seed_results = seed_pull_request_ideas(
                db,
                days=args.days,
                repo=args.repo,
                min_score=args.min_score,
                dry_run=args.dry_run,
            )
            items = build_pull_request_digest(
                db,
                days=args.days,
                repo=args.repo,
                min_score=args.min_score,
            )
        else:
            seed_results = None
            items = build_pull_request_digest(
                db,
                days=args.days,
                repo=args.repo,
                min_score=args.min_score,
            )

    output = (
        format_results_json(items, seed_results)
        if args.json
        else format_results_table(items, seed_results)
    )
    print(output)


if __name__ == "__main__":
    main()
