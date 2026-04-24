#!/usr/bin/env python3
"""Seed content ideas from failed GitHub workflow runs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.workflow_failure_seeder import (
    DEFAULT_MIN_SCORE,
    WorkflowFailureSeedResult,
    seed_workflow_failure_ideas,
)


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_results_table(results: list[WorkflowFailureSeedResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    proposed = sum(1 for result in results if result.status == "proposed")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} proposed={proposed} skipped={skipped}"]
    lines.append(f"{'Status':8s}  {'ID':>4s}  {'Score':>5s}  {'Repo':20s}  {'Workflow':22s}  Reason")
    lines.append(
        f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 5:>5s}  "
        f"{'-' * 20:20s}  {'-' * 22:22s}  {'-' * 32}"
    )
    if not results:
        lines.append("none      ----  -----  --------------------  ----------------------  no workflow runs")
        return "\n".join(lines)

    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        lines.append(
            f"{result.status:8s}  "
            f"{idea_id:>4s}  "
            f"{result.score:5.1f}  "
            f"{_shorten(result.repo_name, 20):20s}  "
            f"{_shorten(result.workflow_name, 22):22s}  "
            f"{result.reason}"
        )
    return "\n".join(lines)


def format_results_json(results: list[WorkflowFailureSeedResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Lookback window in days for eligible workflow runs (default: 7)",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum score required to create an idea (default: {DEFAULT_MIN_SCORE:g})",
    )
    parser.add_argument("--limit", type=int, default=25, help="Maximum workflow runs to inspect")
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
        results = seed_workflow_failure_ideas(
            db,
            days=args.days,
            min_score=args.min_score,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    print(format_results_json(results) if args.json else format_results_table(results))


if __name__ == "__main__":
    main()
