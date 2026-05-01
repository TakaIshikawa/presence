#!/usr/bin/env python3
"""Seed content ideas from repeated failed/cancelled GitHub workflow runs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from synthesis.workflow_run_idea_seeder import (
    DEFAULT_DAYS,
    DEFAULT_MIN_FAILURES,
    WorkflowRunIdeaSeedResult,
    seed_workflow_run_ideas,
)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _shorten(text: str | None, width: int = 70) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def format_results_text(results: list[WorkflowRunIdeaSeedResult]) -> str:
    created = sum(1 for result in results if result.status == "created")
    proposed = sum(1 for result in results if result.status == "proposed")
    skipped = sum(1 for result in results if result.status == "skipped")
    lines = [f"created={created} proposed={proposed} skipped={skipped}"]
    lines.append(
        f"{'Status':8s}  {'ID':>4s}  {'Count':>5s}  {'Repo':20s}  "
        f"{'Workflow':22s}  {'Branch':14s}  Reason"
    )
    lines.append(
        f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 5:>5s}  "
        f"{'-' * 20:20s}  {'-' * 22:22s}  {'-' * 14:14s}  {'-' * 32}"
    )
    if not results:
        lines.append(
            "none      ----  -----  --------------------  ----------------------  "
            "--------------  no repeated workflow runs"
        )
        return "\n".join(lines)
    for result in results:
        idea_id = str(result.idea_id) if result.idea_id is not None else "-"
        lines.append(
            f"{result.status:8s}  "
            f"{idea_id:>4s}  "
            f"{result.failure_count:5d}  "
            f"{_shorten(result.repo_name, 20):20s}  "
            f"{_shorten(result.workflow_name, 22):22s}  "
            f"{_shorten(result.branch, 14):14s}  "
            f"{result.reason}"
        )
    return "\n".join(lines)


def format_results_json(results: list[WorkflowRunIdeaSeedResult]) -> str:
    return json.dumps([result.to_dict() for result in results], indent=2, sort_keys=True)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for eligible workflow runs (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-failures",
        type=_positive_int,
        default=DEFAULT_MIN_FAILURES,
        help=(
            "Minimum failed/cancelled runs in a workflow/branch group "
            f"before seeding (default: {DEFAULT_MIN_FAILURES})."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show ideas that would be created without writing to the database.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    with script_context() as (_config, db):
        results = seed_workflow_run_ideas(
            db,
            days=args.days,
            min_failures=args.min_failures,
            dry_run=args.dry_run,
        )
    if args.format == "json":
        print(format_results_json(results))
    else:
        print(format_results_text(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
