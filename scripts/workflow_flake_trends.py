#!/usr/bin/env python3
"""Report likely flaky GitHub Actions workflow trends."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.workflow_flake_trends import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_RUNS,
    build_workflow_flake_trends_report,
    format_workflow_flake_trends_json,
    format_workflow_flake_trends_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Look back at recent workflow runs (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-runs",
        type=_positive_int,
        default=DEFAULT_MIN_RUNS,
        help=f"Minimum grouped workflow runs to report (default: {DEFAULT_MIN_RUNS}).",
    )
    parser.add_argument(
        "--repo",
        help="Limit report to a repository full name, for example owner/name.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic JSON instead of compact text.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        with script_context() as (_config, db):
            report = build_workflow_flake_trends_report(
                db,
                days=args.days,
                min_runs=args.min_runs,
                repo=args.repo,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_workflow_flake_trends_json(report))
    else:
        print(format_workflow_flake_trends_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
