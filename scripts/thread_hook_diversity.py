#!/usr/bin/env python3
"""Report repetitive opening hook structures in X thread candidates."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.thread_hook_diversity import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MAX_SHARE,
    DEFAULT_STATUSES,
    build_thread_hook_diversity_report,
    format_thread_hook_diversity_json,
)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _share(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid share: {value}") from exc
    if not 0 < parsed <= 1:
        raise argparse.ArgumentTypeError("value must be greater than 0 and at most 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Recent thread candidate lookback in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--status",
        action="append",
        help=(
            "Queued publish status to include. Repeatable. "
            f"Defaults to: {', '.join(DEFAULT_STATUSES)}."
        ),
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum thread candidates to audit; 0 means no limit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--max-share",
        type=_share,
        default=DEFAULT_MAX_SHARE,
        help=f"Flag categories above this share of candidates (default: {DEFAULT_MAX_SHARE}).",
    )
    parser.add_argument(
        "--fail-on-findings",
        action="store_true",
        help="Return exit code 2 when hook diversity findings are present.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        statuses = tuple(args.status) if args.status else DEFAULT_STATUSES
        limit = None if args.limit == 0 else args.limit
        with script_context() as (_config, db):
            report = build_thread_hook_diversity_report(
                db,
                days=args.days,
                status=statuses,
                limit=limit,
                max_share=args.max_share,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_thread_hook_diversity_json(report))
    if args.fail_on_findings and report.totals["finding_count"]:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
