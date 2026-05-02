#!/usr/bin/env python3
"""Report drift in GitHub issue and pull request labels."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.github_label_drift import (  # noqa: E402
    build_github_label_drift_report,
    format_github_label_drift_json,
    format_github_label_drift_text,
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
        default=14,
        help="Number of days in the recent label window (default: 14)",
    )
    parser.add_argument(
        "--compare-days",
        type=_positive_int,
        default=14,
        help="Number of days in the preceding comparison window (default: 14)",
    )
    parser.add_argument("--repo", help="Only include activity from this repo_name")
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
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
            report = build_github_label_drift_report(
                db,
                days=args.days,
                compare_days=args.compare_days,
                repo=args.repo,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_github_label_drift_json(report))
    else:
        print(format_github_label_drift_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
