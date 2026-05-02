#!/usr/bin/env python3
"""Export read-only Dependabot remediation seeds."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.dependabot_remediation_seed import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    SEVERITY_ORDER,
    build_dependabot_remediation_seed_report,
    format_dependabot_remediation_seed_json,
    format_dependabot_remediation_seed_markdown,
)


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
        help=f"Lookback window in days for Dependabot alerts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--severity",
        choices=tuple(SEVERITY_ORDER),
        default="medium",
        help="Minimum alert severity to export (default: medium).",
    )
    parser.add_argument(
        "--repo",
        help="Limit report to a repository full name, for example owner/name.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum grouped seeds to export (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="markdown",
        help="Output format (default: markdown).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_dependabot_remediation_seed_report(
                db,
                days=args.days,
                severity=args.severity,
                repo=args.repo,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_dependabot_remediation_seed_json(report))
    else:
        print(format_dependabot_remediation_seed_markdown(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
