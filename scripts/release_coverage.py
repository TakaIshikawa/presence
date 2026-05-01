#!/usr/bin/env python3
"""Report GitHub releases without generated-content follow-up."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.release_coverage import (  # noqa: E402
    build_release_coverage_report,
    format_json_report,
    format_text_report,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Lookback window in days for release activity (default: 30).",
    )
    parser.add_argument("--repo", help="Only include releases for this repo_name.")
    parser.add_argument(
        "--min-age-hours",
        type=float,
        default=12,
        help="Ignore releases newer than this many hours (default: 12).",
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
    if args.days <= 0:
        raise SystemExit("--days must be positive")
    if args.min_age_hours < 0:
        raise SystemExit("--min-age-hours must be non-negative")

    with script_context() as (_config, db):
        report = build_release_coverage_report(
            db,
            days=args.days,
            repo=args.repo,
            min_age_hours=args.min_age_hours,
        )

    if args.format == "json":
        print(format_json_report(report))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
