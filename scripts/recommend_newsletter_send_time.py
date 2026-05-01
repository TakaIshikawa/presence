#!/usr/bin/env python3
"""Recommend newsletter send windows from historical engagement."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_send_time import (  # noqa: E402
    NewsletterSendTimeRecommender,
    format_json_report,
    format_text_report,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to look back by newsletter sent_at (default: 90)",
    )
    parser.add_argument(
        "--min-sample",
        type=int,
        default=3,
        help="Minimum sends with engagement required per window (default: 3)",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum recommended windows to include (default: 10)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    with script_context() as (_config, db):
        report = NewsletterSendTimeRecommender(db).recommend(
            days=args.days,
            min_sample=args.min_sample,
            limit=args.limit,
        )

    if args.format == "json":
        print(format_json_report(report))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
