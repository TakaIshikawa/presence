#!/usr/bin/env python3
"""Report newsletter topic retention and engagement."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_topic_retention import (  # noqa: E402
    DEFAULT_LOOKBACK_ISSUES,
    DEFAULT_MIN_SENDS,
    build_newsletter_topic_retention_report,
    format_newsletter_topic_retention_json,
    format_newsletter_topic_retention_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--issues",
        type=int,
        default=DEFAULT_LOOKBACK_ISSUES,
        help=f"Recent newsletter issues to inspect (default: {DEFAULT_LOOKBACK_ISSUES}).",
    )
    parser.add_argument(
        "--min-sends",
        type=int,
        default=DEFAULT_MIN_SENDS,
        help=f"Minimum issue appearances before a topic is fully included (default: {DEFAULT_MIN_SENDS}).",
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
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            report = build_newsletter_topic_retention_report(
                db,
                lookback_issues=args.issues,
                min_sends=args.min_sends,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_topic_retention_json(report))
    else:
        print(format_newsletter_topic_retention_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
