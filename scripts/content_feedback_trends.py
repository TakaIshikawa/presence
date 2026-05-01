#!/usr/bin/env python3
"""Report durable manual content feedback trends."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_feedback_trends import (  # noqa: E402
    VALID_FEEDBACK_TYPES,
    build_content_feedback_trends_report,
    format_content_feedback_trends_json,
    format_content_feedback_trends_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)",
    )
    parser.add_argument(
        "--feedback-type",
        choices=sorted(VALID_FEEDBACK_TYPES),
        default="all",
        help="Feedback type to include (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum grouped rows and repeated reasons to include (default: 10)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="text",
        help="Output format (default: text)",
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
            report = build_content_feedback_trends_report(
                db,
                days=args.days,
                feedback_type=args.feedback_type,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_feedback_trends_json(report))
    else:
        print(format_content_feedback_trends_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
