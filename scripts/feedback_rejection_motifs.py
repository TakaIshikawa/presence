#!/usr/bin/env python3
"""Report recurring motifs in rejected or revised generated content."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.feedback_rejection_motifs import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_COUNT,
    build_feedback_rejection_motifs_report,
    format_feedback_rejection_motifs_json,
    format_feedback_rejection_motifs_text,
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
        help=f"Number of days to look back (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--min-count",
        type=_positive_int,
        default=DEFAULT_MIN_COUNT,
        help=f"Minimum motif occurrences to include (default: {DEFAULT_MIN_COUNT})",
    )
    parser.add_argument(
        "--content-type",
        help="Optional generated_content.content_type filter",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
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
            report = build_feedback_rejection_motifs_report(
                db,
                days=args.days,
                min_count=args.min_count,
                content_type=args.content_type,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    output = (
        format_feedback_rejection_motifs_json(report)
        if args.format == "json"
        else format_feedback_rejection_motifs_text(report)
    )
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
