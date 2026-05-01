#!/usr/bin/env python3
"""Report stale rhetorical pattern trends in generated content."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.stale_pattern_trends import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT_EXAMPLES,
    build_stale_pattern_trends,
    format_stale_pattern_trends_json,
    format_stale_pattern_trends_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--content-type",
        default="all",
        help="Generated content_type to scan (default: all)",
    )
    parser.add_argument(
        "--limit-examples",
        type=int,
        default=DEFAULT_LIMIT_EXAMPLES,
        help=f"Maximum examples per pattern (default: {DEFAULT_LIMIT_EXAMPLES})",
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
            report = build_stale_pattern_trends(
                db,
                days=args.days,
                content_type=args.content_type,
                limit_examples=args.limit_examples,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_stale_pattern_trends_json(report))
    else:
        print(format_stale_pattern_trends_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
