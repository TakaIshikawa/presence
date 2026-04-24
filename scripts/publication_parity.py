#!/usr/bin/env python3
"""Audit cross-platform publication parity for generated content."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publication_parity import (
    DEFAULT_PLATFORMS,
    find_publication_parity_gaps,
    format_json_report,
    format_text_report,
)
from runner import script_context


def _parse_platforms(value: str) -> tuple[str, ...]:
    platforms = tuple(platform.strip() for platform in value.split(",") if platform.strip())
    invalid = [platform for platform in platforms if platform not in DEFAULT_PLATFORMS]
    if invalid:
        raise argparse.ArgumentTypeError(f"unsupported platforms: {', '.join(invalid)}")
    if len(platforms) < 2:
        raise argparse.ArgumentTypeError("at least two platforms are required")
    return platforms


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back by generated_content.created_at (default: 30)",
    )
    parser.add_argument(
        "--platforms",
        type=_parse_platforms,
        default=DEFAULT_PLATFORMS,
        help="Comma-separated platforms to compare (default: x,bluesky)",
    )
    parser.add_argument(
        "--include-queued",
        action="store_true",
        help="Treat active queued/held state as parity coverage",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--fail-on-missing",
        action="store_true",
        help="Exit with code 1 when parity gaps are found",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        issues = find_publication_parity_gaps(
            db.conn,
            days=args.days,
            platforms=args.platforms,
            include_queued=args.include_queued,
        )

    if args.format == "json":
        print(format_json_report(issues))
    else:
        print(format_text_report(issues))

    if args.fail_on_missing and issues:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
