#!/usr/bin/env python3
"""Plan read-only refresh actions for stale or incomplete link metadata."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.link_metadata_refresh import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STALE_DAYS,
    SOURCE_TYPES,
    format_link_metadata_refresh_json,
    format_link_metadata_refresh_text,
    plan_link_metadata_refresh,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-type",
        choices=SOURCE_TYPES,
        default="all",
        help="Rows to scan (default: all)",
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=DEFAULT_STALE_DAYS,
        help=f"Metadata age threshold in days (default: {DEFAULT_STALE_DAYS})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum candidates to return (default: {DEFAULT_LIMIT})",
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
            report = plan_link_metadata_refresh(
                db,
                source_type=args.source_type,
                stale_days=args.stale_days,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_link_metadata_refresh_json(report))
    else:
        print(format_link_metadata_refresh_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
