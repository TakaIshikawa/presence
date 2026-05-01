#!/usr/bin/env python3
"""Export publish queue dead-letter candidates for operator handling."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_dead_letters import (  # noqa: E402
    DEFAULT_DAYS,
    build_publish_dead_letter_report,
    format_csv_report,
    format_json_report,
    format_text_report,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Failure and hold lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        choices=("all", "x", "bluesky"),
        default="all",
        help="Platform to include (default: all).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json", "csv"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--include-held",
        action="store_true",
        help="Include permanently held queue items alongside terminal failures.",
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
            report = build_publish_dead_letter_report(
                db,
                days=args.days,
                platform=args.platform,
                include_held=args.include_held,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_json_report(report))
    elif args.format == "csv":
        print(format_csv_report(report))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
