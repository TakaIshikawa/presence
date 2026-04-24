#!/usr/bin/env python3
"""Scan queued publish items for near-duplicate scheduled slots."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_collision import (
    DEFAULT_DAYS_AHEAD,
    DEFAULT_WINDOW_MINUTES,
    collisions_to_json,
    format_text_collisions,
    scan_publish_collisions,
)
from runner import script_context


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=DEFAULT_WINDOW_MINUTES,
        help=f"Collision window in minutes (default: {DEFAULT_WINDOW_MINUTES})",
    )
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=DEFAULT_DAYS_AHEAD,
        help=f"How many days ahead to inspect (default: {DEFAULT_DAYS_AHEAD})",
    )
    parser.add_argument(
        "--platform",
        choices=["all", "x", "bluesky"],
        default="all",
        help="Platform to scan; 'all' scans effective X and Bluesky slots",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    parser.add_argument(
        "--fail-on-collision",
        action="store_true",
        help="Exit nonzero when any collisions are found",
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
        collisions = scan_publish_collisions(
            db,
            window_minutes=args.window_minutes,
            days_ahead=args.days_ahead,
            platform=args.platform,
        )

    if args.format == "json":
        print(collisions_to_json(collisions))
    else:
        print(format_text_collisions(collisions))

    if args.fail_on_collision and collisions:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
