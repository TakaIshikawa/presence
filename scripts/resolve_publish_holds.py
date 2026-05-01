#!/usr/bin/env python3
"""Recommend read-only actions for held publish queue items."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_hold_resolver import (  # noqa: E402
    DEFAULT_DAYS,
    VALID_PLATFORMS,
    build_publish_hold_resolution,
    format_publish_hold_resolution_json,
    format_publish_hold_resolution_text,
)
from runner import script_context  # noqa: E402


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
        help=f"Recent held queue window to consider (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        choices=VALID_PLATFORMS,
        default="all",
        help="Platform to include (default: all).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output deterministic JSON instead of text.",
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
            report = build_publish_hold_resolution(
                db,
                days=args.days,
                platform=args.platform,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_publish_hold_resolution_json(report))
    else:
        print(format_publish_hold_resolution_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
