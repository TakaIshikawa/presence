#!/usr/bin/env python3
"""Plan read-only revival actions for quiet content campaigns."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.campaign_revival import (  # noqa: E402
    DEFAULT_DAYS_IDLE,
    build_campaign_revival_report,
    format_campaign_revival_json,
    format_campaign_revival_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days-idle",
        type=int,
        default=DEFAULT_DAYS_IDLE,
        help=f"Days without generated content before revival is recommended (default: {DEFAULT_DAYS_IDLE}).",
    )
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Inspect a single campaign id instead of all active and paused campaigns.",
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
            report = build_campaign_revival_report(
                db,
                days_idle=args.days_idle,
                campaign_id=args.campaign_id,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_campaign_revival_json(report))
    else:
        print(format_campaign_revival_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
