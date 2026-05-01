#!/usr/bin/env python3
"""Report generated campaign planned-topic outcomes."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_topic_outcomes import (  # noqa: E402
    DEFAULT_DAYS,
    build_campaign_topic_outcomes_report,
    format_campaign_topic_outcomes_json,
    format_campaign_topic_outcomes_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Only include generated planned topics for this campaign.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print JSON instead of the default text table.",
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
            report = build_campaign_topic_outcomes_report(
                db,
                campaign_id=args.campaign_id,
                days=args.days,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_campaign_topic_outcomes_json(report))
    else:
        print(format_campaign_topic_outcomes_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
