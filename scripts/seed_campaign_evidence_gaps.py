#!/usr/bin/env python3
"""Seed content ideas from thin campaign evidence gaps."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.campaign_evidence_gap_seeder import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    DEFAULT_MIN_EVIDENCE,
    format_campaign_evidence_gap_seed_json,
    format_campaign_evidence_gap_seed_text,
    seed_campaign_evidence_gaps,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Only seed gaps for this campaign.",
    )
    parser.add_argument(
        "--days-ahead",
        type=int,
        default=DEFAULT_DAYS_AHEAD,
        help=f"Upcoming target date window in days (default: {DEFAULT_DAYS_AHEAD}).",
    )
    parser.add_argument(
        "--min-evidence",
        type=int,
        default=DEFAULT_MIN_EVIDENCE,
        help=(
            "Minimum total evidence count before a topic is considered ready "
            f"(default: {DEFAULT_MIN_EVIDENCE})."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report eligible ideas without writing content_ideas.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum eligible planned topics to inspect (default: 25).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
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
            report = seed_campaign_evidence_gaps(
                db,
                campaign_id=args.campaign_id,
                days_ahead=args.days_ahead,
                min_evidence=args.min_evidence,
                dry_run=args.dry_run,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_campaign_evidence_gap_seed_json(report))
    else:
        print(format_campaign_evidence_gap_seed_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
