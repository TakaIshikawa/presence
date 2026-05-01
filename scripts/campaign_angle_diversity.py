#!/usr/bin/env python3
"""Detect repetitive planned-topic angles inside campaigns."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.campaign_angle_diversity import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_SIMILARITY_THRESHOLD,
    DEFAULT_STATUSES,
    build_campaign_angle_diversity_report,
    format_campaign_angle_diversity_json,
    format_campaign_angle_diversity_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Limit report to one campaign ID. Defaults to all active campaigns.",
    )
    parser.add_argument(
        "--status",
        action="append",
        default=None,
        help=(
            "Planned topic status to include. May be repeated or comma-separated "
            f"(default: {','.join(DEFAULT_STATUSES)})"
        ),
    )
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=DEFAULT_SIMILARITY_THRESHOLD,
        help=f"Minimum duplicate similarity score (default: {DEFAULT_SIMILARITY_THRESHOLD})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum duplicate groups to return (default: {DEFAULT_LIMIT})",
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
            report = build_campaign_angle_diversity_report(
                db,
                campaign_id=args.campaign_id,
                statuses=args.status,
                similarity_threshold=args.similarity_threshold,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_campaign_angle_diversity_json(report))
    else:
        print(format_campaign_angle_diversity_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
