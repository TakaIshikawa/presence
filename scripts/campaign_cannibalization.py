#!/usr/bin/env python3
"""Detect planned-topic overlap across active campaigns."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.campaign_cannibalization import (  # noqa: E402
    DEFAULT_MIN_SIMILARITY,
    build_campaign_cannibalization_report,
    export_to_json,
    format_text_report,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--campaign-id",
        type=int,
        help="Show overlaps involving one campaign. Defaults to all active campaigns.",
    )
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=DEFAULT_MIN_SIMILARITY,
        help=f"Minimum overlap score to report (default: {DEFAULT_MIN_SIMILARITY})",
    )
    parser.add_argument(
        "--include-generated",
        action="store_true",
        help="Include planned_topics rows already marked generated or linked to content.",
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
    try:
        with script_context() as (_config, db):
            report = build_campaign_cannibalization_report(
                db,
                campaign_id=args.campaign_id,
                min_similarity=args.min_similarity,
                include_generated=args.include_generated,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(export_to_json(report))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
