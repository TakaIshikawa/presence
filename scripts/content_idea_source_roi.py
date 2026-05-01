#!/usr/bin/env python3
"""Report content idea source ROI from downstream content performance."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_idea_source_roi import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_IDEAS,
    build_content_idea_source_roi_report,
    format_content_idea_source_roi_json,
    format_content_idea_source_roi_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-ideas",
        type=int,
        default=DEFAULT_MIN_IDEAS,
        help=f"Minimum ideas required for a source row (default: {DEFAULT_MIN_IDEAS}).",
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
            report = build_content_idea_source_roi_report(
                db,
                days=args.days,
                min_ideas=args.min_ideas,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_content_idea_source_roi_json(report))
    else:
        print(format_content_idea_source_roi_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
