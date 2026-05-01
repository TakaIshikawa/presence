#!/usr/bin/env python3
"""Build newsletter sections from high-performing published X threads."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.thread_newsletter_sections import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_SCORE,
    ThreadNewsletterSectionBuilder,
    export_to_json,
    format_markdown,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back by publish and engagement time (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum engagement score, using fallback for missing rows (default: {DEFAULT_MIN_SCORE:g})",
    )
    parser.add_argument(
        "--topic",
        action="append",
        default=None,
        help="Topic filter; may be repeated",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum sections to include (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument("--json", action="store_true", help="Print stable JSON output")
    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Print Markdown output suitable for a newsletter draft",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    with script_context() as (_config, db):
        export = ThreadNewsletterSectionBuilder(db).build_export(
            days=args.days,
            min_score=args.min_score,
            topics=args.topic,
            limit=args.limit,
        )

    if args.json:
        print(export_to_json(export))
    else:
        print(format_markdown(export), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
