#!/usr/bin/env python3
"""Report stale or repeatedly reused source content in newsletter sends."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_source_freshness import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MAX_REUSE_COUNT,
    DEFAULT_MAX_SOURCE_AGE_DAYS,
    build_newsletter_source_freshness,
    format_newsletter_source_freshness_json,
    format_newsletter_source_freshness_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--max-source-age-days",
        type=int,
        default=DEFAULT_MAX_SOURCE_AGE_DAYS,
        help=(
            "Warn when a source was created more than this many days before "
            f"the send (default: {DEFAULT_MAX_SOURCE_AGE_DAYS})"
        ),
    )
    parser.add_argument(
        "--max-reuse-count",
        type=int,
        default=DEFAULT_MAX_REUSE_COUNT,
        help=(
            "Warn when a source appears in more than this many filtered sends "
            f"(default: {DEFAULT_MAX_REUSE_COUNT})"
        ),
    )
    parser.add_argument(
        "--issue-id",
        default=None,
        help="Only inspect newsletter sends with this issue_id",
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
            report = build_newsletter_source_freshness(
                db,
                days=args.days,
                max_source_age_days=args.max_source_age_days,
                max_reuse_count=args.max_reuse_count,
                issue_id=args.issue_id,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_source_freshness_json(report))
    else:
        print(format_newsletter_source_freshness_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
