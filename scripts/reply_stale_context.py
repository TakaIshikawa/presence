#!/usr/bin/env python3
"""Audit queued reply drafts for stale source context."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_stale_context import (  # noqa: E402
    DEFAULT_MAX_AGE_HOURS,
    build_reply_stale_context_report,
    format_reply_stale_context_json,
    format_reply_stale_context_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=DEFAULT_MAX_AGE_HOURS,
        help=f"Draft age threshold for stale context (default: {DEFAULT_MAX_AGE_HOURS:g}).",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--status-filter",
        action="append",
        default=None,
        help="Reply status to include; repeat for multiple statuses or use 'all'.",
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
            report = build_reply_stale_context_report(
                db,
                max_age_hours=args.max_age_hours,
                status_filter=args.status_filter or ["pending"],
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_stale_context_json(report))
    else:
        print(format_reply_stale_context_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
