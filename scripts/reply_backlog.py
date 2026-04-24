#!/usr/bin/env python3
"""Report pending reply backlog triage buckets."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_backlog import (  # noqa: E402
    DEFAULT_DAYS,
    build_reply_backlog_report,
    format_text_report,
)
from runner import script_context  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize pending reply_queue drafts by urgency and review risk."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Look back this many days for pending replies (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of item-level rows to include after urgency sorting.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON.",
    )
    parser.add_argument(
        "--min-age-hours",
        type=float,
        default=0.0,
        help="Only include replies at least this old.",
    )
    parser.add_argument(
        "--include-low-priority",
        action="store_true",
        help="Include low-priority replies in the triage report.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING if args.json else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = build_reply_backlog_report(
            db,
            days=args.days,
            limit=args.limit,
            min_age_hours=args.min_age_hours,
            include_low_priority=args.include_low_priority,
        )

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
