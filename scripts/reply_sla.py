#!/usr/bin/env python3
"""Report pending reply SLA breach and deadline risk."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_sla import (  # noqa: E402
    DEFAULT_HIGH_HOURS,
    DEFAULT_LOW_HOURS,
    DEFAULT_NORMAL_HOURS,
    build_reply_sla_report,
    format_json_report,
    format_text_report,
)
from runner import script_context  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize pending reply_queue drafts by SLA breach risk."
    )
    parser.add_argument(
        "--high-hours",
        type=float,
        default=DEFAULT_HIGH_HOURS,
        help=f"SLA window for high-priority replies (default: {DEFAULT_HIGH_HOURS}).",
    )
    parser.add_argument(
        "--normal-hours",
        type=float,
        default=DEFAULT_NORMAL_HOURS,
        help=f"SLA window for normal-priority replies (default: {DEFAULT_NORMAL_HOURS}).",
    )
    parser.add_argument(
        "--low-hours",
        type=float,
        default=DEFAULT_LOW_HOURS,
        help=f"SLA window for low-priority replies (default: {DEFAULT_LOW_HOURS}).",
    )
    parser.add_argument(
        "--platform",
        choices=["x", "bluesky"],
        help="Filter to a single platform.",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of item-level rows to include after SLA ordering.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    for name in ("high_hours", "normal_hours", "low_hours"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")

    logging.basicConfig(
        level=logging.WARNING if args.format == "json" else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        report = build_reply_sla_report(
            db,
            high_hours=args.high_hours,
            normal_hours=args.normal_hours,
            low_hours=args.low_hours,
            platform=args.platform,
            limit=args.limit,
        )

    if args.format == "json":
        print(format_json_report(report))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
