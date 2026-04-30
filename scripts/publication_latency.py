#!/usr/bin/env python3
"""Report publication latency from queueing and scheduling to platform success."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_latency import (  # noqa: E402
    format_publication_latency_json,
    format_publication_latency_text,
    build_publication_latency_report,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to look back (default: 7)",
    )
    parser.add_argument(
        "--platform",
        choices=["all", "x", "bluesky"],
        default="all",
        help="Platform to include (default: all)",
    )
    parser.add_argument(
        "--queued-threshold-minutes",
        type=float,
        default=60.0,
        help="Flag queue-created-to-success latency above this many minutes (default: 60)",
    )
    parser.add_argument(
        "--scheduled-threshold-minutes",
        type=float,
        default=15.0,
        help="Flag scheduled-to-success latency above this many minutes (default: 15)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "text"],
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
            report = build_publication_latency_report(
                db,
                days=args.days,
                platform=args.platform,
                queued_threshold_minutes=args.queued_threshold_minutes,
                scheduled_threshold_minutes=args.scheduled_threshold_minutes,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publication_latency_json(report))
    else:
        print(format_publication_latency_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
