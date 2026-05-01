#!/usr/bin/env python3
"""Plan read-only revalidation actions for pending publish queue rows."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_queue_revalidation import (  # noqa: E402
    VALID_PLATFORMS,
    VALID_STATUSES,
    format_publish_queue_revalidation_json,
    format_publish_queue_revalidation_text,
    plan_publish_queue_revalidation,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--status",
        default="all",
        choices=VALID_STATUSES,
        help="Queue status to scan; all means queued and held (default: all)",
    )
    parser.add_argument(
        "--platform",
        default="all",
        choices=VALID_PLATFORMS,
        help="Queued platform target to scan (default: all)",
    )
    parser.add_argument(
        "--min-age-hours",
        type=float,
        default=0.0,
        help="Only scan rows at least this many hours past scheduled_at (default: 0)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of queue rows to scan after status/platform filtering",
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
            report = plan_publish_queue_revalidation(
                db,
                status=args.status,
                platform=args.platform,
                min_age_hours=args.min_age_hours,
                limit=args.limit,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publish_queue_revalidation_json(report))
    else:
        print(format_publish_queue_revalidation_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
