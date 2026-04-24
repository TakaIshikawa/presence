#!/usr/bin/env python3
"""Export planned content calendar as iCalendar."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.calendar_export import export_calendar
from runner import script_context


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to include from the reporting start date",
    )
    parser.add_argument(
        "--output",
        help="Write ICS output to this file instead of stdout",
    )
    parser.add_argument(
        "--include-queue",
        action="store_true",
        help="Include scheduled publish queue items and publication retries",
    )
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=None,
        help="Reporting start date in YYYY-MM-DD format; defaults to today",
    )
    args = parser.parse_args(argv)
    if args.days < 1:
        parser.error("--days must be positive")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    start = args.start_date or date.today()

    with script_context() as (_config, db):
        ics = export_calendar(
            db,
            start=start,
            days=args.days,
            include_queue=args.include_queue,
        )

    if args.output:
        Path(args.output).write_text(ics, encoding="utf-8")
    else:
        sys.stdout.write(ics)


if __name__ == "__main__":
    main()
