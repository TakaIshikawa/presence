#!/usr/bin/env python3
"""Report planned topic schedule collisions."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.planned_topic_schedule_collisions import (  # noqa: E402
    DEFAULT_LIMIT,
    build_planned_topic_schedule_collisions_report,
    format_planned_topic_schedule_collisions_json,
    format_planned_topic_schedule_collisions_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_planned_topic_schedule_collisions_report(db, limit=args.limit)
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_planned_topic_schedule_collisions_json(report) if args.format == "json" else format_planned_topic_schedule_collisions_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
