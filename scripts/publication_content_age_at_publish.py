#!/usr/bin/env python3
"""Report content age when publications go live."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_content_age_at_publish import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_STALE_HOURS,
    build_publication_content_age_at_publish_report,
    format_publication_content_age_at_publish_json,
    format_publication_content_age_at_publish_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--stale-hours", type=_positive_float, default=DEFAULT_STALE_HOURS)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_publication_content_age_at_publish_report(db, days=args.days, stale_hours=args.stale_hours)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_text = args.table or args.format == "text"
    print(format_publication_content_age_at_publish_text(report) if as_text else format_publication_content_age_at_publish_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
