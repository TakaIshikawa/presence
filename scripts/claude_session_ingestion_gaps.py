#!/usr/bin/env python3
"""Report Claude session ingestion gaps compared with GitHub commits."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.session_ingestion_gaps import (  # noqa: E402
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MIN_COMMITS,
    build_session_ingestion_gaps_report,
    format_session_ingestion_gaps_json,
    format_session_ingestion_gaps_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--min-commits", type=_positive_int, default=DEFAULT_MIN_COMMITS)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_session_ingestion_gaps_report(
                db,
                lookback_days=args.lookback_days,
                min_commits=args.min_commits,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.json or args.format == "json":
        print(format_session_ingestion_gaps_json(report))
    else:
        print(format_session_ingestion_gaps_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
