#!/usr/bin/env python3
"""Report proactive action relationship context coverage."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.proactive_relationship_coverage import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_LOOKBACK_DAYS,
    build_proactive_relationship_coverage_report,
    format_proactive_relationship_coverage_json,
    format_proactive_relationship_coverage_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_proactive_relationship_coverage_report(
                db,
                lookback_days=args.lookback_days,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_proactive_relationship_coverage_json(report)
        if args.format == "json"
        else format_proactive_relationship_coverage_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
