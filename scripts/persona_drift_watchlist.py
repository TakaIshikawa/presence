#!/usr/bin/env python3
"""Report generated content whose persona signals may be drifting."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.persona_drift_watchlist import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_SEVERITY,
    build_persona_drift_watchlist_report,
    format_persona_drift_watchlist_json,
    format_persona_drift_watchlist_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--min-severity", type=int, default=DEFAULT_MIN_SEVERITY)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_persona_drift_watchlist_report(
                db,
                days=args.days,
                min_severity=args.min_severity,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.json or args.format == "json":
        print(format_persona_drift_watchlist_json(report))
    else:
        print(format_persona_drift_watchlist_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
