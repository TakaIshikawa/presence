#!/usr/bin/env python3
"""Report GitHub activity attribution lag."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.github_activity_attribution_lag import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    build_github_activity_attribution_lag_report,
    format_github_activity_attribution_lag_json,
    format_github_activity_attribution_lag_text,
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
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_github_activity_attribution_lag_report(
                db,
                days=args.days,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_github_activity_attribution_lag_json(report)
        if args.format == "json"
        else format_github_activity_attribution_lag_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
