#!/usr/bin/env python3
"""Report source domains that need citation rotation."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.source_domain_rotation_gaps import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_ROLLING_SHARE,
    DEFAULT_WINDOW_SIZE,
    build_source_domain_rotation_gaps_report_from_db,
    format_source_domain_rotation_gaps_json,
    format_source_domain_rotation_gaps_text,
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


def _share(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid share: {value}") from exc
    if not 0 < parsed <= 1:
        raise argparse.ArgumentTypeError("share must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window-size", type=_positive_int, default=DEFAULT_WINDOW_SIZE)
    parser.add_argument("--max-rolling-share", type=_share, default=DEFAULT_MAX_ROLLING_SHARE)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Output text table.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_source_domain_rotation_gaps_report_from_db(
                db,
                window_size=args.window_size,
                max_rolling_share=args.max_rolling_share,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_source_domain_rotation_gaps_text(report) if args.table or args.format == "text" else format_source_domain_rotation_gaps_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
