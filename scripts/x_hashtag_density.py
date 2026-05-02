#!/usr/bin/env python3
"""Report X post hashtag density and style drift."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.x_hashtag_density import (  # noqa: E402
    DEFAULT_BASELINE_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MAX_HASHTAG_CHAR_SHARE,
    DEFAULT_MAX_HASHTAGS,
    DEFAULT_RECENT_DAYS,
    DEFAULT_REPEATED_SET_THRESHOLD,
    build_x_hashtag_density_report,
    format_x_hashtag_density_json,
    format_x_hashtag_density_text,
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
        raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if parsed <= 0 or parsed > 1:
        raise argparse.ArgumentTypeError("value must be greater than 0 and at most 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--recent-days",
        type=_positive_int,
        default=DEFAULT_RECENT_DAYS,
        help=f"Recent window in days for posts to flag (default: {DEFAULT_RECENT_DAYS}).",
    )
    parser.add_argument(
        "--baseline-days",
        type=_positive_int,
        default=DEFAULT_BASELINE_DAYS,
        help=f"Baseline window before the recent window (default: {DEFAULT_BASELINE_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum rows to inspect across both windows (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--max-hashtags",
        type=_positive_int,
        default=DEFAULT_MAX_HASHTAGS,
        help=f"Flag posts above this hashtag count (default: {DEFAULT_MAX_HASHTAGS}).",
    )
    parser.add_argument(
        "--max-hashtag-char-share",
        type=_share,
        default=DEFAULT_MAX_HASHTAG_CHAR_SHARE,
        help=(
            "Flag posts whose hashtag characters exceed this share of visible copy "
            f"(default: {DEFAULT_MAX_HASHTAG_CHAR_SHARE})."
        ),
    )
    parser.add_argument(
        "--repeated-set-threshold",
        type=_positive_int,
        default=DEFAULT_REPEATED_SET_THRESHOLD,
        help=(
            "Flag identical non-empty hashtag sets repeated at least this many times "
            f"(default: {DEFAULT_REPEATED_SET_THRESHOLD})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)
        with script_context() as (_config, db):
            report = build_x_hashtag_density_report(
                db,
                recent_days=args.recent_days,
                baseline_days=args.baseline_days,
                limit=args.limit,
                max_hashtags=args.max_hashtags,
                max_hashtag_char_share=args.max_hashtag_char_share,
                repeated_set_threshold=args.repeated_set_threshold,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_x_hashtag_density_json(report))
    else:
        print(format_x_hashtag_density_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
