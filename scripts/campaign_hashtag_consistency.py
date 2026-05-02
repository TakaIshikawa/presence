#!/usr/bin/env python3
"""Report inconsistent hashtag usage across campaign posts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.campaign_hashtag_consistency import (  # noqa: E402
    DEFAULT_MAX_HASHTAGS,
    build_campaign_hashtag_consistency_report,
    format_campaign_hashtag_consistency_json,
    format_campaign_hashtag_consistency_text,
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
    parser.add_argument(
        "--campaign",
        help="Only audit one campaign by id or exact name.",
    )
    parser.add_argument(
        "--max-hashtags",
        type=_positive_int,
        default=DEFAULT_MAX_HASHTAGS,
        help=f"Maximum hashtags allowed per content item (default: {DEFAULT_MAX_HASHTAGS}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_campaign_hashtag_consistency_report(
                db,
                campaign=args.campaign,
                max_hashtags=args.max_hashtags,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_campaign_hashtag_consistency_json(report))
    else:
        print(format_campaign_hashtag_consistency_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
