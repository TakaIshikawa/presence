#!/usr/bin/env python3
"""Report pending draft review age distribution."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.draft_review_age_distribution import (  # noqa: E402
    build_draft_review_age_distribution_report,
    format_draft_review_age_distribution_json,
    format_draft_review_age_distribution_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--oldest-limit", type=_positive_int, default=10)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_draft_review_age_distribution_report(db, oldest_limit=args.oldest_limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_draft_review_age_distribution_json(report)
        if args.format == "json"
        else format_draft_review_age_distribution_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
