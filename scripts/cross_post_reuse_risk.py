#!/usr/bin/env python3
"""Report near-identical generated content reused across platforms."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.cross_post_reuse_risk import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_SIMILARITY,
    build_cross_post_reuse_risk_report,
    format_cross_post_reuse_risk_json,
    format_cross_post_reuse_risk_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _similarity(value: str) -> float:
    parsed = float(value)
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--min-similarity", type=_similarity, default=DEFAULT_MIN_SIMILARITY)
    parser.add_argument("--platform")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_cross_post_reuse_risk_report(
                    conn,
                    days=args.days,
                    min_similarity=args.min_similarity,
                    platform=args.platform,
                )
        else:
            with script_context() as (_config, db):
                report = build_cross_post_reuse_risk_report(
                    db,
                    days=args.days,
                    min_similarity=args.min_similarity,
                    platform=args.platform,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_cross_post_reuse_risk_json(report) if args.format == "json" else format_cross_post_reuse_risk_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
