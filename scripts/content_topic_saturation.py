#!/usr/bin/env python3
"""Report content topic saturation."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_topic_saturation import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_LOW_CONFIDENCE_THRESHOLD,
    DEFAULT_OVERUSED_TOPIC_COUNT,
    DEFAULT_STALE_AFTER_DAYS,
    build_content_topic_saturation_report,
    format_content_topic_saturation_json,
    format_content_topic_saturation_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--stale-after-days", type=int, default=DEFAULT_STALE_AFTER_DAYS)
    parser.add_argument("--overused-topic-count", type=int, default=DEFAULT_OVERUSED_TOPIC_COUNT)
    parser.add_argument("--low-confidence-threshold", type=float, default=DEFAULT_LOW_CONFIDENCE_THRESHOLD)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_content_topic_saturation_report(
                db,
                days=args.days,
                stale_after_days=args.stale_after_days,
                overused_topic_count=args.overused_topic_count,
                low_confidence_threshold=args.low_confidence_threshold,
                limit=args.limit,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_content_topic_saturation_json(report) if args.format == "json" else format_content_topic_saturation_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
