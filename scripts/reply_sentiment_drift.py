#!/usr/bin/env python3
"""Report reply quality sentiment drift over time."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_sentiment_drift import (  # noqa: E402
    DEFAULT_BUCKET,
    DEFAULT_DAYS,
    DEFAULT_LOW_QUALITY_THRESHOLD,
    DEFAULT_MIN_BUCKET_SAMPLE,
    DEFAULT_TOP_N,
    build_reply_sentiment_drift_report,
    format_reply_sentiment_drift_json,
    format_reply_sentiment_drift_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days",
        "--days",
        dest="days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--bucket",
        choices=("day", "week"),
        default=DEFAULT_BUCKET,
        help=f"Trend bucket size (default: {DEFAULT_BUCKET}).",
    )
    parser.add_argument(
        "--platform",
        help="Only include one publication platform, e.g. x, bluesky, linkedin, or mastodon.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Number of repeated low-quality target handles to show (default: {DEFAULT_TOP_N}).",
    )
    parser.add_argument(
        "--low-quality-threshold",
        type=float,
        default=DEFAULT_LOW_QUALITY_THRESHOLD,
        help=(
            "Quality score below this value counts as low quality "
            f"(default: {DEFAULT_LOW_QUALITY_THRESHOLD:.1f})."
        ),
    )
    parser.add_argument(
        "--min-bucket-sample",
        type=int,
        default=DEFAULT_MIN_BUCKET_SAMPLE,
        help=(
            "Minimum scored drafts in a bucket before using it for drift warnings "
            f"(default: {DEFAULT_MIN_BUCKET_SAMPLE})."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print deterministic JSON instead of the default text report.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        with script_context() as (_config, db):
            report = build_reply_sentiment_drift_report(
                db,
                days=args.days,
                bucket=args.bucket,
                platform=args.platform,
                top_n=args.top_n,
                low_quality_threshold=args.low_quality_threshold,
                min_bucket_sample=args.min_bucket_sample,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_reply_sentiment_drift_json(report))
    else:
        print(format_reply_sentiment_drift_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
