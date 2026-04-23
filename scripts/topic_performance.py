#!/usr/bin/env python3
"""Report topic-level engagement performance for calibration."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.topic_performance import (  # noqa: E402
    TopicPerformanceAnalyzer,
    format_topic_performance_json,
    format_topic_performance_table,
)
from runner import script_context  # noqa: E402


def _parse_topics(values: list[str] | None) -> list[str]:
    """Parse repeated or comma-separated topic arguments."""
    if not values:
        return []

    topics: list[str] = []
    for value in values:
        for item in value.split(","):
            topic = item.strip()
            if topic:
                topics.append(topic)
    return topics


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Look back this many days (default: 90)",
    )
    parser.add_argument(
        "--platform",
        default="all",
        choices=["all", "x", "bluesky"],
        help="Restrict to one platform (default: all)",
    )
    parser.add_argument(
        "--content-type",
        help="Restrict to a specific content type",
    )
    parser.add_argument(
        "--topics",
        nargs="*",
        help="Optional topic filter. Accepts space-separated or comma-separated values.",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=1,
        help="Require at least this many samples per topic (default: 1)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output machine-readable JSON",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    requested_topics = _parse_topics(args.topics)

    with script_context() as (_config, db):
        analyzer = TopicPerformanceAnalyzer(db)
        report = analyzer.build_topic_performance_report(
            topics=requested_topics or None,
            days=args.days,
            content_type=args.content_type,
            platform=args.platform,
            min_samples=args.min_samples,
        )

    if args.json:
        print(format_topic_performance_json(report))
    else:
        print(format_topic_performance_table(report))


if __name__ == "__main__":
    main()
