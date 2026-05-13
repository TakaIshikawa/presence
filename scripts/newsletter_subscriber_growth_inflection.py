#!/usr/bin/env python3
"""Report newsletter subscriber growth inflections."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_subscriber_growth_inflection import (  # noqa: E402
    DEFAULT_CHURN_SPIKE_DELTA,
    DEFAULT_DAYS,
    DEFAULT_GROWTH_DROP_DELTA,
    DEFAULT_GROWTH_SPIKE_DELTA,
    DEFAULT_LIMIT,
    build_newsletter_subscriber_growth_inflection_report,
    format_newsletter_subscriber_growth_inflection_json,
    format_newsletter_subscriber_growth_inflection_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--growth-spike-delta", type=int, default=DEFAULT_GROWTH_SPIKE_DELTA)
    parser.add_argument("--growth-drop-delta", type=int, default=DEFAULT_GROWTH_DROP_DELTA)
    parser.add_argument("--churn-spike-delta", type=float, default=DEFAULT_CHURN_SPIKE_DELTA)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_newsletter_subscriber_growth_inflection_report(
                db,
                days=args.days,
                growth_spike_delta=args.growth_spike_delta,
                growth_drop_delta=args.growth_drop_delta,
                churn_spike_delta=args.churn_spike_delta,
                limit=args.limit,
            )
    except (sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_newsletter_subscriber_growth_inflection_json(report) if args.format == "json" else format_newsletter_subscriber_growth_inflection_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
