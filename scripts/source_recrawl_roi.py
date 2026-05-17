#!/usr/bin/env python3
"""Rank knowledge sources by recrawl ROI."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_recrawl_roi import (  # noqa: E402
    DEFAULT_FAILURE_WEIGHT,
    DEFAULT_STALENESS_WEIGHT,
    DEFAULT_USAGE_WEIGHT,
    build_source_recrawl_roi_report,
    format_source_recrawl_roi_json,
    format_source_recrawl_roi_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staleness-weight", type=float, default=DEFAULT_STALENESS_WEIGHT)
    parser.add_argument("--usage-weight", type=float, default=DEFAULT_USAGE_WEIGHT)
    parser.add_argument("--failure-weight", type=float, default=DEFAULT_FAILURE_WEIGHT)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_source_recrawl_roi_report(
                db,
                staleness_weight=args.staleness_weight,
                usage_weight=args.usage_weight,
                failure_weight=args.failure_weight,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_source_recrawl_roi_json(report)
        if args.format == "json"
        else format_source_recrawl_roi_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
