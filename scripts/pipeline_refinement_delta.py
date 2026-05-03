#!/usr/bin/env python3
"""Pipeline refinement delta report CLI."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.pipeline_refinement_delta import (  # noqa: E402
    DEFAULT_DAYS,
    build_pipeline_refinement_delta_report,
    format_pipeline_refinement_delta_csv,
    format_pipeline_refinement_delta_json,
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


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _float_value(value: str) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=str,
        default=None,
        help="Database path (default: use runner script_context)",
    )
    parser.add_argument(
        "--content-type",
        type=str,
        default=None,
        help="Filter by content_type (e.g., x_thread, x_post)",
    )
    parser.add_argument(
        "--outcome",
        type=str,
        default=None,
        help="Filter by outcome (e.g., published, below_threshold, all_filtered)",
    )
    parser.add_argument(
        "--refinement-picked",
        type=str,
        default=None,
        help="Filter by refinement_picked (e.g., REFINED, ORIGINAL)",
    )
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days (default: {DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--min-delta",
        type=_float_value,
        default=None,
        help="Minimum absolute delta to include in details (default: no filter)",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=None,
        help="Maximum number of detail rows; 0 means no limit (default: no limit)",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (default: json)",
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
        if args.db:
            # Use explicit database path
            import sqlite3
            from storage.db import Database
            conn = sqlite3.connect(args.db)
            conn.row_factory = sqlite3.Row
            db = Database(conn)
        else:
            # Use runner script_context
            with script_context() as (_config, db):
                report = build_pipeline_refinement_delta_report(
                    db,
                    content_type=args.content_type,
                    outcome=args.outcome,
                    refinement_picked=args.refinement_picked,
                    days=args.days,
                    min_delta=args.min_delta,
                    limit=None if args.limit == 0 else args.limit,
                )

                if args.format == "csv":
                    print(format_pipeline_refinement_delta_csv(report))
                else:
                    print(format_pipeline_refinement_delta_json(report))

                return 0

        # If using explicit db path
        report = build_pipeline_refinement_delta_report(
            db,
            content_type=args.content_type,
            outcome=args.outcome,
            refinement_picked=args.refinement_picked,
            days=args.days,
            min_delta=args.min_delta,
            limit=None if args.limit == 0 else args.limit,
        )

        if args.format == "csv":
            print(format_pipeline_refinement_delta_csv(report))
        else:
            print(format_pipeline_refinement_delta_json(report))

        return 0

    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
