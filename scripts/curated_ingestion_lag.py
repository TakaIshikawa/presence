#!/usr/bin/env python3
"""Report lag between curated publication, ingestion, embedding, and first use."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.curated_ingestion_lag import (  # noqa: E402
    DEFAULT_LAG_THRESHOLD_HOURS,
    DEFAULT_LIMIT,
    build_curated_ingestion_lag_report,
    format_curated_ingestion_lag_json,
    format_curated_ingestion_lag_text,
)


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _load_rows(path: str) -> list[dict[str, object]]:
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError(f"{path} must contain a JSON array")
    return [dict(row) for row in data]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows-json", required=True, help="JSON array of curated item rows")
    parser.add_argument("--lag-threshold-hours", type=_positive_float, default=DEFAULT_LAG_THRESHOLD_HOURS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        report = build_curated_ingestion_lag_report(
            _load_rows(args.rows_json),
            lag_threshold_hours=args.lag_threshold_hours,
            limit=args.limit,
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_curated_ingestion_lag_text(report)
        if args.table or args.format == "text"
        else format_curated_ingestion_lag_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
