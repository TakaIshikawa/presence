#!/usr/bin/env python3
"""Report unusual newsletter link-click distributions."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_click_anomalies import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_DOMINANCE_THRESHOLD,
    build_newsletter_click_anomaly_report,
    format_newsletter_click_anomaly_json,
    format_newsletter_click_anomaly_text,
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


def _share(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if parsed <= 0 or parsed > 1:
        raise argparse.ArgumentTypeError("value must be greater than 0 and at most 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days by sent_at (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--dominance-threshold",
        type=_share,
        default=DEFAULT_DOMINANCE_THRESHOLD,
        help=(
            "Flag links whose click share meets or exceeds this value "
            f"(default: {DEFAULT_DOMINANCE_THRESHOLD})."
        ),
    )
    parser.add_argument(
        "--send-id",
        type=_positive_int,
        help="Only inspect one newsletter send id.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_newsletter_click_anomaly_report(
                db,
                days=args.days,
                dominance_threshold=args.dominance_threshold,
                send_id=args.send_id,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_click_anomaly_json(report))
    else:
        print(format_newsletter_click_anomaly_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
