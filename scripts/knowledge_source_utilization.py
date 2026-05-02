#!/usr/bin/env python3
"""Report ingested knowledge sources with low downstream utilization."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_utilization import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_ITEMS,
    DEFAULT_UNUSED_THRESHOLD,
    build_knowledge_source_utilization_report,
    format_knowledge_source_utilization_json,
    format_knowledge_source_utilization_text,
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


def _threshold(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Look back at recently ingested knowledge rows (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-items",
        type=_positive_int,
        default=DEFAULT_MIN_ITEMS,
        help=f"Minimum grouped knowledge rows to report (default: {DEFAULT_MIN_ITEMS}).",
    )
    parser.add_argument(
        "--unused-threshold",
        type=_threshold,
        default=DEFAULT_UNUSED_THRESHOLD,
        help=(
            "Minimum unused share for a source group, from 0 to 1 "
            f"(default: {DEFAULT_UNUSED_THRESHOLD})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
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
            report = build_knowledge_source_utilization_report(
                db,
                days=args.days,
                min_items=args.min_items,
                unused_threshold=args.unused_threshold,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_knowledge_source_utilization_json(report))
    else:
        print(format_knowledge_source_utilization_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
