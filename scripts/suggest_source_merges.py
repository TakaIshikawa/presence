#!/usr/bin/env python3
"""Suggest likely duplicate curated_sources rows for manual merging."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_merge_suggestions import (  # noqa: E402
    DEFAULT_MIN_CONFIDENCE,
    build_source_merge_suggestion_report,
    format_source_merge_suggestion_json,
    format_source_merge_suggestion_text,
)
from runner import script_context  # noqa: E402


def _confidence(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid confidence: {value}") from exc
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("confidence must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-type", help="Only include one curated source type.")
    parser.add_argument("--status", help="Only include curated sources with one status.")
    parser.add_argument(
        "--min-confidence",
        type=_confidence,
        default=DEFAULT_MIN_CONFIDENCE,
        help=f"Minimum suggestion confidence from 0 to 1 (default: {DEFAULT_MIN_CONFIDENCE}).",
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
            report = build_source_merge_suggestion_report(
                db,
                source_type=args.source_type,
                status=args.status,
                min_confidence=args.min_confidence,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_source_merge_suggestion_json(report))
    else:
        print(format_source_merge_suggestion_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
