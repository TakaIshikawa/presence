#!/usr/bin/env python3
"""Report continuity quality across generated X thread candidates."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from synthesis.thread_continuity import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_OPENING_TOKENS,
    DEFAULT_MIN_OVERLAP,
    build_thread_continuity_report,
    format_thread_continuity_json,
    read_thread_continuity_input,
)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_path", help="JSON or JSONL file of candidate X threads.")
    parser.add_argument(
        "--min-overlap",
        type=_non_negative_float,
        default=DEFAULT_MIN_OVERLAP,
        help=(
            "Minimum adjacent-post key-term overlap before flagging an abrupt shift "
            f"(default: {DEFAULT_MIN_OVERLAP})."
        ),
    )
    parser.add_argument(
        "--max-opening-tokens",
        type=_positive_int,
        default=DEFAULT_MAX_OPENING_TOKENS,
        help=(
            "Opening token count used to detect repeated starts "
            f"(default: {DEFAULT_MAX_OPENING_TOKENS})."
        ),
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum thread records to emit (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        records = read_thread_continuity_input(args.input_path)
        report = build_thread_continuity_report(
            records,
            min_overlap=args.min_overlap,
            max_opening_tokens=args.max_opening_tokens,
            limit=args.limit,
        )
    except (OSError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_thread_continuity_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
