#!/usr/bin/env python3
"""Plan source reattachment for orphaned knowledge rows."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.orphan_source_reattachment import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_CONFIDENCE,
    build_knowledge_source_reattachment_plan,
    format_knowledge_source_reattachment_json,
    format_knowledge_source_reattachment_text,
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


def _confidence(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid confidence: {value}") from exc
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum orphaned knowledge rows to inspect (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--min-confidence",
        type=_confidence,
        default=DEFAULT_MIN_CONFIDENCE,
        help=(
            "Minimum confidence required to emit a candidate "
            f"(default: {DEFAULT_MIN_CONFIDENCE})."
        ),
    )
    parser.add_argument(
        "--source-type",
        help="Only inspect orphaned knowledge rows with this source_type.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")
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
            plan = build_knowledge_source_reattachment_plan(
                db,
                limit=args.limit,
                min_confidence=args.min_confidence,
                source_type=args.source_type,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_knowledge_source_reattachment_json(plan))
    else:
        print(format_knowledge_source_reattachment_text(plan))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
