#!/usr/bin/env python3
"""Audit commit_prompt_links quality for recent commits and Claude messages."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.commit_session_audit import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MAX_GAP_HOURS,
    DEFAULT_MIN_CONFIDENCE,
    build_commit_session_audit_report,
    format_commit_session_audit_json,
    format_commit_session_audit_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of days to look back (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-confidence",
        type=float,
        default=DEFAULT_MIN_CONFIDENCE,
        help=(
            "Minimum acceptable commit/message link confidence "
            f"(default: {DEFAULT_MIN_CONFIDENCE})."
        ),
    )
    parser.add_argument(
        "--max-gap-hours",
        type=float,
        default=DEFAULT_MAX_GAP_HOURS,
        help=(
            "Maximum acceptable commit/message timestamp gap in hours "
            f"(default: {DEFAULT_MAX_GAP_HOURS})."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output deterministic JSON instead of text.",
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
            report = build_commit_session_audit_report(
                db,
                days=args.days,
                min_confidence=args.min_confidence,
                max_gap_hours=args.max_gap_hours,
            )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_commit_session_audit_json(report))
    else:
        print(format_commit_session_audit_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
