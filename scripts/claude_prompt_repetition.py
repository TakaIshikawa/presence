#!/usr/bin/env python3
"""Report repeated Claude Code user prompts from recent sessions."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.claude_prompt_repetition import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_THRESHOLD,
    build_claude_prompt_repetition_report,
    format_claude_prompt_repetition_json,
    format_claude_prompt_repetition_text,
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for Claude messages (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--threshold",
        type=_positive_int,
        default=DEFAULT_THRESHOLD,
        help=f"Minimum repeated prompts per session/signature to report (default: {DEFAULT_THRESHOLD}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum flagged sessions to print (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--project-path",
        help="Restrict to one claude_messages.project_path when that column exists.",
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
            report = build_claude_prompt_repetition_report(
                db,
                days=args.days,
                threshold=args.threshold,
                limit=args.limit,
                project_path=args.project_path,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_claude_prompt_repetition_json(report))
    else:
        print(format_claude_prompt_repetition_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
