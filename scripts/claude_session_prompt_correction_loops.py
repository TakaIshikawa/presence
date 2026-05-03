#!/usr/bin/env python3
"""Report Claude Code prompt correction loops from parsed session events."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.claude_session_prompt_correction_loops import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_SNIPPET_CHARS,
    build_claude_session_prompt_correction_loops_report,
    format_claude_session_prompt_correction_loops_json,
    read_claude_session_rows,
)


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
    parser.add_argument("input_path", help="JSON or JSONL file of parsed Claude session events.")
    parser.add_argument(
        "--max-snippet-chars",
        type=_positive_int,
        default=DEFAULT_SNIPPET_CHARS,
        help=f"Maximum snippet length in characters (default: {DEFAULT_SNIPPET_CHARS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum correction loop rows to emit (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        rows = read_claude_session_rows(args.input_path)
        report = build_claude_session_prompt_correction_loops_report(
            rows,
            max_snippet_chars=args.max_snippet_chars,
            limit=args.limit,
        )
    except (OSError, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_claude_session_prompt_correction_loops_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
