#!/usr/bin/env python3
"""Lint generated image prompts before visual publishing."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.image_prompt_safety import (  # noqa: E402
    DEFAULT_DAYS,
    build_image_prompt_safety_report,
    format_image_prompt_safety_json,
    format_image_prompt_safety_text,
    should_fail,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--content-id", type=int, help="Only lint one generated_content id.")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Only include generated content from the last N days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on",
        choices=("warn", "error"),
        default="error",
        help="Exit non-zero when findings are at least this severity (default: error).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_image_prompt_safety_report(
                    conn,
                    content_id=args.content_id,
                    days=args.days,
                    fail_on=args.fail_on,
                )
        else:
            with script_context() as (_config, db):
                report = build_image_prompt_safety_report(
                    db,
                    content_id=args.content_id,
                    days=args.days,
                    fail_on=args.fail_on,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_image_prompt_safety_json(report))
    else:
        print(format_image_prompt_safety_text(report))
    return 1 if should_fail(report["findings"], fail_on=args.fail_on) else 0


if __name__ == "__main__":
    raise SystemExit(main())
