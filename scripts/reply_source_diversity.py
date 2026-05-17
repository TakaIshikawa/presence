#!/usr/bin/env python3
"""Report reply draft source concentration."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_source_diversity import (  # noqa: E402
    DEFAULT_CONCENTRATION_THRESHOLD,
    DEFAULT_DAYS,
    build_reply_source_diversity_report,
    format_reply_source_diversity_json,
    format_reply_source_diversity_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--threshold", type=float, default=DEFAULT_CONCENTRATION_THRESHOLD)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_reply_source_diversity_report(
                db,
                days=args.days,
                concentration_threshold=args.threshold,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_reply_source_diversity_json(report)
        if args.format == "json"
        else format_reply_source_diversity_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
