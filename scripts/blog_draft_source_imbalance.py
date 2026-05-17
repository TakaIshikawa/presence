#!/usr/bin/env python3
"""Report blog drafts with concentrated source usage."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_draft_source_imbalance import (  # noqa: E402
    DEFAULT_CONCENTRATION_THRESHOLD,
    DEFAULT_MIN_SOURCES,
    build_blog_draft_source_imbalance_report,
    format_blog_draft_source_imbalance_json,
    format_blog_draft_source_imbalance_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--threshold", type=float, default=DEFAULT_CONCENTRATION_THRESHOLD)
    parser.add_argument("--min-sources", type=int, default=DEFAULT_MIN_SOURCES)
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_blog_draft_source_imbalance_report(
                db,
                concentration_threshold=args.threshold,
                min_sources=args.min_sources,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_blog_draft_source_imbalance_json(report)
        if args.format == "json"
        else format_blog_draft_source_imbalance_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
