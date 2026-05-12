#!/usr/bin/env python3
"""Report generated blog candidates by publication readiness."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_publication_readiness import (  # noqa: E402
    DEFAULT_MAX_SOURCE_AGE_DAYS,
    build_blog_publication_readiness_report,
    format_blog_publication_readiness_json,
    format_blog_publication_readiness_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ready-only", action="store_true")
    parser.add_argument("--max-source-age-days", type=_positive_int, default=DEFAULT_MAX_SOURCE_AGE_DAYS)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_blog_publication_readiness_report(
                db,
                ready_only=args.ready_only,
                max_source_age_days=args.max_source_age_days,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if args.json or args.format == "json":
        print(format_blog_publication_readiness_json(report))
    else:
        print(format_blog_publication_readiness_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
