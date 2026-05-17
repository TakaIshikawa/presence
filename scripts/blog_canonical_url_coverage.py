#!/usr/bin/env python3
"""Report blog canonical URL coverage and slug mismatches."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_canonical_url_coverage import (  # noqa: E402
    build_blog_canonical_url_coverage_report,
    format_blog_canonical_url_coverage_json,
    format_blog_canonical_url_coverage_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-base-url", help="Expected blog URL prefix for slug checks.")
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_blog_canonical_url_coverage_report(
                db,
                expected_base_url=args.expected_base_url,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_blog_canonical_url_coverage_json(report)
        if args.format == "json"
        else format_blog_canonical_url_coverage_text(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
