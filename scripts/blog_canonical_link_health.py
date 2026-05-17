#!/usr/bin/env python3
"""Report generated blog post canonical URL health."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_canonical_link_health import (  # noqa: E402
    DEFAULT_SITE_BASE_URL,
    build_blog_canonical_link_health_report,
    format_blog_canonical_link_health_json,
    format_blog_canonical_link_health_table,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site-base-url", default=DEFAULT_SITE_BASE_URL)
    parser.add_argument("--format", choices=("json", "table"), default="json")
    parser.add_argument("--table", action="store_true", help="Print table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_blog_canonical_link_health_report(db, site_base_url=args.site_base_url)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_table = args.table or args.format == "table"
    print(format_blog_canonical_link_health_table(report) if as_table else format_blog_canonical_link_health_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
