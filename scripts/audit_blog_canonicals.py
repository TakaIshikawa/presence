#!/usr/bin/env python3
"""Audit blog markdown canonical URLs and publication identity."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_canonical_audit import (  # noqa: E402
    DEFAULT_BLOG_PATH,
    build_blog_canonical_audit_report,
    format_blog_canonical_audit_json,
    format_blog_canonical_audit_text,
)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path",
        default=DEFAULT_BLOG_PATH,
        help=f"Blog draft/output directory or markdown file to audit (default: {DEFAULT_BLOG_PATH}).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when blocking canonical audit issues are found.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print deterministic JSON instead of the default text report.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_blog_canonical_audit_report(args.path)

    if args.json:
        print(format_blog_canonical_audit_json(report))
    else:
        print(format_blog_canonical_audit_text(report))

    if args.strict and report.blocking_issue_count:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
