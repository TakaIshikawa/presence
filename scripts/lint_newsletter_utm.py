#!/usr/bin/env python3
"""Lint newsletter links for required UTM parameters before delivery."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.newsletter_utm_linter import (  # noqa: E402
    build_newsletter_utm_lint_report_for_issue,
    format_newsletter_utm_lint_json,
    format_newsletter_utm_lint_text,
    lint_newsletter_utm_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--file",
        help="Rendered HTML or Markdown newsletter file to lint.",
    )
    source.add_argument(
        "--issue-id",
        help="Look up the latest newsletter_sends row for this issue id.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic JSON instead of compact text.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.file:
            text = Path(args.file).read_text()
            report = lint_newsletter_utm_text(text, source=args.file)
        else:
            with script_context() as (_config, db):
                report = build_newsletter_utm_lint_report_for_issue(db, args.issue_id)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(format_newsletter_utm_lint_json(report))
    else:
        print(format_newsletter_utm_lint_text(report))

    if report.blocking_issue_count:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
