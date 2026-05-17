#!/usr/bin/env python3
"""Rank broken newsletter links by reader impact."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_dead_link_impact import (  # noqa: E402
    build_newsletter_dead_link_impact_report_from_db,
    format_newsletter_dead_link_impact_json,
    format_newsletter_dead_link_impact_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_newsletter_dead_link_impact_report_from_db(db)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_newsletter_dead_link_impact_text(report) if args.format == "text" else format_newsletter_dead_link_impact_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
