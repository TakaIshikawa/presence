#!/usr/bin/env python3
"""Flag content that relies too heavily on quoted source text."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.source_quote_density import (  # noqa: E402
    DEFAULT_MAX_QUOTE_DENSITY,
    build_source_quote_density_report_from_db,
    format_source_quote_density_json,
    format_source_quote_density_text,
)
from runner import script_context  # noqa: E402


def _density(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid density: {value}") from exc
    if parsed < 0 or parsed > 1:
        raise argparse.ArgumentTypeError("density must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-quote-density", type=_density, default=DEFAULT_MAX_QUOTE_DENSITY)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_source_quote_density_report_from_db(db, max_quote_density=args.max_quote_density)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_source_quote_density_text(report) if args.format == "text" else format_source_quote_density_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
