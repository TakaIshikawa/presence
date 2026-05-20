#!/usr/bin/env python3
"""Report prompt version token and cost efficiency."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.prompt_version_cost_efficiency import (  # noqa: E402
    DEFAULT_EXPENSIVE_COST,
    DEFAULT_LIMIT,
    build_prompt_version_cost_efficiency_report_from_db,
    format_prompt_version_cost_efficiency_json,
    format_prompt_version_cost_efficiency_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--expensive-cost", type=float, default=DEFAULT_EXPENSIVE_COST)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {"expensive_cost": args.expensive_cost, "limit": args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_prompt_version_cost_efficiency_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_prompt_version_cost_efficiency_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_prompt_version_cost_efficiency_text(report)
        if args.format == "text"
        else format_prompt_version_cost_efficiency_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
