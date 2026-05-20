#!/usr/bin/env python3
"""Compare prompt_versions usage counts with observed prediction references."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.prompt_version_usage_drift import (  # noqa: E402
    DEFAULT_MIN_DELTA,
    build_prompt_version_usage_drift_report_from_db,
    format_prompt_version_usage_drift_json,
    format_prompt_version_usage_drift_text,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--prompt-type", help="Only include one prompt type.")
    parser.add_argument("--min-delta", type=_non_negative_int, default=DEFAULT_MIN_DELTA)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"prompt_type": args.prompt_type, "min_delta": args.min_delta}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_prompt_version_usage_drift_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_prompt_version_usage_drift_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_prompt_version_usage_drift_text(report) if args.format == "text" else format_prompt_version_usage_drift_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
