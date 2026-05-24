#!/usr/bin/env python3
"""Export publication failure replay seeds."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publication_failure_replay_seed_export import DEFAULT_LIMIT, DEFAULT_LOOKBACK_DAYS, build_publication_failure_replay_seed_export_from_db, format_publication_failure_replay_seed_export_json, format_publication_failure_replay_seed_export_jsonl  # noqa: E402
from runner import script_context  # noqa: E402


def _positive_int(v: str) -> int:
    parsed = int(v)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "jsonl"), default="json")
    p.add_argument("--platform")
    p.add_argument("--retryable-only", action="store_true")
    p.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    p.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return p.parse_args(argv)


def main(argv=None) -> int:
    try:
        a = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn:
                conn.row_factory = sqlite3.Row
                export = build_publication_failure_replay_seed_export_from_db(conn, platform=a.platform, retryable_only=a.retryable_only, lookback_days=a.lookback_days, limit=a.limit)
        else:
            with script_context() as (_config, db):
                export = build_publication_failure_replay_seed_export_from_db(db, platform=a.platform, retryable_only=a.retryable_only, lookback_days=a.lookback_days, limit=a.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_publication_failure_replay_seed_export_jsonl(export) if a.format == "jsonl" else format_publication_failure_replay_seed_export_json(export), end="" if a.format == "jsonl" else "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
