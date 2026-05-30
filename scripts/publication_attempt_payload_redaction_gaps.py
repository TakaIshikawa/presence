#!/usr/bin/env python3
"""Report publication attempt payload redaction gaps."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_attempt_payload_redaction_gaps import (  # noqa: E402
    DEFAULT_LIMIT,
    build_publication_attempt_payload_redaction_gaps_report_from_db,
    format_publication_attempt_payload_redaction_gaps_json,
    format_publication_attempt_payload_redaction_gaps_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--now", type=_datetime)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publication_attempt_payload_redaction_gaps_report_from_db(conn, limit=args.limit, now=args.now)
        else:
            with script_context() as (_config, db):
                report = build_publication_attempt_payload_redaction_gaps_report_from_db(db, limit=args.limit, now=args.now)
    except (OSError, sqlite3.Error, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_publication_attempt_payload_redaction_gaps_text(report) if args.format == "text" else format_publication_attempt_payload_redaction_gaps_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
