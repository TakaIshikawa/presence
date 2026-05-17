#!/usr/bin/env python3
"""Report stale knowledge linked to reply drafts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_draft_knowledge_freshness import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_STALE_DAYS,
    build_reply_draft_knowledge_freshness_report,
    format_reply_draft_knowledge_freshness_json,
    format_reply_draft_knowledge_freshness_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--stale-days", type=_positive_int, default=DEFAULT_STALE_DAYS)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_reply_draft_knowledge_freshness_report(db, days=args.days, stale_days=args.stale_days)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_text = args.table or args.format == "text"
    print(format_reply_draft_knowledge_freshness_text(report) if as_text else format_reply_draft_knowledge_freshness_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
