#!/usr/bin/env python3
"""Export Reply Draft Context Leakage."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.reply_draft_context_leakage import DEFAULT_LIMIT, build_reply_draft_context_leakage_report_from_db, format_reply_draft_context_leakage_json, format_reply_draft_context_leakage_text  # noqa: E402
from runner import script_context  # noqa: E402
def _positive_int(value: str) -> int:
    try: parsed = int(value)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0: raise argparse.ArgumentTypeError("value must be positive")
    return parsed
def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format", choices=("json", "text"), default="json"); p.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT); return p.parse_args(argv)
def main(argv=None) -> int:
    try: args = parse_args(argv)
    except SystemExit as exc: return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row; report = build_reply_draft_context_leakage_report_from_db(conn, limit=args.limit)
        else:
            with script_context() as (_config, db): report = build_reply_draft_context_leakage_report_from_db(db, limit=args.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr); return 1
    print(format_reply_draft_context_leakage_text(report) if args.format == "text" else format_reply_draft_context_leakage_json(report)); return 0
if __name__ == "__main__": raise SystemExit(main())
