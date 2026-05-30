#!/usr/bin/env python3
"""Report publication attempts that succeeded after retry."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.publication_attempt_success_after_retry import DEFAULT_LIMIT, build_publication_attempt_success_after_retry_report_from_db, format_publication_attempt_success_after_retry_json, format_publication_attempt_success_after_retry_text  # noqa: E402
from runner import script_context  # noqa: E402

def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--limit",type=_positive_int,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None)->int:
    try: args=parse_args(argv)
    except SystemExit as exc: return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn: conn.row_factory=sqlite3.Row; report=build_publication_attempt_success_after_retry_report_from_db(conn,limit=args.limit)
        else:
            with script_context() as (_config,db): report=build_publication_attempt_success_after_retry_report_from_db(db,limit=args.limit)
    except (OSError,sqlite3.Error,TypeError,ValueError) as exc:
        print(f"error: {exc}",file=sys.stderr); return 1
    print(format_publication_attempt_success_after_retry_text(report) if args.format=="text" else format_publication_attempt_success_after_retry_json(report)); return 0
def _positive_int(v):
    try: parsed=int(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from exc
    if parsed<=0: raise argparse.ArgumentTypeError("value must be positive")
    return parsed
if __name__=="__main__": raise SystemExit(main())
