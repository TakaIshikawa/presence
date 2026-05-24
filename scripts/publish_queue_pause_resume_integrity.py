#!/usr/bin/env python3
"""Run the publish queue pause resume integrity report."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.publish_queue_pause_resume_integrity import build_publish_queue_pause_resume_integrity_report_from_db, format_publish_queue_pause_resume_integrity_json, format_publish_queue_pause_resume_integrity_text  # noqa: E402
from runner import script_context  # noqa: E402

def _positive_int(v):
    try: p=int(v)
    except ValueError as e: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from e
    if p<=0: raise argparse.ArgumentTypeError("value must be positive")
    return p

def _nonnegative_float(v):
    try: p=float(v)
    except ValueError as e: raise argparse.ArgumentTypeError(f"invalid number: {v}") from e
    if p<0: raise argparse.ArgumentTypeError("value must be non-negative")
    return p

def _ratio(v):
    p=_nonnegative_float(v)
    if p>1: raise argparse.ArgumentTypeError("value must be at most 1")
    return p

def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json","text"), default="json")
    p.add_argument("--max-pause-hours", type=float, default=24)
    p.add_argument("--actor")
    p.add_argument("--limit", type=_positive_int, default=100)
    return p.parse_args(argv)

def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    kw={'max_pause_hours':a.max_pause_hours,'actor':a.actor,'limit':a.limit}
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn:
                conn.row_factory=sqlite3.Row; report=build_publish_queue_pause_resume_integrity_report_from_db(conn, **kw)
        else:
            with script_context() as (_config, db): report=build_publish_queue_pause_resume_integrity_report_from_db(db, **kw)
    except (OSError, sqlite3.Error, TypeError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr); return 1
    print(format_publish_queue_pause_resume_integrity_text(report) if a.format=="text" else format_publish_queue_pause_resume_integrity_json(report)); return 0
if __name__=="__main__": raise SystemExit(main())
