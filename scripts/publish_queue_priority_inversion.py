#!/usr/bin/env python3
"""Publish Queue Priority Inversion."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.publish_queue_priority_inversion import DEFAULT_LIMIT, build_publish_queue_priority_inversion_report_from_db, format_publish_queue_priority_inversion_json, format_publish_queue_priority_inversion_text
from runner import script_context

def _pos_int(v):
    p=int(v)
    if p<=0: raise argparse.ArgumentTypeError("value must be positive")
    return p
def _nonneg_int(v):
    p=int(v)
    if p<0: raise argparse.ArgumentTypeError("value must be non-negative")
    return p
def _nonneg_float(v):
    p=float(v)
    if p<0: raise argparse.ArgumentTypeError("value must be non-negative")
    return p
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json","text"), default="json")
    p.add_argument("--limit", type=_pos_int, default=DEFAULT_LIMIT)
    p.add_argument("--window-hours", type=_pos_int, default=168)
    p.add_argument("--min-priority-gap", type=_nonneg_int, default=1)
    p.add_argument("--platform")

    return p.parse_args(argv)
def main(argv=None):
    try:
        a=parse_args(argv); kwargs={"window_hours":a.window_hours,"min_priority_gap":a.min_priority_gap,"platform":a.platform,"limit":a.limit}
        if a.db:
            with sqlite3.connect(a.db) as conn:
                conn.row_factory=sqlite3.Row; report=build_publish_queue_priority_inversion_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db): report=build_publish_queue_priority_inversion_report_from_db(db, **kwargs)
    except SystemExit as exc:
        return int(exc.code or 0)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr); return 1
    print(format_publish_queue_priority_inversion_text(report) if a.format=="text" else format_publish_queue_priority_inversion_json(report)); return 0
if __name__=="__main__": raise SystemExit(main())
