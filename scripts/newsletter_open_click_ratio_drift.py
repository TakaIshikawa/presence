#!/usr/bin/env python3
"""Report newsletter open/click ratio drift."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.newsletter_open_click_ratio_drift import DEFAULT_LIMIT, build_newsletter_open_click_ratio_drift_report_from_db, format_newsletter_open_click_ratio_drift_json, format_newsletter_open_click_ratio_drift_text
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
    p.add_argument("--min-ratio", type=_nonneg_float, default=0.1)
    p.add_argument("--drop-threshold", type=_nonneg_float, default=0.25)

    return p.parse_args(argv)
def main(argv=None):
    try:
        a=parse_args(argv)
        kwargs={"min_ratio":a.min_ratio,"drop_threshold":a.drop_threshold,"limit":a.limit}
        if a.db:
            with sqlite3.connect(a.db) as conn:
                conn.row_factory=sqlite3.Row
                report=build_newsletter_open_click_ratio_drift_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report=build_newsletter_open_click_ratio_drift_report_from_db(db, **kwargs)
    except SystemExit as exc:
        return int(exc.code or 0)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr); return 1
    print(format_newsletter_open_click_ratio_drift_text(report) if a.format=="text" else format_newsletter_open_click_ratio_drift_json(report))
    return 0
if __name__=="__main__": raise SystemExit(main())
