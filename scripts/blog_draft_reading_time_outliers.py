#!/usr/bin/env python3
"""Export Blog Draft Reading Time Outliers."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.blog_draft_reading_time_outliers import DEFAULT_DAYS,DEFAULT_LIMIT,DEFAULT_MAX_MINUTES,DEFAULT_MIN_MINUTES,build_blog_draft_reading_time_outliers_report_from_db,format_blog_draft_reading_time_outliers_json,format_blog_draft_reading_time_outliers_text  # noqa:E402
from runner import script_context  # noqa:E402
def _pos_float(v):
    try:n=float(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid number: {v}") from exc
    if n<=0: raise argparse.ArgumentTypeError("value must be positive")
    return n
def _pos_int(v):
    try:n=int(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from exc
    if n<=0: raise argparse.ArgumentTypeError("value must be positive")
    return n
def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--days",type=_pos_int,default=DEFAULT_DAYS); p.add_argument("--limit",type=_pos_int,default=DEFAULT_LIMIT); p.add_argument("--min-minutes",type=_pos_float,default=DEFAULT_MIN_MINUTES); p.add_argument("--max-minutes",type=_pos_float,default=DEFAULT_MAX_MINUTES); p.add_argument("--format",choices=("json","text"),default="json")
    try:
        a=p.parse_args(argv); kw={"days":a.days,"limit":a.limit,"min_minutes":a.min_minutes,"max_minutes":a.max_minutes}
        if a.db:
            with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_blog_draft_reading_time_outliers_report_from_db(c,**kw)
        else:
            with script_context() as (_cfg,db): r=build_blog_draft_reading_time_outliers_report_from_db(db,**kw)
    except SystemExit as exc: return int(exc.code or 0)
    except (OSError,sqlite3.Error,TypeError,ValueError) as exc: print(f"error: {exc}",file=sys.stderr); return 1
    print(format_blog_draft_reading_time_outliers_text(r) if a.format=="text" else format_blog_draft_reading_time_outliers_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
