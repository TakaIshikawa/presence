#!/usr/bin/env python3
"""Report knowledge source author overexposure."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.knowledge_source_author_overexposure import DEFAULT_LIMIT, DEFAULT_MAX_AUTHOR_SHARE, DEFAULT_WINDOW_DAYS, build_knowledge_source_author_overexposure_report_from_db, format_knowledge_source_author_overexposure_json, format_knowledge_source_author_overexposure_text  # noqa:E402
from runner import script_context  # noqa:E402
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--window-days",type=_positive_int,default=DEFAULT_WINDOW_DAYS); p.add_argument("--max-author-share",type=_share,default=DEFAULT_MAX_AUTHOR_SHARE); p.add_argument("--limit",type=_positive_int,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as exc: return int(exc.code or 0)
    try:
        kw={"window_days":a.window_days,"max_author_share":a.max_author_share,"limit":a.limit}
        if a.db:
            with sqlite3.connect(a.db) as conn: conn.row_factory=sqlite3.Row; r=build_knowledge_source_author_overexposure_report_from_db(conn,**kw)
        else:
            with script_context() as (_c,db): r=build_knowledge_source_author_overexposure_report_from_db(db,**kw)
    except (OSError,sqlite3.Error,TypeError,ValueError) as exc: print(f"error: {exc}",file=sys.stderr); return 1
    print(format_knowledge_source_author_overexposure_text(r) if a.format=="text" else format_knowledge_source_author_overexposure_json(r)); return 0
def _positive_int(v):
    try: n=int(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from exc
    if n<=0: raise argparse.ArgumentTypeError("value must be positive")
    return n
def _share(v):
    try: n=float(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid number: {v}") from exc
    if n<0 or n>1: raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return n
if __name__=="__main__": raise SystemExit(main())
