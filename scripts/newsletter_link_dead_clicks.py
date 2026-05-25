#!/usr/bin/env python3
"""Export Newsletter Link Dead Clicks."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.newsletter_link_dead_clicks import build_newsletter_link_dead_clicks_report_from_db, format_newsletter_link_dead_clicks_json, format_newsletter_link_dead_clicks_text  # noqa:E402
from runner import script_context  # noqa:E402
def _pos(v):
    try: p=int(v)
    except ValueError as e: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from e
    if p<=0: raise argparse.ArgumentTypeError("value must be positive")
    return p
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--since"); p.add_argument("--min-clicks",type=_pos,default=1); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    try:
        kw={"since":a.since,"min_clicks":a.min_clicks}
        if a.db:
            with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_newsletter_link_dead_clicks_report_from_db(c,**kw)
        else:
            with script_context() as (_config,db): r=build_newsletter_link_dead_clicks_report_from_db(db,**kw)
    except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
    print(format_newsletter_link_dead_clicks_text(r) if a.format=="text" else format_newsletter_link_dead_clicks_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
