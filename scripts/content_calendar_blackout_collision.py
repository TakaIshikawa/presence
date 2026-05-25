#!/usr/bin/env python3
"""Export Content Calendar Blackout Collision."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.content_calendar_blackout_collision import DEFAULT_LIMIT, DEFAULT_LOOKAHEAD_DAYS, build_content_calendar_blackout_collision_report_from_db, format_content_calendar_blackout_collision_json, format_content_calendar_blackout_collision_text  # noqa: E402
from runner import script_context  # noqa: E402
def _pos(v):
    try:n=int(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from exc
    if n<=0: raise argparse.ArgumentTypeError("value must be positive")
    return n
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--lookahead-days",type=_pos,default=DEFAULT_LOOKAHEAD_DAYS); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); return p.parse_args(argv)
def main(argv=None):
    try:a=parse_args(argv)
    except SystemExit as exc: return int(exc.code or 0)
    try:
        kw={"lookahead_days":a.lookahead_days,"limit":a.limit}
        if a.db:
            with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_content_calendar_blackout_collision_report_from_db(c,**kw)
        else:
            with script_context() as (_cfg,db): r=build_content_calendar_blackout_collision_report_from_db(db,**kw)
    except (OSError,sqlite3.Error,TypeError,ValueError) as exc: print(f"error: {exc}",file=sys.stderr); return 1
    print(format_content_calendar_blackout_collision_text(r) if a.format=="text" else format_content_calendar_blackout_collision_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
