#!/usr/bin/env python3
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from engagement.proactive_action_priority_inversion import DEFAULT_LIMIT, DEFAULT_LOOKBACK_DAYS, DEFAULT_OPEN_STATUS, build_proactive_action_priority_inversion_report_from_db, format_proactive_action_priority_inversion_json, format_proactive_action_priority_inversion_text
from runner import script_context
def _pos(v): 
    n=int(v)
    if n<=0: raise argparse.ArgumentTypeError("value must be positive")
    return n
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--open-status", default=DEFAULT_OPEN_STATUS); p.add_argument("--lookback-days", type=_pos, default=DEFAULT_LOOKBACK_DAYS); p.add_argument("--limit", type=_pos, default=DEFAULT_LIMIT); p.add_argument("--format", choices=("json","text"), default="json"); return p.parse_args(argv)
def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    kw={"open_status": a.open_status or None, "lookback_days": a.lookback_days, "limit": a.limit}
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn: r=build_proactive_action_priority_inversion_report_from_db(conn, **kw)
        else:
            with script_context() as (_c, db): r=build_proactive_action_priority_inversion_report_from_db(db, **kw)
    except Exception as e: print(f"error: {e}", file=sys.stderr); return 1
    print(format_proactive_action_priority_inversion_text(r) if a.format=="text" else format_proactive_action_priority_inversion_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
