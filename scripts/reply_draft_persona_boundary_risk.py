#!/usr/bin/env python3
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from engagement.reply_draft_persona_boundary_risk import DEFAULT_LIMIT, DEFAULT_STATUS, DEFAULT_WINDOW_DAYS, build_reply_draft_persona_boundary_risk_report_from_db, format_reply_draft_persona_boundary_risk_json, format_reply_draft_persona_boundary_risk_text
from runner import script_context
def _pos(v):
    n=int(v)
    if n<=0: raise argparse.ArgumentTypeError("value must be positive")
    return n
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--status", default=DEFAULT_STATUS); p.add_argument("--window-days", type=_pos, default=DEFAULT_WINDOW_DAYS); p.add_argument("--limit", type=_pos, default=DEFAULT_LIMIT); p.add_argument("--format", choices=("json","text"), default="json"); return p.parse_args(argv)
def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    kw={"status": a.status or None, "window_days": a.window_days, "limit": a.limit}
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn: r=build_reply_draft_persona_boundary_risk_report_from_db(conn, **kw)
        else:
            with script_context() as (_c, db): r=build_reply_draft_persona_boundary_risk_report_from_db(db, **kw)
    except Exception as e: print(f"error: {e}", file=sys.stderr); return 1
    print(format_reply_draft_persona_boundary_risk_text(r) if a.format=="text" else format_reply_draft_persona_boundary_risk_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
