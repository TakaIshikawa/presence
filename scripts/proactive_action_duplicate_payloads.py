#!/usr/bin/env python3
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from engagement.proactive_action_duplicate_payloads import DEFAULT_LIMIT, DEFAULT_MIN_LENGTH, DEFAULT_STATUS, build_proactive_action_duplicate_payloads_report_from_db, format_proactive_action_duplicate_payloads_json, format_proactive_action_duplicate_payloads_text
from runner import script_context
def _pos(v):
    n=int(v)
    if n<=0: raise argparse.ArgumentTypeError("value must be positive")
    return n
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--status", default=DEFAULT_STATUS); p.add_argument("--min-length", type=_pos, default=DEFAULT_MIN_LENGTH); p.add_argument("--limit", type=_pos, default=DEFAULT_LIMIT); p.add_argument("--format", choices=("json","text"), default="json"); return p.parse_args(argv)
def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    kw={"status": a.status or None, "min_length": a.min_length, "limit": a.limit}
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn: r=build_proactive_action_duplicate_payloads_report_from_db(conn, **kw)
        else:
            with script_context() as (_c, db): r=build_proactive_action_duplicate_payloads_report_from_db(db, **kw)
    except Exception as e: print(f"error: {e}", file=sys.stderr); return 1
    print(format_proactive_action_duplicate_payloads_text(r) if a.format=="text" else format_proactive_action_duplicate_payloads_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
