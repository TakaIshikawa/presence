#!/usr/bin/env python3
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent/"src"))
from evaluation.publication_channel_cooldown_violations import DEFAULT_COOLDOWN_MINUTES, DEFAULT_LIMIT, DEFAULT_WINDOW_DAYS, build_publication_channel_cooldown_violations_report_from_db, format_publication_channel_cooldown_violations_json, format_publication_channel_cooldown_violations_text
from runner import script_context
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
def parse_args(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--db"); p.add_argument("--cooldown-minutes",type=_pos,default=DEFAULT_COOLDOWN_MINUTES); p.add_argument("--channel"); p.add_argument("--window-days",type=_pos,default=DEFAULT_WINDOW_DAYS); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
 try:a=parse_args(argv)
 except SystemExit as e:return int(e.code or 0)
 kw={"cooldown_minutes":a.cooldown_minutes,"channel":a.channel,"window_days":a.window_days,"limit":a.limit}
 try:
  if a.db:
   with sqlite3.connect(a.db) as c:r=build_publication_channel_cooldown_violations_report_from_db(c,**kw)
  else:
   with script_context() as (_cfg,db):r=build_publication_channel_cooldown_violations_report_from_db(db,**kw)
 except Exception as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_publication_channel_cooldown_violations_text(r) if a.format=="text" else format_publication_channel_cooldown_violations_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
