#!/usr/bin/env python3
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent/"src"))
from evaluation.newsletter_image_load_weight import DEFAULT_LIMIT, DEFAULT_MAX_IMAGES, DEFAULT_MAX_TOTAL_KB, build_newsletter_image_load_weight_report_from_db, format_newsletter_image_load_weight_json, format_newsletter_image_load_weight_text
from runner import script_context
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
def parse_args(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--db"); p.add_argument("--max-images",type=_pos,default=DEFAULT_MAX_IMAGES); p.add_argument("--max-total-kb",type=_pos,default=DEFAULT_MAX_TOTAL_KB); p.add_argument("--issue-id"); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
 try:a=parse_args(argv)
 except SystemExit as e:return int(e.code or 0)
 kw={"max_images":a.max_images,"max_total_kb":a.max_total_kb,"issue_id":a.issue_id,"limit":a.limit}
 try:
  if a.db:
   with sqlite3.connect(a.db) as c:r=build_newsletter_image_load_weight_report_from_db(c,**kw)
  else:
   with script_context() as (_cfg,db):r=build_newsletter_image_load_weight_report_from_db(db,**kw)
 except Exception as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_newsletter_image_load_weight_text(r) if a.format=="text" else format_newsletter_image_load_weight_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
