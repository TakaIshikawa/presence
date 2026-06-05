#!/usr/bin/env python3
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.blog_draft_heading_structure_gaps import DEFAULT_LIMIT,DEFAULT_MAX_HEADING_LENGTH,build_blog_draft_heading_structure_gaps_report_from_db,format_blog_draft_heading_structure_gaps_json,format_blog_draft_heading_structure_gaps_text
from runner import script_context
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
def parse_args(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--db"); p.add_argument("--max-heading-length",type=_pos,default=DEFAULT_MAX_HEADING_LENGTH); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
 try:a=parse_args(argv)
 except SystemExit as e:return int(e.code or 0)
 try:
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_blog_draft_heading_structure_gaps_report_from_db(c,max_heading_length=a.max_heading_length,limit=a.limit)
  else:
   with script_context() as (_x,db): r=build_blog_draft_heading_structure_gaps_report_from_db(db,max_heading_length=a.max_heading_length,limit=a.limit)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_blog_draft_heading_structure_gaps_text(r) if a.format=="text" else format_blog_draft_heading_structure_gaps_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
