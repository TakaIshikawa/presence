#!/usr/bin/env python3
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent/"src"))
from evaluation.knowledge_source_author_diversity import DEFAULT_LIMIT, DEFAULT_MAX_AUTHOR_SHARE, DEFAULT_WINDOW_DAYS, build_knowledge_source_author_diversity_report_from_db, format_knowledge_source_author_diversity_json, format_knowledge_source_author_diversity_text
from runner import script_context
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
def _share(v):
 f=float(v)
 if f<0 or f>1: raise argparse.ArgumentTypeError("value must be between 0 and 1")
 return f
def parse_args(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--db"); p.add_argument("--window-days",type=_pos,default=DEFAULT_WINDOW_DAYS); p.add_argument("--max-author-share",type=_share,default=DEFAULT_MAX_AUTHOR_SHARE); p.add_argument("--source-kind"); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
 try:a=parse_args(argv)
 except SystemExit as e:return int(e.code or 0)
 kw={"window_days":a.window_days,"max_author_share":a.max_author_share,"source_kind":a.source_kind,"limit":a.limit}
 try:
  if a.db:
   with sqlite3.connect(a.db) as c:r=build_knowledge_source_author_diversity_report_from_db(c,**kw)
  else:
   with script_context() as (_cfg,db):r=build_knowledge_source_author_diversity_report_from_db(db,**kw)
 except Exception as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_knowledge_source_author_diversity_text(r) if a.format=="text" else format_knowledge_source_author_diversity_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
