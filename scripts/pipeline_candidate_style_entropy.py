#!/usr/bin/env python3
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.pipeline_candidate_style_entropy import DEFAULT_LIMIT,DEFAULT_MIN_ENTROPY,DEFAULT_MIN_UNIQUE_FORMATS,build_pipeline_candidate_style_entropy_report_from_db,format_pipeline_candidate_style_entropy_json,format_pipeline_candidate_style_entropy_text
from runner import script_context
def parse_args(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--db"); p.add_argument("--min-entropy",type=float,default=DEFAULT_MIN_ENTROPY); p.add_argument("--min-unique-formats",type=_pos,default=DEFAULT_MIN_UNIQUE_FORMATS); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
 try:a=parse_args(argv)
 except SystemExit as e:return int(e.code or 0)
 try:
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_pipeline_candidate_style_entropy_report_from_db(c,min_entropy=a.min_entropy,min_unique_formats=a.min_unique_formats,limit=a.limit)
  else:
   with script_context() as (_x,db): r=build_pipeline_candidate_style_entropy_report_from_db(db,min_entropy=a.min_entropy,min_unique_formats=a.min_unique_formats,limit=a.limit)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_pipeline_candidate_style_entropy_text(r) if a.format=="text" else format_pipeline_candidate_style_entropy_json(r)); return 0
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
if __name__=="__main__": raise SystemExit(main())
