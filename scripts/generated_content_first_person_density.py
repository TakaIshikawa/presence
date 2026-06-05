#!/usr/bin/env python3
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.generated_content_first_person_density import DEFAULT_LIMIT,DEFAULT_LOOKBACK_DAYS,DEFAULT_MIN_DENSITY,DEFAULT_MIN_WORDS,build_generated_content_first_person_density_report_from_db,format_generated_content_first_person_density_json,format_generated_content_first_person_density_text
from runner import script_context
def parse_args(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--db"); p.add_argument("--lookback-days",type=_pos,default=DEFAULT_LOOKBACK_DAYS); p.add_argument("--content-type"); p.add_argument("--min-density",type=float,default=DEFAULT_MIN_DENSITY); p.add_argument("--min-words",type=_pos,default=DEFAULT_MIN_WORDS); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
 try:a=parse_args(argv)
 except SystemExit as e:return int(e.code or 0)
 kw={"lookback_days":a.lookback_days,"content_type":a.content_type,"min_density":a.min_density,"min_words":a.min_words,"limit":a.limit}
 try:
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_generated_content_first_person_density_report_from_db(c,**kw)
  else:
   with script_context() as (_x,db): r=build_generated_content_first_person_density_report_from_db(db,**kw)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_generated_content_first_person_density_text(r) if a.format=="text" else format_generated_content_first_person_density_json(r)); return 0
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
if __name__=="__main__": raise SystemExit(main())
