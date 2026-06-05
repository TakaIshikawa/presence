#!/usr/bin/env python3
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.publication_attempt_provider_error_taxonomy import DEFAULT_LIMIT,DEFAULT_LOOKBACK_DAYS,DEFAULT_MIN_COUNT,build_publication_attempt_provider_error_taxonomy_report_from_db,format_publication_attempt_provider_error_taxonomy_json,format_publication_attempt_provider_error_taxonomy_text
from runner import script_context
def parse_args(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--db"); p.add_argument("--lookback-days",type=_pos,default=DEFAULT_LOOKBACK_DAYS); p.add_argument("--min-count",type=_pos,default=DEFAULT_MIN_COUNT); p.add_argument("--provider"); p.add_argument("--platform"); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
 try:a=parse_args(argv)
 except SystemExit as e:return int(e.code or 0)
 try:
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_publication_attempt_provider_error_taxonomy_report_from_db(c,lookback_days=a.lookback_days,min_count=a.min_count,provider=a.provider,platform=a.platform,limit=a.limit)
  else:
   with script_context() as (_x,db): r=build_publication_attempt_provider_error_taxonomy_report_from_db(db,lookback_days=a.lookback_days,min_count=a.min_count,provider=a.provider,platform=a.platform,limit=a.limit)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_publication_attempt_provider_error_taxonomy_text(r) if a.format=="text" else format_publication_attempt_provider_error_taxonomy_json(r)); return 0
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
if __name__=="__main__": raise SystemExit(main())
