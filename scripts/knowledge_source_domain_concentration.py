#!/usr/bin/env python3
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.knowledge_source_domain_concentration import DEFAULT_LIMIT, DEFAULT_LOOKBACK_DAYS, DEFAULT_MAX_DOMAIN_SHARE, DEFAULT_MIN_SOURCES, build_knowledge_source_domain_concentration_report_from_db, format_knowledge_source_domain_concentration_json, format_knowledge_source_domain_concentration_text  # noqa:E402
from runner import script_context  # noqa:E402
def parse_args(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--db"); p.add_argument("--lookback-days",type=_pos,default=DEFAULT_LOOKBACK_DAYS); p.add_argument("--max-domain-share",type=float,default=DEFAULT_MAX_DOMAIN_SHARE); p.add_argument("--min-sources",type=_pos,default=DEFAULT_MIN_SOURCES); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
 try:a=parse_args(argv)
 except SystemExit as exc:return int(exc.code or 0)
 kw={"lookback_days":a.lookback_days,"max_domain_share":a.max_domain_share,"min_sources":a.min_sources,"limit":a.limit}
 try:
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_knowledge_source_domain_concentration_report_from_db(c,**kw)
  else:
   with script_context() as (_x,db): r=build_knowledge_source_domain_concentration_report_from_db(db,**kw)
 except (OSError,sqlite3.Error,TypeError,ValueError) as exc: print(f"error: {exc}",file=sys.stderr); return 1
 print(format_knowledge_source_domain_concentration_text(r) if a.format=="text" else format_knowledge_source_domain_concentration_json(r)); return 0
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
if __name__=="__main__": raise SystemExit(main())
