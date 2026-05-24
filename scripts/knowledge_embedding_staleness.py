#!/usr/bin/env python3
"""Report stale knowledge embeddings."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.knowledge_embedding_staleness import DEFAULT_LIMIT,DEFAULT_MAX_AGE_DAYS,build_knowledge_embedding_staleness_report_from_db,format_knowledge_embedding_staleness_json,format_knowledge_embedding_staleness_text  # noqa:E402
from runner import script_context  # noqa:E402
def pos(v:str)->int:
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
def main(argv:list[str]|None=None)->int:
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--max-age-days",type=pos,default=DEFAULT_MAX_AGE_DAYS); p.add_argument("--current-model"); p.add_argument("--limit",type=pos,default=DEFAULT_LIMIT)
 try:
  a=p.parse_args(argv); kw={"max_age_days":a.max_age_days,"current_model":a.current_model,"limit":a.limit}
  if a.db:
   with sqlite3.connect(a.db) as c: r=build_knowledge_embedding_staleness_report_from_db(c,**kw)
  else:
   with script_context() as (_c,db): r=build_knowledge_embedding_staleness_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_knowledge_embedding_staleness_text(r) if a.format=="text" else format_knowledge_embedding_staleness_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
