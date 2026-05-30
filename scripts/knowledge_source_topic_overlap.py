#!/usr/bin/env python3
"""Export Knowledge Source Topic Overlap."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.knowledge_source_topic_overlap import *  # noqa:E402,F403
from runner import script_context  # noqa:E402
def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--days",type=int,default=DEFAULT_DAYS); p.add_argument("--limit",type=int,default=DEFAULT_LIMIT); p.add_argument("--topic"); p.add_argument("--min-sources",type=int,default=DEFAULT_MIN_SOURCES); p.add_argument("--format",choices=("json","text"),default="json")
 try:
  a=p.parse_args(argv); kw={"days":a.days,"limit":a.limit,"topic":a.topic,"min_sources":a.min_sources}
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_knowledge_source_topic_overlap_report_from_db(c,**kw)
  else:
   with script_context() as (_cfg,db): r=build_knowledge_source_topic_overlap_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_knowledge_source_topic_overlap_text(r) if a.format=="text" else format_knowledge_source_topic_overlap_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
