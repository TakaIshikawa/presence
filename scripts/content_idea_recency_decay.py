#!/usr/bin/env python3
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.content_idea_recency_decay import DEFAULT_LIMIT, DEFAULT_STALE_AFTER_DAYS, DEFAULT_WARNING_AFTER_DAYS, build_content_idea_recency_decay_report_from_db, format_content_idea_recency_decay_json, format_content_idea_recency_decay_text  # noqa:E402
from runner import script_context  # noqa:E402
def parse_args(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--db"); p.add_argument("--stale-after-days",type=_pos,default=DEFAULT_STALE_AFTER_DAYS); p.add_argument("--warning-after-days",type=_pos,default=DEFAULT_WARNING_AFTER_DAYS); p.add_argument("--status",action="append",dest="statuses"); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
 try:a=parse_args(argv)
 except SystemExit as exc:return int(exc.code or 0)
 kw={"stale_after_days":a.stale_after_days,"warning_after_days":a.warning_after_days,"statuses":a.statuses or ("new","open","planned","draft"),"limit":a.limit}
 try:
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_content_idea_recency_decay_report_from_db(c,**kw)
  else:
   with script_context() as (_x,db): r=build_content_idea_recency_decay_report_from_db(db,**kw)
 except (OSError,sqlite3.Error,TypeError,ValueError) as exc: print(f"error: {exc}",file=sys.stderr); return 1
 print(format_content_idea_recency_decay_text(r) if a.format=="text" else format_content_idea_recency_decay_json(r)); return 0
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
if __name__=="__main__": raise SystemExit(main())
