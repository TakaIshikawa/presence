#!/usr/bin/env python3
"""Report publish queue schedule window utilization."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.publish_queue_schedule_window_utilization import DEFAULT_LIMIT,DEFAULT_OVERFILLED,DEFAULT_UNDERUSED,build_publish_queue_schedule_window_utilization_report_from_db,format_publish_queue_schedule_window_utilization_json,format_publish_queue_schedule_window_utilization_text  # noqa:E402
from runner import script_context  # noqa:E402
def pos(v:str)->int:
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
def nonneg(v:str)->float:
 n=float(v)
 if n<0: raise argparse.ArgumentTypeError("value must be non-negative")
 return n
def main(argv:list[str]|None=None)->int:
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--timezone",default="UTC"); p.add_argument("--underused-threshold",type=nonneg,default=DEFAULT_UNDERUSED); p.add_argument("--overfilled-threshold",type=nonneg,default=DEFAULT_OVERFILLED); p.add_argument("--limit",type=pos,default=DEFAULT_LIMIT)
 try:
  a=p.parse_args(argv); kw={"timezone":a.timezone,"underused_threshold":a.underused_threshold,"overfilled_threshold":a.overfilled_threshold,"limit":a.limit}
  if a.db:
   with sqlite3.connect(a.db) as c: r=build_publish_queue_schedule_window_utilization_report_from_db(c,**kw)
  else:
   with script_context() as (_c,db): r=build_publish_queue_schedule_window_utilization_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_publish_queue_schedule_window_utilization_text(r) if a.format=="text" else format_publish_queue_schedule_window_utilization_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
