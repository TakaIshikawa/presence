#!/usr/bin/env python3
"""Publish Queue Schedule Window Utilization."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/'src'))
from evaluation.publish_queue_schedule_window_utilization import build_publish_queue_schedule_window_utilization_report_from_db,format_publish_queue_schedule_window_utilization_json,format_publish_queue_schedule_window_utilization_text # noqa:E402
from runner import script_context # noqa:E402
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError('value must be positive')
 return n
def _nonneg(v):
 n=int(v)
 if n<0: raise argparse.ArgumentTypeError('value must be non-negative')
 return n
def _float(v): return float(v)
def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__); p.add_argument('--db'); p.add_argument('--format',choices=('json','text'),default='json'); p.add_argument('--timezone',dest='timezone_name',default='UTC'); p.add_argument('--underused-threshold',type=_float,default=.5); p.add_argument('--overfilled-threshold',type=_float,default=1.0); p.add_argument('--limit',type=_pos,default=50)
 try:
  a=p.parse_args(argv); kw={'timezone_name':a.timezone_name,'underused_threshold':a.underused_threshold,'overfilled_threshold':a.overfilled_threshold,'limit':a.limit}
  r=None
  if a.db:
   with sqlite3.connect(a.db) as conn: r=build_publish_queue_schedule_window_utilization_report_from_db(conn,**kw)
  else:
   with script_context() as (_c,db): r=build_publish_queue_schedule_window_utilization_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f'error: {e}',file=sys.stderr); return 1
 print(format_publish_queue_schedule_window_utilization_text(r) if a.format=='text' else format_publish_queue_schedule_window_utilization_json(r)); return 0
if __name__=='__main__': raise SystemExit(main())
