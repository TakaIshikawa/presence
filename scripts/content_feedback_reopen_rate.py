#!/usr/bin/env python3
"""Content Feedback Reopen Rate."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/'src'))
from evaluation.content_feedback_reopen_rate import build_content_feedback_reopen_rate_report_from_db,format_content_feedback_reopen_rate_json,format_content_feedback_reopen_rate_text # noqa:E402
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
 p=argparse.ArgumentParser(description=__doc__); p.add_argument('--db'); p.add_argument('--format',choices=('json','text'),default='json'); p.add_argument('--window-days',type=_pos,default=30); p.add_argument('--min-resolved',type=_pos,default=1); p.add_argument('--limit',type=_pos,default=50)
 try:
  a=p.parse_args(argv); kw={'window_days':a.window_days,'min_resolved':a.min_resolved,'limit':a.limit}
  if a.db:
   with sqlite3.connect(a.db) as conn: r=build_content_feedback_reopen_rate_report_from_db(conn,**kw)
  else:
   with script_context() as (_c,db): r=build_content_feedback_reopen_rate_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f'error: {e}',file=sys.stderr); return 1
 print(format_content_feedback_reopen_rate_text(r) if a.format=='text' else format_content_feedback_reopen_rate_json(r)); return 0
if __name__=='__main__': raise SystemExit(main())
