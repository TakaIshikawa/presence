#!/usr/bin/env python3
"""Publication Attempt Retry Reason Drift."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/'src'))
from evaluation.publication_attempt_retry_reason_drift import build_publication_attempt_retry_reason_drift_report_from_db,format_publication_attempt_retry_reason_drift_json,format_publication_attempt_retry_reason_drift_text # noqa:E402
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
 p=argparse.ArgumentParser(description=__doc__); p.add_argument('--db'); p.add_argument('--format',choices=('json','text'),default='json'); p.add_argument('--baseline-days',type=_pos,default=14); p.add_argument('--current-days',type=_pos,default=7); p.add_argument('--min-delta',type=_float,default=.25); p.add_argument('--limit',type=_pos,default=50)
 try:
  a=p.parse_args(argv); kw={'baseline_days':a.baseline_days,'current_days':a.current_days,'min_delta':a.min_delta,'limit':a.limit}
  r=None
  if a.db:
   with sqlite3.connect(a.db) as conn: r=build_publication_attempt_retry_reason_drift_report_from_db(conn,**kw)
  else:
   with script_context() as (_c,db): r=build_publication_attempt_retry_reason_drift_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f'error: {e}',file=sys.stderr); return 1
 print(format_publication_attempt_retry_reason_drift_text(r) if a.format=='text' else format_publication_attempt_retry_reason_drift_json(r)); return 0
if __name__=='__main__': raise SystemExit(main())
