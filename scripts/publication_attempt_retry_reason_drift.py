#!/usr/bin/env python3
"""Report publication attempt retry reason drift."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.publication_attempt_retry_reason_drift import DEFAULT_BASELINE_DAYS,DEFAULT_CURRENT_DAYS,DEFAULT_LIMIT,DEFAULT_MIN_DELTA,build_publication_attempt_retry_reason_drift_report_from_db,format_publication_attempt_retry_reason_drift_json,format_publication_attempt_retry_reason_drift_text  # noqa:E402
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
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--baseline-days",type=pos,default=DEFAULT_BASELINE_DAYS); p.add_argument("--current-days",type=pos,default=DEFAULT_CURRENT_DAYS); p.add_argument("--min-delta",type=nonneg,default=DEFAULT_MIN_DELTA); p.add_argument("--limit",type=pos,default=DEFAULT_LIMIT)
 try:
  a=p.parse_args(argv); kw={"baseline_days":a.baseline_days,"current_days":a.current_days,"min_delta":a.min_delta,"limit":a.limit}
  if a.db:
   with sqlite3.connect(a.db) as c: r=build_publication_attempt_retry_reason_drift_report_from_db(c,**kw)
  else:
   with script_context() as (_c,db): r=build_publication_attempt_retry_reason_drift_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_publication_attempt_retry_reason_drift_text(r) if a.format=="text" else format_publication_attempt_retry_reason_drift_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
