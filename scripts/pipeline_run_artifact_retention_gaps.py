#!/usr/bin/env python3
"""Pipeline Run Artifact Retention Gaps."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/'src'))
from evaluation.pipeline_run_artifact_retention_gaps import build_pipeline_run_artifact_retention_gaps_report_from_db,format_pipeline_run_artifact_retention_gaps_json,format_pipeline_run_artifact_retention_gaps_text # noqa:E402
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
 p=argparse.ArgumentParser(description=__doc__); p.add_argument('--db'); p.add_argument('--format',choices=('json','text'),default='json'); p.add_argument('--max-age-days',type=_nonneg,default=30); p.add_argument('--max-size-bytes',type=_pos,default=100000000); p.add_argument('--limit',type=_pos,default=50)
 try:
  a=p.parse_args(argv); kw={'max_age_days':a.max_age_days,'max_size_bytes':a.max_size_bytes,'limit':a.limit}
  if a.db:
   with sqlite3.connect(a.db) as conn: r=build_pipeline_run_artifact_retention_gaps_report_from_db(conn,**kw)
  else:
   with script_context() as (_c,db): r=build_pipeline_run_artifact_retention_gaps_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f'error: {e}',file=sys.stderr); return 1
 print(format_pipeline_run_artifact_retention_gaps_text(r) if a.format=='text' else format_pipeline_run_artifact_retention_gaps_json(r)); return 0
if __name__=='__main__': raise SystemExit(main())
