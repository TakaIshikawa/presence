#!/usr/bin/env python3
"""Export Model Usage Stage Budget Burn."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.model_usage_stage_budget_burn import *  # noqa:E402,F403
from runner import script_context  # noqa:E402
def _pos(v):
 try:n=float(v)
 except ValueError as e: raise argparse.ArgumentTypeError(f"invalid number: {v}") from e
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--days",type=int,default=DEFAULT_DAYS); p.add_argument("--limit",type=int,default=DEFAULT_LIMIT); p.add_argument("--stage"); p.add_argument("--budget-usd",type=_pos,default=DEFAULT_BUDGET_USD); p.add_argument("--token-budget",type=int,default=DEFAULT_TOKEN_BUDGET); p.add_argument("--format",choices=("json","text"),default="json")
 try:
  a=p.parse_args(argv); kw={"days":a.days,"limit":a.limit,"stage":a.stage,"budget_usd":a.budget_usd,"token_budget":a.token_budget}
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_model_usage_stage_budget_burn_report_from_db(c,**kw)
  else:
   with script_context() as (_cfg,db): r=build_model_usage_stage_budget_burn_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_model_usage_stage_budget_burn_text(r) if a.format=="text" else format_model_usage_stage_budget_burn_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
