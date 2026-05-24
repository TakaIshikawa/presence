#!/usr/bin/env python3
"""Report GitHub activity review request lag."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.github_activity_review_request_lag import DEFAULT_LIMIT, DEFAULT_SLA_HOURS, build_github_activity_review_request_lag_report_from_db, format_github_activity_review_request_lag_json, format_github_activity_review_request_lag_text  # noqa: E402
from runner import script_context  # noqa: E402
def _pos(v): n=int(v); 0<n or (_ for _ in ()).throw(argparse.ArgumentTypeError("value must be positive")); return n
def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--sla-hours",type=_pos,default=DEFAULT_SLA_HOURS)
 try:
  a=p.parse_args(argv); kw={"limit":a.limit,"sla_hours":a.sla_hours}
  if a.db:
   with sqlite3.connect(a.db) as conn: r=build_github_activity_review_request_lag_report_from_db(conn,**kw)
  else:
   with script_context() as (_c,db): r=build_github_activity_review_request_lag_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_github_activity_review_request_lag_text(r) if a.format=="text" else format_github_activity_review_request_lag_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
