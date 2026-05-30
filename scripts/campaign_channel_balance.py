#!/usr/bin/env python3
"""Export Campaign Channel Balance."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.campaign_channel_balance import *  # noqa:E402,F403
from runner import script_context  # noqa:E402
def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--days",type=int,default=DEFAULT_DAYS); p.add_argument("--campaign"); p.add_argument("--limit",type=int,default=DEFAULT_LIMIT); p.add_argument("--max-channel-share",type=float,default=DEFAULT_MAX_CHANNEL_SHARE); p.add_argument("--format",choices=("json","text"),default="json")
 try:
  a=p.parse_args(argv); kw={"days":a.days,"campaign":a.campaign,"limit":a.limit,"max_channel_share":a.max_channel_share}
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_campaign_channel_balance_report_from_db(c,**kw)
  else:
   with script_context() as (_cfg,db): r=build_campaign_channel_balance_report_from_db(db,**kw)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_campaign_channel_balance_text(r) if a.format=="text" else format_campaign_channel_balance_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
