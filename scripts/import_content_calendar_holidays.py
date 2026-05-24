#!/usr/bin/env python3
"""Import content calendar holidays."""
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.content_calendar_holiday_import import import_content_calendar_holidays,parse_holiday_payload,format_content_calendar_holiday_import_json,format_content_calendar_holiday_import_text  # noqa:E402
from runner import script_context  # noqa:E402
def main(argv:list[str]|None=None)->int:
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("input"); p.add_argument("--db"); p.add_argument("--region-default",default="global"); p.add_argument("--dry-run",action="store_true"); p.add_argument("--format",choices=("json","text"),default="json")
 try:
  a=p.parse_args(argv); rows=parse_holiday_payload(Path(a.input).read_text(),region_default=a.region_default)
  if a.db:
   with sqlite3.connect(a.db) as c: r=import_content_calendar_holidays(c,rows,dry_run=a.dry_run)
  else:
   with script_context() as (_c,db): r=import_content_calendar_holidays(db,rows,dry_run=a.dry_run)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_content_calendar_holiday_import_text(r) if a.format=="text" else format_content_calendar_holiday_import_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
