#!/usr/bin/env python3
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from ingestion.blog_search_console_query_import import *  # noqa:E402,F403
from runner import script_context  # noqa:E402
def main(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("input"); p.add_argument("--db"); p.add_argument("--dry-run",action="store_true"); p.add_argument("--format",choices=("json","text"),default="json")
 try:
  a=p.parse_args(argv); rows=parse_blog_search_console_query_payload(Path(a.input).read_text())
  if a.db:
   with sqlite3.connect(a.db) as c: r=import_blog_search_console_queries(c,rows,dry_run=a.dry_run)
  else:
   with script_context() as (_cfg,db): r=import_blog_search_console_queries(db,rows,dry_run=a.dry_run)
 except SystemExit as e: return int(e.code or 0)
 except Exception as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_blog_search_console_query_import_text(r) if a.format=="text" else format_blog_search_console_query_import_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
