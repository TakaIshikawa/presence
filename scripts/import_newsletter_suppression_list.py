#!/usr/bin/env python3
"""Import newsletter suppression records."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.newsletter_suppression_list_import import parse_newsletter_suppression_records, import_newsletter_suppression_records, format_newsletter_suppression_import_json, format_newsletter_suppression_import_text  # noqa: E402
def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db",required=True); p.add_argument("--input",required=True); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--dry-run",action="store_true"); p.add_argument("--strict",action="store_true")
 try:
  a=p.parse_args(argv); rows=parse_newsletter_suppression_records(a.input,strict=a.strict)
  with sqlite3.connect(a.db) as conn: r=import_newsletter_suppression_records(conn,rows,dry_run=a.dry_run,strict=a.strict)
 except SystemExit as e: return int(e.code or 0)
 except (OSError,sqlite3.Error,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
 print(format_newsletter_suppression_import_text(r) if a.format=="text" else format_newsletter_suppression_import_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
