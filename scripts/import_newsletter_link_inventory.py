#!/usr/bin/env python3
"""Import newsletter link inventory."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from ingestion.newsletter_link_inventory_import import import_newsletter_link_inventory, format_newsletter_link_inventory_import_json, format_newsletter_link_inventory_import_text  # noqa:E402
from runner import script_context  # noqa:E402
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--db'); p.add_argument('--input',required=True); p.add_argument('--dry-run',action='store_true'); p.add_argument('--format',choices=('json','text'),default='json'); return p.parse_args(argv)
def main(argv=None):
    try:a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    try:
        if a.db:
            with sqlite3.connect(a.db) as c: r=import_newsletter_link_inventory(c,a.input,dry_run=a.dry_run)
        else:
            with script_context() as (_cfg,db): r=import_newsletter_link_inventory(db,a.input,dry_run=a.dry_run)
    except (OSError,sqlite3.Error,ValueError,TypeError) as e: print(f"error: {e}",file=sys.stderr); return 1
    print(format_newsletter_link_inventory_import_text(r) if a.format=='text' else format_newsletter_link_inventory_import_json(r)); return 0
if __name__=='__main__': raise SystemExit(main())
