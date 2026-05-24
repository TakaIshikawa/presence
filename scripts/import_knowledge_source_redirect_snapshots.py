#!/usr/bin/env python3
from __future__ import annotations
import argparse, json, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
from knowledge.source_redirect_snapshot_import import import_knowledge_source_redirect_snapshots, load_redirect_snapshot_rows  # noqa: E402

def _nonnegative_int(v):
    p=int(v)
    if p<0: raise argparse.ArgumentTypeError('value must be non-negative')
    return p
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--db', required=True); p.add_argument('--input', required=True); p.add_argument('--dry-run', action='store_true'); p.add_argument('--max-redirects', type=_nonnegative_int); return p.parse_args(argv)
def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    try:
        rows=load_redirect_snapshot_rows(a.input)
        with sqlite3.connect(a.db) as conn: summary=import_knowledge_source_redirect_snapshots(conn, rows, dry_run=a.dry_run, max_redirects=a.max_redirects)
    except (OSError, sqlite3.Error, ValueError, json.JSONDecodeError) as e:
        print(f'error: {e}', file=sys.stderr); return 1
    print(json.dumps(summary, indent=2, sort_keys=True)); return 0
if __name__=='__main__': raise SystemExit(main())
