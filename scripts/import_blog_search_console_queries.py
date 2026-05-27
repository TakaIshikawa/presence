#!/usr/bin/env python3
"""Import blog Search Console query metrics."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from ingestion.blog_search_console_query_import import format_blog_search_console_query_import_json, format_blog_search_console_query_import_text, import_blog_search_console_queries  # noqa:E402
from runner import script_context  # noqa:E402
def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--input", required=True); p.add_argument("--dry-run", action="store_true"); p.add_argument("--format", choices=("json", "text"), default="json"); return p.parse_args(argv)
def main(argv=None):
    try: a = parse_args(argv)
    except SystemExit as exc: return int(exc.code or 0)
    try:
        if a.db:
            with sqlite3.connect(a.db) as c: c.row_factory = sqlite3.Row; s = import_blog_search_console_queries(c, a.input, dry_run=a.dry_run)
        else:
            with script_context() as (_cfg, db): s = import_blog_search_console_queries(db.conn, a.input, dry_run=a.dry_run)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc: print(f"error: {exc}", file=sys.stderr); return 1
    print(format_blog_search_console_query_import_text(s) if a.format == "text" else format_blog_search_console_query_import_json(s)); return 0
if __name__ == "__main__": raise SystemExit(main())
