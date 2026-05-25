#!/usr/bin/env python3
"""Export Publication Media Attachment Mismatch."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.publication_media_attachment_mismatch import build_publication_media_attachment_mismatch_report_from_db, format_publication_media_attachment_mismatch_json, format_publication_media_attachment_mismatch_text  # noqa:E402
from runner import script_context  # noqa:E402
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--platform"); p.add_argument("--since"); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    try:
        kw={"platform":a.platform,"since":a.since}
        if a.db:
            with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_publication_media_attachment_mismatch_report_from_db(c,**kw)
        else:
            with script_context() as (_config,db): r=build_publication_media_attachment_mismatch_report_from_db(db,**kw)
    except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
    print(format_publication_media_attachment_mismatch_text(r) if a.format=="text" else format_publication_media_attachment_mismatch_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
