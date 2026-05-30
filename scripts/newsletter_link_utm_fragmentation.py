#!/usr/bin/env python3
"""Report newsletter UTM fragmentation."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.newsletter_link_utm_fragmentation import DEFAULT_LIMIT, DEFAULT_MIN_VARIANTS, build_newsletter_link_utm_fragmentation_report_from_db, format_newsletter_link_utm_fragmentation_json, format_newsletter_link_utm_fragmentation_text  # noqa: E402
from runner import script_context  # noqa: E402
def _pos(v): n=int(v); 0 < n or (_ for _ in ()).throw(argparse.ArgumentTypeError("value must be positive")); return n
def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--min-variants",type=_pos,default=DEFAULT_MIN_VARIANTS)
    try:
        a=p.parse_args(argv); kw={"limit":a.limit,"min_variants":a.min_variants}
        if a.db:
            with sqlite3.connect(a.db) as conn: r=build_newsletter_link_utm_fragmentation_report_from_db(conn,**kw)
        else:
            with script_context() as (_c,db): r=build_newsletter_link_utm_fragmentation_report_from_db(db,**kw)
    except SystemExit as e: return int(e.code or 0)
    except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
    print(format_newsletter_link_utm_fragmentation_text(r) if a.format=="text" else format_newsletter_link_utm_fragmentation_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
