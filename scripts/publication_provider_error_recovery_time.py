#!/usr/bin/env python3
"""Export Publication Provider Error Recovery Time."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent/"src"))
from evaluation.publication_provider_error_recovery_time import DEFAULT_LIMIT, DEFAULT_MAX_HOURS, build_publication_provider_error_recovery_time_report_from_db, format_publication_provider_error_recovery_time_json, format_publication_provider_error_recovery_time_text  # noqa: E402
from runner import script_context  # noqa: E402
def _pos_int(v):
    try:n=int(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from exc
    if n<=0: raise argparse.ArgumentTypeError("value must be positive")
    return n
def _pos_float(v):
    try:n=float(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid number: {v}") from exc
    if n<=0: raise argparse.ArgumentTypeError("value must be positive")
    return n
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--limit",type=_pos_int,default=DEFAULT_LIMIT); p.add_argument("--max-hours",type=_pos_float,default=DEFAULT_MAX_HOURS); return p.parse_args(argv)
def main(argv=None):
    try:a=parse_args(argv)
    except SystemExit as exc: return int(exc.code or 0)
    try:
        kw={"limit":a.limit,"max_hours":a.max_hours}
        if a.db:
            with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_publication_provider_error_recovery_time_report_from_db(c,**kw)
        else:
            with script_context() as (_cfg,db): r=build_publication_provider_error_recovery_time_report_from_db(db,**kw)
    except (OSError,sqlite3.Error,TypeError,ValueError) as exc: print(f"error: {exc}",file=sys.stderr); return 1
    print(format_publication_provider_error_recovery_time_text(r) if a.format=="text" else format_publication_provider_error_recovery_time_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
