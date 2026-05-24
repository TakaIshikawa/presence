#!/usr/bin/env python3
"""Report model usage context window pressure."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.model_usage_context_window_pressure import DEFAULT_LIMIT, DEFAULT_PRESSURE_THRESHOLD, build_model_usage_context_window_pressure_report_from_db, format_model_usage_context_window_pressure_json, format_model_usage_context_window_pressure_text  # noqa: E402
from runner import script_context  # noqa: E402
def _pos(v): n=int(v); 0 < n or (_ for _ in ()).throw(argparse.ArgumentTypeError("value must be positive")); return n
def _thr(v): n=float(v); 0 < n <= 1 or (_ for _ in ()).throw(argparse.ArgumentTypeError("value must be between 0 and 1")); return n
def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--pressure-threshold",type=_thr,default=DEFAULT_PRESSURE_THRESHOLD)
    try:
        a=p.parse_args(argv); kw={"limit":a.limit,"pressure_threshold":a.pressure_threshold}
        if a.db:
            with sqlite3.connect(a.db) as conn: r=build_model_usage_context_window_pressure_report_from_db(conn,**kw)
        else:
            with script_context() as (_c,db): r=build_model_usage_context_window_pressure_report_from_db(db,**kw)
    except SystemExit as e: return int(e.code or 0)
    except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
    print(format_model_usage_context_window_pressure_text(r) if a.format=="text" else format_model_usage_context_window_pressure_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
