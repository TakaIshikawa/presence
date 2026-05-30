#!/usr/bin/env python3
"""Report publication attempt provider latency jitter."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.publication_attempt_provider_latency_jitter import DEFAULT_JITTER_THRESHOLD, DEFAULT_LIMIT, DEFAULT_LOOKBACK_DAYS, build_publication_attempt_provider_latency_jitter_report_from_db, format_publication_attempt_provider_latency_jitter_json, format_publication_attempt_provider_latency_jitter_text  # noqa: E402
from runner import script_context  # noqa: E402
def _pos_int(v): n=int(v); 0 < n or (_ for _ in ()).throw(argparse.ArgumentTypeError("value must be positive")); return n
def _pos_float(v): n=float(v); 0 < n or (_ for _ in ()).throw(argparse.ArgumentTypeError("value must be positive")); return n
def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format",choices=("json","text"),default="json"); p.add_argument("--limit",type=_pos_int,default=DEFAULT_LIMIT); p.add_argument("--lookback-days",type=_pos_int,default=DEFAULT_LOOKBACK_DAYS); p.add_argument("--jitter-threshold",type=_pos_float,default=DEFAULT_JITTER_THRESHOLD)
    try:
        a=p.parse_args(argv); kw={"limit":a.limit,"lookback_days":a.lookback_days,"jitter_threshold":a.jitter_threshold}
        if a.db:
            with sqlite3.connect(a.db) as conn: r=build_publication_attempt_provider_latency_jitter_report_from_db(conn,**kw)
        else:
            with script_context() as (_c,db): r=build_publication_attempt_provider_latency_jitter_report_from_db(db,**kw)
    except SystemExit as e: return int(e.code or 0)
    except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
    print(format_publication_attempt_provider_latency_jitter_text(r) if a.format=="text" else format_publication_attempt_provider_latency_jitter_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
