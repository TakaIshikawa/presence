#!/usr/bin/env python3
"""Export Model Usage Cache Savings Opportunities."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.model_usage_cache_savings_opportunities import DEFAULT_LIMIT, DEFAULT_LOOKBACK_DAYS, build_model_usage_cache_savings_opportunities_report_from_db, format_model_usage_cache_savings_opportunities_json, format_model_usage_cache_savings_opportunities_text  # noqa:E402
from runner import script_context  # noqa:E402
def _pos(v):
    try: n = int(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from exc
    if n <= 0: raise argparse.ArgumentTypeError("value must be positive")
    return n
def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format", choices=("json", "text"), default="json"); p.add_argument("--limit", type=_pos, default=DEFAULT_LIMIT); p.add_argument("--lookback-days", type=_pos, default=DEFAULT_LOOKBACK_DAYS); return p.parse_args(argv)
def main(argv=None):
    try: a = parse_args(argv)
    except SystemExit as exc: return int(exc.code or 0)
    try:
        kw = {"limit": a.limit, "lookback_days": a.lookback_days}
        if a.db:
            with sqlite3.connect(a.db) as c: c.row_factory = sqlite3.Row; r = build_model_usage_cache_savings_opportunities_report_from_db(c, **kw)
        else:
            with script_context() as (_cfg, db): r = build_model_usage_cache_savings_opportunities_report_from_db(db, **kw)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc: print(f"error: {exc}", file=sys.stderr); return 1
    print(format_model_usage_cache_savings_opportunities_text(r) if a.format == "text" else format_model_usage_cache_savings_opportunities_json(r)); return 0
if __name__ == "__main__": raise SystemExit(main())
