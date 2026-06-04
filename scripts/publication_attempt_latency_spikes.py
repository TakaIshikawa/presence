#!/usr/bin/env python3
from __future__ import annotations
import argparse,sqlite3,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.publication_attempt_latency_spikes import DEFAULT_LIMIT, DEFAULT_LOOKBACK_DAYS, DEFAULT_MIN_DURATION_MS, DEFAULT_PERCENTILE_BASELINE, build_publication_attempt_latency_spikes_report_from_db, format_publication_attempt_latency_spikes_json, format_publication_attempt_latency_spikes_text  # noqa:E402
from runner import script_context  # noqa:E402
def parse_args(argv=None):
 p=argparse.ArgumentParser(); p.add_argument("--db"); p.add_argument("--lookback-days",type=_pos,default=DEFAULT_LOOKBACK_DAYS); p.add_argument("--min-duration-ms",type=_pos,default=DEFAULT_MIN_DURATION_MS); p.add_argument("--percentile-baseline",type=float,default=DEFAULT_PERCENTILE_BASELINE); p.add_argument("--platform"); p.add_argument("--limit",type=_pos,default=DEFAULT_LIMIT); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
 try:a=parse_args(argv)
 except SystemExit as exc:return int(exc.code or 0)
 kw={"lookback_days":a.lookback_days,"min_duration_ms":a.min_duration_ms,"percentile_baseline":a.percentile_baseline,"platform":a.platform,"limit":a.limit}
 try:
  if a.db:
   with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_publication_attempt_latency_spikes_report_from_db(c,**kw)
  else:
   with script_context() as (_x,db): r=build_publication_attempt_latency_spikes_report_from_db(db,**kw)
 except (OSError,sqlite3.Error,TypeError,ValueError) as exc: print(f"error: {exc}",file=sys.stderr); return 1
 print(format_publication_attempt_latency_spikes_text(r) if a.format=="text" else format_publication_attempt_latency_spikes_json(r)); return 0
def _pos(v):
 n=int(v)
 if n<=0: raise argparse.ArgumentTypeError("value must be positive")
 return n
if __name__=="__main__": raise SystemExit(main())
