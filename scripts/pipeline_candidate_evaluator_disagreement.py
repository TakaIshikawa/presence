#!/usr/bin/env python3
"""Export Pipeline Candidate Evaluator Disagreement."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.pipeline_candidate_evaluator_disagreement import DEFAULT_MIN_SCORE_SPREAD, build_pipeline_candidate_evaluator_disagreement_report_from_db, format_pipeline_candidate_evaluator_disagreement_json, format_pipeline_candidate_evaluator_disagreement_text  # noqa:E402
from runner import script_context  # noqa:E402
def _share(v):
    try: p=float(v)
    except ValueError as e: raise argparse.ArgumentTypeError(f"invalid number: {v}") from e
    if p<0 or p>1: raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return p
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--min-score-spread",type=_share,default=DEFAULT_MIN_SCORE_SPREAD); p.add_argument("--since"); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    try:
        kw={"min_score_spread":a.min_score_spread,"since":a.since}
        if a.db:
            with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_pipeline_candidate_evaluator_disagreement_report_from_db(c,**kw)
        else:
            with script_context() as (_config,db): r=build_pipeline_candidate_evaluator_disagreement_report_from_db(db,**kw)
    except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
    print(format_pipeline_candidate_evaluator_disagreement_text(r) if a.format=="text" else format_pipeline_candidate_evaluator_disagreement_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
