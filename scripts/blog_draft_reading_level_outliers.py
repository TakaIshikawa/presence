#!/usr/bin/env python3
"""Export Blog Draft Reading Level Outliers."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent.parent/"src"))
from evaluation.blog_draft_reading_level_outliers import DEFAULT_MAX_AVG_SENTENCE_WORDS, DEFAULT_MAX_COMPLEX_WORD_RATIO, DEFAULT_MIN_WORDS, build_blog_draft_reading_level_outliers_report_from_db, format_blog_draft_reading_level_outliers_json, format_blog_draft_reading_level_outliers_text  # noqa:E402
from runner import script_context  # noqa:E402
def _pos_float(v):
    try: p=float(v)
    except ValueError as e: raise argparse.ArgumentTypeError(f"invalid number: {v}") from e
    if p<=0: raise argparse.ArgumentTypeError("value must be positive")
    return p
def _pos_int(v):
    try: p=int(v)
    except ValueError as e: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from e
    if p<=0: raise argparse.ArgumentTypeError("value must be positive")
    return p
def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--max-average-sentence-words",type=_pos_float,default=DEFAULT_MAX_AVG_SENTENCE_WORDS); p.add_argument("--max-complex-word-ratio",type=_pos_float,default=DEFAULT_MAX_COMPLEX_WORD_RATIO); p.add_argument("--min-words",type=_pos_int,default=DEFAULT_MIN_WORDS); p.add_argument("--format",choices=("json","text"),default="json"); return p.parse_args(argv)
def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    try:
        kw={"max_average_sentence_words":a.max_average_sentence_words,"max_complex_word_ratio":a.max_complex_word_ratio,"min_words":a.min_words}
        if a.db:
            with sqlite3.connect(a.db) as c: c.row_factory=sqlite3.Row; r=build_blog_draft_reading_level_outliers_report_from_db(c,**kw)
        else:
            with script_context() as (_config,db): r=build_blog_draft_reading_level_outliers_report_from_db(db,**kw)
    except (OSError,sqlite3.Error,TypeError,ValueError) as e: print(f"error: {e}",file=sys.stderr); return 1
    print(format_blog_draft_reading_level_outliers_text(r) if a.format=="text" else format_blog_draft_reading_level_outliers_json(r)); return 0
if __name__=="__main__": raise SystemExit(main())
