#!/usr/bin/env python3
"""Run the blog draft external link diversity report."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.blog_draft_external_link_diversity import build_blog_draft_external_link_diversity_report_from_db, format_blog_draft_external_link_diversity_json, format_blog_draft_external_link_diversity_text  # noqa: E402
from runner import script_context  # noqa: E402

def _positive_int(v):
    try: p=int(v)
    except ValueError as e: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from e
    if p<=0: raise argparse.ArgumentTypeError("value must be positive")
    return p

def _nonnegative_float(v):
    try: p=float(v)
    except ValueError as e: raise argparse.ArgumentTypeError(f"invalid number: {v}") from e
    if p<0: raise argparse.ArgumentTypeError("value must be non-negative")
    return p

def _ratio(v):
    p=_nonnegative_float(v)
    if p>1: raise argparse.ArgumentTypeError("value must be at most 1")
    return p

def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json","text"), default="json")
    p.add_argument("--internal-domain", action="append")
    p.add_argument("--min-domains", type=_positive_int, default=2)
    p.add_argument("--max-domain-share", type=_ratio, default=0.7)
    p.add_argument("--limit", type=_positive_int, default=100)
    return p.parse_args(argv)

def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as e: return int(e.code or 0)
    kw={'internal_domain':a.internal_domain,'min_domains':a.min_domains,'max_domain_share':a.max_domain_share,'limit':a.limit}
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn:
                conn.row_factory=sqlite3.Row; report=build_blog_draft_external_link_diversity_report_from_db(conn, **kw)
        else:
            with script_context() as (_config, db): report=build_blog_draft_external_link_diversity_report_from_db(db, **kw)
    except (OSError, sqlite3.Error, TypeError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr); return 1
    print(format_blog_draft_external_link_diversity_text(report) if a.format=="text" else format_blog_draft_external_link_diversity_json(report)); return 0
if __name__=="__main__": raise SystemExit(main())
