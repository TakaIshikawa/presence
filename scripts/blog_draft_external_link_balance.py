#!/usr/bin/env python3
"""Report blog drafts with weak or imbalanced external citation links."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.blog_draft_external_link_balance import DEFAULT_LIMIT, DEFAULT_LOOKBACK_DAYS, DEFAULT_MAX_DOMAIN_SHARE, DEFAULT_MAX_EXTERNAL_LINKS, DEFAULT_MIN_EXTERNAL_LINKS, DEFAULT_OWN_DOMAINS, DEFAULT_STATUS, build_blog_draft_external_link_balance_report_from_db, format_blog_draft_external_link_balance_json, format_blog_draft_external_link_balance_text  # noqa:E402
from runner import script_context  # noqa:E402

def parse_args(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--min-external-links", type=_pos, default=DEFAULT_MIN_EXTERNAL_LINKS)
    p.add_argument("--max-external-links", type=_pos, default=DEFAULT_MAX_EXTERNAL_LINKS)
    p.add_argument("--max-domain-share", type=_share, default=DEFAULT_MAX_DOMAIN_SHARE)
    p.add_argument("--own-domain", action="append", dest="own_domains", default=list(DEFAULT_OWN_DOMAINS))
    p.add_argument("--status", action="append", default=list(DEFAULT_STATUS))
    p.add_argument("--lookback-days", type=_pos, default=DEFAULT_LOOKBACK_DAYS)
    p.add_argument("--limit", type=_pos, default=DEFAULT_LIMIT)
    p.add_argument("--format", choices=("json","text"), default="json")
    return p.parse_args(argv)

def main(argv=None):
    try: a=parse_args(argv)
    except SystemExit as exc: return int(exc.code or 0)
    kw={"min_external_links":a.min_external_links,"max_external_links":a.max_external_links,"max_domain_share":a.max_domain_share,"own_domains":a.own_domains,"status":a.status,"lookback_days":a.lookback_days,"limit":a.limit}
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn:
                conn.row_factory=sqlite3.Row; report=build_blog_draft_external_link_balance_report_from_db(conn, **kw)
        else:
            with script_context() as (_ctx, db): report=build_blog_draft_external_link_balance_report_from_db(db, **kw)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr); return 1
    print(format_blog_draft_external_link_balance_text(report) if a.format=="text" else format_blog_draft_external_link_balance_json(report))
    return 0

def _pos(v):
    try: n=int(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {v}") from exc
    if n<=0: raise argparse.ArgumentTypeError("value must be positive")
    return n

def _share(v):
    try: n=float(v)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid number: {v}") from exc
    if not 0<n<=1: raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return n

if __name__=="__main__": raise SystemExit(main())
