#!/usr/bin/env python3
"""Report publishable blog drafts with too few internal links."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.blog_draft_internal_link_gaps import DEFAULT_ALLOWED_DOMAINS, DEFAULT_LIMIT, DEFAULT_LOOKBACK_DAYS, DEFAULT_MIN_INTERNAL_LINKS, build_blog_draft_internal_link_gaps_report_from_db, format_blog_draft_internal_link_gaps_json, format_blog_draft_internal_link_gaps_text  # noqa:E402
from runner import script_context  # noqa:E402

def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--min-internal-links", type=_positive_int, default=DEFAULT_MIN_INTERNAL_LINKS)
    p.add_argument("--allowed-domain", action="append", dest="allowed_domains", default=list(DEFAULT_ALLOWED_DOMAINS))
    p.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    p.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    p.add_argument("--format", choices=("json", "text"), default="json")
    return p.parse_args(argv)

def main(argv=None):
    try: a = parse_args(argv)
    except SystemExit as exc: return int(exc.code or 0)
    kw = {"min_internal_links": a.min_internal_links, "allowed_domains": a.allowed_domains, "lookback_days": a.lookback_days, "limit": a.limit}
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn:
                conn.row_factory = sqlite3.Row; report = build_blog_draft_internal_link_gaps_report_from_db(conn, **kw)
        else:
            with script_context() as (_ctx, db):
                report = build_blog_draft_internal_link_gaps_report_from_db(db, **kw)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr); return 1
    print(format_blog_draft_internal_link_gaps_text(report) if a.format == "text" else format_blog_draft_internal_link_gaps_json(report))
    return 0

def _positive_int(value):
    try: parsed = int(value)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0: raise argparse.ArgumentTypeError("value must be positive")
    return parsed

if __name__ == "__main__":
    raise SystemExit(main())
