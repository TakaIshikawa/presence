#!/usr/bin/env python3
"""Blog Newsletter Topic Echo Risk."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.blog_newsletter_topic_echo_risk import build_blog_newsletter_topic_echo_risk_report_from_db, format_blog_newsletter_topic_echo_risk_json, format_blog_newsletter_topic_echo_risk_text  # noqa:E402
from runner import script_context  # noqa:E402

def _positive_int(value: str) -> int:
    try: parsed=int(value)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0: raise argparse.ArgumentTypeError("value must be positive")
    return parsed

def parse_args(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json","text"), default="json")
    parser.add_argument("--cooldown-days", type=_positive_int, default=14); parser.add_argument("--similarity-threshold", type=float, default=0.5); parser.add_argument("--limit", type=_positive_int, default=50)
    return parser.parse_args(argv)

def main(argv=None):
    try:
        args=parse_args(argv); kwargs={"cooldown_days": args.cooldown_days, "similarity_threshold": args.similarity_threshold, "limit": args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn: report=build_blog_newsletter_topic_echo_risk_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db): report=build_blog_newsletter_topic_echo_risk_report_from_db(db, **kwargs)
    except SystemExit as exc: return int(exc.code or 0)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr); return 1
    print(format_blog_newsletter_topic_echo_risk_text(report) if args.format == "text" else format_blog_newsletter_topic_echo_risk_json(report)); return 0
if __name__ == "__main__": raise SystemExit(main())
