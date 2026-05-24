#!/usr/bin/env python3
"""Export Blog Newsletter Topic Echo Risk."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.blog_newsletter_topic_echo_risk import (DEFAULT_COOLDOWN_DAYS, DEFAULT_SIMILARITY, DEFAULT_LIMIT, build_blog_newsletter_topic_echo_risk_report_from_db, format_blog_newsletter_topic_echo_risk_json, format_blog_newsletter_topic_echo_risk_text)  # noqa: E402
from runner import script_context  # noqa: E402

def _positive_int(value: str) -> int:
    try: parsed = int(value)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0: raise argparse.ArgumentTypeError("value must be positive")
    return parsed

def _non_negative_int(value: str) -> int:
    try: parsed = int(value)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0: raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed

def _positive_float(value: str) -> float:
    try: parsed = float(value)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed <= 0: raise argparse.ArgumentTypeError("value must be positive")
    return parsed

def _share(value: str) -> float:
    try: parsed = float(value)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0 or parsed > 1: raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', help='SQLite database path. Defaults to configured database.')
    parser.add_argument('--format', choices=('json','text'), default='json')
    parser.add_argument('--cooldown-days', type=_positive_int, default=DEFAULT_COOLDOWN_DAYS)
    parser.add_argument('--similarity-threshold', type=_share, default=DEFAULT_SIMILARITY)
    parser.add_argument('--limit', type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)

def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {'cooldown_days': args.cooldown_days, 'similarity_threshold': args.similarity_threshold, 'limit': args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_blog_newsletter_topic_echo_risk_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_blog_newsletter_topic_echo_risk_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_blog_newsletter_topic_echo_risk_text(report) if args.format == 'text' else format_blog_newsletter_topic_echo_risk_json(report))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
