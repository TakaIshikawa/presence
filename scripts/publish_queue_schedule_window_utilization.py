#!/usr/bin/env python3
"""Export Publish Queue Schedule Window Utilization."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.publish_queue_schedule_window_utilization import (DEFAULT_UNDERUSED, DEFAULT_OVERFILLED, DEFAULT_LIMIT, build_publish_queue_schedule_window_utilization_report_from_db, format_publish_queue_schedule_window_utilization_json, format_publish_queue_schedule_window_utilization_text)  # noqa: E402
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
    parser.add_argument('--timezone', default='UTC')
    parser.add_argument('--underused-threshold', type=_share, default=DEFAULT_UNDERUSED)
    parser.add_argument('--overfilled-threshold', type=_positive_float, default=DEFAULT_OVERFILLED)
    parser.add_argument('--limit', type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)

def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {'timezone': args.timezone, 'underused_threshold': args.underused_threshold, 'overfilled_threshold': args.overfilled_threshold, 'limit': args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publish_queue_schedule_window_utilization_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_publish_queue_schedule_window_utilization_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_publish_queue_schedule_window_utilization_text(report) if args.format == 'text' else format_publish_queue_schedule_window_utilization_json(report))
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
