#!/usr/bin/env python3
"""Publication Attempt Retry Reason Drift."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.publication_attempt_retry_reason_drift import build_publication_attempt_retry_reason_drift_report_from_db, format_publication_attempt_retry_reason_drift_json, format_publication_attempt_retry_reason_drift_text  # noqa:E402
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
    parser.add_argument("--baseline-days", type=_positive_int, default=30); parser.add_argument("--current-days", type=_positive_int, default=7); parser.add_argument("--min-delta", type=float, default=0.2); parser.add_argument("--limit", type=_positive_int, default=50)
    return parser.parse_args(argv)

def main(argv=None):
    try:
        args=parse_args(argv); kwargs={"baseline_days": args.baseline_days, "current_days": args.current_days, "min_delta": args.min_delta, "limit": args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn: report=build_publication_attempt_retry_reason_drift_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db): report=build_publication_attempt_retry_reason_drift_report_from_db(db, **kwargs)
    except SystemExit as exc: return int(exc.code or 0)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr); return 1
    print(format_publication_attempt_retry_reason_drift_text(report) if args.format == "text" else format_publication_attempt_retry_reason_drift_json(report)); return 0
if __name__ == "__main__": raise SystemExit(main())
