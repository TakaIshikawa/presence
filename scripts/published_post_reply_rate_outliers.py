#!/usr/bin/env python3
"""Export Published Post Reply Rate Outliers."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.published_post_reply_rate_outliers import DEFAULT_LIMIT, DEFAULT_MIN_SAMPLE, DEFAULT_RATIO, build_published_post_reply_rate_outliers_report_from_db, format_published_post_reply_rate_outliers_json, format_published_post_reply_rate_outliers_text  # noqa: E402
from runner import script_context  # noqa: E402
def _positive_int(value: str) -> int:
    try: parsed = int(value)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0: raise argparse.ArgumentTypeError("value must be positive")
    return parsed
def _positive_float(value: str) -> float:
    try: parsed = float(value)
    except ValueError as exc: raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if parsed <= 0: raise argparse.ArgumentTypeError("value must be positive")
    return parsed
def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("--db"); p.add_argument("--format", choices=("json", "text"), default="json"); p.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT); p.add_argument("--min-sample", type=_positive_int, default=DEFAULT_MIN_SAMPLE); p.add_argument("--ratio", type=_positive_float, default=DEFAULT_RATIO); p.add_argument("--channel"); return p.parse_args(argv)
def main(argv=None) -> int:
    try: args = parse_args(argv)
    except SystemExit as exc: return int(exc.code or 0)
    try:
        kwargs = {"limit": args.limit, "min_sample": args.min_sample, "ratio": args.ratio, "channel": args.channel}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row; report = build_published_post_reply_rate_outliers_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db): report = build_published_post_reply_rate_outliers_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr); return 1
    print(format_published_post_reply_rate_outliers_text(report) if args.format == "text" else format_published_post_reply_rate_outliers_json(report)); return 0
if __name__ == "__main__": raise SystemExit(main())
