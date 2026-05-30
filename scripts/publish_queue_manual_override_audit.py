#!/usr/bin/env python3
"""Report publish queue manual override audit findings."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.publish_queue_manual_override_audit import DEFAULT_LIMIT, DEFAULT_LOOKBACK_DAYS, DEFAULT_MAX_AGE_HOURS, DEFAULT_STATUS, build_publish_queue_manual_override_audit_report_from_db, format_publish_queue_manual_override_audit_json, format_publish_queue_manual_override_audit_text  # noqa: E402
from runner import script_context  # noqa: E402


def _pos(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return number


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--limit", type=_pos, default=DEFAULT_LIMIT)
    parser.add_argument("--lookback-days", type=_pos, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--max-age-hours", type=_pos, default=DEFAULT_MAX_AGE_HOURS)
    parser.add_argument("--status", default=DEFAULT_STATUS)
    return parser.parse_args(argv)


def main(argv=None):
    try:
        args = parse_args(argv)
        kwargs = {
            "limit": args.limit,
            "lookback_days": args.lookback_days,
            "max_age_hours": args.max_age_hours,
            "status": args.status or None,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_publish_queue_manual_override_audit_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_cfg, db):
                report = build_publish_queue_manual_override_audit_report_from_db(db, **kwargs)
    except SystemExit as exc:
        return int(exc.code or 0)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_publish_queue_manual_override_audit_text(report)
        if args.format == "text"
        else format_publish_queue_manual_override_audit_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
