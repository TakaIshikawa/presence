#!/usr/bin/env python3
"""Report newsletter destination domains with decaying click share."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.newsletter_click_destination_decay import DEFAULT_BASELINE_DAYS, DEFAULT_LIMIT, DEFAULT_MIN_BASELINE_CLICKS, DEFAULT_MIN_SHARE_DROP, DEFAULT_RECENT_DAYS, build_newsletter_click_destination_decay_report_from_db, format_newsletter_click_destination_decay_json, format_newsletter_click_destination_decay_text  # noqa:E402
from runner import script_context  # noqa:E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--recent-days", type=_pos, default=DEFAULT_RECENT_DAYS)
    p.add_argument("--baseline-days", type=_pos, default=DEFAULT_BASELINE_DAYS)
    p.add_argument("--min-baseline-clicks", type=_pos, default=DEFAULT_MIN_BASELINE_CLICKS)
    p.add_argument("--min-share-drop", type=float, default=DEFAULT_MIN_SHARE_DROP)
    p.add_argument("--domain", action="append")
    p.add_argument("--limit", type=_pos, default=DEFAULT_LIMIT)
    p.add_argument("--format", choices=("json", "text"), default="json")
    return p.parse_args(argv)


def main(argv=None):
    try:
        a = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kw = {"recent_days": a.recent_days, "baseline_days": a.baseline_days, "min_baseline_clicks": a.min_baseline_clicks, "min_share_drop": a.min_share_drop, "domain": a.domain, "limit": a.limit}
    try:
        if a.db:
            with sqlite3.connect(a.db) as c:
                c.row_factory = sqlite3.Row
                r = build_newsletter_click_destination_decay_report_from_db(c, **kw)
        else:
            with script_context() as (_x, db):
                r = build_newsletter_click_destination_decay_report_from_db(db, **kw)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_newsletter_click_destination_decay_text(r) if a.format == "text" else format_newsletter_click_destination_decay_json(r))
    return 0


def _pos(v):
    try:
        n = int(v)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {v}") from exc
    if n <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return n


if __name__ == "__main__":
    raise SystemExit(main())
