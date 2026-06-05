#!/usr/bin/env python3
"""Report newsletter images with missing or weak alt text."""
from __future__ import annotations
import argparse, sqlite3, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from evaluation.newsletter_image_alt_text_gaps import DEFAULT_LIMIT, build_newsletter_image_alt_text_gaps_report_from_db, format_newsletter_image_alt_text_gaps_json, format_newsletter_image_alt_text_gaps_text  # noqa:E402
from runner import script_context  # noqa:E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--limit", type=_pos, default=DEFAULT_LIMIT)
    p.add_argument("--format", choices=("json", "text"), default="json")
    return p.parse_args(argv)


def main(argv=None):
    try:
        a = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if a.db:
            with sqlite3.connect(a.db) as c:
                c.row_factory = sqlite3.Row
                r = build_newsletter_image_alt_text_gaps_report_from_db(c, limit=a.limit)
        else:
            with script_context() as (_x, db):
                r = build_newsletter_image_alt_text_gaps_report_from_db(db, limit=a.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_newsletter_image_alt_text_gaps_text(r) if a.format == "text" else format_newsletter_image_alt_text_gaps_json(r))
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
