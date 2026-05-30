#!/usr/bin/env python3
"""Report reply platform cursor integrity."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from _focused_report_cli import non_negative_int, positive_int, run  # noqa: E402
from evaluation.reply_platform_cursor_integrity import DEFAULT_LIMIT, DEFAULT_MAX_AGE_HOURS, build_reply_platform_cursor_integrity_report_from_db, format_reply_platform_cursor_integrity_json, format_reply_platform_cursor_integrity_text  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "text"), default="json")
    p.add_argument("--max-age-hours", type=non_negative_int, default=DEFAULT_MAX_AGE_HOURS)
    p.add_argument("--limit", type=positive_int, default=DEFAULT_LIMIT)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return run(args, build_reply_platform_cursor_integrity_report_from_db, format_reply_platform_cursor_integrity_json, format_reply_platform_cursor_integrity_text, {"max_age_hours": args.max_age_hours, "limit": args.limit})


if __name__ == "__main__":
    raise SystemExit(main())
