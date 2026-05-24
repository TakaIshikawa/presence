#!/usr/bin/env python3
"""Report reply platform cursor integrity."""

from __future__ import annotations

from _batch_gap_report_cli import non_negative_float, positive_int, run
from evaluation.reply_platform_cursor_integrity import DEFAULT_LIMIT, DEFAULT_MAX_AGE_HOURS, build_reply_platform_cursor_integrity_report_from_db, format_reply_platform_cursor_integrity_json, format_reply_platform_cursor_integrity_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_reply_platform_cursor_integrity_report_from_db, json_formatter=format_reply_platform_cursor_integrity_json, text_formatter=format_reply_platform_cursor_integrity_text, options=[("max_age_hours", non_negative_float, DEFAULT_MAX_AGE_HOURS), ("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
