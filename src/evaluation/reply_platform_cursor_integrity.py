"""Report reply platform cursor integrity."""

from __future__ import annotations

from ._batch_gap_reports import reply_cursor_from_db as build_reply_platform_cursor_integrity_report_from_db
from ._batch_gap_reports import reply_cursor_report as build_reply_platform_cursor_integrity_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text

DEFAULT_MAX_AGE_HOURS = 48


def format_reply_platform_cursor_integrity_json(report):
    return format_json(report)


def format_reply_platform_cursor_integrity_text(report):
    return format_text("Reply Platform Cursor Integrity", report)
