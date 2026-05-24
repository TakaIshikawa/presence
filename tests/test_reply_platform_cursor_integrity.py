from datetime import datetime, timezone
import sqlite3

from evaluation.reply_platform_cursor_integrity import build_reply_platform_cursor_integrity_report, build_reply_platform_cursor_integrity_report_from_db

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_reply_platform_cursor_integrity():
    report = build_reply_platform_cursor_integrity_report([
        {"id": 1, "cursor": "", "platform_updated_at": "2026-04-20T00:00:00+00:00", "legacy_last_mention_id": "1", "x_cursor": "2"},
        {"id": 2, "cursor": "x", "platform_updated_at": "2026-04-20T00:00:00+00:00", "missing_platform_state": 1},
    ], now=NOW)
    assert {"missing_cursor", "stale_platform_cursor", "legacy_cursor_divergence", "missing_platform_state"} <= {f["reason"] for f in report["findings"]}
    assert build_reply_platform_cursor_integrity_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["reply_state"]
