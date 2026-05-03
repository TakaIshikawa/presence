"""Tests for publish failure reasons reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from output.publish_failure_reasons import (
    build_publish_failure_reasons_report,
    format_publish_failure_reasons_json,
    format_publish_failure_reasons_text,
)


NOW = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "publish_failure_reasons.py"
)
spec = importlib.util.spec_from_file_location("publish_failure_reasons_script", SCRIPT_PATH)
publish_failure_reasons_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_failure_reasons_script)


def test_classify_failures_into_reason_buckets():
    """Test that common error patterns are classified into stable reason buckets."""
    rows = [
        {
            "id": 1,
            "content_id": 100,
            "platform": "x",
            "attempted_at": "2026-05-01T10:00:00+00:00",
            "success": 0,
            "error": "401 Unauthorized: invalid token",
        },
        {
            "id": 2,
            "content_id": 101,
            "platform": "bluesky",
            "attempted_at": "2026-05-01T10:05:00+00:00",
            "success": 0,
            "error": "429 Too Many Requests",
        },
        {
            "id": 3,
            "content_id": 102,
            "platform": "x",
            "attempted_at": "2026-05-01T10:10:00+00:00",
            "success": 0,
            "error": "Status is a duplicate",
        },
        {
            "id": 4,
            "content_id": 103,
            "platform": "x",
            "attempted_at": "2026-05-01T10:15:00+00:00",
            "success": 0,
            "error": "Media upload failed: unsupported file type",
        },
        {
            "id": 5,
            "content_id": 104,
            "platform": "bluesky",
            "attempted_at": "2026-05-01T10:20:00+00:00",
            "success": 0,
            "error": "Connection timeout",
        },
        {
            "id": 6,
            "content_id": 105,
            "platform": "x",
            "attempted_at": "2026-05-01T10:25:00+00:00",
            "success": 0,
            "error": "Some weird error",
        },
        {
            "id": 7,
            "content_id": 106,
            "platform": "x",
            "attempted_at": "2026-05-01T10:30:00+00:00",
            "success": 1,
            "error": None,
        },
    ]

    report = build_publish_failure_reasons_report(rows, days=7, now=NOW)

    assert len(report.items) == 6
    assert report.items[0].reason == "network"
    assert report.items[1].reason == "rate_limit"
    assert report.items[2].reason == "auth"
    assert report.items[3].reason == "duplicate"
    assert report.items[4].reason == "media"
    assert report.items[5].reason == "unknown"

    assert len(report.summaries) == 6
    assert report.totals["failure_count"] == 6
    assert report.totals["channel_count"] == 2


def test_filter_by_channel():
    """Test that channel filtering works correctly."""
    rows = [
        {
            "id": 1,
            "content_id": 100,
            "platform": "x",
            "attempted_at": "2026-05-01T10:00:00+00:00",
            "success": 0,
            "error": "Rate limit exceeded",
        },
        {
            "id": 2,
            "content_id": 101,
            "platform": "bluesky",
            "attempted_at": "2026-05-01T10:05:00+00:00",
            "success": 0,
            "error": "Unauthorized",
        },
        {
            "id": 3,
            "content_id": 102,
            "platform": "x",
            "attempted_at": "2026-05-01T10:10:00+00:00",
            "success": 0,
            "error": "Duplicate post",
        },
    ]

    report = build_publish_failure_reasons_report(rows, days=7, channel="x", now=NOW)

    assert len(report.items) == 2
    assert all(item.channel == "x" for item in report.items)
    assert report.items[0].reason == "duplicate"
    assert report.items[1].reason == "rate_limit"

    assert len(report.summaries) == 2


def test_summaries_include_counts_by_channel_and_reason():
    """Test that summaries correctly aggregate by channel and reason."""
    rows = [
        {
            "id": 1,
            "content_id": 100,
            "platform": "x",
            "attempted_at": "2026-05-01T10:00:00+00:00",
            "success": 0,
            "error": "401 Unauthorized",
        },
        {
            "id": 2,
            "content_id": 101,
            "platform": "x",
            "attempted_at": "2026-05-01T10:05:00+00:00",
            "success": 0,
            "error": "403 Forbidden",
        },
        {
            "id": 3,
            "content_id": 102,
            "platform": "bluesky",
            "attempted_at": "2026-05-01T10:10:00+00:00",
            "success": 0,
            "error": "Authentication failed",
        },
    ]

    report = build_publish_failure_reasons_report(rows, days=7, now=NOW)

    assert len(report.summaries) == 2
    bluesky_auth = [s for s in report.summaries if s.channel == "bluesky" and s.reason == "auth"][0]
    assert bluesky_auth.failure_count == 1

    x_auth = [s for s in report.summaries if s.channel == "x" and s.reason == "auth"][0]
    assert x_auth.failure_count == 2


def test_unknown_fallback_for_unrecognized_errors():
    """Test that unrecognized errors fall back to 'unknown' reason."""
    rows = [
        {
            "id": 1,
            "content_id": 100,
            "platform": "x",
            "attempted_at": "2026-05-01T10:00:00+00:00",
            "success": 0,
            "error": "Something completely unexpected happened",
        },
        {
            "id": 2,
            "content_id": 101,
            "platform": "bluesky",
            "attempted_at": "2026-05-01T10:05:00+00:00",
            "success": 0,
            "error": "",
        },
    ]

    report = build_publish_failure_reasons_report(rows, days=7, now=NOW)

    assert len(report.items) == 2
    assert all(item.reason == "unknown" for item in report.items)


def test_json_formatter_produces_deterministic_output():
    """Test that JSON output is deterministic and properly structured."""
    rows = [
        {
            "id": 1,
            "content_id": 100,
            "platform": "x",
            "attempted_at": "2026-05-01T10:00:00+00:00",
            "success": 0,
            "error": "Rate limit exceeded",
        },
    ]

    report = build_publish_failure_reasons_report(rows, days=7, now=NOW)
    payload = json.loads(format_publish_failure_reasons_json(report))

    assert payload["artifact_type"] == "publish_failure_reasons"
    assert list(payload.keys()) == sorted(payload.keys())
    assert "filters" in payload
    assert "generated_at" in payload
    assert "items" in payload
    assert "summaries" in payload
    assert "totals" in payload

    assert payload["filters"]["days"] == 7
    assert payload["totals"]["failure_count"] == 1


def test_text_formatter_includes_filters_totals_and_summaries():
    """Test that text output includes all key information."""
    rows = [
        {
            "id": 1,
            "content_id": 100,
            "platform": "x",
            "attempted_at": "2026-05-01T10:00:00+00:00",
            "success": 0,
            "error": "401 Unauthorized",
        },
        {
            "id": 2,
            "content_id": 101,
            "platform": "bluesky",
            "attempted_at": "2026-05-01T10:05:00+00:00",
            "success": 0,
            "error": "Rate limit exceeded",
        },
    ]

    report = build_publish_failure_reasons_report(rows, days=7, now=NOW)
    text = format_publish_failure_reasons_text(report)

    assert "Publish Failure Reasons" in text
    assert "Generated: 2026-05-04T12:00:00+00:00" in text
    assert "Filters: days=7 channel=None" in text
    assert "Totals: failures=2 channels=2 rows=2" in text
    assert "Summary by Channel and Reason:" in text
    assert "- channel=bluesky reason=rate_limit failures=1" in text
    assert "- channel=x reason=auth failures=1" in text
    assert "Failed Items (sample):" in text


def test_sqlite_cli_with_channel_filter(capsys, tmp_path):
    """Test CLI script with database and channel filtering."""
    db_path = tmp_path / "publish.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            queue_id INTEGER,
            content_id INTEGER,
            platform TEXT,
            attempted_at TEXT,
            success INTEGER,
            error TEXT,
            error_category TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO publication_attempts (id, content_id, platform, attempted_at, success, error) VALUES (?, ?, ?, ?, ?, ?)",
        (1, 100, "x", "2026-05-01T10:00:00+00:00", 0, "429 Too Many Requests"),
    )
    conn.execute(
        "INSERT INTO publication_attempts (id, content_id, platform, attempted_at, success, error) VALUES (?, ?, ?, ?, ?, ?)",
        (2, 101, "bluesky", "2026-05-01T10:05:00+00:00", 0, "401 Unauthorized"),
    )
    conn.execute(
        "INSERT INTO publication_attempts (id, content_id, platform, attempted_at, success, error) VALUES (?, ?, ?, ?, ?, ?)",
        (3, 102, "x", "2026-05-01T10:10:00+00:00", 1, None),
    )
    conn.commit()
    conn.close()

    assert publish_failure_reasons_script.main(["--db", str(db_path), "--days", "7"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["items"]) == 2
    assert len(payload["summaries"]) == 2

    assert (
        publish_failure_reasons_script.main(
            ["--db", str(db_path), "--days", "7", "--channel", "x"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["items"]) == 1
    assert payload["items"][0]["channel"] == "x"
    assert payload["items"][0]["reason"] == "rate_limit"

    assert (
        publish_failure_reasons_script.main(
            ["--db", str(db_path), "--days", "7", "--format", "text"]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "Filters: days=7 channel=None" in output
    assert "Totals: failures=2 channels=2 rows=2" in output
    assert "- channel=bluesky reason=auth failures=1" in output
    assert "- channel=x reason=rate_limit failures=1" in output

    assert publish_failure_reasons_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_error_excerpt_truncation():
    """Test that long error messages are properly truncated."""
    long_error = "A" * 300
    rows = [
        {
            "id": 1,
            "content_id": 100,
            "platform": "x",
            "attempted_at": "2026-05-01T10:00:00+00:00",
            "success": 0,
            "error": long_error,
        },
    ]

    report = build_publish_failure_reasons_report(rows, days=7, now=NOW)

    assert len(report.items) == 1
    assert len(report.items[0].error_excerpt) <= 240
    assert report.items[0].error_excerpt.endswith("...")


def test_empty_and_missing_error_fields():
    """Test handling of missing or empty error fields."""
    rows = [
        {
            "id": 1,
            "content_id": 100,
            "platform": "x",
            "attempted_at": "2026-05-01T10:00:00+00:00",
            "success": 0,
            "error": None,
        },
        {
            "id": 2,
            "content_id": 101,
            "platform": "bluesky",
            "attempted_at": "2026-05-01T10:05:00+00:00",
            "success": 0,
        },
    ]

    report = build_publish_failure_reasons_report(rows, days=7, now=NOW)

    assert len(report.items) == 2
    assert all(item.reason == "unknown" for item in report.items)
    assert all(item.error_excerpt == "" for item in report.items)


def test_respects_lookback_window():
    """Test that only failures within the lookback window are included."""
    rows = [
        {
            "id": 1,
            "content_id": 100,
            "platform": "x",
            "attempted_at": "2026-04-01T10:00:00+00:00",
            "success": 0,
            "error": "Old error",
        },
        {
            "id": 2,
            "content_id": 101,
            "platform": "x",
            "attempted_at": "2026-05-01T10:00:00+00:00",
            "success": 0,
            "error": "Recent error",
        },
    ]

    report = build_publish_failure_reasons_report(rows, days=7, now=NOW)

    assert len(report.items) == 1
    assert report.items[0].content_id == 101
