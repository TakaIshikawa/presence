from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_feedback_reopen_rate import (
    build_content_feedback_reopen_rate_report,
    build_content_feedback_reopen_rate_report_from_db,
    format_content_feedback_reopen_rate_json,
    format_content_feedback_reopen_rate_text,
)


NOW = datetime(2026, 5, 24, 12, 0, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "content_feedback_reopen_rate.py"
spec = importlib.util.spec_from_file_location("content_feedback_reopen_rate_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_sequences_reopened_after_resolution():
    report = build_content_feedback_reopen_rate_report(
        [
            {"feedback_id": "f1", "status": "resolved", "reviewer": "ava", "content_type": "blog", "resolution_reason": "fixed", "event_at": "2026-05-20T10:00:00+00:00"},
            {"feedback_id": "f1", "status": "reopened", "reviewer": "ava", "content_type": "blog", "event_at": "2026-05-21T16:00:00+00:00"},
            {"feedback_id": "f2", "status": "resolved", "reviewer": "ava", "content_type": "blog", "resolution_reason": "fixed", "event_at": "2026-05-22T10:00:00+00:00"},
            {"feedback_id": "f3", "status": "open", "reviewer": "ava", "content_type": "blog", "event_at": "2026-05-22T12:00:00+00:00"},
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "content_feedback_reopen_rate"
    assert report["totals"]["resolved_count"] == 2
    assert report["totals"]["reopened_count"] == 1
    assert report["totals"]["reopen_rate"] == 0.5
    assert report["totals"]["median_time_to_reopen_hours"] == 30.0
    assert report["reviewer_breakdown"] == [{"reviewer": "ava", "reopened_count": 1}]
    assert report["findings"][0]["resolution_reason"] == "fixed"


def test_db_loader_uses_optional_events_and_reports_missing_optional_table():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE content_feedback (
            id TEXT, reviewer TEXT, content_type TEXT, status TEXT,
            resolution_reason TEXT, resolved_at TEXT
        );
        INSERT INTO content_feedback VALUES ('f1', 'ava', 'blog', 'resolved', 'fixed', '2026-05-20T10:00:00+00:00');
        """
    )
    report = build_content_feedback_reopen_rate_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["content_feedback_events"]
    assert report["totals"]["resolved_count"] == 1

    conn.execute("CREATE TABLE content_feedback_events (feedback_id TEXT, status TEXT, event_at TEXT, reviewer TEXT, content_type TEXT, resolution_reason TEXT)")
    conn.execute("INSERT INTO content_feedback_events VALUES ('f1', 'reopened', '2026-05-21T10:00:00+00:00', 'ava', 'blog', 'fixed')")
    report = build_content_feedback_reopen_rate_report_from_db(conn, now=NOW)
    assert report["totals"]["reopened_count"] == 1


def test_rendering_and_cli_validation(tmp_path, capsys):
    conn = sqlite3.connect(tmp_path / "feedback.sqlite")
    conn.executescript(
        """
        CREATE TABLE content_feedback (id TEXT, reviewer TEXT, content_type TEXT, status TEXT, resolution_reason TEXT, resolved_at TEXT);
        CREATE TABLE content_feedback_events (feedback_id TEXT, status TEXT, event_at TEXT, reviewer TEXT, content_type TEXT, resolution_reason TEXT);
        INSERT INTO content_feedback VALUES ('f1', 'ava', 'blog', 'resolved', 'fixed', '2026-05-20T10:00:00+00:00');
        INSERT INTO content_feedback_events VALUES ('f1', 'reopened', '2026-05-21T10:00:00+00:00', 'ava', 'blog', 'fixed');
        """
    )
    conn.close()

    report = build_content_feedback_reopen_rate_report([], now=NOW)
    assert json.loads(format_content_feedback_reopen_rate_json(report))["artifact_type"] == "content_feedback_reopen_rate"
    assert "Content Feedback Reopen Rate" in format_content_feedback_reopen_rate_text(report)
    assert script.main(["--db", str(tmp_path / "feedback.sqlite"), "--format", "text", "--window-days", "30", "--min-resolved", "1"]) == 0
    assert "reopen_rate" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
