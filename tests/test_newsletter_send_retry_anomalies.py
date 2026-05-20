"""Tests for newsletter send retry anomaly reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.newsletter_send_retry_anomalies import (
    build_newsletter_send_retry_anomalies_report,
    build_newsletter_send_retry_anomalies_report_from_db,
    format_newsletter_send_retry_anomalies_json,
    format_newsletter_send_retry_anomalies_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_send_retry_anomalies.py"
spec = importlib.util.spec_from_file_location("newsletter_send_retry_anomalies_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_send_attempts (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            segment_id TEXT,
            status TEXT,
            error_message TEXT,
            attempted_at TEXT
        )"""
    )
    return conn


def _attempt(conn: sqlite3.Connection, attempt_id: int, issue: str, segment: str, status: str, hours_ago: float, error: str | None = None) -> None:
    conn.execute(
        "INSERT INTO newsletter_send_attempts VALUES (?, ?, ?, ?, ?, ?)",
        (attempt_id, issue, segment, status, error, (NOW - timedelta(hours=hours_ago)).isoformat()),
    )
    conn.commit()


def test_builder_flags_repeated_failures_threshold_and_long_window():
    rows = [
        {"id": 1, "issue_id": "issue-1", "recipient_id": "seg-a", "status": "failed", "error": "timeout", "attempted_at": (NOW - timedelta(hours=30)).isoformat()},
        {"id": 2, "issue_id": "issue-1", "recipient_id": "seg-a", "status": "failed", "error": "smtp", "attempted_at": (NOW - timedelta(hours=20)).isoformat()},
        {"id": 3, "issue_id": "issue-2", "recipient_id": "seg-b", "status": "queued", "attempted_at": (NOW - timedelta(hours=3)).isoformat()},
    ]
    report = build_newsletter_send_retry_anomalies_report(rows, retry_threshold=3, now=NOW)
    assert report["artifact_type"] == "newsletter_send_retry_anomalies"
    assert report["summary"]["finding_count"] == 1
    finding = report["findings"][0]
    assert finding["newsletter_issue_id"] == "issue-1"
    assert finding["recipient_identifier"] == "seg-a"
    assert finding["retry_count"] == 2
    assert finding["latest_status"] == "failed"
    assert finding["latest_error"] == "smtp"
    assert finding["first_attempt_at"] == (NOW - timedelta(hours=30)).isoformat()


def test_db_adapter_reads_schema_tolerant_columns():
    conn = _conn()
    _attempt(conn, 1, "issue-1", "segment-a", "failed", 40, "timeout")
    _attempt(conn, 2, "issue-1", "segment-a", "failed", 10, "rate limit")
    report = build_newsletter_send_retry_anomalies_report_from_db(conn, retry_threshold=3, now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["gap_reasons"] == ["repeated_failed_attempts", "retry_window_exceeds_expected_cadence"]


def test_missing_schema_and_formatters(tmp_path, capsys):
    missing = build_newsletter_send_retry_anomalies_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["newsletter_send_attempts|newsletter_sends"]

    report = build_newsletter_send_retry_anomalies_report(
        [{"id": 1, "issue_id": "i", "status": "failed", "attempted_at": NOW.isoformat()}],
        now=NOW,
    )
    assert json.loads(format_newsletter_send_retry_anomalies_json(report))["artifact_type"] == "newsletter_send_retry_anomalies"
    assert "Newsletter Send Retry Anomalies" in format_newsletter_send_retry_anomalies_text(report)

    db_path = tmp_path / "newsletter.sqlite"
    conn = _conn()
    _attempt(conn, 1, "issue-1", "segment-a", "failed", 2)
    _attempt(conn, 2, "issue-1", "segment-a", "failed", 1)
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--days", "60", "--retry-threshold", "3", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Newsletter Send Retry Anomalies" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--days", "0"])
