"""Tests for newsletter issue approval staleness reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.newsletter_issue_approval_staleness import (
    build_newsletter_issue_approval_staleness_report,
    build_newsletter_issue_approval_staleness_report_from_db,
    format_newsletter_issue_approval_staleness_json,
    format_newsletter_issue_approval_staleness_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_issue_approval_staleness.py"
spec = importlib.util.spec_from_file_location("newsletter_issue_approval_staleness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_issues (
            id INTEGER PRIMARY KEY,
            subject TEXT,
            status TEXT,
            review_started_at TEXT,
            scheduled_for TEXT,
            reviewer TEXT,
            approver TEXT
        )"""
    )
    return conn


def test_builder_assigns_warning_and_critical_severity():
    rows = [
        {"issue_id": 1, "subject_or_title": "One", "status": "pending_approval", "started_at": (NOW - timedelta(hours=80)).isoformat(), "reviewer": "r"},
        {"issue_id": 2, "subject_or_title": "Two", "status": "review", "started_at": (NOW - timedelta(hours=30)).isoformat()},
        {"issue_id": 3, "status": "draft", "started_at": (NOW - timedelta(hours=100)).isoformat()},
    ]
    report = build_newsletter_issue_approval_staleness_report(rows, warning_hours=24, critical_hours=72, now=NOW)
    assert report["artifact_type"] == "newsletter_issue_approval_staleness"
    assert [f["severity"] for f in report["findings"]] == ["critical", "warning"]
    assert report["summary"]["by_status_severity"] == [
        {"status": "pending_approval", "severity": "critical", "count": 1},
        {"status": "review", "severity": "warning", "count": 1},
    ]


def test_db_adapter_cli_and_missing_schema(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO newsletter_issues VALUES (1, 'Subject', 'pending_approval', ?, ?, 'reviewer-a', 'approver-b')", ((NOW - timedelta(hours=80)).isoformat(), (NOW + timedelta(days=1)).isoformat()))
    conn.execute("INSERT INTO newsletter_issues VALUES (2, 'Done', 'published', ?, NULL, NULL, NULL)", ((NOW - timedelta(hours=100)).isoformat(),))
    conn.commit()
    report = build_newsletter_issue_approval_staleness_report_from_db(conn, warning_hours=24, critical_hours=72, now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["subject_or_title"] == "Subject"
    assert report["findings"][0]["reviewer"] == "reviewer-a"
    assert json.loads(format_newsletter_issue_approval_staleness_json(report))["artifact_type"] == "newsletter_issue_approval_staleness"
    assert "Newsletter Issue Approval Staleness" in format_newsletter_issue_approval_staleness_text(report)

    db_path = tmp_path / "issues.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--warning-hours", "24", "--critical-hours", "72", "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["summary"]["finding_count"] >= 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Newsletter Issue Approval Staleness" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--critical-hours", "1", "--warning-hours", "2"])

    missing = build_newsletter_issue_approval_staleness_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["newsletter_issues|newsletter_drafts|newsletter_issue_queue"]
