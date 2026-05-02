"""Tests for newsletter reading-time variance reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_reading_time_variance import (
    build_newsletter_reading_time_variance_report,
    format_newsletter_reading_time_variance_json,
    format_newsletter_reading_time_variance_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_reading_time_variance.py"
spec = importlib.util.spec_from_file_location("newsletter_reading_time_variance_script", SCRIPT_PATH)
newsletter_reading_time_variance_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_reading_time_variance_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _ensure_body_column(db) -> None:
    columns = {row["name"] for row in db.conn.execute("PRAGMA table_info(newsletter_sends)")}
    if "body" not in columns:
        db.conn.execute("ALTER TABLE newsletter_sends ADD COLUMN body TEXT")
        db.conn.commit()


def _send(
    db,
    issue_id: str,
    subject: str,
    body: str,
    *,
    days_ago: int = 0,
    status: str = "sent",
    metadata: dict | None = None,
) -> int:
    _ensure_body_column(db)
    send_id = db.insert_newsletter_send(
        issue_id=issue_id,
        subject=subject,
        content_ids=[101],
        subscriber_count=100,
        status=status,
        metadata=metadata,
    )
    sent_at = NOW - timedelta(days=days_ago)
    db.conn.execute(
        "UPDATE newsletter_sends SET sent_at = ?, body = ? WHERE id = ?",
        (sent_at.isoformat(), body, send_id),
    )
    db.conn.commit()
    return send_id


def _words(prefix: str, count: int) -> str:
    return " ".join(f"{prefix}{index}" for index in range(count))


def test_recent_sent_newsletters_are_ranked_by_absolute_variance(db):
    _send(db, "issue-short", "Short", _words("short", 100), days_ago=1)
    _send(db, "issue-median", "Median", _words("mid", 200), days_ago=2)
    _send(db, "issue-long", "Long", _words("long", 500), days_ago=3)
    _send(db, "issue-draft", "Draft", _words("draft", 900), days_ago=1, status="draft")

    report = build_newsletter_reading_time_variance_report(
        db,
        days=30,
        limit=10,
        min_words_per_minute=100,
        now=NOW,
    )

    assert report.totals == {
        "finding_count": 3,
        "flagged_count": 2,
        "missing_content_count": 0,
        "sends_scanned": 3,
        "sends_with_content": 3,
    }
    assert [finding.issue_id for finding in report.findings] == [
        "issue-long",
        "issue-short",
        "issue-median",
    ]
    assert report.findings[0].warnings == ("unusually_long",)
    assert report.findings[1].warnings == ("unusually_short",)
    assert report.findings[0].median_estimated_read_minutes == 2.0
    assert report.findings[0].ratio_to_median == 2.5


def test_metadata_body_is_used_when_direct_content_columns_are_unavailable():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            subject TEXT,
            status TEXT,
            metadata TEXT,
            sent_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, metadata, sent_at)
           VALUES (1, 'issue-html', 'HTML issue', 'sent', ?, ?)""",
        (
            json.dumps(
                {
                    "payload": {
                        "html": "<h1>Title</h1><p>Read the practical update today.</p>"
                    }
                }
            ),
            NOW.isoformat(),
        ),
    )
    conn.commit()

    report = build_newsletter_reading_time_variance_report(
        conn,
        days=7,
        min_words_per_minute=100,
        now=NOW,
    )

    assert report.missing_columns == {}
    assert report.findings[0].content_source == "newsletter_sends.metadata.payload.html"
    assert report.findings[0].word_count == 6


def test_json_is_sorted_and_text_has_empty_state(db):
    report = build_newsletter_reading_time_variance_report(db, days=7, now=NOW)
    payload = json.loads(format_newsletter_reading_time_variance_json(report))
    text = format_newsletter_reading_time_variance_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "newsletter_reading_time_variance"
    assert payload["filters"]["days"] == 7
    assert "No recent sent newsletters with usable body content found." in text


def test_missing_table_and_missing_content_fields_are_reported():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row

    missing_table = build_newsletter_reading_time_variance_report(empty, now=NOW)
    assert missing_table.missing_tables == ("newsletter_sends",)

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE newsletter_sends (id INTEGER PRIMARY KEY, sent_at TEXT)")
    partial_report = build_newsletter_reading_time_variance_report(partial, now=NOW)

    assert partial_report.missing_columns == {
        "newsletter_sends": ("body|content|html|text|metadata",)
    }


def test_invalid_builder_and_cli_numeric_args_return_expected_errors(db, monkeypatch, capsys):
    with pytest.raises(ValueError, match="days must be positive"):
        build_newsletter_reading_time_variance_report(db, days=0)

    monkeypatch.setattr(
        newsletter_reading_time_variance_script,
        "script_context",
        lambda: _script_context(db),
    )
    assert newsletter_reading_time_variance_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_cli_supports_valid_json_invocation(db, monkeypatch, capsys):
    _send(db, "issue-cli", "CLI issue", _words("cli", 100), days_ago=1)
    monkeypatch.setattr(
        newsletter_reading_time_variance_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = newsletter_reading_time_variance_script.main(
        [
            "--days",
            "30",
            "--limit",
            "5",
            "--min-words-per-minute",
            "100",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["min_words_per_minute"] == 100
    assert payload["findings"][0]["issue_id"] == "issue-cli"
