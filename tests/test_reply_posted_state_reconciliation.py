"""Tests for reply posted state reconciliation reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_posted_state_reconciliation import (
    build_reply_posted_state_reconciliation_report,
    build_reply_posted_state_reconciliation_report_from_db,
    format_reply_posted_state_reconciliation_json,
    format_reply_posted_state_reconciliation_text,
)


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_posted_state_reconciliation.py"
spec = importlib.util.spec_from_file_location("reply_posted_state_reconciliation_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            posted_tweet_id TEXT,
            posted_platform_id TEXT,
            posted_at TEXT
        );
        CREATE TABLE reply_review_events (
            id INTEGER PRIMARY KEY,
            reply_queue_id INTEGER,
            event_type TEXT,
            new_status TEXT,
            created_at TEXT
        );
        """
    )
    return conn


def test_builder_flags_all_reconciliation_issue_types():
    report = build_reply_posted_state_reconciliation_report(
        [
            {"reply_queue_id": 1, "status": "posted", "posted_tweet_id": "", "posted_platform_id": "", "posted_at": ""},
            {"reply_queue_id": 2, "status": "approved", "posted_tweet_id": "tweet-2", "posted_platform_id": "x-2", "posted_at": ""},
            {"reply_queue_id": 3, "status": "failed", "posted_tweet_id": "", "posted_platform_id": "", "posted_at": ""},
            {"reply_queue_id": 4, "status": "posted", "posted_tweet_id": "tweet-4", "posted_platform_id": "x-4", "posted_at": "2026-05-20T11:00:00+00:00"},
        ],
        [
            {"event_id": 1, "reply_queue_id": 3, "new_status": "posted", "created_at": "2026-05-20T10:00:00+00:00"},
            {"event_id": 2, "reply_queue_id": 4, "new_status": "failed", "created_at": "2026-05-20T11:00:00+00:00"},
        ],
        now=NOW,
    )
    payload = json.loads(format_reply_posted_state_reconciliation_json(report))

    assert payload["artifact_type"] == "reply_posted_state_reconciliation"
    assert payload["summary"]["by_issue_type"] == {
        "non_posted_with_posted_identifiers": 1,
        "posted_missing_posted_at": 1,
        "posted_missing_posted_platform_id": 1,
        "posted_missing_posted_tweet_id": 1,
        "review_event_status_disagreement": 2,
    }
    flat = [item for group in payload["findings"] for item in group["items"]]
    assert [item["issue_type"] for item in flat] == [
        "posted_missing_posted_tweet_id",
        "posted_missing_posted_platform_id",
        "posted_missing_posted_at",
        "non_posted_with_posted_identifiers",
        "review_event_status_disagreement",
        "review_event_status_disagreement",
    ]
    assert "reply_queue_id | status | issue_type" in format_reply_posted_state_reconciliation_text(report)


def test_from_db_schema_gaps_empty_state_and_limit_ordering():
    missing = build_reply_posted_state_reconciliation_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["reply_queue"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY)")
    gaps = build_reply_posted_state_reconciliation_report_from_db(bad, now=NOW)
    assert gaps["missing_columns"]["reply_queue"] == ["posted_at", "posted_platform_id", "posted_tweet_id", "status"]

    conn = _conn()
    conn.execute("INSERT INTO reply_queue VALUES (1, 'posted', NULL, NULL, NULL)")
    conn.execute("INSERT INTO reply_queue VALUES (2, 'approved', 'tweet-2', NULL, NULL)")
    conn.execute("INSERT INTO reply_queue VALUES (3, 'failed', NULL, NULL, NULL)")
    conn.execute("INSERT INTO reply_review_events VALUES (1, 3, 'reviewed', 'posted', '2026-05-20T10:00:00+00:00')")
    conn.execute("INSERT INTO reply_review_events VALUES (2, 3, 'reviewed', 'failed', '2026-05-20T11:00:00+00:00')")
    report = build_reply_posted_state_reconciliation_report_from_db(conn, limit=2, now=NOW)

    flat = [item for group in report["findings"] for item in group["items"]]
    assert [item["issue_type"] for item in flat] == ["posted_missing_posted_tweet_id", "posted_missing_posted_platform_id"]
    assert report["summary"]["finding_count"] == 4
    assert report["summary"]["shown_count"] == 2

    clean = _conn()
    clean.execute("INSERT INTO reply_queue VALUES (1, 'posted', 'tweet-1', 'x-1', '2026-05-20T10:00:00+00:00')")
    clean_report = build_reply_posted_state_reconciliation_report_from_db(clean, now=NOW)
    assert clean_report["empty_state"]["is_empty"] is True
    assert "No reply posted state reconciliation issues found" in format_reply_posted_state_reconciliation_text(clean_report)


def test_cli_json_text_and_validation(tmp_path, capsys):
    db_path = tmp_path / "reply.sqlite"
    conn = _conn(db_path)
    conn.execute("INSERT INTO reply_queue VALUES (1, 'posted', NULL, NULL, NULL)")
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--limit", "1"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "reply_posted_state_reconciliation"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Posted State Reconciliation" in capsys.readouterr().out
    assert script.main(["--limit", "0"]) == 2
    with pytest.raises(ValueError, match="limit must be positive"):
        build_reply_posted_state_reconciliation_report([], limit=0)
