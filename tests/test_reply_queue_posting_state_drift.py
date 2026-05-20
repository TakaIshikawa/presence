"""Tests for reply queue posting state drift reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.reply_queue_posting_state_drift import (
    build_reply_queue_posting_state_drift_report,
    build_reply_queue_posting_state_drift_report_from_db,
    format_reply_queue_posting_state_drift_json,
    format_reply_queue_posting_state_drift_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_queue_posting_state_drift.py"
spec = importlib.util.spec_from_file_location("reply_queue_posting_state_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE reply_queue (
               id INTEGER PRIMARY KEY,
               status TEXT,
               platform TEXT,
               posted_tweet_id TEXT,
               posted_platform_id TEXT,
               posted_at TEXT,
               approved_at TEXT,
               created_at TEXT,
               draft_text TEXT,
               quality_score REAL,
               reviewed_at TEXT,
               review_status TEXT,
               reviewer_id TEXT
           );"""
    )
    return conn


def test_builder_detects_posting_state_drift_and_counts():
    report = build_reply_queue_posting_state_drift_report(
        [
            {"reply_id": 1, "status": "posted", "platform": "x"},
            {"reply_id": 2, "status": "approved", "platform": "bluesky", "approved_at": _ts(30)},
            {"reply_id": 3, "status": "pending", "platform": "x", "draft_text": "Ready", "quality_score": 0.9},
            {"reply_id": 4, "status": "pending", "platform": ""},
            {"reply_id": 5, "status": "pending", "platform": "forum"},
        ],
        sla_hours=24,
        now=NOW,
    )

    assert report["artifact_type"] == "reply_queue_posting_state_drift"
    assert report["summary"]["by_issue_type"] == {
        "approved_stale_without_posting": 1,
        "missing_or_unsupported_platform": 2,
        "pending_high_quality_draft_without_review": 1,
        "posted_missing_posted_at": 1,
        "posted_missing_posted_platform_id": 1,
        "posted_missing_posted_tweet_id": 1,
    }
    assert {"status": "approved", "platform": "bluesky", "issue_type": "approved_stale_without_posting", "count": 1} in report["issue_counts"]


def test_db_loader_uses_configurable_sla_and_platform_validation():
    conn = _conn()
    conn.execute("INSERT INTO reply_queue (id, status, platform, approved_at) VALUES (1, 'approved', 'x', ?)", (_ts(12),))
    conn.execute("INSERT INTO reply_queue (id, status, platform, approved_at) VALUES (2, 'approved', 'x', ?)", (_ts(30),))
    conn.execute("INSERT INTO reply_queue (id, status, platform) VALUES (3, 'pending', 'unknown-site')")

    report = build_reply_queue_posting_state_drift_report_from_db(conn, sla_hours=24, now=NOW)

    assert [item["reply_id"] for item in report["drift_items"]] == [2, 3]
    assert report["drift_items"][0]["age_hours"] == 30.0


def test_missing_table_cli_and_validation(tmp_path, capsys):
    missing = build_reply_queue_posting_state_drift_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["reply_queue"]

    conn = _conn()
    conn.execute("INSERT INTO reply_queue (id, status, platform) VALUES (1, 'posted', 'x')")
    conn.commit()
    db_path = tmp_path / "reply.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["drift_count"] == 3
    assert script.main(["--db", str(db_path), "--format", "text", "--limit", "1"]) == 0
    assert "Reply Queue Posting State Drift" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--sla-hours", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_formatters_and_invalid_thresholds():
    report = build_reply_queue_posting_state_drift_report([], now=NOW)
    assert json.loads(format_reply_queue_posting_state_drift_json(report))["artifact_type"] == "reply_queue_posting_state_drift"
    assert "No reply queue posting state drift found" in format_reply_queue_posting_state_drift_text(report)
    with pytest.raises(ValueError, match="sla_hours must be positive"):
        build_reply_queue_posting_state_drift_report([], sla_hours=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_reply_queue_posting_state_drift_report([], limit=0)
