"""Tests for reply queue duplicate target reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_queue_duplicate_target_report import (
    build_reply_queue_duplicate_target_report,
    build_reply_queue_duplicate_target_report_from_db,
    format_reply_queue_duplicate_target_report_json,
    format_reply_queue_duplicate_target_report_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_queue_duplicate_target_report.py"
spec = importlib.util.spec_from_file_location("reply_queue_duplicate_target_report_script", SCRIPT_PATH)
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
            platform TEXT,
            status TEXT,
            draft_text TEXT,
            detected_at TEXT,
            inbound_tweet_id TEXT,
            inbound_url TEXT,
            inbound_cid TEXT,
            platform_metadata TEXT
        );
        """
    )
    return conn


def _insert(conn: sqlite3.Connection, reply_id: int, *, target: str | None, draft: str, status: str = "pending", platform: str = "x", detected_at: str = "2026-05-20T10:00:00+00:00") -> None:
    conn.execute(
        """INSERT INTO reply_queue
           (id, platform, status, draft_text, detected_at, inbound_tweet_id, inbound_url, inbound_cid, platform_metadata)
           VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL)""",
        (reply_id, platform, status, draft, detected_at, target),
    )
    conn.commit()


def test_builder_finds_duplicate_pending_pending_after_sent_conflicts_and_missing_target():
    report = build_reply_queue_duplicate_target_report(
        [
            {"id": 1, "platform": "x", "status": "pending", "draft_text": "First reply", "inbound_tweet_id": "p1"},
            {"id": 2, "platform": "x", "status": "pending", "draft_text": "Different reply", "inbound_tweet_id": "p1"},
            {"id": 3, "platform": "x", "status": "sent", "draft_text": "Already sent", "inbound_tweet_id": "p1"},
            {"id": 4, "platform": "bluesky", "status": "pending", "draft_text": "No target"},
            {"id": 5, "platform": "x", "status": "pending", "draft_text": "Clean", "inbound_tweet_id": "p2"},
        ],
        now=NOW,
    )
    payload = json.loads(format_reply_queue_duplicate_target_report_json(report))

    assert payload["artifact_type"] == "reply_queue_duplicate_target_report"
    assert payload["summary"]["by_issue_type"] == {
        "conflicting_draft_text": 1,
        "duplicate_pending_drafts": 1,
        "missing_target_metadata": 1,
        "pending_after_sent": 1,
    }
    duplicate = next(group for group in payload["groups"] if group["target_key"] == "post:p1")
    assert duplicate["platform"] == "x"
    assert duplicate["reply_ids"] == [1, 2, 3]
    assert {finding["issue_type"] for finding in duplicate["findings"]} == {
        "duplicate_pending_drafts",
        "pending_after_sent",
        "conflicting_draft_text",
    }
    assert payload["summary"]["sample_reply_ids"][:4] == [4, 1, 2, 3]


def test_db_loader_platform_filter_include_resolved_and_limit():
    conn = _conn()
    _insert(conn, 1, target="same", draft="Draft one", status="pending", platform="x")
    _insert(conn, 2, target="same", draft="Draft two", status="pending", platform="x")
    _insert(conn, 3, target="same", draft="Resolved draft", status="dismissed", platform="x")
    _insert(conn, 4, target="same", draft="Bluesky draft", status="pending", platform="bluesky")

    report = build_reply_queue_duplicate_target_report_from_db(conn, platform="x", include_resolved=False, limit=1, now=NOW)
    assert report["summary"]["scanned_count"] == 2
    assert len(report["groups"]) == 1
    assert report["groups"][0]["reply_ids"] == [1, 2]

    with_resolved = build_reply_queue_duplicate_target_report_from_db(conn, platform="x", include_resolved=True, now=NOW)
    assert with_resolved["summary"]["scanned_count"] == 3
    assert with_resolved["groups"][0]["reply_ids"] == [1, 2, 3]


def test_empty_and_missing_schema_outputs():
    report = build_reply_queue_duplicate_target_report([], now=NOW)
    assert report["empty_state"]["is_empty"] is True
    assert "No reply queue duplicate target issues found." in format_reply_queue_duplicate_target_report_text(report)

    missing_table = build_reply_queue_duplicate_target_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table["missing_tables"] == ["reply_queue"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY)")
    missing_columns = build_reply_queue_duplicate_target_report_from_db(bad, now=NOW)
    assert missing_columns["missing_columns"] == {"reply_queue": ["draft_text", "inbound_tweet_id|inbound_url|inbound_cid|conversation_id|platform_metadata"]}


def test_cli_supports_database_path_format_platform_include_resolved_and_limit(tmp_path, capsys):
    db_path = tmp_path / "reply.sqlite"
    conn = _conn(db_path)
    _insert(conn, 1, target="same", draft="Draft one", status="pending", platform="x")
    _insert(conn, 2, target="same", draft="Draft two", status="dismissed", platform="x")
    conn.close()

    assert script.main(["--db", str(db_path), "--platform", "x", "--include-resolved", "--limit", "5", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {"include_resolved": True, "limit": 5, "platform": "x"}
    assert payload["groups"][0]["reply_ids"] == [1, 2]

    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Queue Duplicate Target Report" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
