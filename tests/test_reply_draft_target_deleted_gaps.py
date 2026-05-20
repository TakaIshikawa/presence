"""Tests for reply draft deleted target gap reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_draft_target_deleted_gaps import (
    build_reply_draft_target_deleted_gaps_report,
    build_reply_draft_target_deleted_gaps_report_from_db,
    format_reply_draft_target_deleted_gaps_json,
    format_reply_draft_target_deleted_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_target_deleted_gaps.py"
spec = importlib.util.spec_from_file_location("reply_draft_target_deleted_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_drafts (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            status TEXT,
            target_tweet_id TEXT,
            target_status TEXT,
            target_deleted INTEGER,
            permalink TEXT,
            created_at TEXT,
            updated_at TEXT
        )"""
    )
    return conn


def test_builder_detects_missing_deleted_and_unlinkable_targets():
    rows = [
        {"id": 1, "platform": "x", "status": "queued", "target_identifier": None, "created_at": NOW.isoformat()},
        {"id": 2, "platform": "x", "status": "queued", "target_identifier": "t2", "target_status": "deleted", "created_at": NOW.isoformat()},
        {"id": 3, "platform": "x", "status": "sent", "target_identifier": "t3", "target_status": "deleted"},
    ]
    report = build_reply_draft_target_deleted_gaps_report(rows, status="queued", now=NOW)
    assert report["artifact_type"] == "reply_draft_target_deleted_gaps"
    assert [f["reply_draft_id"] for f in report["findings"]] == [1, 2]
    assert report["findings"][0]["gap_reason"] == "missing_target_identifier"
    assert report["summary"]["by_platform_status"] == [{"platform": "x", "draft_status": "queued", "count": 2}]


def test_db_adapter_and_cli(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO reply_drafts VALUES (1, 'x', 'queued', 'tweet-1', 'deleted', 0, 'https://x.com/a/status/1', ?, ?)", (NOW.isoformat(), NOW.isoformat()))
    conn.execute("INSERT INTO reply_drafts VALUES (2, 'x', 'queued', 'tweet-2', 'ok', 0, 'https://x.com/a/status/2', ?, ?)", (NOW.isoformat(), NOW.isoformat()))
    conn.commit()
    report = build_reply_draft_target_deleted_gaps_report_from_db(conn, status="queued", now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["target_identifier"] == "tweet-1"
    assert json.loads(format_reply_draft_target_deleted_gaps_json(report))["artifact_type"] == "reply_draft_target_deleted_gaps"
    assert "Reply Draft Target Deleted Gaps" in format_reply_draft_target_deleted_gaps_text(report)

    db_path = tmp_path / "reply.sqlite"
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--status", "queued", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Draft Target Deleted Gaps" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])


def test_missing_schema_metadata():
    missing = build_reply_draft_target_deleted_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["reply_drafts|reply_queue"]
