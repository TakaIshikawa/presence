"""Tests for stale follow-up promise reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_draft_followup_promise_staleness import (
    build_reply_draft_followup_promise_staleness_report,
    build_reply_draft_followup_promise_staleness_report_from_db,
    format_reply_draft_followup_promise_staleness_json,
    format_reply_draft_followup_promise_staleness_text,
)


NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_draft_followup_promise_staleness.py"
spec = importlib.util.spec_from_file_location("reply_draft_followup_promise_staleness_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_stale_promises_are_flagged_and_resolved_metadata_is_excluded():
    rows = [
        {"id": 1, "platform": "x", "status": "queued", "text": "I'll follow up tomorrow.", "created_at": "2026-04-28T10:00:00+00:00"},
        {"id": 2, "platform": "x", "status": "queued", "text": "I'll check and send it.", "created_at": "2026-04-28T10:00:00+00:00", "metadata": {"followup_status": "completed"}},
        {"id": 3, "platform": "x", "status": "queued", "text": "Thanks!", "created_at": "2026-04-28T10:00:00+00:00"},
    ]
    report = build_reply_draft_followup_promise_staleness_report(rows, stale_hours=24, now=NOW)
    assert report["artifact_type"] == "reply_draft_followup_promise_staleness"
    assert [f["reply_draft_id"] for f in report["findings"]] == [1]
    assert report["findings"][0]["promised_phrase"] == "I'll follow up"
    assert report["findings"][0]["suggested_action"] == "create_or_resolve_followup_action"


def test_db_adapter_optional_followups_formatters_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE reply_drafts (id INTEGER PRIMARY KEY, platform TEXT, status TEXT, content TEXT, metadata TEXT, created_at TEXT);
        CREATE TABLE reply_followups (id INTEGER PRIMARY KEY, reply_draft_id INTEGER, status TEXT, completed_at TEXT);
        """
    )
    conn.execute("INSERT INTO reply_drafts VALUES (1, 'x', 'queued', 'I will circle back with details.', NULL, '2026-04-29T08:00:00+00:00')")
    conn.execute("INSERT INTO reply_drafts VALUES (2, 'x', 'queued', 'I will circle back with details.', NULL, '2026-04-29T08:00:00+00:00')")
    conn.execute("INSERT INTO reply_followups VALUES (1, 2, 'completed', '2026-04-30T08:00:00+00:00')")
    report = build_reply_draft_followup_promise_staleness_report_from_db(conn, stale_hours=24, now=NOW)
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["status"] == "queued"
    assert json.loads(format_reply_draft_followup_promise_staleness_json(report))["artifact_type"] == "reply_draft_followup_promise_staleness"
    assert "Reply Draft Follow-up Promise Staleness" in format_reply_draft_followup_promise_staleness_text(report)

    db_path = tmp_path / "reply.sqlite"
    disk = sqlite3.connect(db_path)
    conn.commit()
    conn.backup(disk)
    disk.close()
    assert script.main(["--db", str(db_path), "--stale-hours", "24"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Draft Follow-up Promise Staleness" in capsys.readouterr().out


def test_missing_schema_and_cli_validation():
    assert build_reply_draft_followup_promise_staleness_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["reply_drafts|reply_queue"]
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, status TEXT)")
    assert build_reply_draft_followup_promise_staleness_report_from_db(conn, now=NOW)["missing_columns"] == {"reply_queue": ["draft_text|content|body|reply_text|text"]}
    with pytest.raises(SystemExit):
        script.parse_args(["--stale-hours", "0"])
