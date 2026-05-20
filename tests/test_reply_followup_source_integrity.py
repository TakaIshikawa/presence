"""Tests for reply follow-up source integrity reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from engagement.reply_followup_source_integrity import (
    build_reply_followup_source_integrity_report,
    build_reply_followup_source_integrity_report_from_db,
    format_reply_followup_source_integrity_json,
    format_reply_followup_source_integrity_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_source_integrity.py"
spec = importlib.util.spec_from_file_location("reply_followup_source_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, status TEXT);
           CREATE TABLE proactive_actions (id INTEGER PRIMARY KEY, status TEXT);
           CREATE TABLE reply_followup_reminders (
               id INTEGER PRIMARY KEY,
               source_type TEXT,
               source_id INTEGER,
               source_reply_id INTEGER,
               source_action_id INTEGER,
               status TEXT,
               due_at TEXT,
               completed_at TEXT,
               dismissed_at TEXT
           );"""
    )
    return conn


def test_builder_detects_mismatches_missing_stale_pending_and_terminal_conflicts():
    report = build_reply_followup_source_integrity_report(
        [
            {"id": 1, "source_type": "reply_queue", "source_id": 10, "source_reply_id": 11, "status": "pending", "source_exists": 1},
            {"id": 2, "source_type": "reply_queue", "source_id": 12, "status": "pending", "source_exists": 0},
            {"id": 3, "source_type": "reply_queue", "source_id": 13, "status": "pending", "source_exists": 1, "source_status": "dismissed"},
            {"id": 4, "source_type": "proactive_action", "source_id": 20, "source_action_id": 21, "status": "completed"},
            {"id": 5, "source_type": "reply_queue", "source_id": 14, "status": "pending", "completed_at": "2026-05-02T10:00:00+00:00"},
        ],
        now=NOW,
    )

    assert report["artifact_type"] == "reply_followup_source_integrity"
    assert report["summary"]["by_issue_type"] == {
        "missing_source": 1,
        "source_field_mismatch": 2,
        "stale_pending_source": 1,
        "terminal_timestamp_conflict": 2,
    }


def test_db_loader_joins_reply_queue_and_proactive_actions():
    conn = _conn()
    conn.execute("INSERT INTO reply_queue VALUES (1, 'dismissed')")
    conn.execute("INSERT INTO proactive_actions VALUES (2, 'active')")
    conn.executemany(
        "INSERT INTO reply_followup_reminders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "reply_queue", 1, 1, None, "pending", "2026-05-01T00:00:00+00:00", None, None),
            (2, "proactive_action", 99, None, 99, "pending", "2026-05-01T00:00:00+00:00", None, None),
            (3, "proactive_action", 2, None, 3, "dismissed", "2026-05-01T00:00:00+00:00", None, None),
        ],
    )
    report = build_reply_followup_source_integrity_report_from_db(conn, now=NOW)
    assert report["summary"]["by_issue_type"]["stale_pending_source"] == 1
    assert report["summary"]["by_issue_type"]["missing_source"] == 1
    assert report["summary"]["by_issue_type"]["source_field_mismatch"] == 1


def test_cli_json_text_filters_and_validation(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO reply_followup_reminders VALUES (1, 'reply_queue', 99, 99, NULL, 'pending', '2026-05-01T00:00:00+00:00', NULL, NULL)")
    conn.commit()
    db_path = tmp_path / "followups.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--source-type", "reply_queue", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Follow-up Source Integrity" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_formatters_and_invalid_limit():
    report = build_reply_followup_source_integrity_report([], now=NOW)
    assert json.loads(format_reply_followup_source_integrity_json(report))["artifact_type"] == "reply_followup_source_integrity"
    assert "No reply follow-up source integrity gaps found" in format_reply_followup_source_integrity_text(report)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_reply_followup_source_integrity_report([], limit=0)
