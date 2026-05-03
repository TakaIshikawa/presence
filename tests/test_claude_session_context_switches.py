"""Tests for Claude session context switch reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_context_switches import (
    build_claude_session_context_switches_report,
    format_claude_session_context_switches_json,
    format_claude_session_context_switches_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_context_switches.py"
spec = importlib.util.spec_from_file_location("claude_session_context_switches_script", SCRIPT_PATH)
claude_session_context_switches_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_context_switches_script)


def _message_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            message_uuid TEXT,
            project_path TEXT,
            timestamp TEXT,
            prompt_text TEXT
        )"""
    )
    return conn


def _insert_message(
    conn: sqlite3.Connection,
    *,
    session_id: str = "sess-a",
    message_uuid: str = "msg-a",
    project_path: str = "/repo",
    timestamp: str = "2026-05-01T10:00:00+00:00",
    prompt_text: str = "fix pytest failures in ingestion report",
) -> None:
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, project_path, timestamp, prompt_text)
           VALUES (?, ?, ?, ?, ?)""",
        (session_id, message_uuid, project_path, timestamp, prompt_text),
    )
    conn.commit()


def test_builder_detects_topic_context_switches_from_sqlite():
    conn = _message_db()
    _insert_message(conn, message_uuid="m1")
    _insert_message(
        conn,
        message_uuid="m2",
        timestamp="2026-05-01T10:05:00+00:00",
        prompt_text="design calendar scheduling interface with availability controls",
    )

    report = build_claude_session_context_switches_report(conn, days=7, now=NOW)
    payload = json.loads(format_claude_session_context_switches_json(report))

    assert payload["artifact_type"] == "claude_session_context_switches"
    assert list(payload) == sorted(payload)
    assert report.totals == {
        "messages_scanned": 2,
        "sessions_scanned": 1,
        "switch_count": 1,
        "switch_sessions": 1,
    }
    assert report.rows[0].session_id == "sess-a"
    assert report.rows[0].switch_type == "topic_shift"
    assert report.rows[0].from_message_uuid == "m1"
    assert report.rows[0].to_message_uuid == "m2"


def test_builder_detects_project_path_changes_from_iterable_rows():
    rows = [
        {
            "session_id": "sess-a",
            "message_uuid": "m1",
            "project_path": "/one",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "prompt_text": "continue fixing pytest failures",
        },
        {
            "session_id": "sess-a",
            "message_uuid": "m2",
            "project_path": "/two",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "prompt_text": "continue fixing pytest failures",
        },
    ]

    report = build_claude_session_context_switches_report(rows, days=7, now=NOW)

    assert report.rows[0].switch_type == "project_path_changed"
    assert report.rows[0].switch_score == 1.0


def test_project_filter_limit_and_missing_tables():
    conn = _message_db()
    _insert_message(conn, session_id="keep", message_uuid="k1", project_path="/keep")
    _insert_message(
        conn,
        session_id="keep",
        message_uuid="k2",
        project_path="/keep",
        timestamp="2026-05-01T10:01:00+00:00",
        prompt_text="ship a billing invoice workflow",
    )
    _insert_message(conn, session_id="skip", message_uuid="s1", project_path="/skip")

    report = build_claude_session_context_switches_report(
        conn,
        days=7,
        project_path="/keep",
        limit=1,
        now=NOW,
    )
    missing = build_claude_session_context_switches_report(sqlite3.connect(":memory:"), now=NOW)

    assert report.filters["project_path_filter_applied"] is True
    assert len(report.rows) == 1
    assert report.rows[0].session_id == "keep"
    assert missing.missing_tables == ("claude_messages",)
    assert "Missing tables: claude_messages" in format_claude_session_context_switches_text(missing)


def test_cli_outputs_json_and_validates_input(capsys, tmp_path):
    db_path = tmp_path / "messages.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            message_uuid TEXT,
            project_path TEXT,
            timestamp TEXT,
            prompt_text TEXT
        )"""
    )
    _insert_message(conn, message_uuid="m1")
    _insert_message(
        conn,
        message_uuid="m2",
        timestamp="2026-05-01T10:05:00+00:00",
        prompt_text="design calendar scheduling interface with availability controls",
    )
    conn.close()

    assert (
        claude_session_context_switches_script.main(
            ["--db", str(db_path), "--days", "7", "--format", "json"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["switch_type"] == "topic_shift"
    assert claude_session_context_switches_script.main(["--threshold", "1.2"]) == 2
    assert "value must be greater than 0 and at most 1" in capsys.readouterr().err
