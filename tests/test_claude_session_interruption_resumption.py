"""Tests for Claude session interruption resumption reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_interruption_resumption import (
    build_claude_session_interruption_resumption_report,
    format_claude_session_interruption_resumption_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_interruption_resumption.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_session_interruption_resumption_script",
    SCRIPT_PATH,
)
claude_session_interruption_resumption_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_interruption_resumption_script)


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
            prompt_text TEXT,
            response_text TEXT
        )"""
    )
    return conn


def _insert_message(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    message_uuid: str,
    timestamp: str,
    prompt_text: str,
    project_path: str = "/repo",
    response_text: str | None = None,
) -> None:
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, project_path, timestamp, prompt_text, response_text)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (session_id, message_uuid, project_path, timestamp, prompt_text, response_text),
    )
    conn.commit()


def test_builder_groups_resumed_and_abandoned_interruptions_from_sqlite():
    conn = _message_db()
    _insert_message(
        conn,
        session_id="sess-interrupted",
        message_uuid="m1",
        timestamp="2026-05-01T10:00:00+00:00",
        prompt_text="Fix ingestion session interruption resumption report tests",
    )
    _insert_message(
        conn,
        session_id="sess-interrupted",
        message_uuid="m2",
        timestamp="2026-05-01T10:10:00+00:00",
        prompt_text="User cancelled the run while fixing ingestion session interruption resumption report tests",
    )
    _insert_message(
        conn,
        session_id="sess-resumed",
        message_uuid="m3",
        timestamp="2026-05-01T10:40:00+00:00",
        prompt_text="Continue fixing ingestion session interruption resumption report tests",
    )
    _insert_message(
        conn,
        session_id="sess-abandoned",
        message_uuid="m4",
        timestamp="2026-05-01T11:00:00+00:00",
        prompt_text="Tool call was aborted while building calendar export workflow",
    )

    report = build_claude_session_interruption_resumption_report(conn, days=7, now=NOW)
    payload = json.loads(format_claude_session_interruption_resumption_json(report))

    assert payload["artifact_type"] == "claude_session_interruption_resumption"
    assert list(payload) == sorted(payload)
    assert report.totals["interrupted"] == 2
    assert report.totals["resumed"] == 1
    assert report.totals["abandoned"] == 1
    assert report.rows == (
        report.rows[0],
    )
    assert report.rows[0].day == "2026-05-01"
    assert report.rows[0].interrupted == 2
    assert report.rows[0].resumed == 1
    assert report.rows[0].abandoned == 1
    assert report.rows[0].median_minutes_to_resume == 30.0


def test_builder_accepts_parsed_session_rows_and_filters_days():
    rows = [
        {
            "session_id": "old",
            "timestamp": "2026-04-01T10:00:00+00:00",
            "project_path": "/repo",
            "prompt_text": "User cancelled stale ingestion report work",
        },
        {
            "session_id": "interrupted",
            "timestamp": "2026-05-01T09:00:00+00:00",
            "project_path": "/repo",
            "prompt_text": "User cancelled flaky command timeout report implementation",
        },
        {
            "session_id": "resumed",
            "timestamp": "2026-05-01T09:15:00+00:00",
            "project_path": "/repo",
            "prompt_text": "Resume flaky command timeout report implementation",
        },
    ]

    report = build_claude_session_interruption_resumption_report(rows, days=7, now=NOW)

    assert report.totals["interrupted"] == 1
    assert report.rows[0].resumed == 1
    assert report.rows[0].median_minutes_to_resume == 15.0


def test_cli_outputs_json_and_validates_days(capsys, tmp_path):
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
            prompt_text TEXT,
            response_text TEXT
        )"""
    )
    _insert_message(
        conn,
        session_id="interrupted",
        message_uuid="m1",
        timestamp="2026-05-01T09:00:00+00:00",
        prompt_text="User cancelled flaky command timeout report implementation",
    )
    conn.close()

    assert (
        claude_session_interruption_resumption_script.main(
            ["--db", str(db_path), "--days", "7"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["interrupted"] == 1
    assert payload["rows"][0]["abandoned"] == 1
    assert claude_session_interruption_resumption_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
