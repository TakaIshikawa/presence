"""Tests for Claude session tool timeout reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_tool_timeout_report import (
    build_claude_session_tool_timeout_report,
    format_claude_session_tool_timeout_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_tool_timeout_report.py"
)
spec = importlib.util.spec_from_file_location("claude_session_tool_timeout_report_script", SCRIPT_PATH)
claude_session_tool_timeout_report_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_tool_timeout_report_script)


def _event_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            project_path TEXT,
            timestamp TEXT,
            tool_name TEXT,
            status TEXT,
            command TEXT,
            output TEXT,
            error_message TEXT,
            metadata TEXT
        )"""
    )
    return conn


def _insert_event(
    conn: sqlite3.Connection,
    *,
    session_id: str = "sess-a",
    timestamp: str = "2026-05-01T10:00:00+00:00",
    tool_name: str = "Bash",
    status: str = "failed",
    command: str | None = "uv run pytest tests/test_widget.py",
    error_message: str | None = "Command timed out after 30s",
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, project_path, timestamp, tool_name, status, command,
            output, error_message, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            session_id,
            "/repo",
            timestamp,
            tool_name,
            status,
            command,
            None,
            error_message,
            metadata_value,
        ),
    )
    conn.commit()


def test_sqlite_builder_groups_timeouts_by_day_and_tool():
    conn = _event_db()
    _insert_event(conn, session_id="sess-a", timestamp="2026-05-01T10:00:00+00:00")
    _insert_event(conn, session_id="sess-b", timestamp="2026-05-01T10:05:00+00:00")
    _insert_event(
        conn,
        session_id="sess-read",
        timestamp="2026-05-01T10:10:00+00:00",
        tool_name="Read",
        command=None,
        error_message="deadline exceeded while reading file",
    )
    _insert_event(
        conn,
        session_id="sess-other",
        timestamp="2026-05-01T10:15:00+00:00",
        error_message="Command failed with exit code 1",
    )

    report = build_claude_session_tool_timeout_report(conn, days=7, now=NOW)
    payload = json.loads(format_claude_session_tool_timeout_json(report))

    assert payload["artifact_type"] == "claude_session_tool_timeout_report"
    assert list(payload) == sorted(payload)
    assert report.totals == {
        "malformed_metadata_count": 0,
        "rows_scanned": 4,
        "session_count": 3,
        "timeout_count": 3,
        "tool_count": 2,
    }
    assert [(row.day, row.tool_name, row.timeout_count) for row in report.rows] == [
        ("2026-05-01", "bash", 2),
        ("2026-05-01", "read", 1),
    ]


def test_row_iterable_input_filters_days_and_nested_metadata():
    rows = [
        {
            "sessionId": "old",
            "timestamp": "2026-04-01T10:00:00+00:00",
            "metadata": {
                "is_error": True,
                "tool_use": {"name": "Bash", "input": {"command": "npm run build"}},
                "tool_result": {"error": "Timed out waiting for build"},
            },
        },
        {
            "sessionId": "new",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "is_error": True,
                "tool_use": {"name": "Bash", "input": {"command": "npm run build"}},
                "tool_result": {"error": "Timed out waiting for build"},
            },
        },
    ]

    report = build_claude_session_tool_timeout_report(rows, days=7, now=NOW)

    assert report.totals["rows_scanned"] == 1
    assert report.totals["timeout_count"] == 1
    assert report.rows[0].representative_session_ids == ("new",)


def test_cli_outputs_json_with_tool_filter_and_validates_days(capsys, tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            project_path TEXT,
            timestamp TEXT,
            tool_name TEXT,
            status TEXT,
            command TEXT,
            output TEXT,
            error_message TEXT,
            metadata TEXT
        )"""
    )
    _insert_event(conn, session_id="sess-cli", tool_name="Bash")
    conn.close()

    assert (
        claude_session_tool_timeout_report_script.main(
            ["--db", str(db_path), "--days", "7", "--tool", "bash"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["tool_name"] == "bash"
    assert payload["rows"][0]["timeout_count"] == 1
    assert claude_session_tool_timeout_report_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
