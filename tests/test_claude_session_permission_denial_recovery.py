"""Tests for Claude session permission denial recovery reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_permission_denial_recovery import (
    build_claude_session_permission_denial_recovery_report,
    format_claude_session_permission_denial_recovery_json,
    format_claude_session_permission_denial_recovery_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_permission_denial_recovery.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_session_permission_denial_recovery_script",
    SCRIPT_PATH,
)
claude_session_permission_denial_recovery_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_permission_denial_recovery_script)


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
            content TEXT,
            metadata TEXT
        )"""
    )
    return conn


def _insert_event(
    conn: sqlite3.Connection,
    *,
    session_id: str = "sess-a",
    project_path: str = "/repo",
    timestamp: str = "2026-05-01T10:00:00+00:00",
    tool_name: str = "approval",
    status: str | None = "denied",
    command: str | None = None,
    content: str | None = "Denied Bash",
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, project_path, timestamp, tool_name, status, command, content, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (session_id, project_path, timestamp, tool_name, status, command, content, metadata_value),
    )
    conn.commit()


def test_recovered_denial_reports_next_successful_tool_and_elapsed_seconds():
    rows = [
        {
            "session_id": "sess-recovered",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "approval",
            "status": "denied",
            "metadata": {
                "tool_use": {"name": "Bash", "input": {"command": "rm -rf build"}},
                "approval": {"decision": "denied"},
            },
            "content": "Permission denied for Bash",
        },
        {
            "session_id": "sess-recovered",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:02:30+00:00",
            "tool_name": "Bash",
            "status": "success",
            "command": "rm -rf ./build",
            "output": "removed build directory",
        },
    ]

    report = build_claude_session_permission_denial_recovery_report(rows, now=NOW)
    payload = json.loads(format_claude_session_permission_denial_recovery_json(report))
    row = report.rows[0]

    assert payload["artifact_type"] == "claude_session_permission_denial_recovery"
    assert list(payload) == sorted(payload)
    assert row.session_id == "sess-recovered"
    assert row.denied_tool == "bash"
    assert row.denied_command == "rm -rf build"
    assert row.recovery_tool == "bash"
    assert row.recovery_command == "rm -rf ./build"
    assert row.elapsed_seconds == 150
    assert row.recovery_bucket == "recovered"
    assert report.totals["recovered_count"] == 1


def test_same_command_retry_is_bucketed_separately():
    rows = [
        {
            "session_id": "sess-same",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "permission_prompt",
            "status": "cancelled",
            "command": "uv run pytest tests/test_api.py",
            "content": "Permission cancelled for Bash",
        },
        {
            "session_id": "sess-same",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "status": "success",
            "command": "uv run pytest tests/test_api.py",
        },
    ]

    report = build_claude_session_permission_denial_recovery_report(rows, now=NOW)

    assert report.rows[0].recovery_bucket == "retried_same_command"
    assert report.totals["retried_same_command_count"] == 1


def test_no_recovery_is_reported_when_no_later_successful_tool_call_exists():
    rows = [
        {
            "session_id": "sess-stalled",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "approval",
            "status": "denied",
            "content": "Denied Write to src/generated.py",
        },
        {
            "session_id": "sess-stalled",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "status": "failed",
            "command": "cat /root/secret",
            "content": "Permission denied",
        },
    ]

    report = build_claude_session_permission_denial_recovery_report(rows, now=NOW)
    row = report.rows[0]

    assert row.recovery_bucket == "no_recovery"
    assert row.recovery_tool is None
    assert row.elapsed_seconds is None
    assert report.totals["no_recovery_count"] == 1


def test_sessions_without_denied_approvals_are_not_reported():
    rows = [
        {
            "session_id": "sess-approved",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "approval",
            "status": "approved",
            "content": "Approved Bash",
        },
        {
            "session_id": "sess-approved",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "status": "success",
            "command": "uv run pytest",
        },
    ]

    report = build_claude_session_permission_denial_recovery_report(rows, now=NOW)

    assert report.rows == ()
    assert report.totals["denied_approval_count"] == 0


def test_sqlite_and_cli_emit_deterministic_json_and_text(capsys, tmp_path):
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
            content TEXT,
            metadata TEXT
        )"""
    )
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:00:00+00:00",
        command="npm run build",
        content="Denied Bash",
    )
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:03:00+00:00",
        tool_name="Bash",
        status="success",
        command="npm run build -- --dry-run",
        content=None,
    )
    conn.close()

    assert (
        claude_session_permission_denial_recovery_script.main(
            ["--db", str(db_path), "--limit", "5"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["elapsed_seconds"] == 180

    assert (
        claude_session_permission_denial_recovery_script.main(
            ["--db", str(db_path), "--format", "text"]
        )
        == 0
    )
    assert "Claude Session Permission Denial Recovery" in capsys.readouterr().out
    assert claude_session_permission_denial_recovery_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    text = format_claude_session_permission_denial_recovery_text(
        build_claude_session_permission_denial_recovery_report(sqlite3.connect(db_path), now=NOW)
    )
    assert "sess-cli" in text
