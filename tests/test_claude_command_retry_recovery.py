"""Tests for Claude command retry recovery reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_command_retry_recovery import (
    build_claude_command_retry_recovery_report,
    detect_command_retry_recoveries,
    format_claude_command_retry_recovery_json,
    format_claude_command_retry_recovery_text,
    group_command_events_by_session,
    load_claude_command_events,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_command_retry_recovery.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_command_retry_recovery_script",
    SCRIPT_PATH,
)
claude_command_retry_recovery_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_command_retry_recovery_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


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
    project_path: str = "/repo",
    timestamp: str = "2026-05-01T10:00:00+00:00",
    tool_name: str = "Bash",
    status: str = "failed",
    command: str | None = "uv run pytest tests/test_widget.py",
    output: str | None = None,
    error_message: str | None = "Command failed with exit code 1",
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
            project_path,
            timestamp,
            tool_name,
            status,
            command,
            output,
            error_message,
            metadata_value,
        ),
    )
    conn.commit()


def test_detects_recovered_failure_in_same_session_with_elapsed_seconds():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-recovered",
        timestamp="2026-05-01T10:00:00+00:00",
        command="uv run pytest tests/test_widget.py -q",
        error_message="Command failed with exit code 1",
    )
    _insert_event(
        conn,
        session_id="sess-recovered",
        timestamp="2026-05-01T10:05:30+00:00",
        status="success",
        command="uv run pytest tests/test_widget.py -q",
        error_message=None,
        output="1 passed",
    )

    report = build_claude_command_retry_recovery_report(
        conn,
        days=7,
        window_minutes=10,
        now=NOW,
    )
    payload = json.loads(format_claude_command_retry_recovery_json(report))
    row = report.rows[0]

    assert payload["artifact_type"] == "claude_command_retry_recovery"
    assert list(payload) == sorted(payload)
    assert row.session_id == "sess-recovered"
    assert row.recovered_command == "uv run pytest tests/test_widget.py -q"
    assert row.elapsed_seconds == 330
    assert row.recovery_category == "same_command_retry"
    assert report.totals["recovered_count"] == 1
    assert report.totals["unrecovered_count"] == 0


def test_unrecovered_failure_is_reported_without_recovery_command():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-unrecovered",
        timestamp="2026-05-01T10:00:00+00:00",
        command="npm run build",
        error_message="Error: Cannot find module vite",
    )

    report = build_claude_command_retry_recovery_report(conn, days=7, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].recovered_command is None
    assert report.rows[0].elapsed_seconds is None
    assert report.rows[0].recovery_category == "unrecovered"
    assert report.totals["unrecovered_count"] == 1


def test_success_in_different_session_does_not_recover_failure():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-fail",
        timestamp="2026-05-01T10:00:00+00:00",
        command="python -m pytest tests/test_api.py",
        error_message="Command failed with exit code 1",
    )
    _insert_event(
        conn,
        session_id="sess-success",
        timestamp="2026-05-01T10:02:00+00:00",
        status="success",
        command="python -m pytest tests/test_api.py",
        error_message=None,
    )

    report = build_claude_command_retry_recovery_report(
        conn,
        days=7,
        window_minutes=10,
        now=NOW,
    )

    assert [(row.session_id, row.recovery_category) for row in report.rows] == [
        ("sess-fail", "unrecovered")
    ]
    assert report.totals["recovered_count"] == 0


def test_success_outside_window_does_not_recover_failure():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-window",
        timestamp="2026-05-01T10:00:00+00:00",
        command="pnpm run test --filter app",
        error_message="Command failed with exit code 1",
    )
    _insert_event(
        conn,
        session_id="sess-window",
        timestamp="2026-05-01T10:31:00+00:00",
        status="success",
        command="pnpm run test --filter app",
        error_message=None,
    )

    report = build_claude_command_retry_recovery_report(
        conn,
        days=7,
        window_minutes=30,
        now=NOW,
    )

    assert report.rows[0].recovered_command is None
    assert report.rows[0].recovery_category == "unrecovered"


def test_metadata_events_group_by_session_and_categorize_command_family():
    rows = [
        {
            "sessionId": "sess-meta",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "is_error": True,
                "tool_use": {"input": {"command": "uv run pytest tests/test_a.py"}},
            },
        },
        {
            "sessionId": "sess-meta",
            "timestamp": "2026-05-01T10:04:00+00:00",
            "metadata": {
                "exit_code": 0,
                "tool_use": {"input": {"command": "python -m pytest tests/test_a.py"}},
            },
        },
    ]

    events, malformed = load_claude_command_events(rows)
    grouped = group_command_events_by_session(events)
    recovery_rows = detect_command_retry_recoveries(
        grouped,
        window=timedelta(minutes=10),
    )

    assert malformed == 0
    assert list(grouped) == ["sess-meta"]
    assert recovery_rows[0].recovery_category == "same_command_family"


def test_text_output_and_cli_support_db_and_format_options(capsys, tmp_path):
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
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:00:00+00:00",
        command="uv run pytest tests/test_cli.py",
    )
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:03:00+00:00",
        status="success",
        command="uv run pytest tests/test_cli.py",
        error_message=None,
    )
    conn.close()

    assert (
        claude_command_retry_recovery_script.main(
            [
                "--db",
                str(db_path),
                "--days",
                "7",
                "--window-minutes",
                "5",
                "--format",
                "json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["elapsed_seconds"] == 180

    assert claude_command_retry_recovery_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    conn = _event_db()
    _insert_event(conn, session_id="sess-text", command="npm run build")
    text = format_claude_command_retry_recovery_text(
        build_claude_command_retry_recovery_report(conn, days=7, now=NOW)
    )
    assert "Claude Command Retry Recovery" in text
    assert "sess-text" in text
