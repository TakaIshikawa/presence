"""Tests for Claude command retry effectiveness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_command_retry_effectiveness import (
    build_claude_command_retry_effectiveness_report,
    classify_command_retry_groups,
    format_claude_command_retry_effectiveness_json,
    group_command_events_by_session_and_signature,
    normalize_command_signature,
)
from ingestion.claude_command_retry_recovery import load_claude_command_events


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_command_retry_effectiveness.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_command_retry_effectiveness_script",
    SCRIPT_PATH,
)
claude_command_retry_effectiveness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_command_retry_effectiveness_script)


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


def test_recovered_retry_group_has_later_matching_success():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-recovered",
        timestamp="2026-05-01T10:00:00+00:00",
        command="uv run pytest tests/test_widget.py -q",
    )
    _insert_event(
        conn,
        session_id="sess-recovered",
        timestamp="2026-05-01T10:05:00+00:00",
        status="success",
        command="uv run pytest tests/test_widget.py -q",
        error_message=None,
        output="1 passed",
    )

    report = build_claude_command_retry_effectiveness_report(conn, days=7, now=NOW)
    payload = json.loads(format_claude_command_retry_effectiveness_json(report))
    row = report.rows[0]

    assert payload["artifact_type"] == "claude_command_retry_effectiveness"
    assert list(payload) == sorted(payload)
    assert row.session_id == "sess-recovered"
    assert row.command_signature == "uv run pytest tests/test_widget.py -q"
    assert row.retry_outcome == "recovered"
    assert row.attempt_count == 2
    assert row.failure_count == 1
    assert row.success_count == 1
    assert row.recovered_failure_count == 1
    assert row.unresolved_failure_count == 0
    assert report.totals["recovered_group_count"] == 1


def test_unresolved_retry_group_has_failures_without_later_success():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-unresolved",
        timestamp="2026-05-01T10:00:00+00:00",
        command="npm run build",
        error_message="Error: Cannot find module vite",
    )
    _insert_event(
        conn,
        session_id="sess-unresolved",
        timestamp="2026-05-01T10:03:00+00:00",
        command="npm run build",
        error_message="Error: Cannot find module vite",
    )

    report = build_claude_command_retry_effectiveness_report(conn, days=7, now=NOW)
    row = report.rows[0]

    assert row.retry_outcome == "unresolved"
    assert row.attempt_count == 2
    assert row.failure_count == 2
    assert row.success_count == 0
    assert row.recovered_failure_count == 0
    assert row.unresolved_failure_count == 2
    assert report.totals["unresolved_group_count"] == 1


def test_flaky_retry_group_has_success_then_later_failure_in_same_signature():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-flaky",
        timestamp="2026-05-01T10:00:00+00:00",
        command="python -m pytest tests/test_api.py",
    )
    _insert_event(
        conn,
        session_id="sess-flaky",
        timestamp="2026-05-01T10:02:00+00:00",
        status="success",
        command="python -m pytest tests/test_api.py",
        error_message=None,
    )
    _insert_event(
        conn,
        session_id="sess-flaky",
        timestamp="2026-05-01T10:05:00+00:00",
        command="python -m pytest tests/test_api.py",
    )

    report = build_claude_command_retry_effectiveness_report(conn, days=7, now=NOW)
    payload = json.loads(format_claude_command_retry_effectiveness_json(report))
    row = payload["rows"][0]

    assert row["retry_outcome"] == "flaky"
    assert row["failure_count"] == 2
    assert row["success_count"] == 1
    assert row["recovered_failure_count"] == 1
    assert row["unresolved_failure_count"] == 1
    assert report.totals["flaky_group_count"] == 1


def test_single_success_history_is_counted_but_not_reported_as_retry_group():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-success",
        timestamp="2026-05-01T10:00:00+00:00",
        status="success",
        command="ruff check .",
        error_message=None,
        output="All checks passed",
    )

    report = build_claude_command_retry_effectiveness_report(conn, days=7, now=NOW)

    assert report.rows == ()
    assert report.totals["command_group_count"] == 1
    assert report.totals["single_success_group_count"] == 1
    assert report.totals["reported_group_count"] == 0


def test_groups_by_session_and_full_command_signature_not_prefix_only():
    rows = [
        {
            "sessionId": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "is_error": True,
                "tool_use": {"input": {"command": "uv run pytest tests/test_a.py"}},
            },
        },
        {
            "sessionId": "sess-a",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {
                "exit_code": 0,
                "tool_use": {"input": {"command": "uv run pytest tests/test_a.py"}},
            },
        },
        {
            "sessionId": "sess-a",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "metadata": {
                "is_error": True,
                "tool_use": {"input": {"command": "uv run pytest tests/test_b.py"}},
            },
        },
        {
            "sessionId": "sess-b",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "metadata": {
                "exit_code": 0,
                "tool_use": {"input": {"command": "uv run pytest tests/test_a.py"}},
            },
        },
    ]

    events, malformed = load_claude_command_events(rows)
    grouped = group_command_events_by_session_and_signature(events)
    retry_rows, single_success_count = classify_command_retry_groups(grouped)

    assert malformed == 0
    assert sorted(grouped) == [
        ("sess-a", "uv run pytest tests/test_a.py"),
        ("sess-a", "uv run pytest tests/test_b.py"),
        ("sess-b", "uv run pytest tests/test_a.py"),
    ]
    assert [(row.session_id, row.command_signature, row.retry_outcome) for row in retry_rows] == [
        ("sess-a", "uv run pytest tests/test_a.py", "recovered"),
        ("sess-a", "uv run pytest tests/test_b.py", "unresolved"),
    ]
    assert single_success_count == 1


def test_cli_outputs_json_with_db_and_validates_days(capsys, tmp_path):
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
        timestamp="2026-05-01T10:02:00+00:00",
        status="success",
        command="uv run pytest tests/test_cli.py",
        error_message=None,
    )
    conn.close()

    assert claude_command_retry_effectiveness_script.main(["--db", str(db_path), "--days", "7"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["retry_outcome"] == "recovered"
    assert claude_command_retry_effectiveness_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_command_signature_normalization_handles_common_wrappers():
    assert (
        normalize_command_signature("FOO=1 sudo uv run pytest tests/test_a.py -q")
        == "uv run pytest tests/test_a.py -q"
    )
