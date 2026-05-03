"""Tests for Claude session approval decision audit reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_session_approval_decision_audit import (
    build_claude_session_approval_decision_audit_report,
    format_claude_session_approval_decision_audit_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_approval_decisions.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_session_approval_decisions_script",
    SCRIPT_PATH,
)
claude_session_approval_decisions_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_approval_decisions_script)


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
    status: str = "denied",
    command: str | None = None,
    content: str | None = "Allow Bash command?",
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


def test_denied_approval_followed_by_bash_and_write_is_flagged():
    rows = [
        {
            "session_id": "sess-risk",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "approval",
            "status": "denied",
            "content": "User denied permission to run rm -rf build",
        },
        {
            "session_id": "sess-risk",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "command": "rm -rf build",
        },
        {
            "session_id": "sess-risk",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "Write",
            "command": "src/generated.py",
        },
    ]

    report = build_claude_session_approval_decision_audit_report(rows, now=NOW)
    payload = json.loads(format_claude_session_approval_decision_audit_json(report))
    row = report.rows[0]

    assert payload["artifact_type"] == "claude_session_approval_decision_audit"
    assert list(payload) == sorted(payload)
    assert row.session_id == "sess-risk"
    assert row.approval_decision == "denied"
    assert row.approval_at == "2026-05-01T10:00:00+00:00"
    assert row.first_follow_up_at == "2026-05-01T10:01:00+00:00"
    assert row.last_follow_up_at == "2026-05-01T10:02:00+00:00"
    assert row.severity == "high"
    assert row.follow_up_count == 2
    assert row.follow_up_evidence[0].summary == "rm -rf build"
    assert report.totals["flagged_approval_count"] == 1


def test_rows_and_sqlite_input_produce_same_flag_shape():
    rows = [
        {
            "session_id": "sess-same",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "permission_prompt",
            "status": "cancelled",
            "content": "Permission cancelled for Write",
        },
        {
            "session_id": "sess-same",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Edit",
            "status": "success",
            "command": "tests/test_example.py",
            "content": None,
        },
    ]
    conn = _event_db()
    for row in rows:
        _insert_event(conn, **row)

    row_report = build_claude_session_approval_decision_audit_report(rows, now=NOW)
    db_report = build_claude_session_approval_decision_audit_report(conn, now=NOW)

    row_payload = row_report.rows[0].to_dict()
    db_payload = db_report.rows[0].to_dict()
    row_payload.pop("source_tables")
    db_payload.pop("source_tables")
    row_payload["follow_up_evidence"][0].pop("source_table")
    db_payload["follow_up_evidence"][0].pop("source_table")
    assert row_payload == db_payload
    assert row_report.totals["flagged_approval_count"] == db_report.totals["flagged_approval_count"]


def test_approved_or_unfollowed_blocked_approvals_are_not_flagged():
    rows = [
        {
            "session_id": "sess-approved",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "approval",
            "status": "approved",
            "content": "Approved Bash command",
        },
        {
            "session_id": "sess-approved",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "command": "uv run pytest",
        },
        {
            "session_id": "sess-denied",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "approval",
            "status": "denied",
            "content": "Denied Write",
        },
        {
            "session_id": "sess-denied",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "tool_name": "Read",
            "content": "README.md",
        },
    ]

    report = build_claude_session_approval_decision_audit_report(rows, now=NOW)

    assert report.rows == ()
    assert report.totals["approved_approval_count"] == 1
    assert report.totals["blocked_approval_count"] == 1


def test_window_size_limits_follow_up_detection():
    rows = [
        {
            "session_id": "sess-window",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "approval",
            "status": "denied",
            "content": "Denied Bash",
        },
        {
            "session_id": "sess-window",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Read",
            "content": "README.md",
        },
        {
            "session_id": "sess-window",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "Bash",
            "command": "npm test",
        },
    ]

    short = build_claude_session_approval_decision_audit_report(rows, window_size=1, now=NOW)
    long = build_claude_session_approval_decision_audit_report(rows, window_size=2, now=NOW)

    assert short.rows == ()
    assert len(long.rows) == 1
    assert long.rows[0].follow_up_evidence[0].event_offset == 2


def test_missing_tables_and_optional_columns_are_reported_without_crashing():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    missing_table_report = build_claude_session_approval_decision_audit_report(empty, now=NOW)
    assert missing_table_report.missing_tables == (
        "claude_session_events",
        "claude_tool_events",
        "claude_events",
    )
    assert missing_table_report.rows == ()

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE claude_session_events (session_id TEXT, timestamp TEXT)")
    partial.execute(
        "INSERT INTO claude_session_events (session_id, timestamp) VALUES (?, ?)",
        ("sess-partial", "2026-05-01T10:00:00+00:00"),
    )
    partial.commit()
    partial_report = build_claude_session_approval_decision_audit_report(partial, now=NOW)

    assert partial_report.missing_columns == {
        "claude_session_events": ("metadata", "status_or_decision", "tool")
    }
    assert partial_report.totals["rows_scanned"] == 1


def test_cli_outputs_json_and_validates_arguments(capsys, tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            status TEXT,
            command TEXT,
            content TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, tool_name, status, command, content)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "sess-cli",
            "2026-05-01T10:00:00+00:00",
            "approval",
            "denied",
            None,
            "Denied Bash",
        ),
    )
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, tool_name, status, command, content)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "sess-cli",
            "2026-05-01T10:01:00+00:00",
            "Bash",
            "success",
            "uv run pytest",
            None,
        ),
    )
    conn.commit()
    conn.close()

    assert (
        claude_session_approval_decisions_script.main(
            ["--db", str(db_path), "--limit", "5", "--window-size", "2"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["follow_up_evidence"][0]["summary"] == "uv run pytest"
    assert claude_session_approval_decisions_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
