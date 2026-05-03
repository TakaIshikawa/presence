"""Tests for Claude tool error taxonomy reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_tool_error_taxonomy import (
    build_claude_tool_error_taxonomy_report,
    classify_error_text,
    format_claude_tool_error_taxonomy_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_tool_error_taxonomy.py"
spec = importlib.util.spec_from_file_location("claude_tool_error_taxonomy_script", SCRIPT_PATH)
claude_tool_error_taxonomy_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_tool_error_taxonomy_script)


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


def test_sqlite_builder_groups_failed_tools_by_error_taxonomy():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:00:00+00:00",
        error_message="Command failed with exit code 1",
    )
    _insert_event(
        conn,
        session_id="sess-b",
        timestamp="2026-05-01T10:01:00+00:00",
        error_message="Command failed with exit code 2",
    )
    _insert_event(
        conn,
        session_id="sess-c",
        timestamp="2026-05-01T10:02:00+00:00",
        tool_name="Read",
        command=None,
        error_message="ENOENT: no such file or directory, open src/missing.py",
    )
    _insert_event(
        conn,
        session_id="sess-ok",
        timestamp="2026-05-01T10:03:00+00:00",
        status="success",
        error_message=None,
    )

    report = build_claude_tool_error_taxonomy_report(conn, days=7, now=NOW)
    payload = json.loads(format_claude_tool_error_taxonomy_json(report))

    assert payload["artifact_type"] == "claude_tool_error_taxonomy"
    assert list(payload) == sorted(payload)
    assert report.totals == {
        "error_event_count": 3,
        "malformed_metadata_count": 0,
        "reported_group_count": 2,
        "rows_scanned": 4,
        "session_count": 3,
    }
    assert [(row.tool_name, row.error_class, row.command_prefix, row.failure_count) for row in report.rows] == [
        ("bash", "command_failed", "uv run pytest", 2),
        ("read", "missing_file", "", 1),
    ]
    assert report.rows[0].representative_session_ids == ("sess-a", "sess-b")
    assert report.rows[0].source_tables == ("claude_session_events",)


def test_error_classifier_covers_common_classes():
    assert classify_error_text("operation timed out after 30s") == "timeout"
    assert classify_error_text("Permission denied while opening file") == "permission_denied"
    assert classify_error_text("Command failed with exit code 1") == "command_failed"
    assert classify_error_text("No such file or directory") == "missing_file"
    assert classify_error_text("JSONDecodeError: invalid json") == "parse_error"
    assert classify_error_text("unexpected tool failure") == "unknown"


def test_row_iterable_input_handles_nested_metadata():
    rows = [
        {
            "sessionId": "sess-meta",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "is_error": True,
                "tool_use": {
                    "name": "Bash",
                    "input": {"command": "npm run build -- --watch"},
                },
                "tool_result": {"error": "Timed out waiting for build"},
            },
        },
        {
            "sessionId": "sess-meta",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {
                "exit_code": 0,
                "tool_use": {"name": "Bash", "input": {"command": "npm run build"}},
            },
        },
    ]

    report = build_claude_tool_error_taxonomy_report(rows, days=7, now=NOW)

    assert report.source_tables == ()
    assert report.rows[0].tool_name == "bash"
    assert report.rows[0].error_class == "timeout"
    assert report.rows[0].command_prefix == "npm run build"
    assert report.totals["rows_scanned"] == 2
    assert report.totals["error_event_count"] == 1


def test_malformed_metadata_is_counted_separately():
    rows = [
        {
            "session_id": "sess-bad",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Read",
            "status": "failed",
            "error_message": "File not found",
            "metadata": "{bad json",
        }
    ]

    report = build_claude_tool_error_taxonomy_report(rows, days=7, now=NOW)

    assert report.totals["malformed_metadata_count"] == 1
    assert report.rows[0].error_class == "missing_file"


def test_missing_source_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_tool_error_taxonomy_report(conn, days=7, now=NOW)

    assert report.rows == ()
    assert report.source_tables == ()
    assert report.missing_tables == ("claude_session_events", "claude_tool_events", "claude_events")
    assert report.totals["rows_scanned"] == 0


def test_tool_filter_is_normalized_and_applied():
    conn = _event_db()
    _insert_event(conn, session_id="sess-bash", tool_name="Bash")
    _insert_event(
        conn,
        session_id="sess-read",
        tool_name="Read",
        command=None,
        error_message="No such file: README.md",
    )

    report = build_claude_tool_error_taxonomy_report(conn, days=7, tool="READ", now=NOW)

    assert report.filters["tool"] == "read"
    assert len(report.rows) == 1
    assert report.rows[0].tool_name == "read"
    assert report.rows[0].session_count == 1


def test_cli_outputs_json_with_db_tool_filter_and_validates_days(capsys, tmp_path):
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
        claude_tool_error_taxonomy_script.main(
            ["--db", str(db_path), "--days", "7", "--tool", "bash", "--format", "json"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["tool_name"] == "bash"
    assert payload["rows"][0]["error_class"] == "command_failed"
    assert claude_tool_error_taxonomy_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
