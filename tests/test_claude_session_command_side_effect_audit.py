"""Tests for Claude session command side-effect audit reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_command_side_effect_audit import (
    build_claude_session_command_side_effect_audit_report,
    format_claude_session_command_side_effect_audit_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_command_side_effects.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_session_command_side_effects_script",
    SCRIPT_PATH,
)
claude_session_command_side_effects_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_command_side_effects_script)


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
            command TEXT,
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
    command: str | None = "rm -rf build",
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, project_path, timestamp, tool_name, command, metadata)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (session_id, project_path, timestamp, tool_name, command, metadata_value),
    )
    conn.commit()


def test_side_effect_commands_and_write_tools_are_reported():
    rows = [
        {
            "session_id": "sess-risk",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "command": "pytest tests/test_widget.py",
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

    report = build_claude_session_command_side_effect_audit_report(rows, now=NOW)
    payload = json.loads(format_claude_session_command_side_effect_audit_json(report))

    assert payload["artifact_type"] == "claude_session_command_side_effect_audit"
    assert list(payload) == sorted(payload)
    assert [row.command for row in report.rows] == ["rm -rf build", "src/generated.py"]
    assert [row.severity for row in report.rows] == ["high", "high"]
    assert report.totals["flagged_event_count"] == 2


def test_sqlite_rows_and_mapping_rows_match_core_shape():
    rows = [
        {
            "session_id": "sess-same",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "command": "git add src/app.py",
        }
    ]
    conn = _event_db()
    for row in rows:
        _insert_event(conn, **row)

    row_report = build_claude_session_command_side_effect_audit_report(rows, now=NOW)
    db_report = build_claude_session_command_side_effect_audit_report(conn, now=NOW)
    row_payload = row_report.rows[0].to_dict()
    db_payload = db_report.rows[0].to_dict()
    row_payload.pop("source_table")
    db_payload.pop("source_table")

    assert row_payload == db_payload


def test_missing_schema_is_reported_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_session_command_side_effect_audit_report(conn, now=NOW)

    assert report.missing_tables == (
        "claude_session_events",
        "claude_tool_events",
        "claude_events",
    )
    assert report.rows == ()


def test_cli_outputs_json_and_validates_limit(capsys, tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            command TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, tool_name, command)
           VALUES (?, ?, ?, ?)""",
        ("sess-cli", "2026-05-01T10:00:00+00:00", "Bash", "touch output.txt"),
    )
    conn.commit()
    conn.close()

    assert claude_session_command_side_effects_script.main(["--db", str(db_path)]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["severity"] == "medium"
    assert claude_session_command_side_effects_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
