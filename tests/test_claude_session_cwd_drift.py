"""Tests for Claude session cwd drift reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_cwd_drift import (
    build_claude_session_cwd_drift_report,
    format_claude_session_cwd_drift_json,
    format_claude_session_cwd_drift_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_cwd_drift.py"
spec = importlib.util.spec_from_file_location("claude_session_cwd_drift_script", SCRIPT_PATH)
claude_session_cwd_drift_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_cwd_drift_script)


def test_no_drift_session_is_not_reported():
    rows = [
        {
            "session_id": "sess-clean",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "command": "uv run pytest",
            "cwd": "/repo",
        },
        {
            "session_id": "sess-clean",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "command": "git status --short",
            "cwd": "/repo",
        },
    ]

    report = build_claude_session_cwd_drift_report(rows, project_root="/repo", now=NOW)
    payload = json.loads(format_claude_session_cwd_drift_json(report))

    assert payload["artifact_type"] == "claude_session_cwd_drift"
    assert list(payload) == sorted(payload)
    assert report.rows == ()
    assert report.totals["command_event_count"] == 2
    assert report.risk_summary["outside_project_root_commands"] == 0


def test_subdirectory_only_drift_is_separate_from_outside_root():
    rows = [
        _event("sess-subdir", "2026-05-01T10:00:00+00:00", "/repo", "pwd"),
        _event("sess-subdir", "2026-05-01T10:01:00+00:00", "/repo/src", "rg foo"),
        _event("sess-subdir", "2026-05-01T10:02:00+00:00", "/repo/tests", "uv run pytest"),
    ]

    report = build_claude_session_cwd_drift_report(rows, project_root="/repo", now=NOW)
    row = report.rows[0]

    assert row.severity == "medium"
    assert row.risk_reason == "repeated_cwd_changes_within_project"
    assert row.cwd_change_count == 2
    assert row.outside_project_root_count == 0
    assert {command.drift_type for command in row.commands} == {"subdirectory_change"}


def test_cross_project_drift_flags_commands_outside_supplied_root():
    rows = [
        _event("sess-cross", "2026-05-01T10:00:00+00:00", "/repo", "git status"),
        _event("sess-cross", "2026-05-01T10:01:00+00:00", "/other/repo", "git add ."),
    ]

    report = build_claude_session_cwd_drift_report(rows, project_root="/repo", now=NOW)
    row = report.rows[0]

    assert row.severity == "high"
    assert row.outside_project_root_count == 1
    assert row.commands[0].cwd == "/other/repo"
    assert row.commands[0].outside_project_root is True
    assert report.risk_summary["outside_project_root_commands"] == 1


def test_multiple_session_aggregation_is_deterministic():
    rows = [
        _event("sess-medium", "2026-05-01T10:00:00+00:00", "/repo", "pwd"),
        _event("sess-medium", "2026-05-01T10:01:00+00:00", "/repo/a", "pwd"),
        _event("sess-medium", "2026-05-01T10:02:00+00:00", "/repo/b", "pwd"),
        _event("sess-high", "2026-05-01T10:00:00+00:00", "/tmp/elsewhere", "rm -rf build"),
        _event("sess-clean", "2026-05-01T10:00:00+00:00", "/repo", "git status"),
    ]

    report = build_claude_session_cwd_drift_report(rows, project_root="/repo", now=NOW)

    assert [row.session_id for row in report.rows] == ["sess-high", "sess-medium"]
    assert report.totals["session_count"] == 3
    assert report.totals["drift_session_count"] == 2
    assert report.risk_summary == {
        "high": 1,
        "medium": 1,
        "outside_project_root_commands": 1,
        "repeated_cwd_change_sessions": 1,
    }


def test_cli_outputs_json_from_sqlite_and_validates_input(capsys, tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            command TEXT,
            cwd TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, tool_name, command, cwd)
           VALUES (?, ?, ?, ?, ?)""",
        ("sess-cli", "2026-05-01T10:00:00+00:00", "Bash", "git status", "/other"),
    )
    conn.commit()
    conn.close()

    assert (
        claude_session_cwd_drift_script.main(
            ["--db", str(db_path), "--project-root", "/repo", "--format", "json"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["outside_project_root_count"] == 1
    assert claude_session_cwd_drift_script.main(["--db", str(db_path)]) == 1
    assert "--project-root is required" in capsys.readouterr().err
    assert "No cwd drift detected." in format_claude_session_cwd_drift_text(
        build_claude_session_cwd_drift_report([], project_root="/repo", now=NOW)
    )


def _event(session_id: str, timestamp: str, cwd: str, command: str) -> dict[str, str]:
    return {
        "session_id": session_id,
        "timestamp": timestamp,
        "tool_name": "Bash",
        "command": command,
        "cwd": cwd,
        "project_root": "/repo",
    }
