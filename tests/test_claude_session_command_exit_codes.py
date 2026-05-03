"""Tests for Claude session command exit-code reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_command_exit_codes import (
    build_claude_session_command_exit_codes_report,
    format_claude_session_command_exit_codes_json,
    format_claude_session_command_exit_codes_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "claude_session_command_exit_codes.py"
)
spec = importlib.util.spec_from_file_location("claude_session_command_exit_codes_script", SCRIPT_PATH)
claude_session_command_exit_codes_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_command_exit_codes_script)


def test_numeric_and_string_exit_codes_group_failures_by_day():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "command": "pytest",
            "exit_code": 1,
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:05:00+00:00",
            "metadata": {"tool_use": {"input": {"command": "npm test"}}, "result": {"exit_code": "1"}},
        },
        {
            "session_id": "sess-ok",
            "timestamp": "2026-05-01T10:10:00+00:00",
            "command": "true",
            "exit_code": 0,
        },
    ]

    report = build_claude_session_command_exit_codes_report(rows, days=7, now=NOW)
    payload = json.loads(format_claude_session_command_exit_codes_json(report))

    assert payload["artifact_type"] == "claude_session_command_exit_codes"
    assert list(payload) == sorted(payload)
    assert len(report.rows) == 1
    assert report.rows[0].day == "2026-05-01"
    assert report.rows[0].exit_code == 1
    assert report.rows[0].failure_count == 2
    assert report.rows[0].session_count == 2
    assert report.rows[0].representative_commands == ("pytest", "npm test")


def test_missing_command_text_malformed_metadata_and_exit_code_filter():
    rows = [
        {"session_id": "sess-bad", "timestamp": "2026-05-01T10:00:00+00:00", "metadata": "{bad json"},
        {
            "session_id": "sess-one",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "output": "Command failed with exit code 2",
        },
        {
            "session_id": "sess-two",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "command": "eslint",
            "exit_code": 1,
        },
    ]

    report = build_claude_session_command_exit_codes_report(
        rows,
        days=7,
        exit_code=2,
        now=NOW,
    )

    assert report.totals["malformed_metadata_count"] == 1
    assert len(report.rows) == 1
    assert report.rows[0].exit_code == 2
    assert report.rows[0].representative_commands == ("unknown command",)


def test_text_formatter_includes_filters_totals_tables_and_rows():
    rows = [
        {
            "_source_table": "claude_session_events",
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "command": "pytest",
            "exit_code": 1,
        },
        {
            "_source_table": "claude_session_events",
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:05:00+00:00",
            "command": "npm test",
            "exit_code": 2,
        },
    ]

    report = build_claude_session_command_exit_codes_report(
        rows,
        days=7,
        include_zero=True,
        now=NOW,
    )
    text = format_claude_session_command_exit_codes_text(report)

    assert "Claude Session Command Exit Codes" in text
    assert "Generated: 2026-05-03T12:00:00+00:00" in text
    assert "Filters: days=7 exit_code=None include_zero=True" in text
    assert "Totals: rows=2 command_events=2 groups=2 sessions=2 malformed_metadata=0" in text
    assert "Source tables: claude_session_events" in text
    assert (
        "- day=2026-05-01 exit_code=1 failures=1 sessions=1 commands=pytest"
        in text
    )
    assert (
        "- day=2026-05-01 exit_code=2 failures=1 sessions=1 commands=npm test"
        in text
    )


def test_sqlite_cli_defaults_to_nonzero_and_can_include_zero(capsys, tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            command TEXT,
            exit_code TEXT,
            metadata TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO claude_session_events (session_id, timestamp, command, exit_code, metadata) VALUES (?, ?, ?, ?, ?)",
        ("sess-fail", "2026-05-01T10:00:00+00:00", "pytest", "1", "{}"),
    )
    conn.execute(
        "INSERT INTO claude_session_events (session_id, timestamp, command, exit_code, metadata) VALUES (?, ?, ?, ?, ?)",
        ("sess-ok", "2026-05-01T10:01:00+00:00", "true", "0", "{}"),
    )
    conn.commit()
    conn.close()

    assert claude_session_command_exit_codes_script.main(["--db", str(db_path), "--days", "7"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [row["exit_code"] for row in payload["rows"]] == [1]

    assert (
        claude_session_command_exit_codes_script.main(
            ["--db", str(db_path), "--days", "7", "--include-zero"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert [row["exit_code"] for row in payload["rows"]] == [0, 1]

    assert (
        claude_session_command_exit_codes_script.main(
            ["--db", str(db_path), "--days", "7", "--format", "text"]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "Filters: days=7 exit_code=None include_zero=False" in output
    assert "Totals: rows=2 command_events=1 groups=1 sessions=1 malformed_metadata=0" in output
    assert "- day=2026-05-01 exit_code=1 failures=1 sessions=1 commands=pytest" in output

    assert claude_session_command_exit_codes_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
