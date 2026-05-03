"""Tests for Claude session command duration outlier reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_command_duration_outliers import (
    build_claude_session_command_duration_outliers_report,
    format_claude_session_command_duration_outliers_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_command_duration_outliers.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_session_command_duration_outliers_script",
    SCRIPT_PATH,
)
claude_session_command_duration_outliers_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_command_duration_outliers_script)


def test_iterable_rows_group_duration_buckets_and_command_prefixes():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "command": "uv run pytest tests/test_a.py",
            "duration_ms": 1_500,
        },
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "command": "uv run pytest tests/test_b.py",
            "duration_ms": 12_000,
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "Read",
            "duration_ms": 100_000,
        },
    ]

    report = build_claude_session_command_duration_outliers_report(
        rows,
        min_duration_ms=1_000,
        now=NOW,
    )
    payload = json.loads(format_claude_session_command_duration_outliers_json(report))

    assert payload["artifact_type"] == "claude_session_command_duration_outliers"
    assert [row.duration_bucket for row in report.rows] == ["10s-59s", "1s-9s"]
    assert {row.command_prefix for row in report.rows} == {"uv run pytest"}
    assert report.totals["command_event_count"] == 2
    assert report.totals["outlier_event_count"] == 2


def test_missing_and_nonnumeric_durations_are_skipped_without_failure():
    rows = [
        {"session_id": "sess-skip", "tool_name": "Bash", "command": "pytest"},
        {
            "session_id": "sess-skip",
            "tool_name": "Bash",
            "command": "pytest",
            "duration_ms": "not-a-number",
        },
        {
            "session_id": "sess-ok",
            "tool_name": "Bash",
            "command": "pytest",
            "duration_seconds": "2.5",
        },
    ]

    report = build_claude_session_command_duration_outliers_report(
        rows,
        min_duration_ms=1,
        now=NOW,
    )

    assert len(report.rows) == 1
    assert report.rows[0].max_duration_ms == 2_500
    assert report.totals["skipped_missing_duration_count"] == 2


def test_top_examples_are_largest_first_and_empty_input_is_stable():
    rows = [
        {
            "session_id": "sess-top",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "command": "npm test -- --runInBand",
            "duration_ms": 10_000,
        },
        {
            "session_id": "sess-top",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "command": "npm test -- --runInBand",
            "duration_ms": 59_000,
        },
        {
            "session_id": "sess-top",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "Bash",
            "command": "npm test -- --runInBand",
            "duration_ms": 20_000,
        },
    ]

    report = build_claude_session_command_duration_outliers_report(
        rows,
        min_duration_ms=1_000,
        now=NOW,
    )
    empty = build_claude_session_command_duration_outliers_report([], now=NOW)

    assert [example.duration_ms for example in report.rows[0].top_examples] == [
        59_000,
        20_000,
        10_000,
    ]
    assert empty.rows == ()
    assert empty.totals["rows_scanned"] == 0


def test_sqlite_and_script_support_json_and_text(capsys, tmp_path):
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
            duration_ms REAL,
            metadata TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, tool_name, command, duration_ms, metadata)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "sess-cli",
            "2026-05-01T10:00:00+00:00",
            "Bash",
            "uv run pytest",
            15_000,
            json.dumps({"result": {"duration_ms": 15_000}}),
        ),
    )
    conn.commit()
    conn.close()

    assert (
        claude_session_command_duration_outliers_script.main(
            ["--db", str(db_path), "--days", "7", "--min-duration-ms", "1000"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["duration_bucket"] == "10s-59s"
    assert (
        claude_session_command_duration_outliers_script.main(
            ["--db", str(db_path), "--format", "text"]
        )
        == 0
    )
    assert "session_id | command_prefix | bucket | count | max_ms" in capsys.readouterr().out
    assert claude_session_command_duration_outliers_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
