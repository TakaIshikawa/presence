"""Tests for Claude command retry backoff reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_command_retry_backoff import (
    build_claude_command_retry_backoff_report,
    detect_command_retry_backoffs,
    format_claude_command_retry_backoff_json,
    format_claude_command_retry_backoff_text,
)
from ingestion.claude_command_retry_recovery import load_claude_command_events


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_command_retry_backoff.py"
spec = importlib.util.spec_from_file_location("claude_command_retry_backoff_script", SCRIPT_PATH)
claude_command_retry_backoff_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_command_retry_backoff_script)


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
    command: str = "uv run pytest tests/test_widget.py -q",
    output: str | None = "exit code 1",
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, project_path, timestamp, tool_name, status, command, output, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (session_id, project_path, timestamp, tool_name, status, command, output, metadata_value),
    )
    conn.commit()


def test_reports_too_fast_repeated_failed_command_backoff():
    conn = _event_db()
    _insert_event(conn, timestamp="2026-05-01T10:00:00+00:00")
    _insert_event(conn, timestamp="2026-05-01T10:00:30+00:00")
    _insert_event(conn, timestamp="2026-05-01T10:05:00+00:00", status="succeeded", output="ok")

    report = build_claude_command_retry_backoff_report(
        conn,
        days=7,
        min_backoff_seconds=60,
        now=NOW,
    )
    payload = json.loads(format_claude_command_retry_backoff_json(report))
    row = report.rows[0]

    assert payload["artifact_type"] == "claude_command_retry_backoff"
    assert list(payload) == sorted(payload)
    assert row.session_id == "sess-a"
    assert row.elapsed_seconds == 30
    assert row.is_too_fast is True
    assert row.command_signature == "uv run pytest tests/test_widget.py -q"
    assert report.totals["retry_pair_count"] == 1
    assert report.totals["too_fast_count"] == 1
    assert report.totals["rows_scanned"] == 3


def test_uses_normalized_signature_and_only_adjacent_failed_retries():
    rows = [
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "status": "failed",
            "command": "FOO=1 sudo uv run pytest tests/test_a.py",
            "output": "exit code 1",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "Bash",
            "status": "failed",
            "command": "uv run pytest tests/test_a.py",
            "output": "exit code 1",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "tool_name": "Bash",
            "status": "failed",
            "command": "uv run pytest tests/test_b.py",
            "output": "exit code 1",
        },
    ]

    events, malformed = load_claude_command_events(rows)
    backoffs = detect_command_retry_backoffs(events, min_backoff_seconds=60)

    assert malformed == 0
    assert len(backoffs) == 1
    assert backoffs[0].elapsed_seconds == 120
    assert backoffs[0].is_too_fast is False
    assert backoffs[0].command_signature == "uv run pytest tests/test_a.py"


def test_missing_table_and_malformed_metadata_are_reported():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    missing = build_claude_command_retry_backoff_report(empty, now=NOW)
    assert missing.missing_tables == (
        "claude_session_events",
        "claude_tool_events",
        "claude_events",
    )
    assert missing.rows == ()

    report = build_claude_command_retry_backoff_report(
        [
            {
                "session_id": "sess-bad",
                "timestamp": "2026-05-01T10:00:00+00:00",
                "tool_name": "Bash",
                "status": "failed",
                "command": "npm run build",
                "metadata": "{bad json",
            }
        ],
        now=NOW,
    )
    assert report.totals["malformed_metadata_count"] == 1


def test_text_output_and_cli_json_validation(capsys, tmp_path):
    conn = _event_db()
    _insert_event(conn, timestamp="2026-05-01T10:00:00+00:00", command="npm run build")
    _insert_event(conn, timestamp="2026-05-01T10:00:20+00:00", command="npm run build")
    text = format_claude_command_retry_backoff_text(
        build_claude_command_retry_backoff_report(conn, days=7, now=NOW)
    )
    assert "Claude Command Retry Backoff" in text
    assert "retry_pairs=1" in text
    assert "too_fast" in text

    db_path = tmp_path / "retry-backoff.db"
    disk = sqlite3.connect(db_path)
    disk.executescript(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            status TEXT,
            command TEXT,
            output TEXT
        );
        INSERT INTO claude_session_events
            (session_id, timestamp, tool_name, status, command, output)
            VALUES ('sess-cli', '2026-05-01T10:00:00+00:00', 'Bash', 'failed', 'npm test', 'exit code 1');
        INSERT INTO claude_session_events
            (session_id, timestamp, tool_name, status, command, output)
            VALUES ('sess-cli', '2026-05-01T10:00:10+00:00', 'Bash', 'failed', 'npm test', 'exit code 1');
        """
    )
    disk.commit()
    disk.close()

    assert (
        claude_command_retry_backoff_script.main(
            [
                "--db",
                str(db_path),
                "--days",
                "7",
                "--min-backoff-seconds",
                "60",
                "--limit",
                "5",
                "--format",
                "json",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["is_too_fast"] is True
    assert claude_command_retry_backoff_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
