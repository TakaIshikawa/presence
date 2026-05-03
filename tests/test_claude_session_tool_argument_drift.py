"""Tests for Claude session tool argument drift reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_tool_argument_drift import (
    build_claude_session_tool_argument_drift_report,
    format_claude_session_tool_argument_drift_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_tool_argument_drift.py"
)
spec = importlib.util.spec_from_file_location("claude_session_tool_argument_drift_script", SCRIPT_PATH)
claude_session_tool_argument_drift_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_tool_argument_drift_script)


def _event_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
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
    timestamp: str | None = "2026-05-01T10:00:00+00:00",
    tool_name: str = "Bash",
    command: str | None = None,
    metadata: dict | str | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, tool_name, command, metadata)
           VALUES (?, ?, ?, ?, ?)""",
        (session_id, timestamp, tool_name, command, metadata_value),
    )
    conn.commit()


def test_iterable_rows_report_only_distinct_argument_key_set_drift():
    rows = [
        {
            "session_id": "sess-drift",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash", "input": {"command": "pytest"}}},
        },
        {
            "session_id": "sess-drift",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {
                "tool_use": {
                    "name": "Bash",
                    "input": {"command": "pytest", "timeout_ms": 1000},
                }
            },
        },
        {
            "session_id": "sess-stable",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "metadata": {"tool_use": {"name": "Read", "input": {"file_path": "a.py"}}},
        },
        {
            "session_id": "sess-stable",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "metadata": {"tool_use": {"name": "Read", "input": {"file_path": "b.py"}}},
        },
    ]

    report = build_claude_session_tool_argument_drift_report(rows, days=7, now=NOW)
    payload = json.loads(format_claude_session_tool_argument_drift_json(report))

    assert payload["artifact_type"] == "claude_session_tool_argument_drift"
    assert list(payload) == sorted(payload)
    assert len(report.rows) == 1
    assert report.rows[0].session_id == "sess-drift"
    assert report.rows[0].tool_name == "bash"
    assert report.rows[0].call_count == 2
    assert report.rows[0].distinct_argument_key_sets == (
        ("command",),
        ("command", "timeout_ms"),
    )
    assert report.rows[0].representative_argument_keys == ("command", "timeout_ms")


def test_iterable_rows_cover_malformed_metadata_missing_timestamps_and_tool_filter():
    rows = [
        {"session_id": "sess-bad", "timestamp": None, "tool_name": "Bash", "metadata": "{bad json"},
        {
            "session_id": "sess-bash",
            "timestamp": None,
            "tool_name": "Bash",
            "input": {"command": "pytest"},
        },
        {
            "session_id": "sess-bash",
            "timestamp": None,
            "tool_name": "Bash",
            "input": {"command": "pytest", "timeout_ms": 1000},
        },
        {
            "session_id": "sess-read",
            "timestamp": None,
            "tool_name": "Read",
            "input": {"file_path": "README.md"},
        },
        {
            "session_id": "sess-read",
            "timestamp": None,
            "tool_name": "Read",
            "input": {"file_path": "README.md", "limit": 10},
        },
    ]

    report = build_claude_session_tool_argument_drift_report(
        rows,
        days=7,
        tool="Bash",
        now=NOW,
    )

    assert report.totals["malformed_metadata_count"] == 1
    assert report.rows[0].session_id == "sess-bash"
    assert report.rows[0].first_seen_at is None
    assert report.rows[0].last_seen_at is None
    assert {row.tool_name for row in report.rows} == {"bash"}


def test_stable_drift_id_is_independent_of_input_order_for_same_key_sets():
    forward = [
        {"session_id": "sess-id", "tool_name": "Edit", "input": {"file_path": "a.py"}},
        {
            "session_id": "sess-id",
            "tool_name": "Edit",
            "input": {"file_path": "a.py", "old_string": "x", "new_string": "y"},
        },
    ]
    reverse = list(reversed(forward))

    first = build_claude_session_tool_argument_drift_report(forward, now=NOW).rows[0]
    second = build_claude_session_tool_argument_drift_report(reverse, now=NOW).rows[0]

    assert first.drift_id == second.drift_id


def test_sqlite_and_script_print_formatter_shape(capsys, tmp_path):
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
            metadata TEXT
        )"""
    )
    _insert_event(conn, session_id="sess-cli", command="pytest")
    _insert_event(
        conn,
        session_id="sess-cli",
        metadata={"tool_use": {"name": "Bash", "input": {"command": "pytest", "timeout_ms": 1000}}},
    )
    conn.close()

    assert (
        claude_session_tool_argument_drift_script.main(
            ["--db", str(db_path), "--days", "7", "--tool", "bash"]
        )
        == 0
    )
    script_payload = json.loads(capsys.readouterr().out)
    formatter_payload = json.loads(
        format_claude_session_tool_argument_drift_json(
            build_claude_session_tool_argument_drift_report(
                sqlite3.connect(db_path),
                days=7,
                tool="bash",
            )
        )
    )

    assert script_payload["artifact_type"] == formatter_payload["artifact_type"]
    assert script_payload["rows"] == formatter_payload["rows"]
    assert claude_session_tool_argument_drift_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
