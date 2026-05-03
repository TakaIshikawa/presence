"""Tests for Claude session tool burst reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_tool_bursts import (
    build_claude_session_tool_bursts_report,
    format_claude_session_tool_bursts_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_tool_bursts.py"
spec = importlib.util.spec_from_file_location("claude_session_tool_bursts_script", SCRIPT_PATH)
claude_session_tool_bursts_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_tool_bursts_script)


def _event_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            command_id TEXT,
            metadata TEXT
        )"""
    )
    return conn


def _insert_event(
    conn: sqlite3.Connection,
    *,
    session_id: str = "sess-a",
    timestamp: str = "2026-05-01T10:00:00+00:00",
    tool_name: str = "Bash",
    command_id: str = "cmd-1",
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, tool_name, command_id, metadata)
           VALUES (?, ?, ?, ?, ?)""",
        (session_id, timestamp, tool_name, command_id, metadata_value),
    )
    conn.commit()


def test_iterable_input_groups_tool_events_into_fixed_windows():
    rows = [
        {
            "session_id": "sess-burst",
            "timestamp": f"2026-05-01T10:0{minute}:00+00:00",
            "tool_name": "Bash" if minute < 3 else "Read",
            "command_id": f"cmd-{minute}",
        }
        for minute in range(5)
    ]
    rows.append(
        {
            "session_id": "sess-burst",
            "timestamp": "2026-05-01T10:05:00+00:00",
            "tool_name": "Write",
            "command_id": "cmd-next-window",
        }
    )

    report = build_claude_session_tool_bursts_report(
        rows,
        days=7,
        window_minutes=5,
        min_tools=5,
        now=NOW,
    )
    payload = json.loads(format_claude_session_tool_bursts_json(report))

    assert payload["artifact_type"] == "claude_session_tool_bursts"
    assert list(payload) == sorted(payload)
    assert report.totals["tool_event_count"] == 6
    assert len(report.rows) == 1
    assert report.rows[0].window_start == "2026-05-01T10:00:00+00:00"
    assert report.rows[0].window_end == "2026-05-01T10:05:00+00:00"
    assert report.rows[0].tool_count == 5
    assert report.rows[0].distinct_tool_count == 2
    assert report.rows[0].dominant_tool_name == "bash"
    assert report.rows[0].representative_command_ids == (
        "cmd-0",
        "cmd-1",
        "cmd-2",
        "cmd-3",
        "cmd-4",
    )


def test_sqlite_input_filters_days_and_uses_metadata_tool_name():
    conn = _event_db()
    _insert_event(conn, timestamp="2026-04-01T10:00:00+00:00", command_id="old")
    _insert_event(conn, command_id="cmd-a", metadata={"tool_use": {"name": "Bash"}})
    _insert_event(
        conn,
        timestamp="2026-05-01T10:01:00+00:00",
        tool_name="Read",
        command_id="cmd-b",
    )

    report = build_claude_session_tool_bursts_report(
        conn,
        days=7,
        window_minutes=5,
        min_tools=2,
        now=NOW,
    )

    assert report.source_tables == ("claude_session_events",)
    assert report.totals["rows_scanned"] == 2
    assert report.rows[0].tool_count == 2
    assert report.rows[0].representative_command_ids == ("cmd-a", "cmd-b")


def test_malformed_timestamps_are_counted_and_skipped():
    rows = [
        {"session_id": "bad", "timestamp": "not-a-date", "tool_name": "Bash"},
        {"session_id": "good", "timestamp": "2026-05-01T10:00:00+00:00", "tool_name": "Bash"},
        {"session_id": "good", "timestamp": "2026-05-01T10:01:00+00:00", "tool_name": "Read"},
    ]

    report = build_claude_session_tool_bursts_report(
        rows,
        days=7,
        window_minutes=5,
        min_tools=2,
        now=NOW,
    )

    assert report.totals["malformed_timestamp_count"] == 1
    assert report.rows[0].session_id == "good"


def test_threshold_filtering_excludes_sparse_windows():
    rows = [
        {"session_id": "sparse", "timestamp": "2026-05-01T10:00:00+00:00", "tool_name": "Bash"},
        {"session_id": "sparse", "timestamp": "2026-05-01T10:01:00+00:00", "tool_name": "Read"},
    ]

    report = build_claude_session_tool_bursts_report(
        rows,
        days=7,
        window_minutes=5,
        min_tools=3,
        now=NOW,
    )

    assert report.rows == ()
    assert report.totals["burst_count"] == 0


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
            command_id TEXT,
            metadata TEXT
        )"""
    )
    _insert_event(conn, session_id="sess-cli", command_id="cmd-1")
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:01:00+00:00",
        tool_name="Read",
        command_id="cmd-2",
    )
    conn.close()

    assert (
        claude_session_tool_bursts_script.main(
            ["--db", str(db_path), "--days", "7", "--window-minutes", "5", "--min-tools", "2"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["tool_count"] == 2
    assert claude_session_tool_bursts_script.main(["--min-tools", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
