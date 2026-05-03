"""Tests for Claude session idle gap reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_idle_gaps import (
    build_claude_session_idle_gaps_report,
    format_claude_session_idle_gaps_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_idle_gaps.py"
spec = importlib.util.spec_from_file_location("claude_session_idle_gaps_script", SCRIPT_PATH)
claude_session_idle_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_idle_gaps_script)


def _event_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
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
) -> None:
    conn.execute(
        "INSERT INTO claude_session_events (session_id, timestamp, tool_name, metadata) VALUES (?, ?, ?, ?)",
        (session_id, timestamp, tool_name, None),
    )
    conn.commit()


def test_events_are_sorted_per_session_before_gap_detection():
    rows = [
        {"session_id": "sess-sort", "timestamp": "2026-05-01T11:00:00+00:00", "tool_name": "Read"},
        {"session_id": "sess-sort", "timestamp": "2026-05-01T10:00:00+00:00", "tool_name": "Bash"},
        {"session_id": "sess-sort", "timestamp": "2026-05-01T10:10:00+00:00", "tool_name": "Write"},
    ]

    report = build_claude_session_idle_gaps_report(rows, days=7, min_gap_minutes=30, now=NOW)
    payload = json.loads(format_claude_session_idle_gaps_json(report))

    assert payload["artifact_type"] == "claude_session_idle_gaps"
    assert list(payload) == sorted(payload)
    assert len(report.rows) == 1
    assert report.rows[0].previous_event_at == "2026-05-01T10:10:00+00:00"
    assert report.rows[0].next_event_at == "2026-05-01T11:00:00+00:00"
    assert report.rows[0].gap_minutes == 50
    assert report.rows[0].gap_id.startswith("claude_session_idle_gap_")


def test_malformed_timestamps_are_counted_and_skipped():
    rows = [
        {"session_id": "bad", "timestamp": "unknown", "tool_name": "Bash"},
        {"session_id": "good", "timestamp": "2026-05-01T10:00:00+00:00", "tool_name": "Read"},
        {"session_id": "good", "timestamp": "2026-05-01T11:00:00+00:00", "tool_name": "Write"},
    ]

    report = build_claude_session_idle_gaps_report(rows, days=7, min_gap_minutes=30, now=NOW)

    assert report.totals["malformed_timestamp_count"] == 1
    assert report.rows[0].session_id == "good"


def test_session_filter_limits_events_and_rows():
    rows = [
        {"session_id": "target", "timestamp": "2026-05-01T10:00:00+00:00", "tool_name": "Bash"},
        {"session_id": "target", "timestamp": "2026-05-01T11:00:00+00:00", "tool_name": "Read"},
        {"session_id": "other", "timestamp": "2026-05-01T10:00:00+00:00", "tool_name": "Bash"},
        {"session_id": "other", "timestamp": "2026-05-01T12:00:00+00:00", "tool_name": "Read"},
    ]

    report = build_claude_session_idle_gaps_report(
        rows,
        days=7,
        min_gap_minutes=30,
        session_id="target",
        now=NOW,
    )

    assert report.filters["session_id"] == "target"
    assert report.totals["session_count"] == 1
    assert [row.session_id for row in report.rows] == ["target"]


def test_missing_source_tables_are_reported_for_empty_sqlite_database():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_session_idle_gaps_report(conn, days=7, now=NOW)

    assert report.source_tables == ()
    assert "claude_session_events" in report.missing_tables
    assert report.rows == ()


def test_cli_invocation_outputs_json_and_validates_arguments(capsys, tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            metadata TEXT
        )"""
    )
    _insert_event(conn, session_id="sess-cli", timestamp="2026-05-01T10:00:00+00:00")
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:45:00+00:00",
        tool_name="Read",
    )
    conn.close()

    assert (
        claude_session_idle_gaps_script.main(
            ["--db", str(db_path), "--days", "7", "--min-gap-minutes", "30"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["gap_minutes"] == 45
    assert claude_session_idle_gaps_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
