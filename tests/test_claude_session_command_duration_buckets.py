"""Tests for Claude session command duration bucket reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_command_duration_buckets import (
    build_claude_session_command_duration_buckets_report,
    format_claude_session_command_duration_buckets_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_command_duration_buckets.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_session_command_duration_buckets_script",
    SCRIPT_PATH,
)
claude_session_command_duration_buckets_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_command_duration_buckets_script)


def test_iterable_rows_group_duration_buckets_by_day():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "command": "pytest",
            "duration_ms": 800,
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:05:00+00:00",
            "metadata": {
                "tool_use": {"input": {"command": "npm test"}},
                "result": {"elapsed_ms": "2500"},
            },
        },
        {
            "session_id": "sess-c",
            "timestamp": "2026-05-02T10:05:00+00:00",
            "metadata": {"input": {"command": "uv run ruff"}, "runtime_ms": 35_000},
        },
    ]

    report = build_claude_session_command_duration_buckets_report(rows, days=7, now=NOW)
    payload = json.loads(format_claude_session_command_duration_buckets_json(report))

    assert payload["artifact_type"] == "claude_session_command_duration_buckets"
    assert list(payload) == sorted(payload)
    assert [row.duration_bucket for row in report.rows] == ["lt_1s", "1s_to_5s", "30s_to_2m"]
    assert report.rows[0].day == "2026-05-01"
    assert report.rows[0].command_event_count == 1
    assert report.rows[0].representative_commands == ("pytest",)
    assert report.rows[1].representative_commands == ("npm test",)
    assert report.rows[2].max_duration_ms == 35_000
    assert report.source_tables == ("rows",)


def test_timestamp_pairs_derive_duration_from_row_and_metadata():
    rows = [
        {
            "session_id": "sess-row",
            "timestamp": "2026-05-01T11:00:00+00:00",
            "command": "sleep 6",
            "started_at": "2026-05-01T11:00:00+00:00",
            "completed_at": "2026-05-01T11:00:06+00:00",
        },
        {
            "session_id": "sess-meta",
            "metadata": {
                "created_at": "2026-05-01T12:00:00+00:00",
                "tool_use": {"input": {"command": "pytest slow"}},
                "tool_result": {
                    "startedAt": "2026-05-01T12:00:00+00:00",
                    "completedAt": "2026-05-01T12:02:10+00:00",
                },
            },
        },
    ]

    report = build_claude_session_command_duration_buckets_report(rows, days=7, now=NOW)

    assert [row.duration_bucket for row in report.rows] == ["5s_to_30s", "gte_2m"]
    assert report.rows[0].min_duration_ms == 6_000
    assert report.rows[1].min_duration_ms == 130_000
    assert report.rows[1].day == "2026-05-01"


def test_unknown_duration_excluded_by_default_and_included_when_requested():
    rows = [
        {
            "session_id": "sess-known",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "command": "pytest",
            "duration_ms": 1_500,
        },
        {
            "session_id": "sess-missing",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {"tool_input": {"command": "npm test"}},
        },
        {
            "session_id": "sess-bad",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "command": "ruff",
            "metadata": "{bad json",
        },
    ]

    default_report = build_claude_session_command_duration_buckets_report(
        rows,
        days=7,
        now=NOW,
    )
    included_report = build_claude_session_command_duration_buckets_report(
        rows,
        days=7,
        include_unknown_duration=True,
        now=NOW,
    )

    assert [row.duration_bucket for row in default_report.rows] == ["1s_to_5s"]
    assert default_report.totals["unknown_duration_event_count"] == 2
    assert default_report.totals["malformed_metadata_count"] == 1
    assert [row.duration_bucket for row in included_report.rows] == ["1s_to_5s", "unknown"]
    assert included_report.rows[1].command_event_count == 2
    assert included_report.rows[1].min_duration_ms is None
    assert included_report.rows[1].representative_commands == ("npm test", "ruff")


def test_sqlite_cli_reads_source_tables_and_validates_positive_values(capsys, tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            command TEXT,
            elapsed_ms TEXT,
            metadata TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO claude_session_events (session_id, timestamp, command, elapsed_ms, metadata) VALUES (?, ?, ?, ?, ?)",
        ("sess-a", "2026-05-01T10:00:00+00:00", "pytest", "400", "{}"),
    )
    conn.execute(
        "INSERT INTO claude_session_events (session_id, timestamp, command, elapsed_ms, metadata) VALUES (?, ?, ?, ?, ?)",
        ("sess-b", "2026-05-01T10:05:00+00:00", "npm test", "6500", "{}"),
    )
    conn.commit()
    conn.close()

    assert (
        claude_session_command_duration_buckets_script.main(
            ["--db", str(db_path), "--days", "7", "--limit", "1"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["source_tables"] == ["claude_session_events"]
    assert payload["missing_tables"] == []
    assert [row["duration_bucket"] for row in payload["rows"]] == ["lt_1s"]
    assert payload["totals"]["bucket_count"] == 1

    assert claude_session_command_duration_buckets_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert claude_session_command_duration_buckets_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_deterministic_json_independent_of_input_order():
    rows = [
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:05:00+00:00",
            "command": "npm test",
            "duration_ms": 1_500,
        },
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "command": "pytest",
            "duration_ms": 1_250,
        },
    ]

    report_a = build_claude_session_command_duration_buckets_report(rows, days=7, now=NOW)
    report_b = build_claude_session_command_duration_buckets_report(
        list(reversed(rows)),
        days=7,
        now=NOW,
    )

    assert format_claude_session_command_duration_buckets_json(report_a) == (
        format_claude_session_command_duration_buckets_json(report_b)
    )
    assert report_a.rows[0].report_id == report_b.rows[0].report_id
    assert report_a.rows[0].representative_commands == ("pytest", "npm test")
