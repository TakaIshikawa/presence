"""Tests for Claude session tool result size reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_tool_result_size import (
    build_claude_session_tool_result_size_report,
    format_claude_session_tool_result_size_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "claude_session_tool_result_size.py"
)
spec = importlib.util.spec_from_file_location("claude_session_tool_result_size_script", SCRIPT_PATH)
claude_session_tool_result_size_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_tool_result_size_script)


def test_iterable_rows_group_tool_results_into_size_buckets():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Read",
            "result": "x" * 1_500,
        },
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Read",
            "metadata": {"tool_result": {"content": "y" * 12_000}},
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "Bash",
            "output": "z" * 120_000,
        },
    ]

    report = build_claude_session_tool_result_size_report(rows, min_size=1, now=NOW)
    payload = json.loads(format_claude_session_tool_result_size_json(report))

    assert payload["artifact_type"] == "claude_session_tool_result_size"
    assert list(payload) == sorted(payload)
    assert [row.size_bucket for row in report.rows] == [
        "100kb-999kb",
        "10kb-99kb",
        "1kb-9kb",
    ]
    assert {(row.tool_name, row.session_id) for row in report.rows} == {
        ("bash", "sess-b"),
        ("read", "sess-a"),
    }


def test_top_examples_are_sorted_by_largest_observed_result_size():
    rows = [
        {
            "session_id": "sess-top",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Read",
            "result": "x" * 10_000,
        },
        {
            "session_id": "sess-top",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Read",
            "result": "x" * 80_000,
        },
        {
            "session_id": "sess-top",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "Read",
            "result": "x" * 40_000,
        },
    ]

    report = build_claude_session_tool_result_size_report(rows, min_size=1, now=NOW)

    assert [example.result_size for example in report.rows[0].top_examples] == [
        80_000,
        40_000,
        10_000,
    ]
    assert report.rows[0].max_result_size == 80_000


def test_missing_content_timestamps_and_unknown_tools_are_handled_gracefully():
    rows = [
        {"session_id": "sess-empty", "timestamp": None},
        {"timestamp": None, "tool_name": "Mystery Tool", "content": "hello"},
        {"metadata": "{bad json", "tool_name": "Read", "result": "x"},
    ]

    report = build_claude_session_tool_result_size_report(rows, min_size=0, now=NOW)

    assert report.totals["malformed_metadata_count"] == 1
    assert report.totals["zero_size_result_count"] == 1
    assert {row.tool_name for row in report.rows} == {"mystery_tool", "read", "unknown"}
    assert {row.session_id for row in report.rows} >= {"sess-empty", "unknown-session"}
    empty_row = next(row for row in report.rows if row.size_bucket == "empty")
    assert empty_row.max_result_size == 0
    assert empty_row.first_seen_at is None
    assert empty_row.top_examples[0].timestamp is None


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
            result TEXT,
            metadata TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, tool_name, result, metadata)
           VALUES (?, ?, ?, ?, ?)""",
        ("sess-cli", "2026-05-01T10:00:00+00:00", "Read", "x" * 12_000, None),
    )
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, tool_name, result, metadata)
           VALUES (?, ?, ?, ?, ?)""",
        (
            "sess-cli",
            "2026-05-01T10:01:00+00:00",
            "Bash",
            None,
            json.dumps({"tool_result": {"content": "y" * 1_500}}, sort_keys=True),
        ),
    )
    conn.commit()
    conn.close()

    assert (
        claude_session_tool_result_size_script.main(
            ["--db", str(db_path), "--days", "7", "--tool", "read", "--min-size", "1"]
        )
        == 0
    )
    script_payload = json.loads(capsys.readouterr().out)
    formatter_payload = json.loads(
        format_claude_session_tool_result_size_json(
            build_claude_session_tool_result_size_report(
                sqlite3.connect(db_path),
                days=7,
                tool="read",
                min_size=1,
            )
        )
    )

    assert script_payload["artifact_type"] == formatter_payload["artifact_type"]
    assert script_payload["rows"] == formatter_payload["rows"]
    assert script_payload["rows"][0]["size_bucket"] == "10kb-99kb"
    assert claude_session_tool_result_size_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
