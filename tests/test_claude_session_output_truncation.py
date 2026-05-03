"""Tests for Claude session output truncation reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_output_truncation import (
    build_claude_session_output_truncation_report,
    format_claude_session_output_truncation_json,
    format_claude_session_output_truncation_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_output_truncation.py"
spec = importlib.util.spec_from_file_location("claude_session_output_truncation_script", SCRIPT_PATH)
claude_session_output_truncation_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_output_truncation_script)


def _event_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            message TEXT,
            content TEXT,
            output TEXT,
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
    message: str | None = None,
    content: str | None = None,
    output: str | None = None,
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, timestamp, tool_name, message, content, output, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (session_id, timestamp, tool_name, message, content, output, metadata_value),
    )
    conn.commit()


def test_sqlite_builder_detects_truncation_in_plain_text_fields():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Bash",
        output="Some long output that was truncated after 500 lines",
    )
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:01:00+00:00",
        tool_name="Grep",
        message="Search results with output omitted for brevity",
    )
    _insert_event(
        conn,
        session_id="sess-b",
        timestamp="2026-05-01T10:02:00+00:00",
        tool_name="Read",
        content="File contents with 1000 lines omitted",
    )

    report = build_claude_session_output_truncation_report(conn, days=7, now=NOW)
    payload = json.loads(format_claude_session_output_truncation_json(report))

    assert payload["artifact_type"] == "claude_session_output_truncation"
    assert list(payload) == sorted(payload)
    assert report.totals["truncation_event_count"] == 3
    assert report.totals["session_count"] == 2
    assert report.totals["reported_group_count"] == 3
    assert report.source_tables == ("claude_messages",)

    rows_by_key = {(r.session_id, r.tool_name, r.marker): r for r in report.rows}
    assert ("sess-a", "bash", "truncated") in rows_by_key
    assert ("sess-a", "grep", "output_omitted") in rows_by_key
    assert ("sess-b", "read", "lines_omitted") in rows_by_key


def test_detects_truncation_in_nested_json_metadata():
    rows = [
        {
            "session_id": "sess-meta",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "tool_use": {"name": "Bash"},
                "output": "Build output truncated at 10000 characters",
            },
        },
        {
            "session_id": "sess-meta",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "content": {"result": "File contents with output omitted"},
            "metadata": {"tool_use": {"name": "Read"}},
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    assert report.source_tables == ()
    assert report.totals["truncation_event_count"] == 2
    rows_by_marker = {r.marker: r for r in report.rows}
    assert "truncated" in rows_by_marker
    assert "output_omitted" in rows_by_marker


def test_detects_explicit_truncation_flag_in_metadata():
    rows = [
        {
            "session_id": "sess-flag",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {
                "tool_use": {"name": "Bash"},
                "truncated": True,
            },
        },
        {
            "session_id": "sess-flag",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {
                "tool_use": {"name": "Grep"},
                "truncated": "exceeded 30000 character limit",
            },
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    assert report.totals["truncation_event_count"] == 2
    flag_rows = [r for r in report.rows if r.marker == "truncated_flag"]
    # Two different tools mean two groups (session+tool+marker)
    assert len(flag_rows) == 2
    assert sum(r.occurrence_count for r in flag_rows) == 2


def test_groups_by_session_tool_and_marker():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Bash",
        output="Output truncated at line 500",
    )
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:01:00+00:00",
        tool_name="Bash",
        output="Another output truncated at line 800",
    )
    _insert_event(
        conn,
        session_id="sess-b",
        timestamp="2026-05-01T10:02:00+00:00",
        tool_name="Bash",
        output="Third output truncated",
    )

    report = build_claude_session_output_truncation_report(conn, days=7, now=NOW)

    assert report.totals["truncation_event_count"] == 3
    assert report.totals["reported_group_count"] == 2
    rows_by_session = {r.session_id: r for r in report.rows}
    assert rows_by_session["sess-a"].occurrence_count == 2
    assert rows_by_session["sess-b"].occurrence_count == 1


def test_malformed_metadata_is_counted_without_failing():
    rows = [
        {
            "session_id": "sess-bad",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "output": "Output truncated",
            "metadata": "{invalid json",
        },
        {
            "session_id": "sess-good",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Read",
            "message": "Lines omitted",
            "metadata": {"tool_use": {"name": "Read"}},
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)

    assert report.totals["malformed_metadata_count"] == 1
    assert report.totals["truncation_event_count"] == 2
    assert report.totals["session_count"] == 2


def test_missing_claude_tables_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE unrelated_table (id INTEGER PRIMARY KEY)")

    report = build_claude_session_output_truncation_report(conn, days=7, now=NOW)

    assert report.rows == ()
    assert report.missing_tables == (
        "claude_messages",
        "claude_message_events",
        "claude_session_events",
    )
    assert report.totals["truncation_event_count"] == 0


def test_json_formatter_produces_deterministic_output():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": "Output truncated",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    json_output = format_claude_session_output_truncation_json(report)
    payload = json.loads(json_output)

    assert list(payload) == sorted(payload)
    assert list(payload["totals"]) == sorted(payload["totals"])


def test_text_formatter_produces_readable_output():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": "Output truncated at 500 lines",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    text_output = format_claude_session_output_truncation_text(report)

    assert "Claude Session Output Truncation Report" in text_output
    assert "Generated:" in text_output
    assert "Truncated Outputs:" in text_output
    assert "session=sess-a" in text_output
    assert "tool=bash" in text_output
    assert "marker=truncated" in text_output


def test_empty_report_text_format():
    conn = _event_db()
    report = build_claude_session_output_truncation_report(conn, days=7, now=NOW)
    text_output = format_claude_session_output_truncation_text(report)

    assert "No truncated outputs detected." in text_output


def test_cli_script_handles_json_format():
    conn = _event_db()
    _insert_event(conn, output="Output truncated")

    with open("/tmp/test_truncation.db", "w") as _:
        pass
    conn2 = sqlite3.connect("/tmp/test_truncation.db")
    conn2.row_factory = sqlite3.Row
    conn2.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            message TEXT,
            content TEXT,
            output TEXT,
            metadata TEXT
        )"""
    )
    conn2.execute(
        """INSERT INTO claude_messages
           (session_id, timestamp, tool_name, output)
           VALUES (?, ?, ?, ?)""",
        ("sess-a", "2026-05-01T10:00:00+00:00", "Bash", "Output truncated"),
    )
    conn2.commit()
    conn2.close()

    exit_code = claude_session_output_truncation_script.main(
        ["--db", "/tmp/test_truncation.db", "--format", "json"]
    )
    assert exit_code == 0


def test_cli_script_handles_text_format():
    conn = _event_db()
    _insert_event(conn, output="Lines omitted")

    with open("/tmp/test_truncation_text.db", "w") as _:
        pass
    conn2 = sqlite3.connect("/tmp/test_truncation_text.db")
    conn2.row_factory = sqlite3.Row
    conn2.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            message TEXT,
            content TEXT,
            output TEXT,
            metadata TEXT
        )"""
    )
    conn2.execute(
        """INSERT INTO claude_messages
           (session_id, timestamp, tool_name, output)
           VALUES (?, ?, ?, ?)""",
        ("sess-a", "2026-05-01T10:00:00+00:00", "Read", "Lines omitted"),
    )
    conn2.commit()
    conn2.close()

    exit_code = claude_session_output_truncation_script.main(
        ["--db", "/tmp/test_truncation_text.db", "--format", "text"]
    )
    assert exit_code == 0


def test_representative_excerpt_limits_length():
    long_text = "This is a very long output that was truncated after many lines. " * 10
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": long_text,
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert len(report.rows[0].representative_excerpt) <= 123


def test_rows_sorted_by_occurrence_count():
    rows = [
        {
            "session_id": "sess-a",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "metadata": {"tool_use": {"name": "Bash"}},
            "output": "Output truncated",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "message": "Lines omitted",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "metadata": {"tool_use": {"name": "Read"}},
            "message": "More lines omitted",
        },
    ]

    report = build_claude_session_output_truncation_report(rows, days=7, now=NOW)
    assert report.rows[0].occurrence_count >= report.rows[-1].occurrence_count
