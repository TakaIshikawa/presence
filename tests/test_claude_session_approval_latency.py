"""Tests for Claude session approval latency reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_approval_latency import (
    build_claude_session_approval_latency_report,
    format_claude_session_approval_latency_json,
    format_claude_session_approval_latency_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_session_approval_latency.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_session_approval_latency_script",
    SCRIPT_PATH,
)
claude_session_approval_latency_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_approval_latency_script)


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
            content TEXT,
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
    tool_name: str = "approval",
    status: str | None = "requested",
    content: str | None = "Allow Bash command?",
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, project_path, timestamp, tool_name, status, content, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (session_id, project_path, timestamp, tool_name, status, content, metadata_value),
    )
    conn.commit()


def test_pairs_request_with_later_decision_and_reports_slow_bucket():
    rows = [
        {
            "session_id": "sess-a",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "approval_prompt",
            "status": "requested",
            "content": "Allow Bash command?",
        },
        {
            "session_id": "sess-a",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:06:00+00:00",
            "tool_name": "approval",
            "status": "approved",
            "content": "User approved Bash command",
        },
    ]

    report = build_claude_session_approval_latency_report(
        rows,
        slow_threshold_seconds=300,
        now=NOW,
    )
    payload = json.loads(format_claude_session_approval_latency_json(report))
    row = report.rows[0]

    assert payload["artifact_type"] == "claude_session_approval_latency"
    assert list(payload) == sorted(payload)
    assert row.session_id == "sess-a"
    assert row.request_at == "2026-05-01T10:00:00+00:00"
    assert row.decision_at == "2026-05-01T10:06:00+00:00"
    assert row.elapsed_seconds == 360
    assert row.is_slow is True
    assert row.approval_decision == "approved"
    assert report.totals == {
        "approved_count": 1,
        "denied_count": 0,
        "malformed_metadata_count": 0,
        "missing_decision_count": 0,
        "paired_count": 1,
        "rows_scanned": 2,
        "slow_count": 1,
    }


def test_stable_session_ordering_pairs_each_request_once():
    rows = [
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "approval",
            "status": "requested",
            "content": "Second request",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "approval",
            "status": "requested",
            "content": "First request",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "tool_name": "approval",
            "status": "denied",
            "content": "User denied first request",
        },
        {
            "session_id": "sess-b",
            "timestamp": "2026-05-01T10:03:00+00:00",
            "tool_name": "approval",
            "status": "approved",
            "content": "User approved second request",
        },
    ]

    report = build_claude_session_approval_latency_report(rows, now=NOW)

    assert [(row.request_text, row.approval_decision) for row in report.rows] == [
        ("First request", "denied"),
        ("Second request", "approved"),
    ]
    assert [row.elapsed_seconds for row in report.rows] == [120, 120]
    assert report.totals["approved_count"] == 1
    assert report.totals["denied_count"] == 1


def test_missing_decision_and_malformed_metadata_are_counted():
    rows = [
        {
            "session_id": "sess-missing",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "permission_prompt",
            "status": "pending",
            "content": "Permission requested",
            "metadata": "{not json",
        },
    ]

    report = build_claude_session_approval_latency_report(rows, now=NOW)

    assert report.rows == ()
    assert report.totals["missing_decision_count"] == 1
    assert report.totals["malformed_metadata_count"] == 1


def test_sqlite_input_filters_by_days_and_matches_rows_shape():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-db",
        timestamp="2026-05-01T10:00:00+00:00",
        status="requested",
    )
    _insert_event(
        conn,
        session_id="sess-db",
        timestamp="2026-05-01T10:02:00+00:00",
        status="approved",
        content="Approved",
    )
    _insert_event(
        conn,
        session_id="sess-old",
        timestamp="2026-04-01T10:00:00+00:00",
        status="requested",
    )

    report = build_claude_session_approval_latency_report(conn, days=7, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].session_id == "sess-db"
    assert report.rows[0].elapsed_seconds == 120
    assert report.totals["rows_scanned"] == 2
    assert report.source_tables == ("claude_session_events",)


def test_missing_tables_and_columns_return_empty_report_with_details():
    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    missing_table_report = build_claude_session_approval_latency_report(empty, now=NOW)

    assert missing_table_report.missing_tables == (
        "claude_session_events",
        "claude_tool_events",
        "claude_events",
    )
    assert missing_table_report.rows == ()

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE claude_session_events (session_id TEXT, status TEXT)")
    partial.execute(
        "INSERT INTO claude_session_events (session_id, status) VALUES (?, ?)",
        ("sess-partial", "requested"),
    )
    partial.commit()
    partial_report = build_claude_session_approval_latency_report(partial, now=NOW)

    assert partial_report.rows == ()
    assert partial_report.totals["rows_scanned"] == 0
    assert partial_report.missing_columns == {"claude_session_events": ("timestamp",)}


def test_text_output_and_cli_json_validation(capsys, tmp_path):
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-text",
        timestamp="2026-05-01T10:00:00+00:00",
        status="requested",
    )
    _insert_event(
        conn,
        session_id="sess-text",
        timestamp="2026-05-01T10:01:00+00:00",
        status="denied",
        content="Denied",
    )
    text = format_claude_session_approval_latency_text(
        build_claude_session_approval_latency_report(conn, days=7, now=NOW)
    )
    assert "Claude Session Approval Latency" in text
    assert "paired=1" in text
    assert "decision=denied" in text

    db_path = tmp_path / "approval-latency.db"
    disk = sqlite3.connect(db_path)
    disk.executescript(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            status TEXT,
            content TEXT
        );
        INSERT INTO claude_session_events
            (session_id, timestamp, tool_name, status, content)
            VALUES ('sess-cli', '2026-05-01T10:00:00+00:00', 'approval', 'requested', 'Allow?');
        INSERT INTO claude_session_events
            (session_id, timestamp, tool_name, status, content)
            VALUES ('sess-cli', '2026-05-01T10:04:00+00:00', 'approval', 'approved', 'Approved');
        """
    )
    disk.commit()
    disk.close()

    assert (
        claude_session_approval_latency_script.main(
            [
                "--db",
                str(db_path),
                "--days",
                "7",
                "--slow-threshold-seconds",
                "120",
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
    assert payload["rows"][0]["is_slow"] is True
    assert claude_session_approval_latency_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
