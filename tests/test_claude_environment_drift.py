"""Tests for Claude session environment drift reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_environment_drift import (
    build_claude_environment_drift_report,
    format_claude_environment_drift_json,
    format_claude_environment_drift_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_environment_drift.py"
spec = importlib.util.spec_from_file_location("claude_environment_drift_script", SCRIPT_PATH)
claude_environment_drift_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_environment_drift_script)


def test_no_drift_when_consistent_package_manager():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/repo", "uv run pytest"),
        _event("sess-1", "2026-05-01T10:01:00+00:00", "/repo", "uv run python test.py"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo", "uv pip install requests"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)
    payload = json.loads(format_claude_environment_drift_json(report))

    assert payload["artifact_type"] == "claude_environment_drift"
    assert list(payload) == sorted(payload)
    assert report.rows == ()
    assert report.totals["command_event_count"] == 3
    assert report.totals["session_count"] == 2
    assert report.risk_summary["conflicting_package_managers"] == 0


def test_conflicting_package_managers_detected():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/repo", "uv run pytest"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo", "pip install requests"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)
    row = report.rows[0]

    assert row.project_path == "/repo"
    assert row.severity == "high"
    assert row.risk_reason == "conflicting_package_managers"
    assert set(row.package_managers) == {"uv", "pip"}
    assert len(row.commands) == 2


def test_conflicting_python_invocations_detected():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/repo", "python test.py"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo", "python3 test.py"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)
    row = report.rows[0]

    assert row.severity == "medium"
    assert row.risk_reason == "conflicting_python_invocations"
    assert set(row.python_invocations) == {"python", "python3"}


def test_uv_run_python_invocation_detected():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/repo", "uv run test.py"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo", "python test.py"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)
    row = report.rows[0]

    assert set(row.python_invocations) == {"uv_run", "python"}


def test_multiple_projects_reported_separately():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/repo-a", "uv run pytest"),
        _event("sess-1", "2026-05-01T10:01:00+00:00", "/repo-a", "pip install requests"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo-b", "npm install"),
        _event("sess-2", "2026-05-02T10:01:00+00:00", "/repo-b", "pnpm install"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)

    assert len(report.rows) == 2
    projects = {row.project_path for row in report.rows}
    assert projects == {"/repo-a", "/repo-b"}


def test_severity_ordering_high_before_medium():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/proj-medium", "python test.py"),
        _event("sess-1", "2026-05-01T10:01:00+00:00", "/proj-medium", "python3 test.py"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/proj-high", "uv run pytest"),
        _event("sess-2", "2026-05-02T10:01:00+00:00", "/proj-high", "pip install requests"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)

    assert report.rows[0].severity == "high"
    assert report.rows[0].project_path == "/proj-high"
    assert report.rows[1].severity == "medium"
    assert report.rows[1].project_path == "/proj-medium"


def test_limit_restricts_reported_projects():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", f"/repo-{i}", f"uv run pytest")
        for i in range(10)
    ] + [
        _event("sess-2", "2026-05-02T10:00:00+00:00", f"/repo-{i}", f"pip install requests")
        for i in range(10)
    ]

    report = build_claude_environment_drift_report(rows, limit=3, now=NOW)

    assert report.totals["drift_project_count"] == 10
    assert report.totals["reported_count"] == 3
    assert len(report.rows) == 3


def test_malformed_metadata_counted_but_does_not_abort():
    rows = [
        {
            "session_id": "sess-1",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "command": "uv run pytest",
            "project_path": "/repo",
            "metadata": "not-valid-json",
        },
        {
            "session_id": "sess-2",
            "timestamp": "2026-05-02T10:00:00+00:00",
            "tool_name": "Bash",
            "command": "pip install requests",
            "project_path": "/repo",
            "metadata": '{"valid": "json"}',
        },
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)

    assert report.totals["malformed_metadata_count"] == 1
    assert len(report.rows) == 1


def test_missing_command_events_are_skipped():
    rows = [
        {
            "session_id": "sess-1",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Read",
            "project_path": "/repo",
        },
        {
            "session_id": "sess-1",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "tool_name": "Bash",
            "command": "uv run pytest",
            "project_path": "/repo",
        },
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)

    assert report.totals["command_event_count"] == 1


def test_json_output_is_deterministic():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/repo", "uv run pytest"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo", "pip install requests"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)
    json_output = format_claude_environment_drift_json(report)
    payload = json.loads(json_output)

    assert list(payload) == sorted(payload)
    assert all(
        list(row) == sorted(row)
        for row in payload.get("rows", [])
    )


def test_text_output_is_readable():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/repo", "uv run pytest"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo", "pip install requests"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)
    text_output = format_claude_environment_drift_text(report)

    assert "Claude Session Environment Drift" in text_output
    assert "project=/repo" in text_output
    assert "severity=high" in text_output


def test_sqlite_backend_with_missing_tables():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_environment_drift_report(conn, now=NOW)

    assert report.missing_tables == (
        "claude_session_events",
        "claude_tool_events",
        "claude_events",
    )
    assert report.rows == ()


def test_sqlite_backend_with_claude_session_events():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE claude_session_events (
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            command TEXT,
            project_path TEXT,
            metadata TEXT
        )
    """)
    conn.execute(
        """
        INSERT INTO claude_session_events
        (session_id, timestamp, tool_name, command, project_path, metadata)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("sess-1", "2026-05-01T10:00:00+00:00", "Bash", "uv run pytest", "/repo", "{}"),
    )
    conn.execute(
        """
        INSERT INTO claude_session_events
        (session_id, timestamp, tool_name, command, project_path, metadata)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ("sess-2", "2026-05-02T10:00:00+00:00", "Bash", "pip install requests", "/repo", "{}"),
    )
    conn.commit()

    report = build_claude_environment_drift_report(conn, days=30, now=NOW)

    assert report.source_tables == ("claude_session_events",)
    assert len(report.rows) == 1
    assert report.rows[0].project_path == "/repo"


def test_days_filter_restricts_lookback():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE claude_session_events (
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            command TEXT,
            project_path TEXT
        )
    """)
    # Old event (outside window)
    conn.execute(
        """
        INSERT INTO claude_session_events
        (session_id, timestamp, tool_name, command, project_path)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("sess-old", "2026-04-01T10:00:00+00:00", "Bash", "uv run pytest", "/repo"),
    )
    # Recent event (inside window)
    conn.execute(
        """
        INSERT INTO claude_session_events
        (session_id, timestamp, tool_name, command, project_path)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("sess-new", "2026-05-01T10:00:00+00:00", "Bash", "pip install requests", "/repo"),
    )
    conn.commit()

    report = build_claude_environment_drift_report(conn, days=7, now=NOW)

    assert report.totals["command_event_count"] == 1
    assert report.rows == ()  # Only one package manager in the window


def test_cli_script_parse_args_defaults():
    args = claude_environment_drift_script.parse_args([])

    assert args.days == 30
    assert args.limit == 100
    assert args.format == "json"
    assert args.db is None


def test_cli_script_parse_args_custom():
    args = claude_environment_drift_script.parse_args([
        "--days", "14",
        "--limit", "50",
        "--format", "text",
        "--db", "/path/to/db.sqlite",
    ])

    assert args.days == 14
    assert args.limit == 50
    assert args.format == "text"
    assert args.db == "/path/to/db.sqlite"


def test_cli_script_rejects_invalid_days():
    try:
        claude_environment_drift_script.parse_args(["--days", "0"])
        assert False, "Expected ArgumentTypeError"
    except SystemExit:
        pass


def test_cli_script_rejects_invalid_limit():
    try:
        claude_environment_drift_script.parse_args(["--limit", "-5"])
        assert False, "Expected ArgumentTypeError"
    except SystemExit:
        pass


def test_command_limit_truncates_evidence():
    rows = [
        _event("sess-1", f"2026-05-01T10:{i:02d}:00+00:00", "/repo", f"uv run test{i}.py")
        for i in range(50)
    ] + [
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo", "pip install requests"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)
    row = report.rows[0]

    assert row.command_count == 51
    assert len(row.commands) == 20  # Limited to 20


def test_npm_and_pnpm_detection():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/repo", "npm install"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo", "pnpm install"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)
    row = report.rows[0]

    assert set(row.package_managers) == {"npm", "pnpm"}
    assert row.severity == "high"


def test_yarn_detection():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/repo", "yarn install"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo", "npm install"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)
    row = report.rows[0]

    assert set(row.package_managers) == {"yarn", "npm"}


def test_node_invocation_detection():
    rows = [
        _event("sess-1", "2026-05-01T10:00:00+00:00", "/repo", "node script.js"),
        _event("sess-2", "2026-05-02T10:00:00+00:00", "/repo", "node other.js"),
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)

    # No drift because both use node
    assert report.rows == ()


def test_unknown_project_path_grouped_together():
    rows = [
        {
            "session_id": "sess-1",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "tool_name": "Bash",
            "command": "uv run pytest",
        },
        {
            "session_id": "sess-2",
            "timestamp": "2026-05-02T10:00:00+00:00",
            "tool_name": "Bash",
            "command": "pip install requests",
        },
    ]

    report = build_claude_environment_drift_report(rows, now=NOW)

    assert len(report.rows) == 1
    assert report.rows[0].project_path == "unknown-project"


def _event(
    session_id: str,
    timestamp: str,
    project_path: str,
    command: str,
) -> dict[str, str]:
    return {
        "session_id": session_id,
        "timestamp": timestamp,
        "tool_name": "Bash",
        "command": command,
        "project_path": project_path,
    }
