"""Tests for Claude command failure summary reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_command_failure_summary import (
    build_claude_command_failure_summary_report,
    format_claude_command_failure_summary_json,
    format_claude_command_failure_summary_text,
    normalize_command_prefix,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "claude_command_failure_summary.py"
)
spec = importlib.util.spec_from_file_location(
    "claude_command_failure_summary_script",
    SCRIPT_PATH,
)
claude_command_failure_summary_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_command_failure_summary_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


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
            error_message TEXT,
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
    command: str | None = "uv run pytest tests/test_widget.py",
    output: str | None = None,
    error_message: str | None = "Command failed with exit code 1",
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, project_path, timestamp, tool_name, status, command,
            output, error_message, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            session_id,
            project_path,
            timestamp,
            tool_name,
            status,
            command,
            output,
            error_message,
            metadata_value,
        ),
    )
    conn.commit()


def test_empty_input_returns_structured_report_without_rows():
    conn = _event_db()

    report = build_claude_command_failure_summary_report(conn, days=7, now=NOW)
    text = format_claude_command_failure_summary_text(report)

    assert report.totals["rows_scanned"] == 0
    assert report.totals["failure_event_count"] == 0
    assert report.rows == ()
    assert report.source_tables == ("claude_session_events",)
    assert "No failed Claude commands found." in text


def test_single_failure_includes_session_timestamps_and_representative_error():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-single",
        project_path="/work/app",
        timestamp="2026-05-01T09:30:00+00:00",
        command="npm run build -- --watch=false",
        error_message="Error: Cannot find module vite at /work/app/node_modules/vite",
    )

    report = build_claude_command_failure_summary_report(conn, days=7, now=NOW)
    payload = json.loads(format_claude_command_failure_summary_json(report))
    row = report.rows[0]

    assert payload["artifact_type"] == "claude_command_failure_summary"
    assert list(payload) == sorted(payload)
    assert row.session_id == "sess-single"
    assert row.project_path == "/work/app"
    assert row.command_prefix == "npm run build"
    assert row.failure_count == 1
    assert row.first_seen_at == "2026-05-01T09:30:00+00:00"
    assert row.last_seen_at == "2026-05-01T09:30:00+00:00"
    assert "Cannot find module vite" in row.representative_error_text
    assert "module vite" in row.error_signature
    assert row.suggested_next_action == "repair_missing_dependency_or_path"


def test_repeated_failures_are_grouped_by_session_command_prefix_and_signature():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-repeat",
        timestamp="2026-05-01T10:00:00+00:00",
        command="uv run pytest tests/test_a.py -q",
        error_message="Command failed with exit code 1: tests/test_a.py::test_thing",
    )
    _insert_event(
        conn,
        session_id="sess-repeat",
        timestamp="2026-05-01T11:00:00+00:00",
        command="uv run pytest tests/test_b.py -q",
        error_message="Command failed with exit code 2: tests/test_b.py::test_thing",
    )
    _insert_event(
        conn,
        session_id="sess-other",
        timestamp="2026-05-01T12:00:00+00:00",
        command="uv run pytest tests/test_c.py -q",
        error_message="Command failed with exit code 1: tests/test_c.py::test_thing",
    )

    report = build_claude_command_failure_summary_report(conn, days=7, now=NOW)
    rows = {(row.session_id, row.command_prefix): row for row in report.rows}

    repeated = rows[("sess-repeat", "uv run pytest")]
    assert repeated.failure_count == 2
    assert repeated.repeated is True
    assert repeated.first_seen_at == "2026-05-01T10:00:00+00:00"
    assert repeated.last_seen_at == "2026-05-01T11:00:00+00:00"
    assert rows[("sess-other", "uv run pytest")].failure_count == 1
    assert report.totals["repeated_group_count"] == 1


def test_distinct_error_signatures_remain_separate_for_same_command_prefix():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-sig",
        timestamp="2026-05-01T10:00:00+00:00",
        command="python -m pytest tests/test_api.py",
        error_message="ModuleNotFoundError: No module named requests",
    )
    _insert_event(
        conn,
        session_id="sess-sig",
        timestamp="2026-05-01T11:00:00+00:00",
        command="python -m pytest tests/test_api.py",
        error_message="Permission denied opening /tmp/secret.txt",
    )

    report = build_claude_command_failure_summary_report(conn, days=7, now=NOW)

    assert [row.failure_count for row in report.rows] == [1, 1]
    assert {row.command_prefix for row in report.rows} == {"python -m pytest"}
    assert len({row.error_signature for row in report.rows}) == 2
    assert {
        row.suggested_next_action for row in report.rows
    } == {"fix_permissions", "repair_missing_dependency_or_path"}


def test_ignores_successes_old_rows_and_parses_metadata_commands():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-old",
        timestamp="2026-04-01T10:00:00+00:00",
        command="ruff check .",
        error_message="Command failed with exit code 1",
    )
    _insert_event(
        conn,
        session_id="sess-ok",
        timestamp="2026-05-01T10:00:00+00:00",
        status="success",
        command="ruff check .",
        error_message=None,
    )
    _insert_event(
        conn,
        session_id="sess-meta",
        timestamp="2026-05-01T11:00:00+00:00",
        status="ok",
        command=None,
        error_message=None,
        metadata={
            "is_error": True,
            "tool_use": {"input": {"command": "pnpm run test --filter app"}},
            "result": {"error": "Timeout waiting for test runner"},
        },
    )

    report = build_claude_command_failure_summary_report(conn, days=7, now=NOW)

    assert [row.session_id for row in report.rows] == ["sess-meta"]
    assert report.rows[0].command_prefix == "pnpm run test"
    assert report.rows[0].suggested_next_action == "raise_timeout_or_reduce_scope"


def test_missing_event_table_is_reported_without_aborting(db):
    report = build_claude_command_failure_summary_report(db, days=7, now=NOW)
    text = format_claude_command_failure_summary_text(report)

    assert report.rows == ()
    assert "claude_session_events" in report.missing_tables
    assert "Missing tables:" in text


def test_cli_outputs_json_and_text(db, monkeypatch, capsys):
    conn = _event_db()
    _insert_event(conn, session_id="sess-cli", command="uv run pytest tests/test_cli.py")
    monkeypatch.setattr(
        claude_command_failure_summary_script,
        "script_context",
        lambda: _script_context(conn),
    )
    monkeypatch.setattr(
        claude_command_failure_summary_script,
        "build_claude_command_failure_summary_report",
        lambda db, **kwargs: build_claude_command_failure_summary_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = claude_command_failure_summary_script.main(
        ["--format", "json", "--days", "7", "--limit", "5"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["command_prefix"] == "uv run pytest"

    assert claude_command_failure_summary_script.main(["--format", "text"]) == 0
    assert "Claude Command Failure Summary" in capsys.readouterr().out
    assert claude_command_failure_summary_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_command_prefix_normalization_handles_common_wrappers():
    assert normalize_command_prefix("uv run pytest tests/test_a.py -q") == "uv run pytest"
    assert normalize_command_prefix("python -m pytest tests/test_a.py") == "python -m pytest"
    assert normalize_command_prefix("npm run build -- --watch=false") == "npm run build"
    assert normalize_command_prefix("FOO=1 sudo git status --short") == "git status --short"
