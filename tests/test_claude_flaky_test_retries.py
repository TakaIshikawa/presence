"""Tests for Claude flaky test retry reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_flaky_test_retries import (
    build_claude_flaky_test_retries_report,
    format_claude_flaky_test_retries_json,
    format_claude_flaky_test_retries_text,
    normalize_test_command,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_flaky_test_retries.py"
spec = importlib.util.spec_from_file_location("claude_flaky_test_retries_script", SCRIPT_PATH)
claude_flaky_test_retries_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_flaky_test_retries_script)


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
    project_path: str = "/repo/presence",
    timestamp: str,
    tool_name: str = "bash",
    status: str = "completed",
    command: str | None = None,
    output: str | None = None,
    error_message: str | None = None,
    metadata: dict | str | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, project_path, timestamp, tool_name, status, command, output, error_message, metadata)
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


def test_detects_fail_then_pass_for_same_normalized_command():
    conn = _event_db()
    _insert_event(
        conn,
        timestamp="2026-05-01T10:00:00+00:00",
        status="failed",
        command="uv run pytest tests/test_widget.py -q",
        error_message="Command failed with exit code 1",
    )
    _insert_event(
        conn,
        timestamp="2026-05-01T10:05:00+00:00",
        status="failed",
        command="python -m pytest tests/test_widget.py -q",
        output="1 failed",
    )
    _insert_event(
        conn,
        timestamp="2026-05-01T10:08:00+00:00",
        status="completed",
        command="pytest tests/test_widget.py -q",
        output="1 passed",
    )

    report = build_claude_flaky_test_retries_report(conn, days=7, now=NOW)
    payload = json.loads(format_claude_flaky_test_retries_json(report))

    assert payload["artifact_type"] == "claude_flaky_test_retries"
    assert payload["totals"]["test_attempts"] == 3
    assert payload["totals"]["candidate_count"] == 1
    retry = payload["retries"][0]
    assert retry["session_id"] == "sess-a"
    assert retry["normalized_command"] == "pytest tests/test_widget.py -q"
    assert retry["failure_count"] == 2
    assert retry["eventual_pass_timestamp"] == "2026-05-01T10:08:00+00:00"
    assert retry["evidence_snippets"]


def test_intervening_edit_resets_failures_before_pass():
    conn = _event_db()
    _insert_event(
        conn,
        timestamp="2026-05-01T10:00:00+00:00",
        status="failed",
        command="npm test -- tests/app.test.ts",
        error_message="Command failed with exit code 1",
    )
    _insert_event(
        conn,
        timestamp="2026-05-01T10:02:00+00:00",
        tool_name="Edit",
        status="completed",
        output="updated src/app.ts",
    )
    _insert_event(
        conn,
        timestamp="2026-05-01T10:04:00+00:00",
        status="completed",
        command="npm run test -- tests/app.test.ts",
        output="1 passed",
    )

    report = build_claude_flaky_test_retries_report(conn, days=7, now=NOW)

    assert report.retries == ()
    assert report.totals["edit_events"] == 1


def test_normalizes_common_pytest_unittest_npm_pnpm_and_uv_forms():
    assert normalize_test_command("uv run pytest tests/test_a.py -q") == "pytest tests/test_a.py -q"
    assert normalize_test_command("python -m pytest tests/test_a.py -q") == "pytest tests/test_a.py -q"
    assert normalize_test_command("uv run python -m unittest tests.test_a") == "unittest tests.test_a"
    assert normalize_test_command("npm run test -- --watch=false") == "npm test -- --watch=false"
    assert normalize_test_command("pnpm run test tests/app.test.ts") == "pnpm test tests/app.test.ts"


def test_no_findings_text_is_stable_for_clean_sessions():
    conn = _event_db()
    _insert_event(
        conn,
        timestamp="2026-05-01T10:00:00+00:00",
        status="completed",
        command="pnpm test",
        output="all tests passed",
    )

    report = build_claude_flaky_test_retries_report(conn, days=7, now=NOW)
    text = format_claude_flaky_test_retries_text(report)

    assert report.retries == ()
    assert "No flaky test retry candidates found." in text


def test_cli_json_text_and_invalid_args_are_stable(db, monkeypatch, capsys):
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:00:00+00:00",
        status="failed",
        command="uv run pytest tests/test_cli.py",
        error_message="Command failed with exit code 1",
    )
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:03:00+00:00",
        status="completed",
        command="pytest tests/test_cli.py",
        output="1 passed",
    )
    monkeypatch.setattr(
        claude_flaky_test_retries_script,
        "script_context",
        lambda: _script_context(conn),
    )
    monkeypatch.setattr(
        claude_flaky_test_retries_script,
        "build_claude_flaky_test_retries_report",
        lambda db, **kwargs: build_claude_flaky_test_retries_report(db, now=NOW, **kwargs),
    )

    assert claude_flaky_test_retries_script.main(["--days", "7", "--limit", "5", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["retries"][0]["session_id"] == "sess-cli"

    assert claude_flaky_test_retries_script.main(["--format", "text"]) == 0
    assert "session=sess-cli" in capsys.readouterr().out

    assert claude_flaky_test_retries_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert claude_flaky_test_retries_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
