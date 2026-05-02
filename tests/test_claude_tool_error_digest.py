"""Tests for Claude Code tool error digest reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_tool_error_digest import (
    build_claude_tool_error_digest,
    format_claude_tool_error_digest_json,
    format_claude_tool_error_digest_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_tool_error_digest.py"
spec = importlib.util.spec_from_file_location("claude_tool_error_digest_script", SCRIPT_PATH)
claude_tool_error_digest_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_tool_error_digest_script)


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
            timestamp TEXT,
            tool_name TEXT,
            status TEXT,
            error_message TEXT,
            metadata TEXT
        )"""
    )
    return conn


def _insert_event(
    conn: sqlite3.Connection,
    *,
    session_id: str,
    timestamp: str,
    tool_name: str | None = None,
    status: str = "failed",
    error_message: str | None = None,
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, tool_name, status, error_message, metadata)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (session_id, timestamp, tool_name, status, error_message, metadata_value),
    )
    conn.commit()


def test_groups_repeated_tool_errors_by_tool_and_normalized_signature():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Bash",
        error_message="Command failed with exit code 1: uv run pytest /tmp/run-123/test_a.py",
    )
    _insert_event(
        conn,
        session_id="sess-b",
        timestamp="2026-05-01T11:00:00+00:00",
        tool_name="bash",
        error_message="Command failed with exit code 2: uv run pytest /private/tmp/run-456/test_b.py",
    )
    _insert_event(
        conn,
        session_id="sess-c",
        timestamp="2026-05-01T12:00:00+00:00",
        tool_name="Read",
        error_message="Error: permission denied opening /tmp/secret.txt",
    )

    report = build_claude_tool_error_digest(conn, days=7, threshold=2, now=NOW)

    assert report["totals"]["rows_scanned"] == 3
    assert report["totals"]["failure_rows"] == 3
    assert report["totals"]["groups"] == 1
    group = report["groups"][0]
    assert group["tool_name"] == "bash"
    assert group["count"] == 2
    assert "exit code <num>" in group["signature"]
    assert "<path>" in group["signature"]
    assert group["session_ids"] == ["sess-a", "sess-b"]
    assert group["latest_at"] == "2026-05-01T11:00:00+00:00"
    assert group["suggested_next_action"] == "repair_command"


def test_metadata_errors_and_malformed_metadata_are_counted():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-meta-1",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name=None,
        status="ok",
        metadata={
            "tool_name": "Edit",
            "is_error": True,
            "error": {"message": "Error: could not parse JSON in /tmp/a.json"},
        },
    )
    _insert_event(
        conn,
        session_id="sess-meta-2",
        timestamp="2026-05-01T11:00:00+00:00",
        tool_name="edit",
        metadata="{not-json",
        error_message="Error: could not parse JSON in /tmp/b.json",
    )

    report = build_claude_tool_error_digest(conn, days=7, threshold=1, now=NOW)

    assert report["totals"]["malformed_metadata_count"] == 1
    assert report["totals"]["failure_rows"] == 2
    assert report["groups"][0]["tool_name"] == "edit"
    assert report["groups"][0]["count"] == 2
    assert report["groups"][0]["suggested_next_action"] == "inspect_tool_input"


def test_json_output_is_deterministic_and_text_lists_examples():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-json",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Read",
        error_message="Error: permission denied opening /tmp/secret-1.txt",
    )

    report = build_claude_tool_error_digest(conn, days=7, threshold=1, now=NOW)
    payload = json.loads(format_claude_tool_error_digest_json(report))
    text = format_claude_tool_error_digest_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_tool_error_digest"
    assert payload["filters"]["threshold"] == 1
    assert payload["schema_gaps"] == {"missing_columns": {}, "missing_tables": []}
    assert "Claude Tool Error Digest" in text
    assert "Top error groups:" in text
    assert "example session=sess-json" in text
    assert "action=fix_permissions" in text


def test_missing_event_tables_return_schema_gaps_without_aborting(db):
    report = build_claude_tool_error_digest(db, days=7, threshold=1, now=NOW)
    text = format_claude_tool_error_digest_text(report)

    assert report["groups"] == []
    assert report["totals"]["rows_scanned"] == 0
    assert "claude_session_events" in report["schema_gaps"]["missing_tables"]
    assert "Missing tables:" in text


def test_cli_supports_db_days_threshold_and_json_output(tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            tool_name TEXT,
            status TEXT,
            error_message TEXT,
            metadata TEXT
        )"""
    )
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Bash",
        error_message="Command failed with exit code 1",
    )
    conn.close()

    exit_code = claude_tool_error_digest_script.main(
        ["--db", str(db_path), "--days", "30", "--threshold", "1", "--format", "json"]
    )

    assert exit_code == 0


def test_cli_uses_script_context_without_db(db, monkeypatch, capsys):
    monkeypatch.setattr(
        claude_tool_error_digest_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = claude_tool_error_digest_script.main(["--threshold", "1", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "claude_tool_error_digest"
