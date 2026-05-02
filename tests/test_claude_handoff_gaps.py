"""Tests for Claude handoff gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_handoff_gaps import (
    build_claude_handoff_gaps_report,
    format_claude_handoff_gaps_json,
    format_claude_handoff_gaps_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_handoff_gaps.py"
spec = importlib.util.spec_from_file_location("claude_handoff_gaps_script", SCRIPT_PATH)
claude_handoff_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_handoff_gaps_script)


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
            role TEXT,
            tool_name TEXT,
            status TEXT,
            content TEXT,
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
    project_path: str = "/repo/presence",
    role: str | None = None,
    tool_name: str | None = None,
    status: str = "ok",
    content: str | None = None,
    error_message: str | None = None,
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, project_path, timestamp, role, tool_name, status, content,
            error_message, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            session_id,
            project_path,
            timestamp,
            role,
            tool_name,
            status,
            content,
            error_message,
            metadata_value,
        ),
    )
    conn.commit()


def test_report_groups_handoff_gap_types_from_session_events():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-clean",
        timestamp="2026-05-01T08:00:00+00:00",
        role="assistant",
        content="Next steps: run the deploy checklist and update the issue.",
    )
    _insert_event(
        conn,
        session_id="sess-missing",
        timestamp="2026-05-01T09:00:00+00:00",
        role="assistant",
        content="Implemented the report builder and formatter.",
    )
    _insert_event(
        conn,
        session_id="sess-blocker",
        timestamp="2026-05-01T10:00:00+00:00",
        role="assistant",
        content="The task is blocked by missing credentials. Need to retry once access exists.",
    )
    _insert_event(
        conn,
        session_id="sess-error",
        timestamp="2026-05-01T11:00:00+00:00",
        tool_name="Bash",
        status="failed",
        error_message="Command failed with exit code 1",
    )
    _insert_event(
        conn,
        session_id="sess-error",
        timestamp="2026-05-01T11:01:00+00:00",
        role="assistant",
        content="Tests are still failing after the change.",
    )

    report = build_claude_handoff_gaps_report(conn, days=7, limit=3, now=NOW)
    payload = json.loads(format_claude_handoff_gaps_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_handoff_gaps"
    assert payload["source_table"] == "claude_session_events"
    assert payload["totals"] == {
        "gap_count": 6,
        "groups": 4,
        "rows_scanned": 5,
        "sessions_flagged": 3,
        "sessions_scanned": 4,
    }
    groups = {group["gap_type"]: group for group in payload["groups"]}
    assert list(groups) == [
        "missing_next_step",
        "unresolved_blocker",
        "dangling_todo",
        "ended_after_error",
    ]
    assert groups["missing_next_step"]["count"] == 3
    assert groups["unresolved_blocker"]["examples"][0]["session_id"] == "sess-blocker"
    assert groups["ended_after_error"]["examples"][0]["error_excerpt"] == (
        "Command failed with exit code 1"
    )


def test_text_formatter_includes_lookback_totals_and_each_gap_type():
    rows = [
        {
            "session_id": "sess-text",
            "timestamp": "2026-05-01T09:00:00+00:00",
            "role": "assistant",
            "content": "Blocked on validation. TODO: finish it before synthesis.",
        }
    ]

    report = build_claude_handoff_gaps_report(rows, days=7, now=NOW)
    text = format_claude_handoff_gaps_text(report)

    assert "Claude Handoff Gaps" in text
    assert "Lookback: days=7" in text
    assert "Totals: sessions=1 flagged=1 gaps=3 rows=1" in text
    assert "- missing_next_step count=1" in text
    assert "- unresolved_blocker count=1" in text
    assert "- dangling_todo count=1" in text


def test_claude_messages_fallback_flags_sessions_ending_on_user_prompt():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            message_uuid TEXT,
            project_path TEXT,
            timestamp TEXT,
            prompt_text TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, project_path, timestamp, prompt_text)
           VALUES (?, ?, ?, ?, ?)""",
        (
            "sess-user",
            "uuid-1",
            "/repo/presence",
            "2026-05-01T09:00:00+00:00",
            "Please continue from the failing publish test.",
        ),
    )
    conn.commit()

    report = build_claude_handoff_gaps_report(conn, days=7, now=NOW)
    payload = report.to_dict()

    assert payload["source_table"] == "claude_messages"
    assert payload["groups"][0]["gap_type"] == "missing_next_step"
    assert payload["groups"][0]["examples"][0]["final_role"] == "user"


def test_missing_tables_and_columns_return_empty_report_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_handoff_gaps_report(conn, now=NOW)

    assert report.groups == ()
    assert report.missing_tables == (
        "claude_session_events",
        "claude_tool_events",
        "claude_events",
        "claude_messages",
    )
    assert "Missing tables:" in format_claude_handoff_gaps_text(report)

    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT
        )"""
    )
    legacy_report = build_claude_handoff_gaps_report(conn, now=NOW)

    assert legacy_report.groups == ()
    assert legacy_report.missing_columns == {"claude_session_events": ("text",)}
    assert legacy_report.totals["rows_scanned"] == 0


def test_cli_json_invalid_args_and_database_errors(db, monkeypatch, capsys):
    monkeypatch.setattr(
        claude_handoff_gaps_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_handoff_gaps_script,
        "build_claude_handoff_gaps_report",
        lambda db, **kwargs: build_claude_handoff_gaps_report(
            [
                {
                    "session_id": "sess-cli",
                    "timestamp": "2026-05-01T09:00:00+00:00",
                    "role": "assistant",
                    "content": "Need to rerun validation.",
                }
            ],
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_handoff_gaps_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = claude_handoff_gaps_script.main(["--limit", "1", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "claude_handoff_gaps"
    assert payload["groups"][0]["examples"][0]["session_id"] == "sess-cli"

    monkeypatch.setattr(
        claude_handoff_gaps_script,
        "build_claude_handoff_gaps_report",
        lambda db, **kwargs: (_ for _ in ()).throw(sqlite3.Error("boom")),
    )
    assert claude_handoff_gaps_script.main([]) == 1
    assert "error: boom" in capsys.readouterr().err
