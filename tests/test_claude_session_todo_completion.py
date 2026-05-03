"""Tests for Claude session todo completion reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_session_todo_completion import (
    build_claude_session_todo_completion_report,
    format_claude_session_todo_completion_json,
    format_claude_session_todo_completion_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "claude_session_todo_completion.py"
)
spec = importlib.util.spec_from_file_location("claude_session_todo_completion_script", SCRIPT_PATH)
claude_session_todo_completion_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_todo_completion_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_message(
    db,
    *,
    session_id: str,
    message_uuid: str,
    prompt_text: str = "Work on the report",
    project_path: str = "/repo/presence",
    timestamp: datetime | None = None,
) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=(timestamp or NOW).isoformat(),
        prompt_text=prompt_text,
    )


def _add_event_table(db) -> None:
    db.conn.execute(
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
    db.conn.commit()


def _add_event(
    db,
    *,
    session_id: str,
    timestamp: datetime | None = None,
    metadata: dict | str | None = None,
    content: str | None = None,
    project_path: str = "/repo/presence",
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    db.conn.execute(
        """INSERT INTO claude_session_events
           (session_id, project_path, timestamp, tool_name, status, content, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            session_id,
            project_path,
            (timestamp or NOW).isoformat(),
            "TodoWrite",
            "success",
            content,
            metadata_value,
        ),
    )
    db.conn.commit()


def test_structured_todo_events_are_grouped_by_session_status(db):
    _add_event_table(db)
    _add_message(db, session_id="sess-complete", message_uuid="msg-complete")
    _add_message(db, session_id="sess-incomplete", message_uuid="msg-incomplete")
    _add_event(
        db,
        session_id="sess-complete",
        metadata={
            "tool_use": {
                "name": "TodoWrite",
                "input": {
                    "todos": [
                        {"content": "implement report", "status": "completed"},
                        {"content": "add tests", "status": "completed"},
                        {"content": "document skipped follow-up", "status": "canceled"},
                    ]
                },
            }
        },
    )
    _add_event(
        db,
        session_id="sess-incomplete",
        metadata={
            "todos": [
                {"content": "wire cli", "status": "in_progress"},
                {"content": "run validation", "status": "pending"},
            ]
        },
    )

    report = build_claude_session_todo_completion_report(db, days=7, now=NOW)
    payload = json.loads(format_claude_session_todo_completion_json(report))
    by_session = {session["session_id"]: session for session in payload["sessions"]}

    assert payload["artifact_type"] == "claude_session_todo_completion"
    assert list(payload) == sorted(payload)
    assert payload["totals"]["sessions_scanned"] == 2
    assert payload["totals"]["sessions_with_todos"] == 2
    assert payload["totals"]["todo_count"] == 5
    assert payload["totals"]["completed"] == 2
    assert payload["totals"]["pending"] == 1
    assert payload["totals"]["in_progress"] == 1
    assert payload["totals"]["canceled"] == 1
    assert payload["totals"]["completion_rate"] == 0.4
    assert by_session["sess-complete"]["status_counts"] == {
        "canceled": 1,
        "completed": 2,
        "in_progress": 0,
        "pending": 0,
    }
    assert by_session["sess-complete"]["completion_rate"] == 0.6667
    assert by_session["sess-incomplete"]["incomplete_count"] == 2


def test_markdown_todo_markers_in_messages_are_counted(db):
    _add_message(
        db,
        session_id="sess-markdown",
        message_uuid="msg-markdown",
        prompt_text=(
            "Plan:\n"
            "- [x] Build the source module\n"
            "- [ ] Add CLI coverage\n"
            "- [~] Drop stale branch cleanup\n"
            "- in_progress: validate tests"
        ),
    )

    report = build_claude_session_todo_completion_report(db, days=7, now=NOW)
    session = report.sessions[0]
    text = format_claude_session_todo_completion_text(report)

    assert session.session_id == "sess-markdown"
    assert session.status_counts == {
        "pending": 1,
        "in_progress": 1,
        "completed": 1,
        "canceled": 1,
    }
    assert session.completion_rate == 0.25
    assert "completion_rate=25.0%" in text
    assert "sessions_with_todos=1" in text


def test_sessions_without_todo_evidence_are_reported_without_errors(db):
    _add_message(db, session_id="sess-empty", message_uuid="msg-empty")

    report = build_claude_session_todo_completion_report(db, days=7, now=NOW)
    payload = report.to_dict()

    assert payload["totals"]["sessions_scanned"] == 1
    assert payload["totals"]["sessions_with_todos"] == 0
    assert payload["totals"]["todo_count"] == 0
    assert payload["totals"]["completion_rate"] is None
    assert payload["sessions"][0]["session_id"] == "sess-empty"
    assert payload["sessions"][0]["todo_count"] == 0
    assert payload["sessions"][0]["completion_rate"] is None


def test_days_and_limit_are_applied_deterministically(db):
    _add_message(
        db,
        session_id="sess-old",
        message_uuid="msg-old",
        prompt_text="- [ ] outside window",
        timestamp=NOW - timedelta(days=30),
    )
    _add_message(
        db,
        session_id="sess-a",
        message_uuid="msg-a",
        prompt_text="- [ ] pending a",
        timestamp=NOW - timedelta(hours=2),
    )
    _add_message(
        db,
        session_id="sess-b",
        message_uuid="msg-b",
        prompt_text="- [x] completed b",
        timestamp=NOW - timedelta(hours=1),
    )

    report = build_claude_session_todo_completion_report(db, days=7, limit=1, now=NOW)

    assert report.totals["sessions_scanned"] == 2
    assert report.totals["todo_count"] == 2
    assert [session.session_id for session in report.sessions] == ["sess-a"]


def test_missing_claude_messages_table_is_structured():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_session_todo_completion_report(conn, days=7, now=NOW)

    assert report.sessions == ()
    assert report.totals["sessions_scanned"] == 0
    assert report.missing_tables == ("claude_messages",)


def test_cli_defaults_to_json_and_supports_days_and_limit(db, monkeypatch, capsys):
    _add_message(
        db,
        session_id="sess-cli",
        message_uuid="msg-cli",
        prompt_text="- [x] ship cli",
    )
    monkeypatch.setattr(
        claude_session_todo_completion_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_session_todo_completion_script,
        "build_claude_session_todo_completion_report",
        lambda db, **kwargs: build_claude_session_todo_completion_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_session_todo_completion_script.main(["--days", "7", "--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["filters"]["days"] == 7
    assert payload["filters"]["limit"] == 5
    assert payload["sessions"][0]["session_id"] == "sess-cli"
    assert payload["totals"]["completion_rate"] == 1.0

    assert claude_session_todo_completion_script.main(["--format", "text"]) == 0
    assert "Claude Session Todo Completion" in capsys.readouterr().out
    assert claude_session_todo_completion_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
