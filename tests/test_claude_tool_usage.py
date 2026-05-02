"""Tests for Claude Code tool usage export."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from synthesis.claude_tool_usage import (
    build_claude_tool_usage_report,
    format_claude_tool_usage_json,
    format_claude_tool_usage_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_claude_tool_usage.py"
spec = importlib.util.spec_from_file_location("export_claude_tool_usage_script", SCRIPT_PATH)
export_claude_tool_usage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_claude_tool_usage_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_message(
    db,
    *,
    message_uuid: str,
    prompt_text: str,
    session_id: str = "sess-1",
    project_path: str = "/repo/presence",
    timestamp: str = "2026-05-01T12:00:00+00:00",
) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=timestamp,
        prompt_text=prompt_text,
    )


def test_returns_per_session_tool_counts_and_aggregate_totals(db):
    _add_message(
        db,
        message_uuid="uuid-1",
        session_id="sess-a",
        prompt_text="Run bash with rg for the failing pytest, then git status.",
    )
    _add_message(
        db,
        message_uuid="uuid-2",
        session_id="sess-a",
        prompt_text="Use read and edit after the error from pytest.",
    )
    _add_message(
        db,
        message_uuid="uuid-3",
        session_id="sess-b",
        prompt_text="write the docs after git diff.",
    )

    report = build_claude_tool_usage_report(db, days=7, now=NOW)
    by_session = {session["session_id"]: session for session in report["sessions"]}

    assert report["aggregate_tool_counts"] == {
        "bash": 1,
        "edit": 1,
        "git": 2,
        "pytest": 2,
        "read": 1,
        "rg": 1,
        "write": 1,
    }
    assert by_session["sess-a"]["message_count"] == 2
    assert by_session["sess-a"]["tool_counts"]["pytest"] == 2
    assert by_session["sess-b"]["tool_counts"] == {"git": 1, "write": 1}
    assert report["totals"]["session_count"] == 2
    assert report["totals"]["tool_mention_count"] == 9


def test_project_path_filtering_applies_when_column_exists(db):
    _add_message(
        db,
        message_uuid="uuid-presence",
        project_path="/repo/presence",
        prompt_text="bash rg pytest",
    )
    _add_message(
        db,
        message_uuid="uuid-other",
        project_path="/repo/other",
        prompt_text="git read write",
    )

    report = build_claude_tool_usage_report(
        db,
        days=7,
        project_path="/repo/presence",
        now=NOW,
    )

    assert report["filters"]["project_path_filter_applied"] is True
    assert report["totals"]["message_count"] == 1
    assert report["aggregate_tool_counts"] == {"bash": 1, "pytest": 1, "rg": 1}


def test_project_path_filter_degrades_when_column_is_absent():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            message_uuid TEXT,
            timestamp TEXT,
            prompt_text TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, timestamp, prompt_text)
           VALUES (?, ?, ?, ?)""",
        ("sess-no-project", "uuid-no-project", "2026-05-01T12:00:00+00:00", "bash git"),
    )

    report = build_claude_tool_usage_report(
        conn,
        days=7,
        project_path="/repo/presence",
        now=NOW,
    )

    assert report["filters"]["project_path_filter_applied"] is False
    assert report["missing_columns"] == {
        "claude_messages": ["project_path", "response_text"]
    }
    assert report["totals"]["message_count"] == 1
    assert report["sessions"][0]["project_path"] is None


def test_response_text_and_error_interruption_indicators_are_counted_separately():
    rows = [
        {
            "session_id": "sess-error",
            "project_path": "/repo/presence",
            "timestamp": "2026-05-01T12:00:00+00:00",
            "prompt_text": "Run bash and rg before pytest.",
            "response_text": "The pytest command failed with exit code 1; user cancelled.",
        },
        {
            "session_id": "sess-clean",
            "project_path": "/repo/presence",
            "timestamp": "2026-05-01T12:30:00+00:00",
            "prompt_text": "read the file and edit the fixture.",
            "response_text": "write completed.",
        },
    ]

    report = build_claude_tool_usage_report(rows, days=7, now=NOW)
    by_session = {session["session_id"]: session for session in report["sessions"]}

    assert by_session["sess-error"]["tool_counts"] == {
        "bash": 1,
        "pytest": 2,
        "rg": 1,
    }
    assert by_session["sess-error"]["error_indicator_count"] == 2
    assert by_session["sess-error"]["interruption_indicator_count"] == 1
    assert by_session["sess-clean"]["error_indicator_count"] == 0
    assert report["high_error_interruption_sessions"][0]["session_id"] == "sess-error"
    assert report["totals"]["error_indicator_count"] == 2
    assert report["totals"]["interruption_indicator_count"] == 1


def test_json_and_text_output_are_stable(db):
    _add_message(
        db,
        message_uuid="uuid-json",
        prompt_text="bash rg pytest error aborted",
    )

    report = build_claude_tool_usage_report(db, days=7, limit=5, now=NOW)
    payload = json.loads(format_claude_tool_usage_json(report))
    text = format_claude_tool_usage_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_tool_usage"
    assert payload["sessions"][0]["tool_counts"] == {"bash": 1, "pytest": 1, "rg": 1}
    assert "Claude Tool Usage" in text
    assert "Aggregate tool counts:" in text
    assert "High error/interruption co-occurrence:" in text


def test_cli_supports_days_project_path_limit_and_json_output(db, monkeypatch, capsys):
    _add_message(
        db,
        message_uuid="uuid-cli",
        project_path="/repo/presence",
        prompt_text="bash git pytest",
    )
    monkeypatch.setattr(
        export_claude_tool_usage_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        export_claude_tool_usage_script,
        "build_claude_tool_usage_report",
        lambda db, **kwargs: build_claude_tool_usage_report(db, now=NOW, **kwargs),
    )

    exit_code = export_claude_tool_usage_script.main(
        [
            "--days",
            "7",
            "--project-path",
            "/repo/presence",
            "--limit",
            "3",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["days"] == 7
    assert payload["filters"]["limit"] == 3
    assert payload["filters"]["project_path"] == "/repo/presence"
    assert payload["aggregate_tool_counts"] == {"bash": 1, "git": 1, "pytest": 1}
