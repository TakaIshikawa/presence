"""Tests for Claude session test evidence gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_test_evidence_gap import (
    build_claude_test_evidence_gap_report,
    format_claude_test_evidence_gap_json,
    format_claude_test_evidence_gap_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_test_evidence_gap.py"
spec = importlib.util.spec_from_file_location("claude_test_evidence_gap_script", SCRIPT_PATH)
claude_test_evidence_gap_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_test_evidence_gap_script)


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
            project_path TEXT,
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
    session_id: str,
    timestamp: str,
    tool_name: str | None = None,
    content: str | None = None,
    metadata: dict | str | None = None,
    project_path: str = "/repo/presence",
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        """INSERT INTO claude_session_events
           (session_id, timestamp, project_path, tool_name, status, content, metadata)
           VALUES (?, ?, ?, ?, 'ok', ?, ?)""",
        (session_id, timestamp, project_path, tool_name, content, metadata_value),
    )
    conn.commit()


def test_report_flags_implementation_sessions_without_test_evidence():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-gap",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Edit",
        content="Fixed a regression in src/ingestion/claude_logs.py behavior.",
        metadata={"file_path": "src/ingestion/claude_logs.py"},
    )
    _insert_event(
        conn,
        session_id="sess-tested",
        timestamp="2026-05-01T11:00:00+00:00",
        tool_name="Edit",
        content="Implemented scripts/report.py and ran uv run pytest tests/test_report.py.",
        metadata={"file_path": "scripts/report.py"},
    )

    report = build_claude_test_evidence_gap_report(conn, days=7, limit=10, now=NOW)
    payload = json.loads(format_claude_test_evidence_gap_json(report))
    text = format_claude_test_evidence_gap_text(report)

    assert payload["artifact_type"] == "claude_test_evidence_gap"
    assert payload["totals"]["sessions_scanned"] == 2
    assert payload["totals"]["implementation_session_count"] == 2
    assert payload["totals"]["sessions_with_test_evidence"] == 1
    assert payload["totals"]["gap_count"] == 1
    assert payload["gaps"][0]["session_id"] == "sess-gap"
    assert payload["gaps"][0]["date"] == "2026-05-01"
    assert payload["gaps"][0]["changed_files"] == ["src/ingestion/claude_logs.py"]
    assert payload["gaps"][0]["missing_evidence_reason"] == (
        "implementation_or_fix_signals_without_nearby_test_evidence"
    )
    assert "source_edit_tool" in payload["gaps"][0]["evidence_signals"]
    assert "run targeted tests for src/ingestion/claude_logs.py" == (
        payload["gaps"][0]["suggested_follow_up"]
    )
    assert "Claude Test Evidence Gap" in text
    assert "session=sess-gap" in text


def test_sessions_with_explicit_test_evidence_are_not_reported_as_gaps():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-test-file",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Edit",
        content="Changed src/output/preview.py and tests/test_preview.py.",
        metadata={"files": ["src/output/preview.py", "tests/test_preview.py"]},
    )

    report = build_claude_test_evidence_gap_report(conn, days=7, limit=10, now=NOW)
    text = format_claude_test_evidence_gap_text(report)

    assert report.totals["implementation_session_count"] == 1
    assert report.totals["sessions_with_test_evidence"] == 1
    assert report.gaps == ()
    assert "No Claude test evidence gaps found." in text


def test_missing_optional_columns_are_reported_without_blocking_scan():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            prompt_text TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO claude_messages (session_id, timestamp, prompt_text)
           VALUES (?, ?, ?)""",
        (
            "sess-message-gap",
            "2026-05-01T09:00:00+00:00",
            "Please fix the bug in scripts/claude_test_evidence_gap.py",
        ),
    )
    conn.commit()

    report = build_claude_test_evidence_gap_report(conn, days=7, limit=5, now=NOW)
    payload = json.loads(format_claude_test_evidence_gap_json(report))

    assert payload["source_tables"] == ["claude_messages"]
    assert "claude_messages" in payload["missing_columns"]
    assert payload["totals"]["gap_count"] == 1
    assert payload["gaps"][0]["session_id"] == "sess-message-gap"


def test_empty_database_returns_no_gaps_state_and_schema_gap():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_test_evidence_gap_report(conn, now=NOW)
    text = format_claude_test_evidence_gap_text(report)

    assert report.missing_tables == (
        "claude_session_events",
        "claude_tool_events",
        "claude_events",
        "claude_messages",
    )
    assert report.totals["sessions_scanned"] == 0
    assert report.gaps == ()
    assert "Missing tables: claude_session_events" in text
    assert "No Claude test evidence gaps found." in text


def test_json_ordering_is_stable_and_limit_is_applied():
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-b",
        timestamp="2026-05-01T11:00:00+00:00",
        tool_name="Edit",
        content="Updated src/b.py",
    )
    _insert_event(
        conn,
        session_id="sess-a",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Edit",
        content="Updated src/a.py",
    )

    first = build_claude_test_evidence_gap_report(conn, days=7, limit=1, now=NOW)
    payload = json.loads(format_claude_test_evidence_gap_json(first))

    assert list(payload) == sorted(payload)
    assert payload["totals"]["gap_count"] == 2
    assert [gap["session_id"] for gap in payload["gaps"]] == ["sess-a"]


def test_cli_validates_positive_arguments_and_emits_json(db, monkeypatch, capsys):
    monkeypatch.setattr(
        claude_test_evidence_gap_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_test_evidence_gap_script,
        "build_claude_test_evidence_gap_report",
        lambda db, **kwargs: build_claude_test_evidence_gap_report(
            _fixture_db(),
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_test_evidence_gap_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = claude_test_evidence_gap_script.main(
        ["--days", "7", "--limit", "1", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "claude_test_evidence_gap"
    assert payload["filters"]["days"] == 7
    assert payload["gaps"][0]["session_id"] == "sess-cli"


def _fixture_db() -> sqlite3.Connection:
    conn = _event_db()
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:00:00+00:00",
        tool_name="Edit",
        content="Fix behavior in scripts/claude_test_evidence_gap.py",
    )
    return conn
