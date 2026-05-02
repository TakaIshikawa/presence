"""Tests for Claude decision mining report."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_decision_miner import (
    build_claude_decision_miner_report,
    format_claude_decision_miner_json,
    format_claude_decision_miner_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "mine_claude_decisions.py"
spec = importlib.util.spec_from_file_location("mine_claude_decisions_script", SCRIPT_PATH)
mine_claude_decisions_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(mine_claude_decisions_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _message_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            message_uuid TEXT,
            project_path TEXT,
            timestamp TEXT,
            prompt_text TEXT,
            response_text TEXT
        )"""
    )
    return conn


def _insert_message(
    conn: sqlite3.Connection,
    *,
    session_id: str = "sess-1",
    timestamp: str = "2026-05-01T10:00:00+00:00",
    project_path: str = "/repo/presence",
    prompt_text: str | None = None,
    response_text: str | None = None,
) -> None:
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, project_path, timestamp, prompt_text, response_text)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            session_id,
            f"uuid-{session_id}-{timestamp}",
            project_path,
            timestamp,
            prompt_text,
            response_text,
        ),
    )
    conn.commit()


def test_extracts_decisions_tradeoffs_and_deferred_alternatives_by_session():
    conn = _message_db()
    _insert_message(
        conn,
        session_id="sess-a",
        prompt_text=(
            "We decided to put the miner in ingestion because it reads raw "
            "claude_messages. Rejected adding an LLM pass instead of regex for now."
        ),
    )
    _insert_message(
        conn,
        session_id="sess-b",
        timestamp="2026-05-01T11:00:00+00:00",
        prompt_text="Defer the migration column until the schema report proves it is needed.",
    )

    report = build_claude_decision_miner_report(conn, days=7, min_confidence=0.7, now=NOW)

    assert report.totals["rows_scanned"] == 2
    assert report.totals["decision_count"] == 3
    assert [session.session_id for session in report.sessions] == ["sess-a", "sess-b"]
    first_decisions = report.sessions[0].decisions
    assert first_decisions[0].decision_type == "technical_decision"
    assert first_decisions[0].project_path == "/repo/presence"
    assert first_decisions[1].decision_type == "rejected_alternative"
    assert report.sessions[1].decisions[0].decision_type == "deferred_alternative"


def test_min_confidence_threshold_filters_lower_confidence_tradeoffs():
    conn = _message_db()
    _insert_message(
        conn,
        prompt_text="Use JSON because the report needs deterministic automation output.",
    )

    low = build_claude_decision_miner_report(conn, min_confidence=0.7, now=NOW)
    high = build_claude_decision_miner_report(conn, min_confidence=0.95, now=NOW)

    assert low.totals["decision_count"] == 1
    assert high.totals["decision_count"] == 0


def test_json_output_is_deterministic_and_text_groups_sessions():
    conn = _message_db()
    _insert_message(
        conn,
        session_id="sess-json",
        prompt_text="Decision is to keep the CLI thin because script_context owns database setup.",
    )

    report = build_claude_decision_miner_report(conn, days=30, now=NOW)
    payload = json.loads(format_claude_decision_miner_json(report))
    text = format_claude_decision_miner_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_decision_miner"
    assert payload["filters"]["days"] == 30
    assert payload["sessions"][0]["session_id"] == "sess-json"
    assert "Claude Decision Miner" in text
    assert "Decisions by session:" in text
    assert "session=sess-json" in text


def test_malformed_and_empty_rows_are_counted_without_crashing():
    rows = [
        {
            "session_id": "sess-empty",
            "project_path": "/repo",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "prompt_text": "",
        },
        {
            "session_id": None,
            "project_path": "/repo",
            "timestamp": None,
            "prompt_text": "We chose the sqlite table because the report is local.",
        },
        {},
    ]

    report = build_claude_decision_miner_report(rows, days=7, min_confidence=0.5, now=NOW)

    assert report.totals["empty_rows"] == 2
    assert report.totals["malformed_rows"] == 1
    assert report.sessions[0].session_id == "unknown-session"


def test_missing_schema_returns_gaps_without_aborting():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_claude_decision_miner_report(conn, now=NOW)
    text = format_claude_decision_miner_text(report)

    assert report.sessions == ()
    assert report.schema_gaps["missing_tables"] == ["claude_messages"]
    assert "Missing tables: claude_messages" in text


def test_missing_required_columns_are_reported_without_aborting():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            project_path TEXT,
            response_text TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO claude_messages (project_path, response_text) VALUES (?, ?)",
        ("/repo", "We decided to keep response text scanning because prompts can be absent."),
    )
    conn.commit()

    report = build_claude_decision_miner_report(conn, min_confidence=0.5, now=NOW)

    assert report.schema_gaps["missing_columns"] == {
        "claude_messages": ["session_id", "timestamp", "prompt_text"]
    }
    assert report.totals["decision_count"] == 1
    assert report.sessions[0].session_id == "unknown-session"


def test_cli_argument_validation_and_script_context(db, monkeypatch, capsys):
    monkeypatch.setattr(
        mine_claude_decisions_script,
        "script_context",
        lambda: _script_context(db),
    )

    assert mine_claude_decisions_script.main(["--days", "0"]) == 2
    assert mine_claude_decisions_script.main(["--min-confidence", "0"]) == 2
    exit_code = mine_claude_decisions_script.main(
        ["--days", "7", "--min-confidence", "0.5", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "claude_decision_miner"
