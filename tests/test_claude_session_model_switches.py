"""Tests for Claude session model switch reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from ingestion.claude_session_model_switches import (
    build_claude_session_model_switches_report,
    format_claude_session_model_switches_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "claude_session_model_switches.py"
spec = importlib.util.spec_from_file_location("claude_session_model_switches_script", SCRIPT_PATH)
claude_session_model_switches_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_model_switches_script)


def _event_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            model TEXT,
            metadata TEXT
        )"""
    )
    return conn


def _insert_event(
    conn: sqlite3.Connection,
    *,
    session_id: str = "sess-a",
    timestamp: str = "2026-05-01T10:00:00+00:00",
    model: str | None = "claude-sonnet-4",
    metadata: str | dict | None = None,
) -> None:
    metadata_value = json.dumps(metadata, sort_keys=True) if isinstance(metadata, dict) else metadata
    conn.execute(
        "INSERT INTO claude_session_events (session_id, timestamp, model, metadata) VALUES (?, ?, ?, ?)",
        (session_id, timestamp, model, metadata_value),
    )
    conn.commit()


def test_iterable_rows_report_adjacent_model_changes():
    rows = [
        {
            "session_id": "sess-switch",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "model": "Claude Sonnet 4",
            "event_id": "evt-1",
        },
        {
            "session_id": "sess-switch",
            "timestamp": "2026-05-01T10:01:00+00:00",
            "model": "Claude Sonnet 4",
            "event_id": "evt-2",
        },
        {
            "session_id": "sess-switch",
            "timestamp": "2026-05-01T10:02:00+00:00",
            "model": "Claude Opus 4",
            "event_id": "evt-3",
        },
    ]

    report = build_claude_session_model_switches_report(rows, days=7, now=NOW)
    payload = json.loads(format_claude_session_model_switches_json(report))

    assert payload["artifact_type"] == "claude_session_model_switches"
    assert list(payload) == sorted(payload)
    assert len(report.rows) == 1
    assert report.rows[0].from_model == "claude-sonnet-4"
    assert report.rows[0].to_model == "claude-opus-4"
    assert report.rows[0].previous_event_id == "evt-2"
    assert report.rows[0].next_event_id == "evt-3"
    assert report.rows[0].switch_id.startswith("claude_session_model_switch_")


def test_sqlite_loading_and_metadata_derived_model_names():
    conn = _event_db()
    _insert_event(conn, model=None, metadata={"response": {"model": "Claude Haiku 4"}})
    _insert_event(
        conn,
        timestamp="2026-05-01T10:01:00+00:00",
        model=None,
        metadata={"request": {"model": "Claude Sonnet 4"}},
    )

    report = build_claude_session_model_switches_report(conn, days=7, now=NOW)

    assert report.source_tables == ("claude_session_events",)
    assert len(report.rows) == 1
    assert report.rows[0].from_model == "claude-haiku-4"
    assert report.rows[0].to_model == "claude-sonnet-4"


def test_filters_apply_to_session_and_either_side_of_switch():
    rows = [
        {"session_id": "target", "timestamp": "2026-05-01T10:00:00+00:00", "model": "a", "id": "1"},
        {"session_id": "target", "timestamp": "2026-05-01T10:01:00+00:00", "model": "b", "id": "2"},
        {"session_id": "target", "timestamp": "2026-05-01T10:02:00+00:00", "model": "c", "id": "3"},
        {"session_id": "other", "timestamp": "2026-05-01T10:00:00+00:00", "model": "b", "id": "4"},
        {"session_id": "other", "timestamp": "2026-05-01T10:01:00+00:00", "model": "d", "id": "5"},
    ]

    report = build_claude_session_model_switches_report(
        rows,
        days=7,
        session_id="target",
        model="b",
        now=NOW,
    )

    assert report.filters["model"] == "b"
    assert [(row.from_model, row.to_model) for row in report.rows] == [("a", "b"), ("b", "c")]


def test_blank_models_are_normalized_to_unknown():
    rows = [
        {"session_id": "sess", "timestamp": "2026-05-01T10:00:00+00:00", "model": "", "id": "1"},
        {"session_id": "sess", "timestamp": "2026-05-01T10:01:00+00:00", "model": "Claude Sonnet", "id": "2"},
    ]

    report = build_claude_session_model_switches_report(rows, days=7, now=NOW)

    assert report.rows[0].from_model == "unknown"
    assert report.rows[0].to_model == "claude-sonnet"


def test_cli_json_output(capsys, tmp_path):
    db_path = tmp_path / "events.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_session_events (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            timestamp TEXT,
            model TEXT,
            metadata TEXT
        )"""
    )
    _insert_event(conn, session_id="sess-cli", model="claude-haiku")
    _insert_event(
        conn,
        session_id="sess-cli",
        timestamp="2026-05-01T10:01:00+00:00",
        model="claude-sonnet",
    )
    conn.close()

    assert (
        claude_session_model_switches_script.main(
            ["--db", str(db_path), "--days", "7", "--model", "claude-sonnet"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["rows"][0]["session_id"] == "sess-cli"
    assert payload["rows"][0]["to_model"] == "claude-sonnet"
    assert claude_session_model_switches_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
