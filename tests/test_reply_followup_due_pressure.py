"""Tests for reply follow-up due pressure reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_followup_due_pressure import (
    build_reply_followup_due_pressure_report,
    format_reply_followup_due_pressure_json,
    format_reply_followup_due_pressure_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_due_pressure.py"
spec = importlib.util.spec_from_file_location("reply_followup_due_pressure_script", SCRIPT_PATH)
reply_followup_due_pressure_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_followup_due_pressure_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_followup_reminders (
            id INTEGER PRIMARY KEY, target_handle TEXT, source_type TEXT, source_id INTEGER, due_at TEXT, status TEXT
        )"""
    )
    return conn


def _reminder(conn: sqlite3.Connection, reminder_id: int, handle: str, due_at: datetime, status: str = "pending") -> None:
    conn.execute("INSERT INTO reply_followup_reminders VALUES (?, ?, 'reply_queue', ?, ?, ?)", (reminder_id, handle, reminder_id, due_at.isoformat(), status))
    conn.commit()


def test_buckets_overdue_due_soon_later_done_dismissed_and_targets():
    conn = _conn()
    _reminder(conn, 1, "alice", NOW - timedelta(hours=2))
    _reminder(conn, 2, "alice", NOW + timedelta(days=1))
    _reminder(conn, 3, "bob", NOW + timedelta(days=20))
    _reminder(conn, 4, "cam", NOW - timedelta(days=1), "done")
    _reminder(conn, 5, "dee", NOW - timedelta(days=1), "dismissed")

    report = build_reply_followup_due_pressure_report(conn, now=NOW, days_ahead=7)

    assert report["due_buckets"] == {"overdue": 1, "due_soon": 1, "later": 1, "done": 1, "dismissed": 1}
    assert report["overdue_examples"][0]["target_handle"] == "alice"
    assert report["target_handle_breakdowns"] == {"alice": 1}


def test_json_text_cli_and_schema_gaps(monkeypatch, capsys):
    conn = _conn()
    _reminder(conn, 1, "alice", NOW - timedelta(hours=2))
    report = build_reply_followup_due_pressure_report(conn, now=NOW)

    assert json.loads(format_reply_followup_due_pressure_json(report))["artifact_type"] == "reply_followup_due_pressure"
    assert "Reply Followup Due Pressure" in format_reply_followup_due_pressure_text(report)
    monkeypatch.setattr(reply_followup_due_pressure_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        reply_followup_due_pressure_script,
        "build_reply_followup_due_pressure_report",
        lambda db, **kwargs: build_reply_followup_due_pressure_report(db, now=NOW, **kwargs),
    )
    assert reply_followup_due_pressure_script.main(["--format", "text", "--limit", "1"]) == 0
    assert "Buckets: overdue=1" in capsys.readouterr().out

    missing = build_reply_followup_due_pressure_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["reply_followup_reminders"]
