"""Tests for reply follow-up completion lag reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.reply_followup_completion_lag import (
    build_reply_followup_completion_lag_report_from_db,
    format_reply_followup_completion_lag_json,
    format_reply_followup_completion_lag_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_completion_lag.py"
spec = importlib.util.spec_from_file_location("reply_followup_completion_lag_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE reply_followup_reminders (
             id INTEGER PRIMARY KEY,
             source_type TEXT,
             target_handle TEXT,
             status TEXT,
             due_at TEXT,
             completed_at TEXT,
             dismissed_at TEXT
           );"""
    )
    return conn


def test_report_groups_lag_by_source_and_target():
    conn = _conn()
    conn.executemany(
        """INSERT INTO reply_followup_reminders
           (id, source_type, target_handle, status, due_at, completed_at, dismissed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (1, "mention", "@a", "pending", "2026-05-20T09:00:00+00:00", None, None),
            (2, "mention", "@a", "completed", "2026-05-20T08:00:00+00:00", "2026-05-20T10:30:00+00:00", None),
            (3, "reply", "@b", "dismissed", "2026-05-20T07:00:00+00:00", None, "2026-05-20T08:30:00+00:00"),
            (4, "reply", "@b", "completed", "2026-05-20T10:00:00+00:00", "2026-05-20T09:30:00+00:00", None),
            (5, "reply", "@b", "pending", "2026-05-20T13:00:00+00:00", None, None),
        ],
    )

    report = build_reply_followup_completion_lag_report_from_db(conn, now=NOW, min_lag_hours=1)

    assert report["artifact_type"] == "reply_followup_completion_lag"
    assert report["summary"]["overdue_pending_count"] == 1
    assert report["summary"]["completed_late_count"] == 1
    assert report["summary"]["dismissed_late_count"] == 1
    assert {(row["source_type"], row["target_handle"]): row for row in report["grouped_summaries"]}[("mention", "@a")]["completed_late_count"] == 1
    assert [item["bucket"] for item in report["late_reminders"]] == ["overdue_pending", "completed_late", "dismissed_late"]
    assert report["late_reminders"][0]["lag_hours"] == 3.0


def test_min_lag_hours_filters_late_examples_but_not_group_counts():
    conn = _conn()
    conn.execute(
        """INSERT INTO reply_followup_reminders
           (id, source_type, target_handle, status, due_at, completed_at)
           VALUES (1, 'mention', '@a', 'completed', '2026-05-20T08:00:00+00:00', '2026-05-20T08:30:00+00:00')"""
    )
    report = build_reply_followup_completion_lag_report_from_db(conn, now=NOW, min_lag_hours=1)

    assert report["late_reminders"] == []
    assert report["grouped_summaries"][0]["completed_late_count"] == 1


def test_formatters_cli_and_schema(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute(
        """INSERT INTO reply_followup_reminders
           (id, source_type, target_handle, status, due_at)
           VALUES (1, 'mention', '@a', 'pending', '2026-05-20T09:00:00+00:00')"""
    )
    report = build_reply_followup_completion_lag_report_from_db(conn, now=NOW)
    assert json.loads(format_reply_followup_completion_lag_json(report))["artifact_type"] == "reply_followup_completion_lag"
    assert "overdue_pending=1" in format_reply_followup_completion_lag_text(report)

    db_path = tmp_path / "followups.sqlite"
    conn.commit()
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat(), "--min-lag-hours", "0"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["overdue_pending_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Follow-up Completion Lag" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json", "--now", NOW.isoformat()]) == 0
    assert json.loads(capsys.readouterr().out)["late_reminders"] == []


def test_missing_schema_and_invalid_min_lag():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_followup_reminders (id INTEGER PRIMARY KEY)")
    report = build_reply_followup_completion_lag_report_from_db(conn, now=NOW)
    assert report["missing_columns"] == {"reply_followup_reminders": ["due_at"]}
    with pytest.raises(ValueError, match="min_lag_hours must be non-negative"):
        build_reply_followup_completion_lag_report_from_db(_conn(), min_lag_hours=-1)
