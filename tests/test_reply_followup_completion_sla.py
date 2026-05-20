"""Tests for reply follow-up completion SLA reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.reply_followup_completion_sla import (
    build_reply_followup_completion_sla_report_from_db,
    format_reply_followup_completion_sla_json,
    format_reply_followup_completion_sla_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_followup_completion_sla.py"
spec = importlib.util.spec_from_file_location("reply_followup_completion_sla_script", SCRIPT_PATH)
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
             target_handle TEXT,
             status TEXT,
             due_at TEXT,
             completed_at TEXT,
             dismissed_at TEXT
           );"""
    )
    return conn


def test_report_counts_overdue_late_and_backlog():
    conn = _conn()
    conn.executemany(
        """INSERT INTO reply_followup_reminders
           (id, target_handle, status, due_at, completed_at, dismissed_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        [
            (1, "@a", "pending", "2026-05-20T09:00:00+00:00", None, None),
            (2, "@a", "completed", "2026-05-20T08:00:00+00:00", "2026-05-20T10:30:00+00:00", None),
            (3, "@b", "dismissed", "2026-05-20T07:00:00+00:00", None, "2026-05-20T08:30:00+00:00"),
            (4, "@b", "pending", "2026-05-20T13:00:00+00:00", None, None),
        ],
    )

    report = build_reply_followup_completion_sla_report_from_db(conn, now=NOW, grace_hours=1)

    assert report["artifact_type"] == "reply_followup_completion_sla"
    assert report["overdue_count"] == 1
    assert report["late_completed_count"] == 1
    assert report["late_dismissed_count"] == 1
    assert report["target_backlog"] == {"@a": 1, "@b": 1}
    assert [finding["issue_type"] for finding in report["findings"]] == [
        "overdue_pending",
        "completed_after_due",
        "dismissed_after_due",
    ]
    assert report["findings"][0]["lateness_hours"] == 3.0


def test_target_filter_limits_findings_and_backlog():
    conn = _conn()
    conn.executemany(
        "INSERT INTO reply_followup_reminders (id, target_handle, status, due_at) VALUES (?, ?, ?, ?)",
        [(1, "@a", "pending", "2026-05-20T09:00:00+00:00"), (2, "@b", "pending", "2026-05-20T09:00:00+00:00")],
    )
    report = build_reply_followup_completion_sla_report_from_db(conn, now=NOW, target_handle="@b")

    assert report["overdue_count"] == 1
    assert report["target_backlog"] == {"@b": 1}
    assert report["findings"][0]["target_handle"] == "@b"


def test_formatters_cli_and_schema(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute("INSERT INTO reply_followup_reminders (id, target_handle, status, due_at) VALUES (1, '@a', 'pending', '2026-05-20T09:00:00+00:00')")
    conn.commit()
    report = build_reply_followup_completion_sla_report_from_db(conn, now=NOW)
    assert json.loads(format_reply_followup_completion_sla_json(report))["artifact_type"] == "reply_followup_completion_sla"
    assert "overdue=1" in format_reply_followup_completion_sla_text(report)

    db_path = tmp_path / "followups.sqlite"
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat(), "--grace-hours", "0"]) == 0
    assert json.loads(capsys.readouterr().out)["overdue_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text", "--target-handle", "@a"]) == 0
    assert "Reply Follow-up Completion SLA" in capsys.readouterr().out

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json", "--now", NOW.isoformat()]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []


def test_missing_schema_and_invalid_grace():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_followup_reminders (id INTEGER PRIMARY KEY)")
    report = build_reply_followup_completion_sla_report_from_db(conn, now=NOW)
    assert report["missing_columns"] == {"reply_followup_reminders": ["due_at"]}
    with pytest.raises(ValueError, match="grace_hours must be non-negative"):
        build_reply_followup_completion_sla_report_from_db(_conn(), grace_hours=-1)
