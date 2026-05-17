"""Tests for thread hook outcome lag reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.thread_hook_outcome_lag import build_thread_hook_outcome_lag_report


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "thread_hook_outcome_lag.py"
spec = importlib.util.spec_from_file_location("thread_hook_outcome_lag_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY, content_type TEXT, content TEXT, published INTEGER, published_at TEXT
        )"""
    )
    conn.execute("CREATE TABLE post_engagement (content_id INTEGER, engagement_score REAL, fetched_at TEXT)")
    return SimpleNamespace(conn=conn)


def test_flags_missing_stale_and_current_hook_feedback():
    db = _db()
    rows = [
        (1, "x_thread", "What should teams measure first?\nSecond post", 1, NOW - timedelta(days=5)),
        (2, "x_thread", "What metric catches this?", 1, NOW - timedelta(days=4)),
        (3, "x_thread", "We shipped a tiny parser yesterday", 1, NOW - timedelta(days=10)),
        (4, "x_thread", "We built a cadence monitor", 1, NOW - timedelta(days=11)),
        (5, "x_thread", "Plain release notes for today", 1, NOW - timedelta(days=2)),
    ]
    for row in rows:
        db.conn.execute("INSERT INTO generated_content VALUES (?, ?, ?, ?, ?)", (*row[:4], row[4].isoformat()))
    db.conn.execute("INSERT INTO post_engagement VALUES (?, ?, ?)", (1, 10.0, (NOW - timedelta(days=2)).isoformat()))
    db.conn.execute("INSERT INTO post_engagement VALUES (?, ?, ?)", (2, 11.0, (NOW - timedelta(days=1)).isoformat()))
    db.conn.execute("INSERT INTO post_engagement VALUES (?, ?, ?)", (3, 4.0, (NOW - timedelta(days=40)).isoformat()))
    db.conn.execute("INSERT INTO post_engagement VALUES (?, ?, ?)", (4, 4.0, (NOW - timedelta(days=35)).isoformat()))
    db.conn.commit()

    report = build_thread_hook_outcome_lag_report(db, stale_after_days=21, now=NOW)

    statuses = {item["style"]: item["status"] for item in report["styles"]}
    assert statuses["question"] == "current"
    assert statuses["build-log"] == "stale"
    assert statuses["plain-summary"] == "missing"
    assert report["totals"]["metric_count"] == 4


def test_cli_json_and_table_output(monkeypatch, capsys):
    db = _db()
    db.conn.execute(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?)",
        (1, "x_thread", "What changed this week?", 1, (NOW - timedelta(days=1)).isoformat()),
    )
    db.conn.commit()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_thread_hook_outcome_lag_report",
        lambda db, **kwargs: build_thread_hook_outcome_lag_report(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "thread_hook_outcome_lag"
    assert script.main(["--table"]) == 0
    assert "Thread Hook Outcome Lag" in capsys.readouterr().out
