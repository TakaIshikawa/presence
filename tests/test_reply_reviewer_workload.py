"""Tests for reply reviewer workload reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_reviewer_workload import (
    build_reply_reviewer_workload_report,
    format_reply_reviewer_workload_json,
    format_reply_reviewer_workload_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_reviewer_workload.py"
spec = importlib.util.spec_from_file_location("reply_reviewer_workload_script", SCRIPT_PATH)
reply_reviewer_workload_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_reviewer_workload_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _reply(db, *, platform="x", priority="normal", quality_score=7.0, status="pending", detected_at=None) -> int:
    cursor = db.conn.execute(
        """INSERT INTO reply_queue
           (inbound_tweet_id, platform, inbound_author_handle, inbound_text, our_tweet_id,
            draft_text, priority, quality_score, status, detected_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            f"in-{platform}-{priority}-{quality_score}-{detected_at}",
            platform,
            "alice",
            "question?",
            "our-1",
            "reply draft",
            priority,
            quality_score,
            status,
            (detected_at or NOW).isoformat(),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_groups_pending_workload_by_platform_priority_when_no_owner_columns(db):
    high = _reply(db, platform="x", priority="high", quality_score=9, detected_at=NOW - timedelta(hours=3))
    normal = _reply(db, platform="bluesky", priority="normal", quality_score=4, detected_at=NOW - timedelta(days=2))
    low = _reply(db, platform="x", priority="low", quality_score=8, detected_at=NOW - timedelta(days=1))
    _reply(db, platform="x", priority="high", status="posted", detected_at=NOW - timedelta(hours=1))

    report = build_reply_reviewer_workload_report(db, now=NOW)
    payload = json.loads(format_reply_reviewer_workload_json(report))
    text = format_reply_reviewer_workload_text(report)

    assert payload["totals"]["pending_reply_count"] == 2
    assert payload["totals"]["by_platform"] == {"bluesky": 1, "x": 1}
    assert payload["totals"]["quality_bands"] == {"low": 1, "high": 1}
    assert {item["reply_id"] for item in payload["representative_replies"]} == {high, normal}
    assert low not in {item["reply_id"] for item in payload["representative_replies"]}
    assert "owner=x:high" in text


def test_include_low_priority_flag_adds_low_priority_replies(db):
    low = _reply(db, priority="low", detected_at=NOW - timedelta(hours=1))

    report = build_reply_reviewer_workload_report(db, include_low_priority=True, now=NOW)

    assert report["totals"]["pending_reply_count"] == 1
    assert report["representative_replies"][0]["reply_id"] == low


def test_reviewer_owner_assignee_and_metadata_columns_are_used():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            priority TEXT,
            quality_score REAL,
            status TEXT,
            detected_at TEXT,
            draft_text TEXT,
            reviewer TEXT,
            owner TEXT,
            assignee TEXT,
            metadata TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO reply_queue VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, "x", "high", 8.5, "pending", NOW.isoformat(), "draft", "riley", None, None, "{}"),
    )
    conn.execute(
        "INSERT INTO reply_queue VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (2, "x", "normal", None, "pending", NOW.isoformat(), "draft", None, None, None, '{"owner": "ops"}'),
    )
    conn.commit()
    try:
        report = build_reply_reviewer_workload_report(conn, now=NOW)
    finally:
        conn.close()

    owners = {group["workload_owner"] for group in report["workload_groups"]}
    assert owners == {"riley", "ops"}
    assert report["totals"]["quality_bands"] == {"high": 1, "unscored": 1}


def test_minimal_reply_queue_schema_does_not_crash():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, status TEXT)")
    conn.execute("INSERT INTO reply_queue VALUES (?, ?)", (1, "pending"))
    conn.commit()
    try:
        report = build_reply_reviewer_workload_report(conn, now=NOW)
    finally:
        conn.close()

    assert report["totals"]["pending_reply_count"] == 1
    assert report["missing_columns"]["reply_queue"] == [
        "detected_at",
        "draft_text",
        "platform",
        "priority",
        "quality_score",
    ]


def test_cli_supports_json_output(db, monkeypatch, capsys):
    reply_id = _reply(db)
    monkeypatch.setattr(reply_reviewer_workload_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        reply_reviewer_workload_script,
        "build_reply_reviewer_workload_report",
        lambda db, **kwargs: build_reply_reviewer_workload_report(db, now=NOW, **kwargs),
    )

    exit_code = reply_reviewer_workload_script.main(["--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["representative_replies"][0]["reply_id"] == reply_id


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(reply_reviewer_workload_script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        reply_reviewer_workload_script,
        "build_reply_reviewer_workload_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    assert reply_reviewer_workload_script.main([]) == 1
    assert "error: db failed" in capsys.readouterr().err
