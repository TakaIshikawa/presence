"""Tests for reply author re-engagement candidate reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_reengagement_candidates import (
    build_reply_reengagement_candidates_report,
    format_reply_reengagement_candidates_json,
    format_reply_reengagement_candidates_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_reengagement_candidates.py"
spec = importlib.util.spec_from_file_location("reply_reengagement_candidates_script", SCRIPT_PATH)
reply_reengagement_candidates_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_reengagement_candidates_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _reply_db(*, optional_tables: bool = True) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            inbound_author_handle TEXT,
            intent TEXT,
            priority TEXT,
            quality_score REAL,
            status TEXT,
            relationship_context TEXT,
            detected_at TEXT,
            reviewed_at TEXT,
            posted_at TEXT,
            posted_tweet_id TEXT,
            posted_platform_id TEXT
        )"""
    )
    if optional_tables:
        conn.execute(
            """CREATE TABLE reply_followup_reminders (
                id INTEGER PRIMARY KEY,
                target_handle TEXT NOT NULL,
                status TEXT NOT NULL,
                due_at TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE proactive_actions (
                id INTEGER PRIMARY KEY,
                target_author_handle TEXT,
                status TEXT,
                created_at TEXT,
                reviewed_at TEXT,
                posted_at TEXT
            )"""
        )
    return conn


def _insert_reply(
    conn: sqlite3.Connection,
    *,
    reply_id: int,
    handle: str,
    status: str = "posted",
    platform: str = "x",
    intent: str = "other",
    priority: str = "normal",
    score: float | None = None,
    relationship: dict[str, object] | None = None,
    detected_at: str = "2026-04-01T09:00:00+00:00",
    reviewed_at: str | None = None,
    posted_at: str | None = None,
    posted_tweet_id: str | None = None,
) -> None:
    conn.execute(
        """INSERT INTO reply_queue
           (id, platform, inbound_author_handle, intent, priority, quality_score,
            status, relationship_context, detected_at, reviewed_at, posted_at,
            posted_tweet_id, posted_platform_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            reply_id,
            platform,
            handle,
            intent,
            priority,
            score,
            status,
            json.dumps(relationship) if relationship is not None else None,
            detected_at,
            reviewed_at,
            posted_at,
            posted_tweet_id,
            None,
        ),
    )
    conn.commit()


def test_scores_and_sorts_reengagement_candidates():
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        handle="@HighValue",
        intent="question",
        priority="high",
        score=9.0,
        relationship={"tier": "trusted", "strength": 0.85},
        posted_at="2026-03-25T09:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=2,
        handle="highvalue",
        intent="bug_report",
        priority="high",
        score=8.0,
        relationship={"tier": "trusted", "strength": 0.8},
        posted_at="2026-04-01T09:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=3,
        handle="casual",
        intent="other",
        priority="normal",
        score=6.0,
        posted_at="2026-03-20T09:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=4,
        handle="pending",
        status="pending",
        score=10.0,
        detected_at="2026-03-20T09:00:00+00:00",
    )

    report = build_reply_reengagement_candidates_report(conn, days=90, min_age_days=14, now=NOW)
    payload = json.loads(format_reply_reengagement_candidates_json(report))
    text = format_reply_reengagement_candidates_text(report)

    assert payload["artifact_type"] == "reply_reengagement_candidates"
    assert payload["totals"]["rows_scanned"] == 3
    assert [candidate["handle"] for candidate in payload["candidates"]] == [
        "highvalue",
        "casual",
    ]
    first = payload["candidates"][0]
    assert first["interaction_count"] == 2
    assert first["avg_quality_score"] == 8.5
    assert first["high_priority_count"] == 2
    assert first["question_intent_count"] == 2
    assert first["relationship_tier"] == "trusted"
    assert first["relationship_strength"] == 0.85
    assert first["score"] > payload["candidates"][1]["score"]
    assert first["excluded_by_cooldown"] is False
    assert "x/@highvalue" in text
    assert "actionable=2" in text


def test_pending_reminders_and_recent_proactive_actions_mark_cooldown():
    conn = _reply_db()
    _insert_reply(
        conn,
        reply_id=1,
        handle="remindme",
        score=8.0,
        posted_at="2026-03-01T09:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=2,
        handle="proactive",
        score=8.0,
        posted_at="2026-03-01T09:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=3,
        handle="fresh",
        score=9.0,
        posted_at="2026-04-25T09:00:00+00:00",
    )
    _insert_reply(
        conn,
        reply_id=4,
        handle="open",
        score=7.0,
        posted_at="2026-03-01T09:00:00+00:00",
    )
    conn.execute(
        "INSERT INTO reply_followup_reminders (target_handle, status, due_at) VALUES (?, ?, ?)",
        ("@RemindMe", "pending", "2026-05-10T00:00:00+00:00"),
    )
    conn.execute(
        """INSERT INTO proactive_actions
           (target_author_handle, status, created_at)
           VALUES (?, ?, ?)""",
        ("Proactive", "posted", "2026-04-28T00:00:00+00:00"),
    )
    conn.commit()

    report = build_reply_reengagement_candidates_report(conn, min_age_days=14, now=NOW)
    by_handle = {candidate.handle: candidate for candidate in report.candidates}

    assert by_handle["open"].excluded_by_cooldown is False
    assert by_handle["remindme"].excluded_by_cooldown is True
    assert by_handle["remindme"].cooldown_reasons == ("pending_followup_reminder",)
    assert by_handle["proactive"].excluded_by_cooldown is True
    assert by_handle["proactive"].cooldown_reasons == ("recent_proactive_action",)
    assert by_handle["fresh"].excluded_by_cooldown is True
    assert by_handle["fresh"].cooldown_reasons == ("last_interaction_too_recent",)
    assert report.totals["actionable_candidates"] == 1
    assert report.totals["cooldown_excluded"] == 3


def test_missing_optional_tables_still_returns_reply_queue_candidates():
    conn = _reply_db(optional_tables=False)
    _insert_reply(
        conn,
        reply_id=1,
        handle="solo",
        status="approved",
        score=7.0,
        reviewed_at="2026-04-01T09:00:00+00:00",
    )

    report = build_reply_reengagement_candidates_report(conn, now=NOW)

    assert [candidate.handle for candidate in report.candidates] == ["solo"]
    assert report.missing_tables == ("reply_followup_reminders", "proactive_actions")
    assert report.candidates[0].last_interaction_at == "2026-04-01T09:00:00+00:00"
    assert "Missing optional tables: reply_followup_reminders, proactive_actions" in (
        format_reply_reengagement_candidates_text(report)
    )


def test_missing_reply_queue_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_reengagement_candidates_report(conn, now=NOW)

    assert report.candidates == ()
    assert report.missing_tables == ("reply_queue",)
    assert report.totals["rows_scanned"] == 0


def test_cli_validates_args_and_emits_json(monkeypatch, capsys):
    db = _reply_db(optional_tables=False)
    _insert_reply(
        db,
        reply_id=1,
        handle="cli",
        status="approved",
        reviewed_at="2026-04-01T09:00:00+00:00",
    )
    monkeypatch.setattr(
        reply_reengagement_candidates_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_reengagement_candidates_script,
        "build_reply_reengagement_candidates_report",
        lambda db, **kwargs: build_reply_reengagement_candidates_report(db, now=NOW, **kwargs),
    )

    assert reply_reengagement_candidates_script.main(["--min-age-days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    assert reply_reengagement_candidates_script.main(
        ["--days", "45", "--min-age-days", "7", "--format", "json"]
    ) == 0
    payload = json.loads(capsys.readouterr().out)

    assert list(payload) == sorted(payload)
    assert payload["filters"]["days"] == 45
    assert payload["filters"]["min_age_days"] == 7
