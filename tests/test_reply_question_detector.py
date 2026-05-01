"""Tests for inbound reply question detection."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.reply_question_detector import (  # noqa: E402
    build_reply_question_report,
    detect_reply_questions,
    format_reply_question_report_text,
    score_reply_question,
)
from detect_reply_questions import main  # noqa: E402


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def _insert_reply(db, tweet_id: str, text: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text=text,
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_direct_questions_score_above_rhetorical_and_generic_mentions(db):
    direct = _insert_reply(
        db,
        "direct",
        "@me Can you help me understand why this fails with a timeout?",
        intent="question",
        priority="high",
    )
    rhetorical = _insert_reply(
        db,
        "rhetorical",
        "@me What could go wrong? This launch plan is chaos.",
        intent="other",
    )
    generic = _insert_reply(db, "generic", "@me interesting thread, any thoughts?")
    _set_detected_at(db, direct, "2026-04-23 10:00:00")
    _set_detected_at(db, rhetorical, "2026-04-23 10:05:00")
    _set_detected_at(db, generic, "2026-04-23 10:10:00")

    findings = detect_reply_questions(db, now=NOW, min_score=1)
    scores = {finding.mention_id: finding.score for finding in findings}

    assert scores["direct"] > scores["rhetorical"]
    assert scores["direct"] > scores["generic"]
    assert scores["direct"] >= 80
    assert "rhetorical pattern: what could go wrong" in next(
        finding.reasons for finding in findings if finding.mention_id == "rhetorical"
    )


def test_default_detection_suppresses_resolved_drafts_unless_included(db):
    pending = _insert_reply(
        db,
        "pending",
        "Could you explain how the retry budget works?",
        draft_text="The retry budget caps attempts per window.",
        status="pending",
        intent="question",
    )
    resolved = _insert_reply(
        db,
        "resolved",
        "Can you help me debug this install error?",
        draft_text="Try reinstalling the package.",
        status="posted",
        intent="question",
    )
    _set_detected_at(db, pending, "2026-04-23 09:00:00")
    _set_detected_at(db, resolved, "2026-04-23 09:05:00")

    default_ids = {finding.mention_id for finding in detect_reply_questions(db, now=NOW)}
    included = detect_reply_questions(db, now=NOW, include_resolved=True)
    included_by_id = {finding.mention_id: finding for finding in included}

    assert default_ids == {"pending"}
    assert "resolved" in included_by_id
    assert included_by_id["resolved"].resolved is True
    assert "resolved reply state" in included_by_id["resolved"].reasons


def test_report_includes_required_fields_and_text_output(db):
    reply_id = _insert_reply(
        db,
        "ask-1",
        "@presence What should I do when the worker keeps crashing after deploy?",
        inbound_author_handle="mona",
        intent="bug_report",
    )
    _set_detected_at(db, reply_id, "2026-04-23 08:00:00")

    report = build_reply_question_report(db, now=NOW)
    item = report["questions"][0]
    text = format_reply_question_report_text(report)

    assert report["total"] == 1
    assert item["mention_id"] == "ask-1"
    assert item["author"] == "mona"
    assert item["question_preview"].startswith("@presence What should I do")
    assert isinstance(item["score"], float)
    assert item["reasons"]
    assert "#ask-1 @mona score=" in text
    assert "reasons:" in text


def test_partial_or_absent_reply_queue_schema_does_not_crash():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    assert detect_reply_questions(conn, now=NOW) == []

    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, inbound_text TEXT)")
    conn.execute("INSERT INTO reply_queue (id, inbound_text) VALUES (1, 'Can you help?')")

    findings = detect_reply_questions(conn, now=NOW)

    assert len(findings) == 1
    assert findings[0].mention_id == "1"
    assert findings[0].author is None


def test_days_filter_and_min_score_are_applied(db):
    old = _insert_reply(db, "old", "Can you explain the old behavior?", intent="question")
    new = _insert_reply(db, "new", "Can you explain the new behavior?", intent="question")
    _set_detected_at(db, old, "2026-04-01 08:00:00")
    _set_detected_at(db, new, "2026-04-23 08:00:00")

    findings = detect_reply_questions(db, days=3, now=NOW)
    high_threshold = detect_reply_questions(db, days=3, min_score=200, now=NOW)

    assert [finding.mention_id for finding in findings] == ["new"]
    assert high_threshold == []


def test_cli_json_output_uses_db_path(file_db, capsys):
    reply_id = _insert_reply(
        file_db,
        "cli-ask",
        "How should I handle errors from the API?",
        intent="question",
    )
    _set_detected_at(file_db, reply_id, "2026-04-23 08:00:00")

    assert main(["--db", str(file_db.db_path), "--days", "30", "--format", "json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days"] == 30
    assert payload["questions"][0]["mention_id"] == "cli-ask"


def test_cli_text_output_uses_script_context(capsys):
    class FakeDb:
        conn = sqlite3.connect(":memory:")

    FakeDb.conn.row_factory = sqlite3.Row
    FakeDb.conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            inbound_tweet_id TEXT,
            platform TEXT,
            inbound_author_handle TEXT,
            inbound_text TEXT,
            intent TEXT,
            priority TEXT,
            status TEXT,
            detected_at TEXT,
            draft_text TEXT
        )"""
    )
    FakeDb.conn.execute(
        """INSERT INTO reply_queue
           (id, inbound_tweet_id, platform, inbound_author_handle, inbound_text, intent,
            priority, status, detected_at, draft_text)
           VALUES (1, 'ctx-ask', 'x', 'taro', 'Could you explain this API error?',
                   'question', 'normal', 'pending', '2026-04-23 08:00:00', '')"""
    )

    with patch("detect_reply_questions.script_context", _mock_script_context(FakeDb())):
        assert main(["--format", "text", "--days", "30"]) == 0

    output = capsys.readouterr().out
    assert "#ctx-ask @taro score=" in output


def test_score_reply_question_returns_none_for_empty_text():
    assert score_reply_question({"id": 1, "inbound_text": ""}) is None
