"""Tests for reply conversation closing recommendations."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from engagement.reply_conversation_closer import (
    build_reply_conversation_closer_report,
    format_reply_conversation_closer_json,
    format_reply_conversation_closer_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "reply_conversation_closer.py"
)
spec = importlib.util.spec_from_file_location("reply_conversation_closer", SCRIPT_PATH)
reply_conversation_closer = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_conversation_closer)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            inbound_tweet_id TEXT,
            platform TEXT,
            inbound_author_handle TEXT,
            inbound_author_id TEXT,
            inbound_text TEXT,
            our_tweet_id TEXT,
            our_platform_id TEXT,
            draft_text TEXT,
            intent TEXT,
            priority TEXT,
            status TEXT,
            detected_at TEXT,
            reviewed_at TEXT,
            posted_at TEXT,
            posted_tweet_id TEXT,
            posted_platform_id TEXT
        )"""
    )
    return conn


def _insert(conn: sqlite3.Connection, **kwargs) -> int:
    defaults = {
        "inbound_tweet_id": f"inbound-{kwargs.get('id', 'x')}",
        "platform": "x",
        "inbound_author_handle": "alice",
        "inbound_author_id": "author-a",
        "inbound_text": "Thanks",
        "our_tweet_id": "our-1",
        "our_platform_id": None,
        "draft_text": "",
        "intent": "appreciation",
        "priority": "normal",
        "status": "pending",
        "detected_at": "2026-05-01T10:00:00+00:00",
        "reviewed_at": None,
        "posted_at": None,
        "posted_tweet_id": None,
        "posted_platform_id": None,
    }
    defaults.update(kwargs)
    columns = [column for column in defaults if column != "id"]
    placeholders = ", ".join("?" for _ in columns)
    cursor = conn.execute(
        f"INSERT INTO reply_queue ({', '.join(columns)}) VALUES ({placeholders})",
        [defaults[column] for column in columns],
    )
    conn.commit()
    return int(cursor.lastrowid)


def test_resolved_question_followed_by_thanks_is_closeable():
    conn = _conn()
    _insert(
        conn,
        id=1,
        inbound_tweet_id="q-1",
        inbound_text="How does the retry budget work?",
        intent="question",
        status="posted",
        detected_at="2026-05-01T08:00:00+00:00",
        posted_at="2026-05-01T08:10:00+00:00",
    )
    _insert(
        conn,
        id=2,
        inbound_tweet_id="thanks-1",
        inbound_text="That helps, thank you!",
        intent="appreciation",
        detected_at="2026-05-01T09:00:00+00:00",
    )

    report = build_reply_conversation_closer_report(conn, now=NOW)
    item = report.recommendations[0]

    assert item.action == "close_with_thanks"
    assert item.reason_codes == ("resolved_question",)
    assert any(
        "How does the retry budget work?" in snippet
        for snippet in item.evidence_snippets
    )
    assert any("thank you" in snippet for snippet in item.evidence_snippets)


def test_unresolved_direct_question_is_not_classified_as_closeable():
    conn = _conn()
    _insert(
        conn,
        inbound_tweet_id="ask-1",
        inbound_text="Could you explain how to recover failed jobs?",
        intent="question",
        detected_at="2026-05-01T11:00:00+00:00",
    )

    item = build_reply_conversation_closer_report(conn, now=NOW).recommendations[0]

    assert item.action == "answer_remaining_question"
    assert item.reason_codes == ("unresolved_direct_ask",)


def test_stale_thread_receives_no_action_with_reason_code():
    conn = _conn()
    _insert(
        conn,
        inbound_tweet_id="old-1",
        inbound_text="Nice writeup, thanks!",
        intent="appreciation",
        detected_at="2026-04-20T10:00:00+00:00",
    )

    item = build_reply_conversation_closer_report(
        conn,
        max_thread_age_hours=48,
        now=NOW,
    ).recommendations[0]

    assert item.action == "no_action"
    assert item.reason_codes == ("stale_thread",)


def test_repeated_back_and_forth_escalates_instead_of_over_engaging():
    conn = _conn()
    for index, text in enumerate(
        [
            "How should I debug the worker?",
            "I tried that and it still fails",
            "Could you look at the logs too?",
            "Still stuck after another deploy",
        ],
        start=1,
    ):
        _insert(
            conn,
            id=index,
            inbound_tweet_id=f"loop-{index}",
            inbound_text=text,
            intent="question" if "?" in text or index == 1 else "other",
            detected_at=f"2026-05-01T0{index}:00:00+00:00",
        )

    item = build_reply_conversation_closer_report(
        conn,
        min_exchange_count=4,
        now=NOW,
    ).recommendations[0]

    assert item.action == "escalate"
    assert item.reason_codes == ("unresolved_direct_ask", "repeated_back_and_forth")
    assert item.exchange_count == 4


def test_json_text_and_cli_outputs_are_stable(capsys):
    conn = _conn()
    _insert(
        conn,
        inbound_tweet_id="cli-1",
        inbound_text="Can you explain the migration order?",
        intent="question",
        inbound_author_handle="taro",
    )
    report = build_reply_conversation_closer_report(conn, now=NOW)

    assert format_reply_conversation_closer_json(
        report
    ) == format_reply_conversation_closer_json(report)
    payload = json.loads(format_reply_conversation_closer_json(report))
    assert payload["recommendations"][0]["action"] == "answer_remaining_question"
    text = format_reply_conversation_closer_text(report)
    assert "Reply Conversation Closer" in text
    assert "reasons=unresolved_direct_ask" in text

    with patch.object(
        reply_conversation_closer,
        "script_context",
        wraps=lambda: _script_context(SimpleNamespace(conn=conn)),
    ):
        assert reply_conversation_closer.main(["--format", "json"]) == 0

    cli_payload = json.loads(capsys.readouterr().out)
    assert (
        cli_payload["recommendations"][0]["thread_id"]
        == payload["recommendations"][0]["thread_id"]
    )


def test_missing_reply_queue_schema_reports_no_threads():
    conn = sqlite3.connect(":memory:")

    report = build_reply_conversation_closer_report(conn, now=NOW)

    assert report.recommendations == ()
    assert report.missing_tables == ("reply_queue",)
