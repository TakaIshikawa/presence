"""Tests for reply intent mix reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_intent_mix import (
    build_reply_intent_mix_report,
    format_reply_intent_mix_json,
    format_reply_intent_mix_text,
    infer_reply_intent,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_intent_mix.py"
spec = importlib.util.spec_from_file_location("reply_intent_mix_script", SCRIPT_PATH)
reply_intent_mix_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_intent_mix_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, inbound_id: str, inbound_text: str, draft_text: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text=inbound_text,
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text=draft_text,
        intent="other",
        priority="normal",
        status="pending",
        platform="x",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_fields(db, reply_id: int, **fields) -> None:
    assignments = ", ".join(f"{field} = ?" for field in fields)
    db.conn.execute(
        f"UPDATE reply_queue SET {assignments} WHERE id = ?",
        (*fields.values(), reply_id),
    )
    db.conn.commit()


def test_report_groups_explicit_intents_by_platform_tier_status_and_outcome(db):
    relationship = json.dumps({"tier_name": "Key Network", "dunbar_tier": 2})
    _insert_reply(
        db,
        "question",
        "Can you explain this?",
        "Use the smaller batch first.",
        intent="question",
        platform="x",
        relationship_context=relationship,
    )
    sent_id = _insert_reply(
        db,
        "sent",
        "Thanks for the post",
        "Appreciate it.",
        intent="appreciation",
        platform="bluesky",
        status="posted",
    )
    _set_fields(db, sent_id, posted_at="2026-05-01T10:00:00+00:00")

    report = build_reply_intent_mix_report(db, now=NOW)

    assert report["counts"]["rows_scanned"] == 2
    assert report["counts"]["pending_replies"] == 1
    assert report["counts"]["reviewed_replies"] == 1
    assert report["by_intent"] == [
        {"intent": "question", "count": 1, "share": 0.5},
        {"intent": "thanks", "count": 1, "share": 0.5},
    ]
    assert {
        (
            group["intent"],
            group["platform"],
            group["relationship_tier"],
            group["status"],
            group["review_outcome"],
        )
        for group in report["groups"]
    } == {
        ("question", "x", "Key Network (tier 2)", "pending", "pending"),
        ("thanks", "bluesky", "unknown", "posted", "sent"),
    }


def test_fallback_inference_when_classifier_columns_are_absent():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            status TEXT,
            inbound_text TEXT,
            draft_text TEXT,
            relationship_context TEXT,
            detected_at TEXT
        )"""
    )
    conn.executemany(
        """INSERT INTO reply_queue
           (id, platform, status, inbound_text, draft_text, relationship_context, detected_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (1, "x", "pending", "How should I debug this?", "Check the logs.", None, "2026-05-01T01:00:00+00:00"),
            (2, "x", "pending", "Thanks for writing this", "Glad it helped.", None, "2026-05-01T02:00:00+00:00"),
            (3, "x", "pending", "Actually the command is wrong", "Good catch.", None, "2026-05-01T03:00:00+00:00"),
            (4, "x", "pending", "The deploy failed with an error", "Try rerunning it.", None, "2026-05-01T04:00:00+00:00"),
            (5, "x", "pending", "Nice update", "Thanks for sharing.", None, "2026-05-01T05:00:00+00:00"),
        ],
    )

    report = build_reply_intent_mix_report(conn, now=NOW)

    assert [item["intent"] for item in report["by_intent"]] == [
        "correction",
        "generic",
        "question",
        "support",
        "thanks",
    ]
    assert report["counts"]["generic_share"] == 0.2


def test_platform_filter_and_pending_only_exclude_reviewed_and_sent(db):
    pending_id = _insert_reply(
        db,
        "x-pending",
        "How?",
        "Use logs.",
        intent="question",
        platform="x",
    )
    reviewed_id = _insert_reply(
        db,
        "x-reviewed",
        "Thanks!",
        "Appreciate it.",
        intent="appreciation",
        platform="x",
        status="reviewed",
    )
    sent_id = _insert_reply(
        db,
        "bsky-sent",
        "Can you help?",
        "Try this.",
        intent="question",
        platform="bluesky",
        status="posted",
    )
    _set_fields(db, reviewed_id, reviewed_at="2026-05-01T09:00:00+00:00")
    _set_fields(db, sent_id, posted_at="2026-05-01T10:00:00+00:00")
    _set_fields(db, pending_id, detected_at="2026-01-01T10:00:00+00:00")

    x_report = build_reply_intent_mix_report(db, platform="x", now=NOW)
    pending_report = build_reply_intent_mix_report(db, include_reviewed=False, now=NOW)

    assert x_report["counts"]["rows_scanned"] == 2
    assert [group["platform"] for group in x_report["groups"]] == ["x", "x"]
    assert pending_report["counts"]["rows_scanned"] == 1
    assert pending_report["counts"]["pending_replies"] == 1
    assert pending_report["counts"]["reviewed_replies"] == 0


def test_generic_share_recommendation_and_stable_output(db):
    _insert_reply(db, "generic-1", "Nice", "Thanks for sharing.", intent="other")
    _insert_reply(db, "generic-2", "Interesting", "Appreciate the note.", intent="unknown")
    _insert_reply(db, "question", "How do I fix this?", "Run the repair.", intent="question")

    report = build_reply_intent_mix_report(db, now=NOW)
    payload = json.loads(format_reply_intent_mix_json(report))
    text = format_reply_intent_mix_text(report)

    assert payload["counts"]["generic_share"] == 0.6667
    assert payload["recommendations"]
    assert list(payload.keys()) == sorted(payload.keys())
    assert "Reply Intent Mix Report" in text
    assert "generic_share=66.7%" in text
    assert "Intents:" in text


def test_missing_reply_queue_schema_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_intent_mix_report(conn, now=NOW)

    assert report["counts"] == {
        "rows_scanned": 0,
        "pending_replies": 0,
        "reviewed_replies": 0,
        "generic_replies": 0,
        "generic_share": 0.0,
    }
    assert report["groups"] == []


def test_infer_reply_intent_prefers_explicit_classifier_columns():
    assert infer_reply_intent(
        {"classification_intent": "support_request", "inbound_text": "Thanks"}
    ) == "support"
    assert infer_reply_intent({"inbound_text": "Can you help?"}, columns=set()) == "question"


def test_cli_supports_pending_only_and_json_output(db, monkeypatch, capsys):
    _insert_reply(db, "pending", "How?", "Use logs.", intent="question")
    reviewed_id = _insert_reply(
        db,
        "reviewed",
        "Thanks!",
        "Appreciate it.",
        intent="appreciation",
        status="reviewed",
    )
    _set_fields(db, reviewed_id, reviewed_at="2026-05-01T10:00:00+00:00")
    monkeypatch.setattr(reply_intent_mix_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        reply_intent_mix_script,
        "build_reply_intent_mix_report",
        lambda db, **kwargs: build_reply_intent_mix_report(db, now=NOW, **kwargs),
    )

    exit_code = reply_intent_mix_script.main(["--pending-only", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["include_reviewed"] is False
    assert payload["counts"]["rows_scanned"] == 1
    assert payload["by_intent"] == [{"intent": "question", "count": 1, "share": 1.0}]
