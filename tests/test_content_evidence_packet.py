"""Tests for generated-content evidence packets."""

from __future__ import annotations

import json

from storage.db import Database


def test_get_content_evidence_packet_returns_none_for_missing_content(db):
    assert db.get_content_evidence_packet(99999) is None


def test_get_content_evidence_packet_combines_content_sources_guards_and_feedback(db):
    db.insert_commit(
        "presence",
        "sha-evidence",
        "feat: add evidence packet",
        "2026-05-01T12:00:00+00:00",
        "taka",
    )
    db.insert_claude_message(
        "session-evidence",
        "msg-evidence",
        "/repo",
        "2026-05-01T11:50:00+00:00",
        "Build evidence packet API",
    )
    db.upsert_github_activity(
        repo_name="presence",
        activity_type="issue",
        number=42,
        title="Need evidence packets",
        state="open",
        author="taka",
        url="https://github.test/presence/issues/42",
        updated_at="2026-05-01T10:00:00+00:00",
        labels=["synthesis"],
        metadata={"priority": "high"},
    )
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-evidence"],
        source_messages=["msg-evidence"],
        source_activity_ids=["presence#42:issue"],
        content="Evidence packet generated copy.",
        eval_score=8.2,
        eval_feedback="clear",
        claim_check_summary={
            "supported_count": 2,
            "unsupported_count": 1,
            "annotation_text": "One claim needs source support.",
        },
        persona_guard_summary={
            "checked": True,
            "passed": False,
            "status": "failed",
            "score": 0.4,
            "reasons": ["too generic"],
            "metrics": {"phrase_overlap": 0.01},
        },
    )
    db.add_content_feedback(
        content_id,
        "reject",
        "Unsupported claim.",
        tags=["unsupported_claim"],
    )

    packet = db.get_content_evidence_packet(content_id)

    assert packet["id"] == content_id
    assert packet["content"] == "Evidence packet generated copy."
    assert packet["content_type"] == "x_post"
    assert packet["source_commits"][0]["commit_sha"] == "sha-evidence"
    assert packet["source_commits"][0]["matched"] is True
    assert packet["source_messages"][0]["message_uuid"] == "msg-evidence"
    assert packet["source_github_activity"][0]["activity_id"] == "presence#42:issue"
    assert packet["source_github_activity"][0]["metadata"] == {"priority": "high"}
    assert packet["claim_check"]["unsupported_count"] == 1
    assert packet["persona_guard"]["reasons"] == ["too generic"]
    assert packet["feedback"][0]["tags"] == ["unsupported_claim"]
    json.dumps(packet)


def test_get_content_evidence_packet_tolerates_missing_optional_tables():
    db = Database(":memory:")
    db.connect()
    db.conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_type TEXT NOT NULL,
            source_commits TEXT,
            source_messages TEXT,
            source_activity_ids TEXT,
            content TEXT NOT NULL,
            eval_score REAL,
            eval_feedback TEXT
        )"""
    )
    db.conn.execute(
        """INSERT INTO generated_content
           (content_type, source_commits, source_messages, source_activity_ids,
            content, eval_score, eval_feedback)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "x_post",
            '["missing-sha"]',
            '["missing-msg"]',
            '["presence#1:issue"]',
            "Partial schema content.",
            6.0,
            "",
        ),
    )
    db.conn.commit()

    try:
        packet = db.get_content_evidence_packet(1)
    finally:
        db.close()

    assert packet["source_commits"] == [
        {"commit_sha": "missing-sha", "source_index": 0, "matched": False}
    ]
    assert packet["source_messages"] == [
        {"message_uuid": "missing-msg", "source_index": 0, "matched": False}
    ]
    assert packet["source_github_activity"] == [
        {"activity_id": "presence#1:issue", "source_index": 0, "matched": False}
    ]
    assert packet["claim_check"] is None
    assert packet["persona_guard"] is None
    assert packet["feedback"] == []
    json.dumps(packet)
