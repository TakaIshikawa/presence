"""Tests for scripts/reply_actions.py."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from reply_actions import build_payload, fetch_reply_rows, format_text_output, main


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def _insert_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="How does this handle retries?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="It retries idempotent operations.",
        intent="question",
        priority="normal",
        platform="x",
        status="pending",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_fetch_reply_rows_filters_by_platform_status_and_days(db):
    old_x = _insert_reply(db, "old-x", platform="x", status="pending")
    fresh_x = _insert_reply(db, "fresh-x", platform="x", status="pending")
    _insert_reply(db, "fresh-bsky", platform="bluesky", status="pending")
    _insert_reply(db, "posted-x", platform="x", status="posted")
    _set_detected_at(db, old_x, "2026-04-20T09:00:00+00:00")
    _set_detected_at(db, fresh_x, "2026-04-23T09:00:00+00:00")

    rows = fetch_reply_rows(
        db,
        platform="x",
        status="pending",
        days=1,
        now=NOW,
    )

    assert [row["inbound_tweet_id"] for row in rows] == ["fresh-x"]


def test_format_text_output_groups_by_action():
    rows = [
        {
            "id": 1,
            "platform": "x",
            "status": "pending",
            "inbound_author_handle": "alice",
            "inbound_tweet_id": "one",
            "inbound_text": "How does this handle retries?",
            "draft_text": "With backoff.",
            "intent": "question",
            "priority": "normal",
            "quality_score": 8.0,
            "quality_flags": "[]",
            "detected_at": "2026-04-23T09:00:00+00:00",
        },
        {
            "id": 2,
            "platform": "x",
            "status": "pending",
            "inbound_author_handle": "bob",
            "inbound_tweet_id": "two",
            "inbound_text": "Thanks, great post",
            "draft_text": "Thanks.",
            "intent": "appreciation",
            "priority": "low",
            "quality_score": None,
            "quality_flags": None,
            "detected_at": "2026-04-23T10:00:00+00:00",
        },
    ]
    from engagement.reply_action_recommender import ReplyActionRecommender

    output = format_text_output(ReplyActionRecommender().recommend_many(rows))

    assert "reply_now (1)" in output
    assert "no_response (1)" in output
    assert "#1 x       @alice" in output
    assert "low-signal mention" in output


def test_build_payload_json_shape():
    from engagement.reply_action_recommender import ReplyActionRecommender

    recommendations = ReplyActionRecommender().recommend_many(
        [
            {
                "id": 1,
                "platform": "x",
                "status": "pending",
                "inbound_author_handle": "alice",
                "inbound_tweet_id": "one",
                "inbound_text": "How does this handle retries?",
                "draft_text": "With backoff.",
                "intent": "question",
                "priority": "normal",
            }
        ]
    )

    payload = build_payload(
        recommendations,
        filters={"platform": "x", "status": "pending", "days": None, "limit": None},
        generated_at=NOW,
    )

    assert payload["generated_at"] == "2026-04-23T12:00:00+00:00"
    assert payload["by_action"] == {"reply_now": 1}
    assert payload["recommendations"][0]["action"] == "reply_now"


def test_main_json_output_is_read_only(db, capsys):
    _insert_reply(db, "json-row")

    with patch("reply_actions.script_context", _mock_script_context(db)):
        assert main(["--json", "--platform", "x"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["platform"] == "x"
    assert payload["recommendations"][0]["action"] == "reply_now"
    stored = db.conn.execute("SELECT status FROM reply_queue WHERE inbound_tweet_id = 'json-row'").fetchone()
    assert stored["status"] == "pending"
