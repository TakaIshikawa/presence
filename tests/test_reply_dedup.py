"""Tests for reply draft deduplication."""

from datetime import datetime, timezone

from engagement.reply_dedup import (
    find_duplicate_reply_draft,
    normalize_reply_text,
    reply_similarity,
)


def test_normalize_reply_text_ignores_urls_handles_and_punctuation():
    assert (
        normalize_reply_text("@alice Thanks for this! https://example.com")
        == "thanks for this"
    )


def test_reply_similarity_matches_near_identical_drafts():
    assert reply_similarity("Thanks for sharing this.", "thanks for sharing this!") == 1.0
    assert reply_similarity("Thanks for sharing this.", "I disagree with this.") < 0.90


def test_find_duplicate_reply_draft_matches_recent_same_author(db):
    reply_id = db.insert_reply_draft(
        inbound_tweet_id="inbound-1",
        inbound_author_handle="Alice",
        inbound_author_id="user-a",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=1,
        our_post_text="Original",
        draft_text="Thanks for sharing this.",
    )
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        ("2026-04-23T10:00:00+00:00", reply_id),
    )
    db.conn.commit()

    match = find_duplicate_reply_draft(
        db=db,
        draft_text="thanks for sharing this!",
        author_handle="@alice",
        platform_target_id="different-target",
        lookback_hours=72,
        now=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert match is not None
    assert match.source_table == "reply_queue"
    assert match.id == reply_id
    assert match.reason == "same_author"


def test_find_duplicate_reply_draft_respects_lookback(db):
    reply_id = db.insert_reply_draft(
        inbound_tweet_id="inbound-old",
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=1,
        our_post_text="Original",
        draft_text="Thanks for sharing this.",
    )
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        ("2026-04-20T10:00:00+00:00", reply_id),
    )
    db.conn.commit()

    assert (
        find_duplicate_reply_draft(
            db=db,
            draft_text="Thanks for sharing this!",
            author_handle="alice",
            platform_target_id=None,
            lookback_hours=24,
            now=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
        )
        is None
    )


def test_find_duplicate_reply_draft_matches_recent_proactive_same_target(db):
    action_id = db.insert_proactive_action(
        action_type="reply",
        target_tweet_id="target-1",
        target_tweet_text="Interesting thread",
        target_author_handle="bob",
        draft_text="This framing is useful.",
    )
    db.conn.execute(
        "UPDATE proactive_actions SET created_at = ? WHERE id = ?",
        ("2026-04-23T10:00:00+00:00", action_id),
    )
    db.conn.commit()

    match = find_duplicate_reply_draft(
        db=db,
        draft_text="This framing is useful!",
        author_handle="carol",
        platform_target_id="target-1",
        lookback_hours=72,
        now=datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert match is not None
    assert match.source_table == "proactive_actions"
    assert match.id == action_id
    assert match.reason == "same_platform_target"
