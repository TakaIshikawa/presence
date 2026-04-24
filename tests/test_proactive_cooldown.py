"""Tests for proactive engagement cooldown guards."""

from datetime import datetime, timedelta, timezone

from engagement.proactive_cooldown import (
    ProactiveCooldownPolicy,
    evaluate_proactive_cooldown,
    find_author_conflicts,
    find_target_conflicts,
)


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _insert_action(db, **overrides):
    defaults = dict(
        action_type="reply",
        target_tweet_id="tweet-1",
        target_tweet_text="Target tweet",
        target_author_handle="alice",
    )
    defaults.update(overrides)
    return db.insert_proactive_action(**defaults)


def _normalized_action(action_id, **overrides):
    defaults = {
        "source": "presence",
        "id": action_id,
        "action_type": "reply",
        "target_handle": "alice",
        "target_tweet_id": "tweet-2",
    }
    defaults.update(overrides)
    return defaults


def test_author_conflicts_include_recent_posted_and_approved_actions(db):
    posted_id = _insert_action(db, target_tweet_id="posted", target_author_handle="Alice")
    approved_id = _insert_action(
        db, target_tweet_id="approved", target_author_handle="@ALICE"
    )
    old_id = _insert_action(db, target_tweet_id="old", target_author_handle="alice")
    db.mark_proactive_posted(posted_id, "posted-result")
    db.conn.execute(
        "UPDATE proactive_actions SET status='approved', reviewed_at=? WHERE id=?",
        ((NOW - timedelta(hours=2)).isoformat(), approved_id),
    )
    db.conn.execute(
        "UPDATE proactive_actions SET status='posted', posted_at=? WHERE id=?",
        ((NOW - timedelta(hours=80)).isoformat(), old_id),
    )
    db.conn.commit()

    conflicts = find_author_conflicts(
        db,
        target_author_handle="@alice",
        cooldown_hours=72,
        now=NOW,
    )

    assert {row["id"] for row in conflicts} == {posted_id, approved_id}


def test_author_cooldown_blocks_pending_action_with_recent_approved_action(db):
    approved_id = _insert_action(db, target_tweet_id="approved")
    pending_id = _insert_action(db, target_tweet_id="pending")
    db.conn.execute(
        "UPDATE proactive_actions SET status='approved', reviewed_at=? WHERE id=?",
        ((NOW - timedelta(hours=1)).isoformat(), approved_id),
    )
    db.conn.commit()

    result = evaluate_proactive_cooldown(
        db,
        _normalized_action(pending_id, target_tweet_id="pending"),
        ProactiveCooldownPolicy(author_cooldown_hours=72, target_cooldown_hours=0),
        now=NOW,
    )

    assert result.blocked is True
    assert "posted/approved" in result.reason


def test_target_conflicts_block_newer_duplicate_pending_action(db):
    older_id = _insert_action(
        db,
        action_type="reply",
        target_tweet_id="same-target",
        target_author_handle="alice",
    )
    newer_id = _insert_action(
        db,
        action_type="quote_tweet",
        target_tweet_id="same-target",
        target_author_handle="bob",
    )
    db.conn.execute(
        "UPDATE proactive_actions SET created_at=? WHERE id=?",
        ((NOW - timedelta(hours=1)).isoformat(), older_id),
    )
    db.conn.execute(
        "UPDATE proactive_actions SET created_at=? WHERE id=?",
        (NOW.isoformat(), newer_id),
    )
    db.conn.commit()

    conflicts = find_target_conflicts(
        db,
        target_tweet_id="same-target",
        current_action_id=newer_id,
        cooldown_hours=24,
        now=NOW,
    )
    survivor_conflicts = find_target_conflicts(
        db,
        target_tweet_id="same-target",
        current_action_id=older_id,
        cooldown_hours=24,
        now=NOW,
    )

    assert [row["id"] for row in conflicts] == [older_id]
    assert survivor_conflicts == []


def test_target_cooldown_ignores_actions_outside_window(db):
    old_id = _insert_action(db, target_tweet_id="same-target")
    current_id = _insert_action(
        db,
        action_type="quote_tweet",
        target_tweet_id="same-target",
        target_author_handle="bob",
    )
    db.conn.execute(
        "UPDATE proactive_actions SET status='posted', posted_at=? WHERE id=?",
        ((NOW - timedelta(hours=25)).isoformat(), old_id),
    )
    db.conn.commit()

    result = evaluate_proactive_cooldown(
        db,
        _normalized_action(
            current_id,
            action_type="quote_tweet",
            target_handle="bob",
            target_tweet_id="same-target",
        ),
        ProactiveCooldownPolicy(author_cooldown_hours=0, target_cooldown_hours=24),
        now=NOW,
    )

    assert result.blocked is False
