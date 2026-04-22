"""Tests for resolve_actions — tweet resolution step."""

import json
import sqlite3
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import tweepy

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from resolve_actions import (
    parse_execution_type,
    is_already_resolved,
    select_tweet_for_action,
    build_resolved_payload,
    resolve_actions,
    _get_x_user_id,
    _resolve_single_action,
)
from engagement.cultivate_bridge import CultivateBridge, ProactiveAction, PersonContext


# --- Helpers ---

CULTIVATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS people (
    id TEXT PRIMARY KEY,
    x_user_id TEXT UNIQUE NOT NULL,
    x_handle TEXT NOT NULL,
    display_name TEXT NOT NULL,
    bio TEXT,
    followers_count INTEGER,
    following_count INTEGER,
    is_self INTEGER DEFAULT 0,
    first_seen TEXT NOT NULL,
    last_updated TEXT NOT NULL,
    relationship_strength REAL,
    engagement_stage INTEGER,
    dunbar_tier INTEGER,
    authenticity_score REAL,
    content_quality_score REAL,
    content_relevance_score REAL,
    verified INTEGER DEFAULT 0,
    profile_image_url TEXT,
    suspended INTEGER DEFAULT 0,
    blocked INTEGER DEFAULT 0,
    prev_relationship_strength REAL,
    cluster_id INTEGER,
    centrality_degree REAL,
    centrality_betweenness REAL
);

CREATE TABLE IF NOT EXISTS interactions (
    id TEXT PRIMARY KEY,
    actor_person_id TEXT NOT NULL,
    target_person_id TEXT NOT NULL,
    interaction_type TEXT NOT NULL,
    x_tweet_id TEXT,
    content_snippet TEXT,
    occurred_at TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    UNIQUE(x_tweet_id, interaction_type)
);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,
    payload TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    processed INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS actions (
    id TEXT PRIMARY KEY,
    decision_id TEXT,
    action_type TEXT NOT NULL,
    target_person_id TEXT NOT NULL,
    description TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'suggested',
    created_at TEXT NOT NULL,
    completed_at TEXT,
    payload TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def _seed_db(conn):
    """Seed in-memory cultivate DB with test data."""
    conn.executescript(CULTIVATE_SCHEMA)

    conn.execute(
        """INSERT INTO people VALUES
        ('self-1', '1001', 'myhandle', 'Me', 'Builder', 500, 200, 1,
         '2025-01-01', '2025-01-01', NULL, NULL, NULL, NULL, NULL, NULL,
         0, NULL, 0, 0, NULL, NULL, NULL, NULL)"""
    )
    conn.execute(
        """INSERT INTO people VALUES
        ('person-a', '2001', 'alice', 'Alice', 'Engineer', 1000, 300, 0,
         '2025-02-01', '2025-03-01', 0.65, 3, 2, 0.85, 0.72, 0.68,
         0, NULL, 0, 0, NULL, NULL, NULL, NULL)"""
    )
    conn.execute(
        """INSERT INTO people VALUES
        ('person-b', '2002', 'bob', 'Bob', NULL, 50, 80, 0,
         '2025-03-01', '2025-03-01', NULL, NULL, NULL, NULL, NULL, NULL,
         0, NULL, 0, 0, NULL, NULL, NULL, NULL)"""
    )

    # Actions: 3 for alice, 1 for bob
    conn.execute(
        """INSERT INTO actions (id, action_type, target_person_id, description,
           status, created_at)
           VALUES ('act-like', 'engage', 'person-a',
                   '[like] Like her latest tweet (@alice)',
                   'suggested', '2026-04-01T00:00:00')"""
    )
    conn.execute(
        """INSERT INTO actions (id, action_type, target_person_id, description,
           status, created_at)
           VALUES ('act-reply', 'strengthen', 'person-a',
                   '[reply] Reply to her tweet about AI (@alice)',
                   'suggested', '2026-04-01T01:00:00')"""
    )
    conn.execute(
        """INSERT INTO actions (id, action_type, target_person_id, description,
           status, created_at)
           VALUES ('act-follow', 'engage', 'person-b',
                   '[follow] Follow to show interest (@bob)',
                   'suggested', '2026-04-01T02:00:00')"""
    )
    conn.execute(
        """INSERT INTO actions (id, action_type, target_person_id, description,
           status, created_at, payload)
           VALUES ('act-resolved', 'engage', 'person-a',
                   '[retweet] RT her latest (@alice)',
                   'suggested', '2026-04-01T03:00:00',
                   '{"execution_type": "retweet", "tweet_id": "tw-99"}')"""
    )

    conn.commit()


@pytest.fixture
def cultivate_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _seed_db(conn)
    yield conn
    conn.close()


@pytest.fixture
def bridge(cultivate_db):
    return CultivateBridge(cultivate_db)


def _make_person_context(handle="alice"):
    return PersonContext(
        x_handle=handle,
        display_name=handle.title(),
        bio="Engineer",
        relationship_strength=0.65,
        engagement_stage=3,
        dunbar_tier=2,
        authenticity_score=0.85,
        content_quality_score=0.72,
        content_relevance_score=0.68,
    )


def _make_tweets(count=3):
    return [
        {
            "id": f"tw-{i}",
            "text": f"Tweet number {i}",
            "created_at": f"2026-04-09T0{i}:00:00",
            "public_metrics": {"like_count": i * 10},
            "reply_settings": "everyone",
        }
        for i in range(count)
    ]


# --- parse_execution_type ---


class TestParseExecutionType:
    def test_like(self):
        assert parse_execution_type("[like] Like their tweets") == "like"

    def test_retweet(self):
        assert parse_execution_type("[retweet] RT their content") == "retweet"

    def test_reply(self):
        assert parse_execution_type("[reply] Reply to @user") == "reply"

    def test_quote_tweet(self):
        assert parse_execution_type("[quote_tweet] Quote their post") == "quote_tweet"

    def test_follow(self):
        assert parse_execution_type("[follow] Follow @user") == "follow"

    def test_invalid_tag(self):
        assert parse_execution_type("[dm] Send a DM") is None

    def test_no_tag(self):
        assert parse_execution_type("Engage with their content") is None

    def test_empty(self):
        assert parse_execution_type("") is None

    def test_tag_not_at_start(self):
        assert parse_execution_type("Some text [like] embedded") is None


# --- is_already_resolved ---


class TestIsAlreadyResolved:
    def test_none_payload(self):
        assert is_already_resolved(None) is False

    def test_empty_payload(self):
        assert is_already_resolved({}) is False

    def test_payload_without_execution_type(self):
        assert is_already_resolved({"tweet_id": "123"}) is False

    def test_resolved_payload(self):
        assert is_already_resolved({"execution_type": "like"}) is True

    def test_empty_string_execution_type(self):
        assert is_already_resolved({"execution_type": ""}) is False


# --- select_tweet_for_action ---


class TestSelectTweetForAction:
    def test_returns_first_for_like(self):
        tweets = _make_tweets(3)
        result = select_tweet_for_action(tweets, "like")
        assert result["id"] == "tw-0"

    def test_returns_first_for_retweet(self):
        tweets = _make_tweets(3)
        result = select_tweet_for_action(tweets, "retweet")
        assert result["id"] == "tw-0"

    def test_returns_first_for_quote_tweet(self):
        tweets = _make_tweets(3)
        result = select_tweet_for_action(tweets, "quote_tweet")
        assert result["id"] == "tw-0"

    def test_reply_finds_open_replies(self):
        tweets = [
            {"id": "1", "text": "locked", "reply_settings": "mentionedUsers"},
            {"id": "2", "text": "open", "reply_settings": "everyone"},
        ]
        result = select_tweet_for_action(tweets, "reply")
        assert result["id"] == "2"

    def test_reply_returns_none_if_all_locked(self):
        tweets = [
            {"id": "1", "text": "locked", "reply_settings": "followers"},
        ]
        assert select_tweet_for_action(tweets, "reply") is None

    def test_reply_defaults_to_everyone(self):
        tweets = [{"id": "1", "text": "no setting"}]
        result = select_tweet_for_action(tweets, "reply")
        assert result["id"] == "1"

    def test_empty_tweets(self):
        assert select_tweet_for_action([], "like") is None


# --- build_resolved_payload ---


class TestBuildResolvedPayload:
    def test_follow_payload(self):
        p = build_resolved_payload("follow", x_user_id="u123")
        assert p["execution_type"] == "follow"
        assert p["x_user_id"] == "u123"
        assert "resolved_at" in p
        assert "tweet_id" not in p

    def test_like_payload_with_tweet(self):
        tweet = {"id": "tw-1", "text": "hello", "reply_settings": "everyone"}
        p = build_resolved_payload("like", tweet=tweet)
        assert p["tweet_id"] == "tw-1"
        assert p["tweet_content"] == "hello"
        assert "draft" not in p

    def test_reply_payload_with_draft(self):
        tweet = {"id": "tw-1", "text": "hello", "reply_settings": "everyone"}
        p = build_resolved_payload("reply", tweet=tweet, draft="Nice work!")
        assert p["draft"] == "Nice work!"
        assert p["tweet_id"] == "tw-1"
        assert p["execution_type"] == "reply"
        assert "quote_tweet_id" not in p

    def test_quote_tweet_payload_marks_quote_target(self):
        tweet = {"id": "tw-1", "text": "hello", "reply_settings": "everyone"}
        p = build_resolved_payload(
            "quote_tweet", tweet=tweet, draft="Worth reading."
        )
        assert p["execution_type"] == "quote_tweet"
        assert p["tweet_id"] == "tw-1"
        assert p["quote_tweet_id"] == "tw-1"
        assert p["quoted_tweet_id"] == "tw-1"
        assert p["draft"] == "Worth reading."

    def test_no_tweet_no_tweet_fields(self):
        p = build_resolved_payload("follow")
        assert "tweet_id" not in p
        assert "tweet_content" not in p


# --- _get_x_user_id ---


class TestGetXUserId:
    def test_known_person(self, bridge):
        assert _get_x_user_id(bridge, "person-a") == "2001"

    def test_unknown_person(self, bridge):
        assert _get_x_user_id(bridge, "nonexistent") is None


# --- _resolve_single_action ---


class TestResolveSingleAction:
    def test_follow_needs_no_tweet(self):
        action = ProactiveAction(
            action_id="a1", action_type="engage",
            target_handle="bob", target_person_id="p1",
            description="[follow] Follow @bob",
        )
        result = _resolve_single_action(
            "follow", action, tweets=[], x_user_id="u1",
            drafter=MagicMock(), my_handle="me",
        )
        assert result["execution_type"] == "follow"
        assert result["x_user_id"] == "u1"

    def test_like_picks_first_tweet(self):
        tweets = _make_tweets(3)
        action = ProactiveAction(
            action_id="a1", action_type="engage",
            target_handle="alice", target_person_id="p1",
            description="[like] Like tweet",
        )
        result = _resolve_single_action(
            "like", action, tweets=tweets, x_user_id="u1",
            drafter=MagicMock(), my_handle="me",
        )
        assert result["execution_type"] == "like"
        assert result["tweet_id"] == "tw-0"

    def test_reply_drafts_content(self):
        tweets = _make_tweets(1)
        drafter = MagicMock()
        drafter.draft.return_value = "Great insight!"

        action = ProactiveAction(
            action_id="a1", action_type="strengthen",
            target_handle="alice", target_person_id="p1",
            description="[reply] Reply to @alice",
            person_context=_make_person_context(),
        )
        result = _resolve_single_action(
            "reply", action, tweets=tweets, x_user_id="u1",
            drafter=drafter, my_handle="me",
        )
        assert result["execution_type"] == "reply"
        assert result["draft"] == "Great insight!"
        drafter.draft.assert_called_once()
        drafter.draft_proactive.assert_not_called()

    def test_quote_tweet_drafts_proactive_commentary(self):
        tweets = _make_tweets(1)
        drafter = MagicMock()
        drafter.draft_proactive.return_value.reply_text = "Worth adding context."

        action = ProactiveAction(
            action_id="a1", action_type="strengthen",
            target_handle="alice", target_person_id="p1",
            description="[quote_tweet] Quote @alice",
            person_context=_make_person_context(),
        )
        result = _resolve_single_action(
            "quote_tweet", action, tweets=tweets, x_user_id="u1",
            drafter=drafter, my_handle="me",
        )
        assert result["execution_type"] == "quote_tweet"
        assert result["quote_tweet_id"] == "tw-0"
        assert result["draft"] == "Worth adding context."
        drafter.draft_proactive.assert_called_once()
        drafter.draft.assert_not_called()

    def test_returns_none_when_no_suitable_tweet(self):
        tweets = [{"id": "1", "text": "locked", "reply_settings": "followers"}]
        action = ProactiveAction(
            action_id="a1", action_type="engage",
            target_handle="alice", target_person_id="p1",
            description="[reply] Reply to @alice",
        )
        result = _resolve_single_action(
            "reply", action, tweets=tweets, x_user_id="u1",
            drafter=MagicMock(), my_handle="me",
        )
        assert result is None


# --- resolve_actions (integration) ---


class TestResolveActions:
    def test_resolves_like_and_follow(self, bridge):
        mock_x = MagicMock()
        mock_x.get_user_tweets.return_value = _make_tweets(3)
        mock_drafter = MagicMock()
        mock_drafter.draft.return_value = "Draft reply"

        stats = resolve_actions(bridge, mock_x, mock_drafter, "myhandle")

        # 4 total: act-like, act-reply, act-follow, act-resolved
        assert stats["total"] == 4
        # act-resolved is already resolved → skipped
        assert stats["skipped"] == 1
        # act-like, act-reply, act-follow should resolve
        assert stats["resolved"] == 3
        assert stats["errors"] == 0

    def test_skips_already_resolved(self, bridge):
        mock_x = MagicMock()
        mock_x.get_user_tweets.return_value = _make_tweets(1)
        mock_drafter = MagicMock()
        mock_drafter.draft.return_value = "Draft text"

        stats = resolve_actions(bridge, mock_x, mock_drafter, "myhandle")

        assert stats["skipped"] == 1

    def test_batches_by_person(self, bridge):
        mock_x = MagicMock()
        mock_x.get_user_tweets.return_value = _make_tweets(3)
        mock_drafter = MagicMock()
        mock_drafter.draft.return_value = "Draft"

        resolve_actions(bridge, mock_x, mock_drafter, "myhandle")

        # person-a has 3 actions (like, reply, resolved), person-b has 1 (follow)
        # Only person-a needs tweets (follow doesn't), and resolved is skipped
        # So get_user_tweets called once for person-a
        assert mock_x.get_user_tweets.call_count == 1

    def test_writes_payload_to_db(self, bridge, cultivate_db):
        mock_x = MagicMock()
        mock_x.get_user_tweets.return_value = _make_tweets(1)
        mock_drafter = MagicMock()
        mock_drafter.draft.return_value = "Draft text"

        resolve_actions(bridge, mock_x, mock_drafter, "myhandle")

        # Check act-like got a payload
        row = cultivate_db.execute(
            "SELECT payload FROM actions WHERE id = 'act-like'"
        ).fetchone()
        payload = json.loads(row["payload"])
        assert payload["execution_type"] == "like"
        assert payload["tweet_id"] == "tw-0"

    def test_handles_missing_x_user_id(self, cultivate_db):
        # Add person with no x_user_id
        cultivate_db.execute(
            """INSERT INTO people VALUES
            ('person-c', '', 'charlie', 'Charlie', NULL, 10, 10, 0,
             '2025-01-01', '2025-01-01', NULL, NULL, NULL, NULL, NULL, NULL,
             0, NULL, 0, 0, NULL, NULL, NULL, NULL)"""
        )
        cultivate_db.execute(
            """INSERT INTO actions (id, action_type, target_person_id, description,
               status, created_at)
               VALUES ('act-c', 'engage', 'person-c',
                       '[like] Like (@charlie)', 'suggested', '2026-04-01')"""
        )
        cultivate_db.commit()

        bridge = CultivateBridge(cultivate_db)
        mock_x = MagicMock()
        mock_x.get_user_tweets.return_value = _make_tweets(1)
        mock_drafter = MagicMock()
        mock_drafter.draft.return_value = "Draft text"

        stats = resolve_actions(bridge, mock_x, mock_drafter, "myhandle")

        assert stats["errors"] >= 1

    def test_handles_tweet_fetch_failure(self, bridge):
        mock_x = MagicMock()
        mock_x.get_user_tweets.side_effect = tweepy.TweepyException("API error")

        stats = resolve_actions(bridge, mock_x, MagicMock(), "myhandle")

        # person-a actions that need tweets should error
        assert stats["errors"] >= 1

    def test_empty_actions(self, cultivate_db):
        cultivate_db.execute("UPDATE actions SET status = 'completed'")
        cultivate_db.commit()
        bridge = CultivateBridge(cultivate_db)

        stats = resolve_actions(bridge, MagicMock(), MagicMock(), "myhandle")

        assert stats["total"] == 0
        assert stats["resolved"] == 0

    def test_unparseable_description(self, cultivate_db):
        cultivate_db.execute(
            """INSERT INTO actions (id, action_type, target_person_id, description,
               status, created_at)
               VALUES ('act-bad', 'engage', 'person-a',
                       'No tag here at all', 'suggested', '2026-04-01')"""
        )
        cultivate_db.commit()
        bridge = CultivateBridge(cultivate_db)

        mock_x = MagicMock()
        mock_x.get_user_tweets.return_value = _make_tweets(1)
        mock_drafter = MagicMock()
        mock_drafter.draft.return_value = "Draft text"

        stats = resolve_actions(bridge, mock_x, mock_drafter, "myhandle")

        assert stats["errors"] >= 1


# --- main() ---


class TestMain:
    @patch("resolve_actions.update_monitoring")
    @patch("resolve_actions.script_context")
    def test_exits_when_cultivate_disabled(self, mock_ctx, mock_mon):
        from contextlib import contextmanager

        config = MagicMock()
        config.cultivate = None
        db = MagicMock()

        @contextmanager
        def ctx():
            yield config, db

        mock_ctx.return_value = ctx()

        from resolve_actions import main
        main()

        mock_mon.assert_not_called()
