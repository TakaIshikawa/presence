"""Tests for account discovery pipeline."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from discover_accounts import (
    _get_candidate_handles, _score_account, discover, sync_following,
)


def _make_config(**proactive_overrides):
    """Build a minimal config for discovery tests."""
    proactive_defaults = dict(
        enabled=True,
        max_daily_replies=5,
        min_relevance=0.50,
        max_tweet_age_hours=24,
        reply_cap_per_account=2,
        search_enabled=False,
        search_keywords=[],
        account_discovery_enabled=True,
        max_candidates_per_run=5,
        min_discovery_relevance=0.45,
        min_discovery_samples=3,
    )
    proactive_defaults.update(proactive_overrides)
    return SimpleNamespace(
        proactive=SimpleNamespace(**proactive_defaults),
        curated_sources=SimpleNamespace(x_accounts=[], blogs=[]),
    )


def _insert_posted_action(db, handle, tweet_id):
    """Insert a proactive action marked as posted."""
    aid = db.insert_proactive_action(
        action_type="reply",
        target_tweet_id=tweet_id,
        target_tweet_text=f"tweet by {handle}",
        target_author_handle=handle,
    )
    db.mark_proactive_posted(aid, f"posted_{tweet_id}")
    return aid


class TestGetCandidateHandles:
    def test_extracts_distinct_handles(self, db):
        _insert_posted_action(db, "user_a", "t1")
        _insert_posted_action(db, "user_b", "t2")
        _insert_posted_action(db, "user_a", "t3")  # duplicate handle

        handles = _get_candidate_handles(db)
        assert set(handles) == {"user_a", "user_b"}

    def test_empty_when_no_actions(self, db):
        handles = _get_candidate_handles(db)
        assert handles == []

    def test_skips_empty_handles(self, db):
        db.conn.execute(
            """INSERT INTO proactive_actions
               (action_type, target_tweet_id, target_tweet_text, target_author_handle)
               VALUES ('reply', 't1', 'text', '')"""
        )
        db.conn.commit()
        handles = _get_candidate_handles(db)
        assert handles == []


class TestScoreAccount:
    def test_returns_relevance_and_count(self):
        x_client = MagicMock()
        x_client.get_user_id.return_value = "123"
        x_client.get_user_tweets.return_value = [
            {"text": "AI agents are transforming dev workflows", "id": "t1"},
            {"text": "Building with LLMs requires new patterns", "id": "t2"},
            {"text": "Short", "id": "t3"},  # Too short, skipped
        ]

        mock_item = MagicMock()
        mock_item.id = 1
        knowledge_store = MagicMock()
        knowledge_store.search_similar.return_value = [(mock_item, 0.65)]

        relevance, count = _score_account(x_client, knowledge_store, "test_user")
        assert relevance == 0.65
        assert count == 2  # 2 valid tweets

    def test_unresolvable_user_returns_zero(self):
        x_client = MagicMock()
        x_client.get_user_id.return_value = None

        relevance, count = _score_account(x_client, MagicMock(), "ghost_user")
        assert relevance == 0.0
        assert count == 0

    def test_no_tweets_returns_zero(self):
        x_client = MagicMock()
        x_client.get_user_id.return_value = "123"
        x_client.get_user_tweets.return_value = []

        relevance, count = _score_account(x_client, MagicMock(), "quiet_user")
        assert relevance == 0.0
        assert count == 0

    def test_filters_low_similarity(self):
        x_client = MagicMock()
        x_client.get_user_id.return_value = "123"
        x_client.get_user_tweets.return_value = [
            {"text": "Completely unrelated topic about gardening tips", "id": "t1"},
        ]

        mock_item = MagicMock()
        knowledge_store = MagicMock()
        knowledge_store.search_similar.return_value = [(mock_item, 0.15)]  # Below 0.3

        relevance, count = _score_account(x_client, knowledge_store, "off_topic")
        assert relevance == 0.0
        assert count == 0

    def test_skips_retweets(self):
        x_client = MagicMock()
        x_client.get_user_id.return_value = "123"
        x_client.get_user_tweets.return_value = [
            {"text": "RT @someone: Their original tweet content here", "id": "t1"},
            {"text": "My own original thought about AI agents", "id": "t2"},
        ]

        mock_item = MagicMock()
        knowledge_store = MagicMock()
        knowledge_store.search_similar.return_value = [(mock_item, 0.70)]

        relevance, count = _score_account(x_client, knowledge_store, "retweeter")
        assert count == 1  # Only original tweet counted

    def test_handles_embedding_errors(self):
        x_client = MagicMock()
        x_client.get_user_id.return_value = "123"
        x_client.get_user_tweets.return_value = [
            {"text": "A valid tweet about building in public", "id": "t1"},
        ]

        knowledge_store = MagicMock()
        knowledge_store.search_similar.side_effect = Exception("Rate limit")

        relevance, count = _score_account(x_client, knowledge_store, "rate_limited")
        assert relevance == 0.0
        assert count == 0


class TestDiscover:
    def test_inserts_above_threshold(self, db):
        _insert_posted_action(db, "relevant_user", "t1")

        config = _make_config(min_discovery_relevance=0.40, min_discovery_samples=1)
        x_client = MagicMock()
        x_client.get_user_id.return_value = "123"
        x_client.get_user_tweets.return_value = [
            {"text": "Building AI agents with tool use patterns", "id": "t1"},
            {"text": "LLM orchestration is the next frontier", "id": "t2"},
        ]

        mock_item = MagicMock()
        knowledge_store = MagicMock()
        knowledge_store.search_similar.return_value = [(mock_item, 0.60)]

        inserted = discover(config, db, x_client, knowledge_store)
        assert inserted == 1

        candidates = db.get_candidate_sources("x_account")
        assert len(candidates) == 1
        assert candidates[0]["identifier"] == "relevant_user"
        assert candidates[0]["discovery_source"] == "proactive_mining"

    def test_below_relevance_threshold_excluded(self, db):
        _insert_posted_action(db, "low_relevance_user", "t1")

        config = _make_config(min_discovery_relevance=0.80, min_discovery_samples=1)
        x_client = MagicMock()
        x_client.get_user_id.return_value = "123"
        x_client.get_user_tweets.return_value = [
            {"text": "Some tweet about something", "id": "t1"},
        ]

        mock_item = MagicMock()
        knowledge_store = MagicMock()
        knowledge_store.search_similar.return_value = [(mock_item, 0.50)]

        inserted = discover(config, db, x_client, knowledge_store)
        assert inserted == 0

    def test_below_sample_count_excluded(self, db):
        _insert_posted_action(db, "few_samples_user", "t1")

        config = _make_config(min_discovery_relevance=0.40, min_discovery_samples=5)
        x_client = MagicMock()
        x_client.get_user_id.return_value = "123"
        # Only 2 tweets, need 5 samples
        x_client.get_user_tweets.return_value = [
            {"text": "First tweet about AI development tools", "id": "t1"},
            {"text": "Second tweet about LLM engineering", "id": "t2"},
        ]

        mock_item = MagicMock()
        knowledge_store = MagicMock()
        knowledge_store.search_similar.return_value = [(mock_item, 0.70)]

        inserted = discover(config, db, x_client, knowledge_store)
        assert inserted == 0

    def test_skips_existing_sources(self, db):
        _insert_posted_action(db, "already_tracked", "t1")
        # Pre-insert as an existing source
        db.sync_config_sources(
            [{"identifier": "already_tracked", "name": "Already"}], "x_account"
        )

        config = _make_config()
        inserted = discover(config, db, MagicMock(), MagicMock())
        assert inserted == 0

    def test_skips_already_candidate(self, db):
        _insert_posted_action(db, "already_candidate", "t1")
        db.insert_candidate_source("x_account", "already_candidate")

        config = _make_config()
        inserted = discover(config, db, MagicMock(), MagicMock())
        assert inserted == 0

    def test_max_candidates_cap(self, db):
        for i in range(10):
            _insert_posted_action(db, f"user_{i}", f"t_{i}")

        config = _make_config(
            max_candidates_per_run=2,
            min_discovery_relevance=0.0,
            min_discovery_samples=0,
        )

        x_client = MagicMock()
        x_client.get_user_id.return_value = "123"
        x_client.get_user_tweets.return_value = [
            {"text": "A relevant tweet about building software", "id": "t1"},
        ]

        mock_item = MagicMock()
        knowledge_store = MagicMock()
        knowledge_store.search_similar.return_value = [(mock_item, 0.70)]

        inserted = discover(config, db, x_client, knowledge_store)
        # Capped at 2 even though 10 handles available
        assert inserted <= 2

    def test_handles_api_errors_gracefully(self, db):
        _insert_posted_action(db, "error_user", "t1")

        config = _make_config(min_discovery_relevance=0.0, min_discovery_samples=0)
        x_client = MagicMock()
        x_client.get_user_id.return_value = None  # Can't resolve

        inserted = discover(config, db, x_client, MagicMock())
        assert inserted == 0
        # No crash, no candidates inserted

    def test_empty_proactive_actions(self, db):
        config = _make_config()
        inserted = discover(config, db, MagicMock(), MagicMock())
        assert inserted == 0


class TestSyncFollowing:
    def test_inserts_following_as_active(self, db):
        x_client = MagicMock()
        x_client.get_following.return_value = [
            {"id": "1", "username": "alice", "name": "Alice Dev"},
            {"id": "2", "username": "bob", "name": "Bob Builder"},
        ]

        inserted = sync_following(db, x_client)
        assert inserted == 2

        active = db.get_active_curated_sources("x_account")
        usernames = {a["identifier"] for a in active}
        assert usernames == {"alice", "bob"}

        # Verify discovery_source
        for row in active:
            assert row["discovery_source"] == "following"

    def test_skips_existing_config_sources(self, db):
        # Pre-insert a config source
        db.sync_config_sources(
            [{"identifier": "alice", "name": "Alice"}], "x_account"
        )

        x_client = MagicMock()
        x_client.get_following.return_value = [
            {"id": "1", "username": "alice", "name": "Alice Dev"},
            {"id": "2", "username": "bob", "name": "Bob Builder"},
        ]

        inserted = sync_following(db, x_client)
        assert inserted == 1  # Only bob is new

        # Alice's entry should still have config as discovery_source
        active = db.get_active_curated_sources("x_account")
        alice = [a for a in active if a["identifier"] == "alice"][0]
        assert alice["discovery_source"] == "config"

    def test_skips_existing_candidates(self, db):
        # Pre-insert as candidate
        db.insert_candidate_source("x_account", "alice", discovery_source="proactive_mining")

        x_client = MagicMock()
        x_client.get_following.return_value = [
            {"id": "1", "username": "alice", "name": "Alice"},
        ]

        inserted = sync_following(db, x_client)
        assert inserted == 0  # alice already exists as candidate

    def test_handles_empty_following(self, db):
        x_client = MagicMock()
        x_client.get_following.return_value = []

        inserted = sync_following(db, x_client)
        assert inserted == 0

    def test_handles_api_error(self, db):
        x_client = MagicMock()
        x_client.get_following.return_value = []  # XClient returns [] on error

        inserted = sync_following(db, x_client)
        assert inserted == 0

    def test_idempotent_on_repeated_runs(self, db):
        following = [
            {"id": "1", "username": "alice", "name": "Alice"},
            {"id": "2", "username": "bob", "name": "Bob"},
        ]

        x_client = MagicMock()
        x_client.get_following.return_value = following

        first = sync_following(db, x_client)
        assert first == 2

        second = sync_following(db, x_client)
        assert second == 0  # Already present, no new inserts

        active = db.get_active_curated_sources("x_account")
        assert len(active) == 2
