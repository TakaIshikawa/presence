"""Tests for the proactive reply discovery pipeline."""

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Import the helper used by the script
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from discover_replies import _is_recent, _batch_score_relevance, discover


def _make_tweet(tweet_id, text, author="karpathy", age_hours=2, reply_settings="everyone"):
    """Build a tweet dict matching XClient.get_user_tweets output."""
    ts = (datetime.now(timezone.utc) - timedelta(hours=age_hours)).isoformat()
    return {
        "id": tweet_id,
        "text": text,
        "created_at": ts,
        "public_metrics": {},
        "reply_settings": reply_settings,
        "author_handle": author,
        "author_id": "12345",
        "discovery_source": "curated_timeline",
    }


def _make_config(proactive_overrides=None):
    """Build a minimal config namespace for discover()."""
    proactive_defaults = {
        "enabled": True,
        "max_daily_replies": 5,
        "min_relevance": 0.50,
        "max_tweet_age_hours": 24,
        "reply_cap_per_account": 2,
        "search_enabled": False,
        "search_keywords": [],
    }
    if proactive_overrides:
        proactive_defaults.update(proactive_overrides)

    config = SimpleNamespace(
        proactive=SimpleNamespace(**proactive_defaults),
        curated_sources=SimpleNamespace(
            x_accounts=[SimpleNamespace(identifier="karpathy")],
        ),
    )
    return config


def _make_batch_scorer(default_relevance=0.75, per_text=None):
    """Build a _batch_score_relevance replacement that assigns scores.

    Args:
        default_relevance: score for all candidates (used when per_text is None)
        per_text: optional dict mapping substring -> relevance for targeted scoring
    """
    def scorer(candidates, knowledge_store, batch_size=128):
        for c in candidates:
            if per_text:
                score = default_relevance
                for substring, rel in per_text.items():
                    if substring in c["text"]:
                        score = rel
                        break
                c["relevance"] = score
            else:
                c["relevance"] = default_relevance
    return scorer


class TestIsRecent:
    def test_recent_tweet(self):
        ts = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        assert _is_recent({"created_at": ts}, max_age_hours=24) is True

    def test_stale_tweet(self):
        ts = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
        assert _is_recent({"created_at": ts}, max_age_hours=24) is False

    def test_missing_created_at(self):
        assert _is_recent({"created_at": ""}, max_age_hours=24) is False

    def test_invalid_timestamp(self):
        assert _is_recent({"created_at": "not-a-date"}, max_age_hours=24) is False


class TestDiscoverFiltering:
    """Test the filtering logic in discover()."""

    def _setup_mocks(self, db, tweets, default_relevance=0.75, per_text=None, min_relevance=0.50):
        x_client = MagicMock()
        x_client.username = "myhandle"
        x_client.get_user_id.return_value = "99999"
        x_client.get_user_tweets.return_value = tweets

        knowledge_store = MagicMock()

        drafter = MagicMock()
        draft_result = MagicMock()
        draft_result.reply_text = "Interesting take!"
        draft_result.knowledge_ids = [(1, 0.75)]
        drafter.draft_proactive.return_value = draft_result

        config = _make_config({"min_relevance": min_relevance})

        return config, x_client, knowledge_store, drafter, default_relevance, per_text

    def _discover_with_batch_patch(self, config, db, x_client, ks, drafter,
                                    default_relevance=0.75, per_text=None, bridge=None):
        scorer = _make_batch_scorer(default_relevance, per_text)
        with patch("discover_replies._batch_score_relevance", side_effect=scorer):
            return discover(config, db, x_client, ks, drafter, bridge)

    def test_filters_stale_tweets(self, db):
        stale = _make_tweet("t1", "Old tweet", age_hours=48)
        config, x_client, ks, drafter, rel, pt = self._setup_mocks(db, [stale])

        inserted = self._discover_with_batch_patch(config, db, x_client, ks, drafter, rel, pt)
        assert inserted == 0

    def test_filters_self_tweets_via_search(self, db):
        """Self-tweets discovered via search should be filtered out."""
        config, x_client, ks, drafter, rel, pt = self._setup_mocks(db, [])
        config.proactive.search_enabled = True
        config.proactive.search_keywords = ["AI"]

        my_tweet = _make_tweet("t1", "My own tweet")
        my_tweet["author_username"] = "myhandle"
        x_client.search_tweets.return_value = [my_tweet]

        inserted = self._discover_with_batch_patch(config, db, x_client, ks, drafter, rel, pt)
        assert inserted == 0

    def test_dedup_skips_existing_actions(self, db):
        tweet = _make_tweet("t1", "Great insight on AI")
        config, x_client, ks, drafter, rel, pt = self._setup_mocks(db, [tweet])

        # Pre-insert the action
        db.insert_proactive_action(
            action_type="reply",
            target_tweet_id="t1",
            target_tweet_text="Great insight on AI",
            target_author_handle="karpathy",
        )

        inserted = self._discover_with_batch_patch(config, db, x_client, ks, drafter, rel, pt)
        assert inserted == 0

    def test_filters_non_replyable_tweets(self, db):
        locked = _make_tweet("t1", "Locked tweet", reply_settings="mentioned_users")
        config, x_client, ks, drafter, rel, pt = self._setup_mocks(db, [locked])

        inserted = self._discover_with_batch_patch(config, db, x_client, ks, drafter, rel, pt)
        assert inserted == 0

    def test_respects_min_relevance_threshold(self, db):
        tweet = _make_tweet("t1", "Off-topic tweet")
        config, x_client, ks, drafter, _, _ = self._setup_mocks(
            db, [tweet], default_relevance=0.30, min_relevance=0.50
        )

        inserted = self._discover_with_batch_patch(config, db, x_client, ks, drafter, 0.30)
        assert inserted == 0

    def test_inserts_above_threshold(self, db):
        tweet = _make_tweet("t1", "AI agents are transforming workflows")
        config, x_client, ks, drafter, rel, pt = self._setup_mocks(db, [tweet])

        inserted = self._discover_with_batch_patch(config, db, x_client, ks, drafter, rel, pt)
        assert inserted == 1

        pending = db.get_pending_proactive_actions()
        assert len(pending) == 1
        assert pending[0]["target_tweet_id"] == "t1"
        assert pending[0]["draft_text"] == "Interesting take!"
        assert pending[0]["discovery_source"] == "curated_timeline"

    def test_per_account_weekly_cap(self, db):
        tweets = [
            _make_tweet("t1", "Tweet 1", author="karpathy"),
            _make_tweet("t2", "Tweet 2", author="karpathy"),
            _make_tweet("t3", "Tweet 3", author="karpathy"),
        ]
        config, x_client, ks, drafter, rel, pt = self._setup_mocks(db, tweets)
        config.proactive.reply_cap_per_account = 2

        # Pre-insert 2 posted replies to karpathy
        for i in range(2):
            aid = db.insert_proactive_action(
                action_type="reply",
                target_tweet_id=f"old_{i}",
                target_tweet_text=f"Old tweet {i}",
                target_author_handle="karpathy",
            )
            db.mark_proactive_posted(aid, f"posted_{i}")

        inserted = self._discover_with_batch_patch(config, db, x_client, ks, drafter, rel, pt)
        assert inserted == 0  # all blocked by weekly cap

    def test_scores_by_relevance(self, db):
        """Higher relevance tweets should be ranked first."""
        tweets = [
            _make_tweet("t_low", "General chat"),
            _make_tweet("t_high", "AI agents build automation"),
        ]
        config, x_client, ks, drafter, _, _ = self._setup_mocks(db, tweets)

        inserted = self._discover_with_batch_patch(
            config, db, x_client, ks, drafter,
            default_relevance=0.60,
            per_text={"automation": 0.90},
        )
        assert inserted == 2

        pending = db.get_pending_proactive_actions()
        # First inserted should have higher relevance (sorted desc)
        scores = [p["relevance_score"] for p in pending]
        assert scores[0] >= scores[1]

    def test_search_source_disabled_by_default(self, db):
        tweet = _make_tweet("t1", "Found via timeline")
        config, x_client, ks, drafter, rel, pt = self._setup_mocks(db, [tweet])
        config.proactive.search_enabled = False
        config.proactive.search_keywords = ["AI agents"]

        self._discover_with_batch_patch(config, db, x_client, ks, drafter, rel, pt)

        # search_tweets should never be called
        x_client.search_tweets.assert_not_called()

    def test_search_source_enabled(self, db):
        config, x_client, ks, drafter, rel, pt = self._setup_mocks(db, [])
        config.proactive.search_enabled = True
        config.proactive.search_keywords = ["AI agents"]

        search_tweet = _make_tweet("s1", "Search result about AI agents")
        search_tweet["author_username"] = "someone"
        x_client.search_tweets.return_value = [search_tweet]

        inserted = self._discover_with_batch_patch(config, db, x_client, ks, drafter, rel, pt)
        x_client.search_tweets.assert_called_once_with("AI agents", max_results=20)
        assert inserted == 1

    def test_no_knowledge_store_uses_zero_relevance(self, db):
        tweet = _make_tweet("t1", "A tweet")
        config, x_client, _, drafter, _, _ = self._setup_mocks(db, [tweet])
        config.proactive.min_relevance = 0.0  # allow zero relevance

        inserted = discover(config, db, x_client, None, drafter)
        assert inserted == 1

        pending = db.get_pending_proactive_actions()
        assert pending[0]["relevance_score"] == 0.0


class TestBatchScoreRelevance:
    """Test the batch scoring function directly."""

    def _make_knowledge_store(self, knowledge_embeddings, candidate_embeddings):
        """Build a mock KnowledgeStore with controlled embeddings."""
        from knowledge.embeddings import serialize_embedding

        ks = MagicMock()

        # Mock conn.execute for loading full knowledge rows
        mock_rows = []
        for i, emb in enumerate(knowledge_embeddings):
            row = {
                "id": i + 1,
                "embedding": serialize_embedding(emb),
                "source_type": "own_post",
                "source_id": f"post_{i}",
                "source_url": None,
                "author": "self",
                "content": f"Knowledge item {i}",
                "insight": None,
                "attribution_required": 0,
                "approved": 1,
                "created_at": "2026-01-01",
            }
            mock_rows.append(row)
        ks.conn.execute.return_value.fetchall.return_value = mock_rows

        # Mock embedder.embed_batch
        ks.embedder.embed_batch.return_value = candidate_embeddings

        return ks

    def test_scores_against_knowledge(self):
        # Knowledge: one item pointing in [1, 0] direction
        knowledge_embs = [[1.0, 0.0]]
        # Candidate: one item at ~45 degrees -> cosine ~0.707
        candidate_embs = [[0.707, 0.707]]

        ks = self._make_knowledge_store(knowledge_embs, candidate_embs)
        candidates = [{"text": "test tweet"}]

        _batch_score_relevance(candidates, ks)

        assert abs(candidates[0]["relevance"] - 0.707) < 0.01

    def test_takes_best_match_across_knowledge(self):
        # Two knowledge items in different directions
        knowledge_embs = [[1.0, 0.0], [0.0, 1.0]]
        # Candidate aligns perfectly with second knowledge item
        candidate_embs = [[0.0, 1.0]]

        ks = self._make_knowledge_store(knowledge_embs, candidate_embs)
        candidates = [{"text": "test tweet"}]

        _batch_score_relevance(candidates, ks)

        assert abs(candidates[0]["relevance"] - 1.0) < 0.01

    def test_empty_candidates(self):
        ks = MagicMock()
        candidates = []
        _batch_score_relevance(candidates, ks)
        # No error, no calls
        ks.conn.execute.assert_not_called()

    def test_no_knowledge_embeddings(self):
        ks = MagicMock()
        ks.conn.execute.return_value.fetchall.return_value = []
        candidates = [{"text": "test"}]

        _batch_score_relevance(candidates, ks)
        assert candidates[0]["relevance"] == 0.0

    def test_embed_batch_failure_falls_back_to_zero(self):
        from knowledge.embeddings import serialize_embedding

        ks = MagicMock()
        ks.conn.execute.return_value.fetchall.return_value = [{
            "id": 1, "embedding": serialize_embedding([1.0, 0.0]),
            "source_type": "own_post", "source_id": "p1", "source_url": None,
            "author": "self", "content": "test", "insight": None,
            "attribution_required": 0, "approved": 1, "created_at": "2026-01-01",
        }]
        ks.embedder.embed_batch.side_effect = Exception("API error")

        candidates = [{"text": "test"}]
        _batch_score_relevance(candidates, ks)
        assert candidates[0]["relevance"] == 0.0

    def test_multiple_candidates_scored(self):
        knowledge_embs = [[1.0, 0.0]]
        # Two candidates at different angles
        candidate_embs = [[1.0, 0.0], [0.0, 1.0]]

        ks = self._make_knowledge_store(knowledge_embs, candidate_embs)
        candidates = [{"text": "aligned"}, {"text": "orthogonal"}]

        _batch_score_relevance(candidates, ks)

        assert abs(candidates[0]["relevance"] - 1.0) < 0.01
        assert abs(candidates[1]["relevance"] - 0.0) < 0.01
