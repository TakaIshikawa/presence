"""Tests for the standalone engagement predictor and validation DB."""

from unittest.mock import patch
import pytest

from evaluation.engagement_predictor import EngagementPredictor
from evaluation.validation_db import ValidationDatabase


class TestResponseParsing:
    @pytest.fixture
    def predictor(self):
        with patch("evaluation.engagement_predictor.anthropic.Anthropic"):
            return EngagementPredictor(api_key="test-key")

    def test_single_tweet_parsing(self, predictor):
        response = (
            "TWEET_1 (id=123):\n"
            "HOOK_STRENGTH: 7\n"
            "SPECIFICITY: 8\n"
            "EMOTIONAL_RESONANCE: 6\n"
            "NOVELTY: 7\n"
            "ACTIONABILITY: 5\n"
            "PREDICTED_ENGAGEMENT: 7\n"
        )
        tweets = [{"id": "123", "text": "test tweet"}]
        results = predictor._parse_batch_response(response, tweets)

        assert len(results) == 1
        assert results[0].predicted_score == 7.0
        assert results[0].hook_strength == 7.0
        assert results[0].specificity == 8.0
        assert results[0].emotional_resonance == 6.0
        assert results[0].novelty == 7.0
        assert results[0].actionability == 5.0
        assert results[0].tweet_id == "123"

    def test_multi_tweet_parsing(self, predictor):
        response = (
            "TWEET_1 (id=111):\n"
            "HOOK_STRENGTH: 8\n"
            "SPECIFICITY: 7\n"
            "EMOTIONAL_RESONANCE: 6\n"
            "NOVELTY: 8\n"
            "ACTIONABILITY: 5\n"
            "PREDICTED_ENGAGEMENT: 7\n\n"
            "TWEET_2 (id=222):\n"
            "HOOK_STRENGTH: 4\n"
            "SPECIFICITY: 3\n"
            "EMOTIONAL_RESONANCE: 3\n"
            "NOVELTY: 2\n"
            "ACTIONABILITY: 3\n"
            "PREDICTED_ENGAGEMENT: 3\n"
        )
        tweets = [
            {"id": "111", "text": "good tweet"},
            {"id": "222", "text": "bad tweet"},
        ]
        results = predictor._parse_batch_response(response, tweets)

        assert len(results) == 2
        assert results[0].predicted_score == 7.0
        assert results[0].hook_strength == 8.0
        assert results[1].predicted_score == 3.0
        assert results[1].novelty == 2.0

    def test_missing_scores_default_to_5(self, predictor):
        response = "TWEET_1 (id=456):\nHOOK_STRENGTH: 8\n"
        tweets = [{"id": "456", "text": "test"}]
        results = predictor._parse_batch_response(response, tweets)

        assert results[0].hook_strength == 8.0
        assert results[0].specificity == 5.0
        assert results[0].predicted_score == 5.0

    def test_decimal_scores(self, predictor):
        response = (
            "TWEET_1 (id=789):\n"
            "HOOK_STRENGTH: 7.5\n"
            "SPECIFICITY: 6.5\n"
            "EMOTIONAL_RESONANCE: 8.0\n"
            "NOVELTY: 5.5\n"
            "ACTIONABILITY: 4.5\n"
            "PREDICTED_ENGAGEMENT: 6.5\n"
        )
        tweets = [{"id": "789", "text": "test"}]
        results = predictor._parse_batch_response(response, tweets)

        assert results[0].hook_strength == 7.5
        assert results[0].predicted_score == 6.5

    def test_split_into_blocks(self, predictor):
        response = (
            "TWEET_1 (id=aaa):\nHOOK_STRENGTH: 7\n\n"
            "TWEET_2 (id=bbb):\nHOOK_STRENGTH: 4\n\n"
            "TWEET_3 (id=ccc):\nHOOK_STRENGTH: 9\n"
        )
        blocks = predictor._split_into_blocks(response, 3)
        assert len(blocks) == 3
        assert "aaa" in blocks[0]
        assert "bbb" in blocks[1]
        assert "ccc" in blocks[2]


class TestValidationDatabase:
    @pytest.fixture
    def db(self):
        vdb = ValidationDatabase(":memory:")
        vdb.connect()
        vdb.init_schema()
        yield vdb
        vdb.close()

    def _create_account(self, db, user_id="1", username="testuser"):
        db.upsert_account(
            user_id=user_id,
            username=username,
            display_name="Test User",
            bio="A test account",
            follower_count=5000,
            following_count=200,
            tweet_count=1000,
        )
        return db.get_account_by_user_id(user_id)

    def test_upsert_account(self, db):
        acct = self._create_account(db)
        assert acct["username"] == "testuser"
        assert acct["follower_count"] == 5000

    def test_upsert_account_updates(self, db):
        self._create_account(db)
        db.upsert_account(
            user_id="1",
            username="testuser",
            display_name="Updated",
            bio="new bio",
            follower_count=6000,
            following_count=210,
            tweet_count=1050,
        )
        acct = db.get_account_by_user_id("1")
        assert acct["follower_count"] == 6000
        assert acct["display_name"] == "Updated"

    def test_insert_tweet(self, db):
        acct = self._create_account(db)
        result = db.insert_tweet(
            tweet_id="t1",
            account_id=acct["id"],
            text="hello world",
            like_count=5,
            retweet_count=1,
            reply_count=2,
            quote_count=0,
            engagement_score=16.0,
            tweet_created_at="2025-01-01T00:00:00Z",
        )
        assert result is not None

    def test_insert_tweet_dedup(self, db):
        acct = self._create_account(db)
        kwargs = dict(
            tweet_id="t1",
            account_id=acct["id"],
            text="hello",
            like_count=5,
            retweet_count=1,
            reply_count=0,
            quote_count=0,
            engagement_score=8.0,
            tweet_created_at="2025-01-01",
        )
        r1 = db.insert_tweet(**kwargs)
        r2 = db.insert_tweet(**kwargs)
        assert r1 is not None
        assert r2 is None

    def test_get_tweets_for_account(self, db):
        acct = self._create_account(db)
        db.insert_tweet("t1", acct["id"], "low", 1, 0, 0, 0, 1.0, "2025-01-01")
        db.insert_tweet("t2", acct["id"], "high", 10, 5, 3, 0, 37.0, "2025-01-02")
        tweets = db.get_tweets_for_account(acct["id"])
        assert len(tweets) == 2
        assert tweets[0]["engagement_score"] > tweets[1]["engagement_score"]

    def test_unevaluated_tweets(self, db):
        acct = self._create_account(db)
        db.insert_tweet("t1", acct["id"], "hello", 5, 1, 0, 0, 8.0, "2025-01-01")

        uneval = db.get_unevaluated_tweets("v1")
        assert len(uneval) == 1
        assert uneval[0]["username"] == "testuser"

        db.insert_evaluation(
            tweet_id="t1",
            evaluator_version="v1",
            model="test-model",
            predicted_score=7.0,
            hook_strength=7.0,
            specificity=6.0,
            emotional_resonance=5.0,
            novelty=6.0,
            actionability=5.0,
            raw_response="...",
        )
        uneval = db.get_unevaluated_tweets("v1")
        assert len(uneval) == 0

    def test_unevaluated_tweets_version_scoped(self, db):
        acct = self._create_account(db)
        db.insert_tweet("t1", acct["id"], "hello", 5, 1, 0, 0, 8.0, "2025-01-01")

        db.insert_evaluation(
            tweet_id="t1", evaluator_version="v1", model="m",
            predicted_score=7.0, hook_strength=7.0, specificity=6.0,
            emotional_resonance=5.0, novelty=6.0, actionability=5.0,
            raw_response="...",
        )
        # Evaluated for v1 but not v2
        uneval_v2 = db.get_unevaluated_tweets("v2")
        assert len(uneval_v2) == 1

    def test_evaluation_upsert(self, db):
        acct = self._create_account(db)
        db.insert_tweet("t1", acct["id"], "hello", 5, 1, 0, 0, 8.0, "2025-01-01")

        db.insert_evaluation(
            tweet_id="t1", evaluator_version="v1", model="m",
            predicted_score=5.0, hook_strength=5.0, specificity=5.0,
            emotional_resonance=5.0, novelty=5.0, actionability=5.0,
            raw_response="first",
        )
        db.insert_evaluation(
            tweet_id="t1", evaluator_version="v1", model="m",
            predicted_score=8.0, hook_strength=8.0, specificity=8.0,
            emotional_resonance=8.0, novelty=8.0, actionability=8.0,
            raw_response="second",
        )
        evals = db.get_evaluations_for_version("v1")
        assert len(evals) == 1
        assert evals[0]["predicted_score"] == 8.0

    def test_get_evaluations_joins(self, db):
        acct = self._create_account(db)
        db.insert_tweet("t1", acct["id"], "hello world", 5, 1, 2, 0, 16.0, "2025-01-01")
        db.insert_evaluation(
            tweet_id="t1", evaluator_version="v1", model="sonnet",
            predicted_score=7.0, hook_strength=7.0, specificity=6.0,
            emotional_resonance=5.0, novelty=6.0, actionability=5.0,
            raw_response="...",
        )
        evals = db.get_evaluations_for_version("v1")
        assert len(evals) == 1
        assert evals[0]["text"] == "hello world"
        assert evals[0]["engagement_score"] == 16.0
        assert evals[0]["username"] == "testuser"

    def test_backtest_run(self, db):
        db.insert_backtest_run(
            run_id="run1",
            evaluator_version="v1",
            model="sonnet",
            num_tweets=100,
            num_accounts=5,
            spearman_overall=0.35,
            spearman_within_account=0.42,
            pearson_log=0.28,
            top_quartile_precision=0.4,
            bottom_quartile_precision=0.5,
            notes="baseline",
        )
        cursor = db.conn.execute("SELECT * FROM backtest_runs WHERE run_id='run1'")
        row = dict(cursor.fetchone())
        assert row["spearman_overall"] == 0.35

    def test_get_all_accounts(self, db):
        self._create_account(db, "1", "alice")
        self._create_account(db, "2", "bob")
        accounts = db.get_all_accounts()
        assert len(accounts) == 2
        assert accounts[0]["username"] == "alice"
