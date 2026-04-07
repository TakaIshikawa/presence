"""Tests for the standalone engagement predictor and validation DB."""

from unittest.mock import MagicMock, patch
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


class TestExtractScore:
    @pytest.fixture
    def predictor(self):
        with patch("evaluation.engagement_predictor.anthropic.Anthropic"):
            return EngagementPredictor(api_key="test-key")

    def test_extracts_integer_score(self, predictor):
        text = "HOOK_STRENGTH: 8\nSPECIFICITY: 7"
        score = predictor._extract_score(text, "HOOK_STRENGTH")
        assert score == 8.0
        assert isinstance(score, float)

    def test_extracts_float_score(self, predictor):
        text = "SPECIFICITY: 7.5\nNOVELTY: 6.0"
        score = predictor._extract_score(text, "SPECIFICITY")
        assert score == 7.5

    def test_returns_default_when_criterion_missing(self, predictor):
        text = "HOOK_STRENGTH: 8\nSPECIFICITY: 7"
        score = predictor._extract_score(text, "NOVELTY")
        assert score == 5.0

    def test_handles_multiple_criteria_extracts_correct_one(self, predictor):
        text = (
            "HOOK_STRENGTH: 8\n"
            "SPECIFICITY: 7.5\n"
            "EMOTIONAL_RESONANCE: 6.0\n"
            "NOVELTY: 9\n"
        )
        assert predictor._extract_score(text, "HOOK_STRENGTH") == 8.0
        assert predictor._extract_score(text, "SPECIFICITY") == 7.5
        assert predictor._extract_score(text, "EMOTIONAL_RESONANCE") == 6.0
        assert predictor._extract_score(text, "NOVELTY") == 9.0

    def test_handles_varying_whitespace(self, predictor):
        text = "HOOK_STRENGTH:8\nSPECIFICITY:  7.5\nNOVELTY:   9"
        assert predictor._extract_score(text, "HOOK_STRENGTH") == 8.0
        assert predictor._extract_score(text, "SPECIFICITY") == 7.5
        assert predictor._extract_score(text, "NOVELTY") == 9.0


class TestSplitIntoBlocks:
    @pytest.fixture
    def predictor(self):
        with patch("evaluation.engagement_predictor.anthropic.Anthropic"):
            return EngagementPredictor(api_key="test-key")

    def test_splits_multi_tweet_response_correctly(self, predictor):
        response = (
            "TWEET_1 (id=111):\nHOOK_STRENGTH: 8\nSPECIFICITY: 7\n\n"
            "TWEET_2 (id=222):\nHOOK_STRENGTH: 4\nSPECIFICITY: 5\n\n"
            "TWEET_3 (id=333):\nHOOK_STRENGTH: 9\nSPECIFICITY: 8\n"
        )
        blocks = predictor._split_into_blocks(response, 3)
        assert len(blocks) == 3
        assert "111" in blocks[0]
        assert "HOOK_STRENGTH: 8" in blocks[0]
        assert "222" in blocks[1]
        assert "HOOK_STRENGTH: 4" in blocks[1]
        assert "333" in blocks[2]
        assert "HOOK_STRENGTH: 9" in blocks[2]

    def test_returns_single_block_when_no_markers(self, predictor):
        response = "Some unstructured response text without markers"
        blocks = predictor._split_into_blocks(response, 1)
        assert len(blocks) == 1
        assert blocks[0] == response

    def test_handles_varying_whitespace_around_markers(self, predictor):
        response = (
            "TWEET_1  (id=aaa)  :\nContent\n\n"
            "TWEET_2(id=bbb):\nMore content\n\n"
            "TWEET_3 (id=ccc) :\nEven more"
        )
        blocks = predictor._split_into_blocks(response, 3)
        assert len(blocks) == 3
        assert "aaa" in blocks[0]
        assert "bbb" in blocks[1]
        assert "ccc" in blocks[2]

    def test_handles_single_tweet_marker(self, predictor):
        response = "TWEET_1 (id=single):\nHOOK_STRENGTH: 7\nSPECIFICITY: 6"
        blocks = predictor._split_into_blocks(response, 1)
        assert len(blocks) == 1
        assert "single" in blocks[0]

    def test_blocks_are_trimmed(self, predictor):
        response = (
            "TWEET_1 (id=aaa):\nContent\n\n\n"
            "TWEET_2 (id=bbb):\nMore\n   "
        )
        blocks = predictor._split_into_blocks(response, 2)
        assert blocks[0].endswith("Content")
        assert blocks[1].endswith("More")


class TestParseBatchResponseComprehensive:
    @pytest.fixture
    def predictor(self):
        with patch("evaluation.engagement_predictor.anthropic.Anthropic"):
            return EngagementPredictor(api_key="test-key")

    def test_parses_complete_multi_tweet_response(self, predictor):
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
            {"id": "111", "text": "first tweet"},
            {"id": "222", "text": "second tweet"},
        ]
        results = predictor._parse_batch_response(response, tweets)

        assert len(results) == 2
        # First tweet
        assert results[0].tweet_id == "111"
        assert results[0].tweet_text == "first tweet"
        assert results[0].predicted_score == 7.0
        assert results[0].hook_strength == 8.0
        assert results[0].specificity == 7.0
        assert results[0].emotional_resonance == 6.0
        assert results[0].novelty == 8.0
        assert results[0].actionability == 5.0
        # Second tweet
        assert results[1].tweet_id == "222"
        assert results[1].tweet_text == "second tweet"
        assert results[1].predicted_score == 3.0
        assert results[1].hook_strength == 4.0

    def test_maps_scores_to_correct_fields(self, predictor):
        response = (
            "TWEET_1 (id=test):\n"
            "HOOK_STRENGTH: 7.5\n"
            "SPECIFICITY: 6.5\n"
            "EMOTIONAL_RESONANCE: 8.0\n"
            "NOVELTY: 5.5\n"
            "ACTIONABILITY: 4.5\n"
            "PREDICTED_ENGAGEMENT: 6.5\n"
        )
        tweets = [{"id": "test", "text": "test content"}]
        results = predictor._parse_batch_response(response, tweets)

        result = results[0]
        assert result.hook_strength == 7.5
        assert result.specificity == 6.5
        assert result.emotional_resonance == 8.0
        assert result.novelty == 5.5
        assert result.actionability == 4.5
        assert result.predicted_score == 6.5

    def test_uses_fallback_block_when_fewer_blocks_than_tweets(self, predictor):
        # Only one block in response but two tweets provided
        response = "TWEET_1 (id=only):\nHOOK_STRENGTH: 7\nSPECIFICITY: 6"
        tweets = [
            {"id": "only", "text": "first"},
            {"id": "missing", "text": "second"},
        ]
        results = predictor._parse_batch_response(response, tweets)

        assert len(results) == 2
        # First tweet parsed from block
        assert results[0].tweet_id == "only"
        assert results[0].hook_strength == 7.0
        # Second tweet uses full response as fallback
        assert results[1].tweet_id == "missing"
        assert results[1].hook_strength == 7.0  # Still finds it in full response

    def test_preserves_tweet_id_and_text_from_input(self, predictor):
        response = "TWEET_1 (id=resp_id):\nHOOK_STRENGTH: 7"
        tweets = [{"id": "input_id", "text": "input text"}]
        results = predictor._parse_batch_response(response, tweets)

        # Uses input tweet id/text, not from response
        assert results[0].tweet_id == "input_id"
        assert results[0].tweet_text == "input text"

    def test_raw_response_preserved_in_result(self, predictor):
        response = (
            "TWEET_1 (id=111):\nHOOK_STRENGTH: 8\nSome reasoning text\n\n"
            "TWEET_2 (id=222):\nHOOK_STRENGTH: 4\nMore reasoning"
        )
        tweets = [
            {"id": "111", "text": "first"},
            {"id": "222", "text": "second"},
        ]
        results = predictor._parse_batch_response(response, tweets)

        assert "TWEET_1 (id=111):" in results[0].raw_response
        assert "Some reasoning text" in results[0].raw_response
        assert "TWEET_2 (id=222):" in results[1].raw_response
        assert "More reasoning" in results[1].raw_response


class TestPredictBatch:
    @pytest.fixture
    def predictor(self):
        with patch("evaluation.engagement_predictor.anthropic.Anthropic") as mock_anthropic:
            client_mock = MagicMock()
            mock_anthropic.return_value = client_mock
            predictor = EngagementPredictor(api_key="test-key", model="test-model")
            predictor.client = client_mock
            return predictor

    def test_loads_and_fills_template_correctly(self, predictor):
        # Mock the prompt file read
        template = (
            "Rate these tweets:\n{tweets}\n\n"
            "Context: {account_context}\n"
            "Total: {num_tweets}\n"
            "First ID: {first_tweet_id}"
        )

        # Mock the API response
        api_response = MagicMock()
        api_response.content = [
            MagicMock(text="TWEET_1 (id=t1):\nHOOK_STRENGTH: 7\nPREDICTED_ENGAGEMENT: 7")
        ]
        predictor.client.messages.create.return_value = api_response

        tweets = [{"id": "t1", "text": "Test tweet"}]

        with patch("pathlib.Path.read_text", return_value=template):
            results = predictor.predict_batch(
                tweets=tweets,
                account_context="@testuser, 5K followers",
                prompt_version="v1",
            )

        # Verify API was called
        predictor.client.messages.create.assert_called_once()
        call_args = predictor.client.messages.create.call_args

        # Verify the filled prompt
        filled_prompt = call_args[1]["messages"][0]["content"]
        assert "Rate these tweets:" in filled_prompt
        assert "TWEET_1 (id=t1):" in filled_prompt
        assert "Test tweet" in filled_prompt
        assert "@testuser, 5K followers" in filled_prompt
        assert "Total: 1" in filled_prompt
        assert "First ID: t1" in filled_prompt

    def test_max_tokens_scales_with_tweet_count(self, predictor):
        api_response = MagicMock()
        api_response.content = [MagicMock(text="TWEET_1:\nPREDICTED_ENGAGEMENT: 5")]
        predictor.client.messages.create.return_value = api_response

        template = "{tweets}"

        with patch("pathlib.Path.read_text", return_value=template):
            # Test with 1 tweet
            predictor.predict_batch(tweets=[{"id": "1", "text": "one"}])
            call_args_1 = predictor.client.messages.create.call_args
            assert call_args_1[1]["max_tokens"] == 200

            # Test with 3 tweets
            predictor.predict_batch(
                tweets=[
                    {"id": "1", "text": "one"},
                    {"id": "2", "text": "two"},
                    {"id": "3", "text": "three"},
                ]
            )
            call_args_3 = predictor.client.messages.create.call_args
            assert call_args_3[1]["max_tokens"] == 600

            # Test with 10 tweets
            predictor.predict_batch(
                tweets=[{"id": str(i), "text": f"tweet {i}"} for i in range(10)]
            )
            call_args_10 = predictor.client.messages.create.call_args
            assert call_args_10[1]["max_tokens"] == 2000

    def test_results_match_input_tweet_count(self, predictor):
        # Mock response with scores for 3 tweets
        api_response = MagicMock()
        api_response.content = [
            MagicMock(
                text=(
                    "TWEET_1 (id=t1):\nPREDICTED_ENGAGEMENT: 7\n\n"
                    "TWEET_2 (id=t2):\nPREDICTED_ENGAGEMENT: 5\n\n"
                    "TWEET_3 (id=t3):\nPREDICTED_ENGAGEMENT: 8"
                )
            )
        ]
        predictor.client.messages.create.return_value = api_response

        tweets = [
            {"id": "t1", "text": "first"},
            {"id": "t2", "text": "second"},
            {"id": "t3", "text": "third"},
        ]

        template = "{tweets}"
        with patch("pathlib.Path.read_text", return_value=template):
            results = predictor.predict_batch(tweets=tweets)

        assert len(results) == 3
        assert results[0].tweet_id == "t1"
        assert results[1].tweet_id == "t2"
        assert results[2].tweet_id == "t3"

    def test_uses_correct_model(self, predictor):
        api_response = MagicMock()
        api_response.content = [MagicMock(text="TWEET_1:\nPREDICTED_ENGAGEMENT: 5")]
        predictor.client.messages.create.return_value = api_response

        template = "{tweets}"

        with patch("pathlib.Path.read_text", return_value=template):
            predictor.predict_batch(tweets=[{"id": "1", "text": "test"}])

        call_args = predictor.client.messages.create.call_args
        assert call_args[1]["model"] == "test-model"

    def test_account_context_included_when_provided(self, predictor):
        api_response = MagicMock()
        api_response.content = [MagicMock(text="TWEET_1:\nPREDICTED_ENGAGEMENT: 5")]
        predictor.client.messages.create.return_value = api_response

        template = "Context: {account_context}\nTweets: {tweets}"

        with patch("pathlib.Path.read_text", return_value=template):
            predictor.predict_batch(
                tweets=[{"id": "1", "text": "test"}],
                account_context="Account: @user, Bio: Test account, 10K followers",
            )

        call_args = predictor.client.messages.create.call_args
        filled_prompt = call_args[1]["messages"][0]["content"]
        assert "Account: @user" in filled_prompt
        assert "10K followers" in filled_prompt


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

    def test_purge_tweet_text(self, db):
        acct = self._create_account(db)
        db.insert_tweet("t1", acct["id"], "hello world", 5, 1, 0, 0, 8.0, "2025-01-01")
        db.insert_tweet("t2", acct["id"], "another tweet", 1, 0, 0, 0, 1.0, "2025-01-02")

        purged = db.purge_tweet_text()
        assert purged == 2

        tweets = db.get_tweets_for_account(acct["id"])
        assert all(t["text"] == "" for t in tweets)
        # IDs and metrics preserved
        assert all(t["tweet_id"] in ("t1", "t2") for t in tweets)
        assert any(t["engagement_score"] == 8.0 for t in tweets)

    def test_purge_idempotent(self, db):
        acct = self._create_account(db)
        db.insert_tweet("t1", acct["id"], "hello", 5, 1, 0, 0, 8.0, "2025-01-01")
        db.purge_tweet_text()
        purged_again = db.purge_tweet_text()
        assert purged_again == 0

    def test_purged_tweet_ids_and_restore(self, db):
        acct = self._create_account(db)
        db.insert_tweet("t1", acct["id"], "hello", 5, 1, 0, 0, 8.0, "2025-01-01")
        db.purge_tweet_text()

        purged_ids = db.get_purged_tweet_ids()
        assert purged_ids == ["t1"]

        db.update_tweet_text("t1", "restored text")
        tweets = db.get_tweets_for_account(acct["id"])
        assert tweets[0]["text"] == "restored text"
        assert db.get_purged_tweet_ids() == []

    def test_get_all_accounts(self, db):
        self._create_account(db, "1", "alice")
        self._create_account(db, "2", "bob")
        accounts = db.get_all_accounts()
        assert len(accounts) == 2
        assert accounts[0]["username"] == "alice"
