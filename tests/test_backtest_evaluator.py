"""Tests for backtest_evaluator.py orchestration."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def _make_config():
    config = MagicMock()
    config.anthropic.api_key = "test-key"
    config.synthesis.eval_model = "eval-model"
    config.x.api_key = "xk"
    config.x.api_secret = "xs"
    config.x.access_token = "xt"
    config.x.access_token_secret = "xts"
    return config


def _make_tweet(tweet_id="100", text="A great tweet about AI", username="dev_alice",
                follower_count=5000, bio="I build things"):
    return {
        "tweet_id": tweet_id,
        "text": text,
        "username": username,
        "follower_count": follower_count,
        "bio": bio,
    }


def _make_prediction(tweet_id="100", score=7.5):
    return SimpleNamespace(
        tweet_id=tweet_id,
        predicted_score=score,
        hook_strength=7.0,
        specificity=7.0,
        emotional_resonance=7.0,
        novelty=7.0,
        actionability=7.0,
        raw_response="raw",
    )


class TestMain:
    @patch("backtest_evaluator.time.sleep")
    @patch("backtest_evaluator.tweepy.Client")
    @patch("backtest_evaluator.ValidationDatabase")
    @patch("backtest_evaluator.EngagementPredictor")
    @patch("backtest_evaluator.script_context")
    def test_no_unevaluated_tweets(self, mock_context, MockPredictor, MockDB,
                                    MockTweepy, mock_sleep):
        mock_context.return_value.__enter__.return_value = (_make_config(), None)
        mock_db = MockDB.return_value
        mock_db.get_purged_tweet_ids.return_value = []
        mock_db.get_unevaluated_tweets.return_value = []

        with patch("sys.argv", ["backtest_evaluator.py", "--version", "v1"]):
            from backtest_evaluator import main
            main()

        MockPredictor.return_value.predict_batch.assert_not_called()

    @patch("backtest_evaluator.time.sleep")
    @patch("backtest_evaluator.tweepy.Client")
    @patch("backtest_evaluator.ValidationDatabase")
    @patch("backtest_evaluator.EngagementPredictor")
    @patch("backtest_evaluator.script_context")
    def test_groups_by_account(self, mock_context, MockPredictor, MockDB,
                                MockTweepy, mock_sleep):
        mock_context.return_value.__enter__.return_value = (_make_config(), None)
        mock_db = MockDB.return_value
        mock_db.get_purged_tweet_ids.return_value = []
        mock_db.get_unevaluated_tweets.return_value = [
            _make_tweet(tweet_id="1", username="alice"),
            _make_tweet(tweet_id="2", username="alice"),
            _make_tweet(tweet_id="3", username="bob"),
        ]
        predictor = MockPredictor.return_value
        predictor.predict_batch.return_value = [
            _make_prediction("1"), _make_prediction("2")
        ]

        with patch("sys.argv", ["backtest_evaluator.py", "--version", "v1"]):
            from backtest_evaluator import main
            main()

        # predict_batch called at least twice (once for alice's batch, once for bob's)
        assert predictor.predict_batch.call_count >= 2

    @patch("backtest_evaluator.time.sleep")
    @patch("backtest_evaluator.tweepy.Client")
    @patch("backtest_evaluator.ValidationDatabase")
    @patch("backtest_evaluator.EngagementPredictor")
    @patch("backtest_evaluator.script_context")
    def test_batch_error_continues(self, mock_context, MockPredictor, MockDB,
                                    MockTweepy, mock_sleep):
        mock_context.return_value.__enter__.return_value = (_make_config(), None)
        mock_db = MockDB.return_value
        mock_db.get_purged_tweet_ids.return_value = []
        mock_db.get_unevaluated_tweets.return_value = [
            _make_tweet(tweet_id="1", username="alice"),
        ]
        predictor = MockPredictor.return_value
        predictor.predict_batch.side_effect = Exception("API error")

        with patch("sys.argv", ["backtest_evaluator.py", "--version", "v1"]):
            from backtest_evaluator import main
            main()

        # No evaluations inserted
        mock_db.insert_evaluation.assert_not_called()

    @patch("backtest_evaluator.time.sleep")
    @patch("backtest_evaluator.tweepy.Client")
    @patch("backtest_evaluator.ValidationDatabase")
    @patch("backtest_evaluator.EngagementPredictor")
    @patch("backtest_evaluator.script_context")
    def test_no_purge_flag(self, mock_context, MockPredictor, MockDB,
                            MockTweepy, mock_sleep):
        mock_context.return_value.__enter__.return_value = (_make_config(), None)
        mock_db = MockDB.return_value
        mock_db.get_purged_tweet_ids.return_value = []
        mock_db.get_unevaluated_tweets.return_value = []

        with patch("sys.argv", ["backtest_evaluator.py", "--version", "v1", "--no-purge"]):
            from backtest_evaluator import main
            main()

        mock_db.purge_tweet_text.assert_not_called()

    @patch("backtest_evaluator.time.sleep")
    @patch("backtest_evaluator.tweepy.Client")
    @patch("backtest_evaluator.ValidationDatabase")
    @patch("backtest_evaluator.EngagementPredictor")
    @patch("backtest_evaluator.script_context")
    def test_empty_text_filtered(self, mock_context, MockPredictor, MockDB,
                                  MockTweepy, mock_sleep):
        mock_context.return_value.__enter__.return_value = (_make_config(), None)
        mock_db = MockDB.return_value
        mock_db.get_purged_tweet_ids.return_value = []
        mock_db.get_unevaluated_tweets.return_value = [
            _make_tweet(tweet_id="1", text=""),  # empty
            _make_tweet(tweet_id="2", text=None),  # None
            _make_tweet(tweet_id="3", text="Real content"),  # valid
        ]
        predictor = MockPredictor.return_value
        predictor.predict_batch.return_value = [_make_prediction("3")]

        with patch("sys.argv", ["backtest_evaluator.py", "--version", "v1", "--no-purge"]):
            from backtest_evaluator import main
            main()

        # Only tweet 3 should be processed (1 and 2 filtered by `if t["text"]`)
        assert predictor.predict_batch.call_count == 1
