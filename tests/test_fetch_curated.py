"""Tests for fetch_curated.py — tweet fetching and filtering."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetch_curated import fetch_user_tweets


# --- helpers ---


def _make_tweet(tweet_id="999", text="A substantial tweet with enough content"):
    tweet = MagicMock()
    tweet.id = int(tweet_id)
    tweet.text = text
    return tweet


# --- TestFetchUserTweets ---


class TestFetchUserTweets:
    def test_returns_tweet_dicts(self):
        x_client = MagicMock()
        x_client.get_user_id.return_value = "12345"
        x_client.get_user_tweets.return_value = [
            {"id": "100", "text": "Hello world"},
        ]

        result = fetch_user_tweets(x_client, "testuser", limit=5)

        assert len(result) == 1
        assert result[0]["id"] == "100"
        assert result[0]["text"] == "Hello world"
        assert "testuser" in result[0]["url"]
        x_client.get_user_id.assert_called_once_with("testuser")
        x_client.get_user_tweets.assert_called_once_with("12345", count=5)

    def test_user_not_found(self):
        x_client = MagicMock()
        x_client.get_user_id.return_value = None

        result = fetch_user_tweets(x_client, "nonexistent")

        assert result == []

    def test_api_error(self):
        x_client = MagicMock()
        x_client.get_user_id.side_effect = Exception("Rate limited")

        result = fetch_user_tweets(x_client, "testuser")

        assert result == []

    def test_uses_cached_user_id_when_db_provided(self):
        x_client = MagicMock()
        x_client.get_user_tweets.return_value = [
            {"id": "100", "text": "Hello world"},
        ]
        db = MagicMock()
        db.get_meta.return_value = "cached-id"

        result = fetch_user_tweets(x_client, "testuser", limit=3, db=db)

        assert result[0]["id"] == "100"
        x_client.get_user_id.assert_not_called()
        x_client.get_user_tweets.assert_called_once_with("cached-id", count=3)


# --- TestMain filtering ---


class TestMainFiltering:
    @patch("fetch_curated.time.sleep")
    @patch("fetch_curated.ingest_curated_post")
    @patch("fetch_curated.InsightExtractor")
    @patch("fetch_curated.KnowledgeStore")
    @patch("fetch_curated.get_embedding_provider")
    @patch("fetch_curated.XClient")
    @patch("fetch_curated.script_context")
    @patch("fetch_curated.fetch_user_tweets")
    def test_skips_retweets_and_short_content(
        self, mock_fetch, mock_script_context, MockXClient,
        mock_embedder, MockStore, MockExtractor, mock_ingest, mock_sleep
    ):
        config = MagicMock()
        config.embeddings.provider = "voyage"
        config.embeddings.api_key = "key"
        config.embeddings.model = "model"
        config.anthropic.api_key = "key"
        config.synthesis.model = "model"
        config.x.api_key = "k"
        config.x.api_secret = "s"
        config.x.access_token = "at"
        config.x.access_token_secret = "ats"
        config.paths.database = ":memory:"
        config.curated_sources.x_accounts = [
            SimpleNamespace(identifier="testuser", license="attribution_required")
        ]

        db = MagicMock()
        mock_script_context.return_value.__enter__.return_value = (config, db)

        mock_fetch.return_value = [
            {"id": "1", "text": "RT @someone: Great post!", "url": "https://x.com/..."},
            {"id": "2", "text": "Short", "url": "https://x.com/..."},
            {"id": "3", "text": "A substantial original tweet about software engineering that is long enough", "url": "https://x.com/..."},
        ]
        MockStore.return_value.exists.return_value = False

        from fetch_curated import main
        main()

        # Only tweet 3 should be ingested (RT skipped, short skipped)
        mock_ingest.assert_called_once()
        assert mock_ingest.call_args[1]["post_id"] == "3"

    @patch("fetch_curated.time.sleep")
    @patch("fetch_curated.ingest_curated_post")
    @patch("fetch_curated.InsightExtractor")
    @patch("fetch_curated.KnowledgeStore")
    @patch("fetch_curated.get_embedding_provider")
    @patch("fetch_curated.XClient")
    @patch("fetch_curated.script_context")
    @patch("fetch_curated.fetch_user_tweets")
    def test_skips_existing(
        self, mock_fetch, mock_script_context, MockXClient,
        mock_embedder, MockStore, MockExtractor, mock_ingest, mock_sleep
    ):
        config = MagicMock()
        config.embeddings.provider = "voyage"
        config.embeddings.api_key = "key"
        config.embeddings.model = "model"
        config.anthropic.api_key = "key"
        config.synthesis.model = "model"
        config.x.api_key = "k"
        config.x.api_secret = "s"
        config.x.access_token = "at"
        config.x.access_token_secret = "ats"
        config.paths.database = ":memory:"
        config.curated_sources.x_accounts = [
            SimpleNamespace(identifier="testuser", license="open")
        ]

        db = MagicMock()
        mock_script_context.return_value.__enter__.return_value = (config, db)

        mock_fetch.return_value = [
            {"id": "1", "text": "A long enough tweet about AI that should be ingested normally", "url": "https://x.com/..."},
        ]
        MockStore.return_value.exists.return_value = True  # Already exists

        from fetch_curated import main
        main()

        mock_ingest.assert_not_called()

    def test_no_embeddings_exits(self, caplog):
        with patch("fetch_curated.script_context") as mock_script_context:
            config = MagicMock()
            config.embeddings = None
            db = MagicMock()
            mock_script_context.return_value.__enter__.return_value = (config, db)

            from fetch_curated import main
            with pytest.raises(SystemExit):
                main()
            assert "embeddings not configured" in caplog.text

    @patch("fetch_curated.script_context")
    def test_circuit_breaker_skips_before_initializing(self, mock_script_context):
        config = MagicMock()
        db = MagicMock()
        db.get_meta.side_effect = lambda key: {
            "x_api_blocked_until": "2999-01-01T00:00:00+00:00",
            "x_api_block_reason": "402 Payment Required",
        }.get(key)
        mock_script_context.return_value.__enter__.return_value = (config, db)

        with patch("fetch_curated.XClient") as MockXClient:
            from fetch_curated import main

            main()

        MockXClient.assert_not_called()
