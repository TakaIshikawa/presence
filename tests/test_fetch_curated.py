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
        user_data = MagicMock()
        user_data.id = 12345
        x_client.client.get_user.return_value = MagicMock(data=user_data)
        x_client.client.get_users_tweets.return_value = MagicMock(
            data=[_make_tweet(tweet_id="100", text="Hello world")]
        )

        result = fetch_user_tweets(x_client, "testuser", limit=5)

        assert len(result) == 1
        assert result[0]["id"] == "100"
        assert result[0]["text"] == "Hello world"
        assert "testuser" in result[0]["url"]

    def test_user_not_found(self):
        x_client = MagicMock()
        x_client.client.get_user.return_value = MagicMock(data=None)

        result = fetch_user_tweets(x_client, "nonexistent")

        assert result == []

    def test_api_error(self):
        x_client = MagicMock()
        x_client.client.get_user.side_effect = Exception("Rate limited")

        result = fetch_user_tweets(x_client, "testuser")

        assert result == []


# --- TestMain filtering ---


class TestMainFiltering:
    @patch("fetch_curated.time.sleep")
    @patch("fetch_curated.ingest_curated_post")
    @patch("fetch_curated.InsightExtractor")
    @patch("fetch_curated.KnowledgeStore")
    @patch("fetch_curated.get_embedding_provider")
    @patch("fetch_curated.XClient")
    @patch("fetch_curated.Database")
    @patch("fetch_curated.load_config")
    @patch("fetch_curated.fetch_user_tweets")
    def test_skips_retweets_and_short_content(
        self, mock_fetch, mock_config, MockDB, MockXClient,
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
        mock_config.return_value = config

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
    @patch("fetch_curated.Database")
    @patch("fetch_curated.load_config")
    @patch("fetch_curated.fetch_user_tweets")
    def test_skips_existing(
        self, mock_fetch, mock_config, MockDB, MockXClient,
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
        mock_config.return_value = config

        mock_fetch.return_value = [
            {"id": "1", "text": "A long enough tweet about AI that should be ingested normally", "url": "https://x.com/..."},
        ]
        MockStore.return_value.exists.return_value = True  # Already exists

        from fetch_curated import main
        main()

        mock_ingest.assert_not_called()

    def test_no_embeddings_exits(self, caplog):
        with patch("fetch_curated.load_config") as mock_config:
            config = MagicMock()
            config.embeddings = None
            mock_config.return_value = config

            from fetch_curated import main
            with pytest.raises(SystemExit):
                main()
            assert "embeddings not configured" in caplog.text
