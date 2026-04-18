"""Tests for XClient media upload functionality."""

from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import tweepy

from output.x_client import XClient, PostResult


@pytest.fixture
def x_client():
    """Create an XClient with mocked tweepy components."""
    with patch("output.x_client.tweepy.Client") as MockClient:
        client = XClient("key", "secret", "token", "token_secret")
        # Reset the mock to clear __init__ call
        client.client = MagicMock()
        client._username = "testuser"
        yield client


class TestCredentialStorage:
    def test_stores_raw_credentials(self):
        with patch("output.x_client.tweepy.Client"):
            client = XClient("key", "secret", "token", "token_secret")
            assert client._api_key == "key"
            assert client._api_secret == "secret"
            assert client._access_token == "token"
            assert client._access_token_secret == "token_secret"


class TestV1Api:
    def test_creates_v1_api_lazily(self, x_client):
        with patch("output.x_client.tweepy.OAuth1UserHandler") as MockAuth, \
             patch("output.x_client.tweepy.API") as MockAPI:
            MockAuth.return_value = MagicMock()
            MockAPI.return_value = MagicMock()

            api = x_client._v1_api
            assert api is not None
            MockAuth.assert_called_once_with(
                "key", "secret", "token", "token_secret"
            )
            MockAPI.assert_called_once()

    def test_caches_v1_api_instance(self, x_client):
        with patch("output.x_client.tweepy.OAuth1UserHandler"), \
             patch("output.x_client.tweepy.API") as MockAPI:
            MockAPI.return_value = MagicMock()

            api1 = x_client._v1_api
            api2 = x_client._v1_api
            assert api1 is api2
            MockAPI.assert_called_once()


class TestUploadMedia:
    def test_upload_returns_media_id(self, x_client):
        mock_api = MagicMock()
        mock_media = MagicMock()
        mock_media.media_id = 12345
        mock_api.media_upload.return_value = mock_media
        x_client._v1_api_instance = mock_api

        result = x_client.upload_media("/path/to/image.png")
        assert result == "12345"
        mock_api.media_upload.assert_called_once_with(filename="/path/to/image.png")

    def test_upload_with_alt_text(self, x_client):
        mock_api = MagicMock()
        mock_media = MagicMock()
        mock_media.media_id = 12345
        mock_api.media_upload.return_value = mock_media
        x_client._v1_api_instance = mock_api

        result = x_client.upload_media("/path/to/image.png", alt_text="Description")
        assert result == "12345"
        mock_api.create_media_metadata.assert_called_once_with(
            12345, alt_text="Description"
        )

    def test_upload_failure_returns_none(self, x_client):
        mock_api = MagicMock()
        mock_api.media_upload.side_effect = tweepy.TweepyException("upload error")
        x_client._v1_api_instance = mock_api

        result = x_client.upload_media("/path/to/image.png")
        assert result is None


class TestPostWithMedia:
    def test_post_with_media_success(self, x_client):
        # Mock upload
        mock_api = MagicMock()
        mock_media = MagicMock()
        mock_media.media_id = 12345
        mock_api.media_upload.return_value = mock_media
        x_client._v1_api_instance = mock_api

        # Mock v2 create_tweet
        x_client.client.create_tweet.return_value = MagicMock(
            data={"id": "999"}
        )

        result = x_client.post_with_media("Hello with image", "/path/image.png")
        assert result.success is True
        assert result.tweet_id == "999"
        assert "999" in result.url
        x_client.client.create_tweet.assert_called_once_with(
            text="Hello with image", media_ids=["12345"]
        )

    def test_post_with_media_upload_fails(self, x_client):
        mock_api = MagicMock()
        mock_api.media_upload.side_effect = tweepy.TweepyException("fail")
        x_client._v1_api_instance = mock_api

        result = x_client.post_with_media("Hello", "/path/image.png")
        assert result.success is False
        assert "upload failed" in result.error.lower()
        x_client.client.create_tweet.assert_not_called()

    def test_post_with_media_tweet_fails(self, x_client):
        mock_api = MagicMock()
        mock_media = MagicMock()
        mock_media.media_id = 12345
        mock_api.media_upload.return_value = mock_media
        x_client._v1_api_instance = mock_api

        x_client.client.create_tweet.side_effect = tweepy.TweepyException("403")

        result = x_client.post_with_media("Hello", "/path/image.png")
        assert result.success is False
        assert "403" in result.error

    def test_post_with_media_and_alt_text(self, x_client):
        mock_api = MagicMock()
        mock_media = MagicMock()
        mock_media.media_id = 12345
        mock_api.media_upload.return_value = mock_media
        x_client._v1_api_instance = mock_api

        x_client.client.create_tweet.return_value = MagicMock(
            data={"id": "999"}
        )

        result = x_client.post_with_media(
            "Hello", "/path/image.png", alt_text="An image"
        )
        assert result.success is True
        mock_api.create_media_metadata.assert_called_once()
