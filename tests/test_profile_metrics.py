"""Tests for profile metrics tracking."""

from unittest.mock import MagicMock, patch

import pytest


class TestProfileMetricsDB:
    """Test profile_metrics table CRUD via Database methods."""

    def test_insert_and_retrieve(self, db):
        row_id = db.insert_profile_metrics(
            platform="x",
            follower_count=150,
            following_count=200,
            tweet_count=500,
            listed_count=3,
        )
        assert row_id > 0

        latest = db.get_latest_profile_metrics("x")
        assert latest is not None
        assert latest["follower_count"] == 150
        assert latest["following_count"] == 200
        assert latest["tweet_count"] == 500
        assert latest["listed_count"] == 3
        assert latest["fetched_at"] is not None

    def test_get_latest_returns_most_recent(self, db):
        db.insert_profile_metrics("x", 100, 50, 300, 1)
        db.insert_profile_metrics("x", 105, 51, 310, 2)
        db.insert_profile_metrics("x", 110, 52, 320, 3)

        latest = db.get_latest_profile_metrics("x")
        assert latest["follower_count"] == 110
        assert latest["tweet_count"] == 320

    def test_platform_isolation(self, db):
        db.insert_profile_metrics("x", 100, 50, 300, 1)
        db.insert_profile_metrics("bluesky", 20, 10, 40, 0)

        x = db.get_latest_profile_metrics("x")
        bsky = db.get_latest_profile_metrics("bluesky")

        assert x["follower_count"] == 100
        assert bsky["follower_count"] == 20

    def test_get_latest_returns_none_when_empty(self, db):
        assert db.get_latest_profile_metrics("x") is None

    def test_listed_count_nullable(self, db):
        db.insert_profile_metrics("x", 100, 50, 300, listed_count=None)
        latest = db.get_latest_profile_metrics("x")
        assert latest["listed_count"] is None


class TestXClientGetProfileMetrics:
    """Test XClient.get_profile_metrics() method."""

    def test_returns_dict_on_success(self):
        from output.x_client import XClient

        with patch("output.x_client.tweepy.Client") as MockClient:
            mock_me = MagicMock()
            mock_me.data.public_metrics = {
                "followers_count": 150,
                "following_count": 200,
                "tweet_count": 500,
                "listed_count": 3,
            }
            MockClient.return_value.get_me.return_value = mock_me

            client = XClient("k", "s", "t", "ts")
            metrics = client.get_profile_metrics()

        assert metrics == {
            "follower_count": 150,
            "following_count": 200,
            "tweet_count": 500,
            "listed_count": 3,
        }

    def test_returns_none_on_failure(self):
        from output.x_client import XClient

        with patch("output.x_client.tweepy.Client") as MockClient:
            MockClient.return_value.get_me.side_effect = Exception("API error")

            client = XClient("k", "s", "t", "ts")
            assert client.get_profile_metrics() is None

    def test_returns_none_when_no_data(self):
        from output.x_client import XClient

        with patch("output.x_client.tweepy.Client") as MockClient:
            mock_me = MagicMock()
            mock_me.data = None
            MockClient.return_value.get_me.return_value = mock_me

            client = XClient("k", "s", "t", "ts")
            assert client.get_profile_metrics() is None
