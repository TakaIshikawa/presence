"""Tests for fetch_curated.py — tweet fetching and filtering."""

from email.message import Message
import logging
import sys
from types import ModuleType
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

if "tweepy" not in sys.modules:
    tweepy_stub = ModuleType("tweepy")
    tweepy_stub.API = object
    tweepy_stub.Client = object
    tweepy_stub.OAuth1UserHandler = object
    tweepy_stub.TweepyException = Exception
    sys.modules["tweepy"] = tweepy_stub

from fetch_curated import fetch_user_tweets
from knowledge.rss import fetch_feed, parse_feed


# --- helpers ---


def _make_tweet(tweet_id="999", text="A substantial tweet with enough content"):
    tweet = MagicMock()
    tweet.id = int(tweet_id)
    tweet.text = text
    return tweet


class _MockFeedResponse:
    def __init__(self, body: str, headers: dict[str, str] | None = None):
        self._body = body.encode("utf-8")
        self.headers = Message()
        for key, value in (headers or {}).items():
            self.headers[key] = value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self._body


def _http_headers(headers: dict[str, str]) -> Message:
    message = Message()
    for key, value in headers.items():
        message[key] = value
    return message


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


# --- RSS fetching ---


class TestFetchCuratedFeeds:
    def test_parse_local_rss_fixture(self):
        fixture = Path(__file__).parent / "fixtures" / "curated_feed.xml"

        entries = parse_feed(fixture.read_text(), limit=2)

        assert len(entries) == 2
        assert entries[0].title == "Building reliable agent loops"
        assert entries[0].link == "https://example.com/agent-loops"
        assert "separate planning" in entries[0].content
        assert entries[1].summary == "Context windows should shape the product interface."

    def test_parse_atom_feed(self):
        atom = """<?xml version="1.0" encoding="utf-8"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
          <entry>
            <title>Newsletter issue</title>
            <link href="https://newsletter.example.com/issues/42" rel="alternate" />
            <published>2026-04-22T10:00:00Z</published>
            <summary>Issue summary</summary>
            <content type="html">&lt;p&gt;Issue content&lt;/p&gt;</content>
          </entry>
        </feed>
        """

        entries = parse_feed(atom, limit=1)

        assert len(entries) == 1
        assert entries[0].title == "Newsletter issue"
        assert entries[0].link == "https://newsletter.example.com/issues/42"
        assert entries[0].summary == "Issue summary"
        assert entries[0].content == "Issue content"
        assert entries[0].published_at == "2026-04-22T10:00:00Z"

    @patch("fetch_curated.ingest_curated_article")
    @patch("fetch_curated.fetch_feed")
    def test_ingests_new_feed_entries_and_skips_existing(self, mock_fetch, mock_ingest):
        from fetch_curated import fetch_curated_feed_source
        from knowledge.rss import FeedFetchResult

        fixture = Path(__file__).parent / "fixtures" / "curated_feed.xml"
        mock_fetch.return_value = FeedFetchResult(parse_feed(fixture.read_text(), limit=2))
        store = MagicMock()
        store.exists.side_effect = [False, True]
        extractor = MagicMock()
        source = SimpleNamespace(
            identifier="example.com",
            name="Example Blog",
            license="open",
            feed_url="https://example.com/feed.xml",
        )

        count = fetch_curated_feed_source(store, extractor, source, limit=2)

        assert count == 1
        store.exists.assert_any_call("curated_article", "https://example.com/agent-loops")
        store.exists.assert_any_call("curated_article", "https://example.com/context-windows")
        mock_ingest.assert_called_once()
        assert mock_ingest.call_args.kwargs["url"] == "https://example.com/agent-loops"
        assert mock_ingest.call_args.kwargs["title"] == "Building reliable agent loops"
        assert mock_ingest.call_args.kwargs["author"] == "Example Blog"
        assert mock_ingest.call_args.kwargs["license_type"] == "open"

    @patch("fetch_curated.ingest_curated_article")
    @patch("fetch_curated.fetch_feed")
    @patch("fetch_curated.discover_feed_candidates")
    def test_autodiscovers_and_caches_missing_feed_url(
        self, mock_discover, mock_fetch, mock_ingest, db
    ):
        from fetch_curated import fetch_curated_feed_source
        from knowledge.rss import FeedCandidate, FeedFetchResult

        fixture = Path(__file__).parent / "fixtures" / "curated_feed.xml"
        db.sync_config_sources(
            [{"identifier": "example.com", "name": "Example"}],
            "blog",
        )
        mock_discover.return_value = [
            FeedCandidate(
                url="https://example.com/feed.xml",
                content_type="application/rss+xml",
                score=90,
            )
        ]
        mock_fetch.return_value = FeedFetchResult(parse_feed(fixture.read_text(), limit=1))
        store = MagicMock()
        store.exists.return_value = False
        source = SimpleNamespace(
            source_type="blog",
            identifier="example.com",
            name="Example",
            license="open",
            feed_url=None,
            homepage_url="https://example.com/blog",
        )

        count = fetch_curated_feed_source(
            store,
            MagicMock(),
            source,
            db=db,
            limit=1,
            autodiscovery_enabled=True,
            autodiscovery_timeout=4.5,
        )

        assert count == 1
        mock_discover.assert_called_once_with("https://example.com/blog", timeout=4.5)
        mock_fetch.assert_called_once()
        assert mock_fetch.call_args.args[0] == "https://example.com/feed.xml"
        assert db.get_curated_source("blog", "example.com")["feed_url"] == "https://example.com/feed.xml"
        mock_ingest.assert_called_once()

    @patch("fetch_curated.ingest_curated_newsletter")
    @patch("fetch_curated.fetch_feed")
    def test_newsletter_feed_entries_use_distinct_source_type(self, mock_fetch, mock_ingest):
        from fetch_curated import fetch_curated_feed_source
        from knowledge.rss import FeedFetchResult

        fixture = Path(__file__).parent / "fixtures" / "curated_feed.xml"
        mock_fetch.return_value = FeedFetchResult(parse_feed(fixture.read_text(), limit=2))
        store = MagicMock()
        store.exists.side_effect = [False, True]
        source = SimpleNamespace(
            source_type="newsletter",
            identifier="newsletter.example.com",
            name="Example Newsletter",
            license="restricted",
            feed_url="https://newsletter.example.com/feed.xml",
        )

        count = fetch_curated_feed_source(store, MagicMock(), source, limit=2)

        assert count == 1
        store.exists.assert_any_call("curated_newsletter", "https://example.com/agent-loops")
        store.exists.assert_any_call("curated_newsletter", "https://example.com/context-windows")
        mock_ingest.assert_called_once()
        assert mock_ingest.call_args.kwargs["url"] == "https://example.com/agent-loops"
        assert mock_ingest.call_args.kwargs["author"] == "Example Newsletter"
        assert mock_ingest.call_args.kwargs["license_type"] == "restricted"

    @patch("fetch_curated.ingest_curated_newsletter")
    @patch("fetch_curated.fetch_feed")
    def test_newsletter_dry_run_reports_without_ingesting(self, mock_fetch, mock_ingest, caplog):
        from fetch_curated import fetch_curated_feed_source
        from knowledge.rss import FeedFetchResult

        caplog.set_level(logging.INFO)
        fixture = Path(__file__).parent / "fixtures" / "curated_feed.xml"
        mock_fetch.return_value = FeedFetchResult(parse_feed(fixture.read_text(), limit=1))
        store = MagicMock()
        store.exists.return_value = False
        source = SimpleNamespace(
            source_type="newsletter",
            identifier="newsletter.example.com",
            name="Example Newsletter",
            license="restricted",
            feed_url="https://newsletter.example.com/feed.xml",
        )

        count = fetch_curated_feed_source(
            store,
            MagicMock(),
            source,
            limit=1,
            dry_run=True,
        )

        assert count == 1
        mock_ingest.assert_not_called()
        assert "[dry-run] Would ingest curated_newsletter entry" in caplog.text

    def test_config_source_uses_cached_discovered_feed_url(self, db):
        from fetch_curated import _active_feed_sources

        db.sync_config_sources(
            [{"identifier": "example.com", "name": "Example"}],
            "blog",
        )
        db.update_curated_source_feed_url("blog", "example.com", "https://example.com/rss")
        config = SimpleNamespace(
            curated_sources=SimpleNamespace(
                blogs=[SimpleNamespace(identifier="example.com", name="Example", feed_url=None)],
                newsletters=[],
            )
        )

        sources = _active_feed_sources(config, db)

        assert sources[0].feed_url == "https://example.com/rss"

    @patch("knowledge.rss.urlopen")
    def test_fetch_feed_persists_headers_from_first_fetch(self, mock_urlopen):
        fixture = Path(__file__).parent / "fixtures" / "curated_feed.xml"
        mock_urlopen.return_value = _MockFeedResponse(
            fixture.read_text(),
            {
                "ETag": '"feed-v1"',
                "Last-Modified": "Wed, 22 Apr 2026 10:00:00 GMT",
            },
        )

        result = fetch_feed("https://example.com/feed.xml", limit=1)

        assert len(result.entries) == 1
        assert result.etag == '"feed-v1"'
        assert result.last_modified == "Wed, 22 Apr 2026 10:00:00 GMT"
        headers = {k.lower(): v for k, v in mock_urlopen.call_args.args[0].header_items()}
        assert "if-none-match" not in headers
        assert "if-modified-since" not in headers

    @patch("knowledge.rss.urlopen")
    def test_fetch_feed_304_is_successful_noop(self, mock_urlopen):
        mock_urlopen.side_effect = HTTPError(
            "https://example.com/feed.xml",
            304,
            "Not Modified",
            _http_headers({"ETag": '"feed-v1"'}),
            None,
        )

        result = fetch_feed(
            "https://example.com/feed.xml",
            etag='"feed-v1"',
            last_modified="Wed, 22 Apr 2026 10:00:00 GMT",
        )

        assert result.not_modified is True
        assert result.entries == []
        assert result.etag == '"feed-v1"'
        assert result.last_modified == "Wed, 22 Apr 2026 10:00:00 GMT"
        headers = {k.lower(): v for k, v in mock_urlopen.call_args.args[0].header_items()}
        assert headers["if-none-match"] == '"feed-v1"'
        assert headers["if-modified-since"] == "Wed, 22 Apr 2026 10:00:00 GMT"

    @patch("fetch_curated.ingest_curated_article")
    @patch("knowledge.rss.urlopen")
    def test_changed_feed_updates_cache_and_ingests(self, mock_urlopen, mock_ingest, db):
        from fetch_curated import fetch_curated_feed_source

        fixture = Path(__file__).parent / "fixtures" / "curated_feed.xml"
        db.sync_config_sources(
            [{"identifier": "example.com", "name": "Example", "feed_url": "https://example.com/feed.xml"}],
            "blog",
        )
        db.update_curated_source_feed_cache(
            "blog",
            "example.com",
            '"feed-v1"',
            "Wed, 22 Apr 2026 10:00:00 GMT",
        )
        source = SimpleNamespace(
            source_type="blog",
            identifier="example.com",
            name="Example",
            license="open",
            feed_url="https://example.com/feed.xml",
            feed_etag='"feed-v1"',
            feed_last_modified="Wed, 22 Apr 2026 10:00:00 GMT",
        )
        mock_urlopen.return_value = _MockFeedResponse(
            fixture.read_text(),
            {
                "ETag": '"feed-v2"',
                "Last-Modified": "Wed, 22 Apr 2026 11:00:00 GMT",
            },
        )
        store = MagicMock()
        store.exists.return_value = False

        count = fetch_curated_feed_source(store, MagicMock(), source, db=db, limit=1)

        assert count == 1
        row = db.get_curated_source("blog", "example.com")
        assert row["feed_etag"] == '"feed-v2"'
        assert row["feed_last_modified"] == "Wed, 22 Apr 2026 11:00:00 GMT"
        headers = {k.lower(): v for k, v in mock_urlopen.call_args.args[0].header_items()}
        assert headers["if-none-match"] == '"feed-v1"'
        assert headers["if-modified-since"] == "Wed, 22 Apr 2026 10:00:00 GMT"
        mock_ingest.assert_called_once()

    @patch("knowledge.rss.urlopen")
    def test_missing_feed_headers_clear_cache(self, mock_urlopen, db):
        from fetch_curated import fetch_curated_feed_source

        fixture = Path(__file__).parent / "fixtures" / "curated_feed.xml"
        db.sync_config_sources(
            [{"identifier": "example.com", "name": "Example", "feed_url": "https://example.com/feed.xml"}],
            "blog",
        )
        db.update_curated_source_feed_cache("blog", "example.com", '"feed-v1"', "old-date")
        source = SimpleNamespace(
            source_type="blog",
            identifier="example.com",
            name="Example",
            license="open",
            feed_url="https://example.com/feed.xml",
            feed_etag='"feed-v1"',
            feed_last_modified="old-date",
        )
        mock_urlopen.return_value = _MockFeedResponse(fixture.read_text(), {})
        store = MagicMock()
        store.exists.return_value = True

        count = fetch_curated_feed_source(store, MagicMock(), source, db=db, limit=1)

        assert count == 0
        row = db.get_curated_source("blog", "example.com")
        assert row["feed_etag"] is None
        assert row["feed_last_modified"] is None

    @patch("fetch_curated.fetch_feed")
    def test_feed_failure_records_source_health(self, mock_fetch, db):
        from fetch_curated import fetch_curated_feed_source

        db.sync_config_sources(
            [{"identifier": "example.com", "name": "Example", "feed_url": "https://example.com/feed.xml"}],
            "blog",
        )
        source = SimpleNamespace(
            source_type="blog",
            identifier="example.com",
            name="Example",
            license="open",
            feed_url="https://example.com/feed.xml",
        )
        mock_fetch.side_effect = ValueError("bad feed")

        with pytest.raises(ValueError):
            fetch_curated_feed_source(
                MagicMock(),
                MagicMock(),
                source,
                db=db,
                failure_threshold=2,
                cooldown_hours=24,
            )

        row = db.get_curated_source("blog", "example.com")
        assert row["last_fetch_status"] == "failure"
        assert row["consecutive_failures"] == 1
        assert "ValueError: bad feed" in row["last_error"]

    @patch("fetch_curated.fetch_feed")
    def test_feed_source_in_cooldown_is_skipped(self, mock_fetch, db, caplog):
        from fetch_curated import fetch_curated_feed_source

        db.sync_config_sources(
            [{"identifier": "example.com", "name": "Example", "feed_url": "https://example.com/feed.xml"}],
            "blog",
        )
        db.record_curated_source_fetch_failure("blog", "example.com", "bad feed")
        db.record_curated_source_fetch_failure("blog", "example.com", "bad feed")
        source = SimpleNamespace(
            source_type="blog",
            identifier="example.com",
            name="Example",
            license="open",
            feed_url="https://example.com/feed.xml",
        )

        count = fetch_curated_feed_source(
            MagicMock(),
            MagicMock(),
            source,
            db=db,
            failure_threshold=2,
            cooldown_hours=24,
        )

        assert count == 0
        mock_fetch.assert_not_called()
        assert "source health cooldown active" in caplog.text
        assert db.get_curated_source("blog", "example.com")["last_fetch_status"] == "quarantined"

    @patch("fetch_curated.ingest_curated_newsletter")
    @patch("fetch_curated.fetch_feed")
    def test_newsletter_ingestion_failure_records_source_health(self, mock_fetch, mock_ingest, db):
        from fetch_curated import fetch_curated_feed_source
        from knowledge.rss import FeedFetchResult

        fixture = Path(__file__).parent / "fixtures" / "curated_feed.xml"
        db.sync_config_sources(
            [
                {
                    "identifier": "newsletter.example.com",
                    "name": "Example Newsletter",
                    "feed_url": "https://newsletter.example.com/feed.xml",
                    "license": "restricted",
                }
            ],
            "newsletter",
        )
        mock_fetch.return_value = FeedFetchResult(parse_feed(fixture.read_text(), limit=1))
        mock_ingest.side_effect = RuntimeError("extractor failed")
        store = MagicMock()
        store.exists.return_value = False
        source = SimpleNamespace(
            source_type="newsletter",
            identifier="newsletter.example.com",
            name="Example Newsletter",
            license="restricted",
            feed_url="https://newsletter.example.com/feed.xml",
        )

        with pytest.raises(RuntimeError):
            fetch_curated_feed_source(
                store,
                MagicMock(),
                source,
                db=db,
                limit=1,
                failure_threshold=2,
                cooldown_hours=24,
            )

        row = db.get_curated_source("newsletter", "newsletter.example.com")
        assert row["last_fetch_status"] == "failure"
        assert row["consecutive_failures"] == 1
        assert "RuntimeError: extractor failed" in row["last_error"]

    def test_x_account_not_found_records_failure(self, db):
        from fetch_curated import _fetch_account_with_health

        db.sync_config_sources(
            [{"identifier": "missing_user", "name": "Missing"}],
            "x_account",
        )
        x_client = MagicMock()
        x_client.last_error = None
        x_client.get_user_id.return_value = None

        tweets = _fetch_account_with_health(
            x_client,
            SimpleNamespace(identifier="missing_user"),
            db,
            limit=5,
            failure_threshold=2,
            cooldown_hours=24,
        )

        assert tweets == []
        row = db.get_curated_source("x_account", "missing_user")
        assert row["last_fetch_status"] == "failure"
        assert row["consecutive_failures"] == 1
        assert "not found" in row["last_error"]


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

    @patch("fetch_curated.InsightExtractor")
    @patch("fetch_curated.KnowledgeStore")
    @patch("fetch_curated.get_embedding_provider")
    @patch("fetch_curated.script_context")
    def test_circuit_breaker_skips_x_before_initializing(
        self, mock_script_context, mock_embedder, MockStore, MockExtractor
    ):
        config = MagicMock()
        config.embeddings.provider = "voyage"
        config.embeddings.api_key = "key"
        config.embeddings.model = "model"
        config.anthropic.api_key = "key"
        config.synthesis.model = "model"
        config.curated_sources.x_accounts = []
        config.curated_sources.blogs = []
        config.curated_sources.newsletters = []
        db = MagicMock()
        db.get_meta.side_effect = lambda key: {
            "x_api_blocked_until": "2999-01-01T00:00:00+00:00",
            "x_api_block_reason": "402 Payment Required",
        }.get(key)
        db.get_active_curated_sources.return_value = []
        mock_script_context.return_value.__enter__.return_value = (config, db)

        with patch("fetch_curated.XClient") as MockXClient:
            from fetch_curated import main

            main()

        MockXClient.assert_not_called()

    @patch("fetch_curated.InsightExtractor")
    @patch("fetch_curated.KnowledgeStore")
    @patch("fetch_curated.get_embedding_provider")
    @patch("fetch_curated.script_context")
    def test_low_x_rate_budget_skips_curated_x_fetch(
        self, mock_script_context, mock_embedder, MockStore, MockExtractor, caplog
    ):
        import logging

        caplog.set_level(logging.WARNING)
        config = MagicMock()
        config.embeddings.provider = "voyage"
        config.embeddings.api_key = "key"
        config.embeddings.model = "model"
        config.anthropic.api_key = "key"
        config.synthesis.model = "model"
        config.curated_sources.x_accounts = [
            SimpleNamespace(identifier="testuser", license="open")
        ]
        config.curated_sources.blogs = []
        config.curated_sources.newsletters = []
        config.rate_limits.x_min_remaining = 5

        db = MagicMock()
        db.get_meta.side_effect = lambda key: {
            "api_rate_limit:x:remaining": "5",
        }.get(key)
        db.get_active_curated_sources.return_value = []
        mock_script_context.return_value.__enter__.return_value = (config, db)

        with (
            patch("fetch_curated.XClient") as MockXClient,
            patch("fetch_curated.fetch_user_tweets") as mock_fetch,
        ):
            from fetch_curated import main

            main()

        MockXClient.assert_not_called()
        mock_fetch.assert_not_called()
        assert "skipping curated X account fetch" in caplog.text
