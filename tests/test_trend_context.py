"""Tests for trend context module (Feature 3)."""

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from knowledge.store import KnowledgeItem
from synthesis.trend_context import TrendContextBuilder


def _make_item(author: str, content: str, insight: str = None) -> KnowledgeItem:
    return KnowledgeItem(
        id=1,
        source_type="curated_x",
        source_id=f"tweet-{author}",
        source_url=f"https://x.com/{author}/status/1",
        author=author,
        content=content,
        insight=insight,
        embedding=None,
        attribution_required=True,
        approved=True,
        created_at=datetime.now(timezone.utc).isoformat(),
    )


class TestFormatContext:
    """Tests for _format_context."""

    def test_includes_themes_and_takes(self):
        builder = TrendContextBuilder.__new__(TrendContextBuilder)
        items = [
            _make_item("karpathy", "LLMs are getting better at reasoning", "Reasoning capabilities improving"),
            _make_item("swyx", "The AI engineer stack is maturing", "AI engineering tooling"),
            _make_item("simonw", "SQLite for everything", "SQLite as universal DB"),
        ]
        themes = ["LLM reasoning improvements", "AI engineer tooling stack"]

        result = builder._format_context(themes, items)

        assert "CURRENT DISCOURSE" in result
        assert "LLM reasoning improvements" in result
        assert "AI engineer tooling stack" in result
        assert "@karpathy" in result
        assert "@swyx" in result
        assert "@simonw" in result
        assert "do NOT force it" in result

    def test_deduplicates_authors(self):
        builder = TrendContextBuilder.__new__(TrendContextBuilder)
        items = [
            _make_item("karpathy", "Post 1"),
            _make_item("karpathy", "Post 2"),
            _make_item("swyx", "Post 3"),
        ]
        result = builder._format_context(["theme1"], items)
        # Should only show karpathy once in notable takes
        assert result.count("@karpathy:") == 1

    def test_uses_insight_over_content(self):
        builder = TrendContextBuilder.__new__(TrendContextBuilder)
        items = [
            _make_item("karpathy", "Very long content here", insight="Key insight"),
        ]
        result = builder._format_context(["theme1"], items)
        assert "Key insight" in result

    def test_falls_back_to_content_when_no_insight(self):
        builder = TrendContextBuilder.__new__(TrendContextBuilder)
        items = [
            _make_item("karpathy", "Short content"),
        ]
        result = builder._format_context(["theme1"], items)
        assert "Short content" in result


class TestBuildContext:
    """Tests for build_context."""

    def test_empty_when_too_few_items(self):
        mock_store = MagicMock()
        mock_store.get_recent_by_source_type.return_value = [
            _make_item("karpathy", "Only one post"),
        ]
        builder = TrendContextBuilder(
            knowledge_store=mock_store,
            api_key="test-key",
        )
        result = builder.build_context()
        assert result == ""

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_builds_context_from_items(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = '["AI agents autonomy", "Code generation quality"]'
        mock_client.messages.create.return_value = mock_response

        mock_store = MagicMock()
        mock_store.get_recent_by_source_type.return_value = [
            _make_item("karpathy", "Agents are getting autonomous"),
            _make_item("swyx", "Code gen is surprisingly good now"),
            _make_item("simonw", "LLMs as database query engines"),
        ]

        builder = TrendContextBuilder(
            knowledge_store=mock_store,
            api_key="test-key",
        )
        result = builder.build_context()

        assert "CURRENT DISCOURSE" in result
        assert "AI agents autonomy" in result
        assert "@karpathy" in result


class TestExtractThemes:
    """Tests for _extract_themes."""

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_parses_json_array(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = '["Theme 1", "Theme 2", "Theme 3"]'
        mock_client.messages.create.return_value = mock_response

        builder = TrendContextBuilder(
            knowledge_store=MagicMock(),
            api_key="test-key",
        )
        themes = builder._extract_themes([_make_item("a", "text")])
        assert themes == ["Theme 1", "Theme 2", "Theme 3"]

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_handles_markdown_wrapped_json(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = '```json\n["Theme 1"]\n```'
        mock_client.messages.create.return_value = mock_response

        builder = TrendContextBuilder(
            knowledge_store=MagicMock(),
            api_key="test-key",
        )
        themes = builder._extract_themes([_make_item("a", "text")])
        assert themes == ["Theme 1"]

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_returns_empty_on_parse_failure(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = "I cannot parse this as themes"
        mock_client.messages.create.return_value = mock_response

        builder = TrendContextBuilder(
            knowledge_store=MagicMock(),
            api_key="test-key",
        )
        themes = builder._extract_themes([_make_item("a", "text")])
        assert themes == []

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_limits_to_5_themes(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = json.dumps([f"Theme {i}" for i in range(10)])
        mock_client.messages.create.return_value = mock_response

        builder = TrendContextBuilder(
            knowledge_store=MagicMock(),
            api_key="test-key",
        )
        themes = builder._extract_themes([_make_item("a", "text")])
        assert len(themes) == 5

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_handles_api_connection_error(self, MockAnthropic):
        """Test that APIConnectionError is caught and returns empty list."""
        import anthropic

        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        # APIConnectionError requires a request parameter
        mock_request = MagicMock()
        mock_client.messages.create.side_effect = anthropic.APIConnectionError(
            message="Connection failed",
            request=mock_request
        )

        builder = TrendContextBuilder(
            knowledge_store=MagicMock(),
            api_key="test-key",
        )
        themes = builder._extract_themes([_make_item("a", "text")])
        assert themes == []

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_handles_api_status_error(self, MockAnthropic):
        """Test that APIStatusError is caught and returns empty list."""
        import anthropic

        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        # APIStatusError requires specific parameters
        mock_client.messages.create.side_effect = anthropic.APIStatusError(
            "Rate limit exceeded",
            response=MagicMock(status_code=429),
            body=None
        )

        builder = TrendContextBuilder(
            knowledge_store=MagicMock(),
            api_key="test-key",
        )
        themes = builder._extract_themes([_make_item("a", "text")])
        assert themes == []

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_handles_generic_exception(self, MockAnthropic):
        """Test that unexpected exceptions are caught and return empty list."""
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client
        mock_client.messages.create.side_effect = RuntimeError("Unexpected error")

        builder = TrendContextBuilder(
            knowledge_store=MagicMock(),
            api_key="test-key",
        )
        themes = builder._extract_themes([_make_item("a", "text")])
        assert themes == []


class TestCaching:
    """Tests for trend context caching via meta table."""

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_caches_result(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = '["Theme 1"]'
        mock_client.messages.create.return_value = mock_response

        mock_store = MagicMock()
        mock_store.get_recent_by_source_type.return_value = [
            _make_item("a", "post 1"),
            _make_item("b", "post 2"),
            _make_item("c", "post 3"),
        ]

        mock_db = MagicMock()
        mock_db.get_meta.return_value = None  # No cache

        builder = TrendContextBuilder(
            knowledge_store=mock_store,
            api_key="test-key",
            db=mock_db,
        )
        builder.build_context()

        # Should have called set_meta to cache
        mock_db.set_meta.assert_called_once()
        call_args = mock_db.set_meta.call_args
        assert call_args[0][0] == "trend_themes"
        cached = json.loads(call_args[0][1])
        assert "themes" in cached
        assert "cached_at" in cached

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_uses_cache_when_fresh(self, MockAnthropic):
        mock_store = MagicMock()

        mock_db = MagicMock()
        cached_data = json.dumps({
            "themes": ["Cached theme"],
            "notable_takes": ["@karpathy: Cached insight"],
            "cached_at": datetime.now(timezone.utc).isoformat(),
        })
        mock_db.get_meta.return_value = cached_data

        builder = TrendContextBuilder(
            knowledge_store=mock_store,
            api_key="test-key",
            db=mock_db,
        )
        result = builder.build_context(cache_ttl_hours=4)

        assert "Cached theme" in result
        # Should NOT have called the knowledge store
        mock_store.get_recent_by_source_type.assert_not_called()

    @patch("synthesis.trend_context.anthropic.Anthropic")
    def test_ignores_stale_cache(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = '["Fresh theme"]'
        mock_client.messages.create.return_value = mock_response

        mock_store = MagicMock()
        mock_store.get_recent_by_source_type.return_value = [
            _make_item("a", "post 1"),
            _make_item("b", "post 2"),
            _make_item("c", "post 3"),
        ]

        stale_time = (
            datetime.now(timezone.utc) - timedelta(hours=5)
        ).isoformat()
        mock_db = MagicMock()
        mock_db.get_meta.return_value = json.dumps({
            "themes": ["Old theme"],
            "cached_at": stale_time,
        })

        builder = TrendContextBuilder(
            knowledge_store=mock_store,
            api_key="test-key",
            db=mock_db,
        )
        result = builder.build_context(cache_ttl_hours=4)

        assert "Fresh theme" in result
        # Should have fetched new data
        mock_store.get_recent_by_source_type.assert_called_once()


class TestKnowledgeStoreGetRecent:
    """Tests for KnowledgeStore.get_recent_by_source_type()."""

    def test_returns_recent_items(self, db):
        from knowledge.store import KnowledgeStore
        mock_embedder = MagicMock()

        store = KnowledgeStore(db.conn, mock_embedder)

        # Insert a recent curated item
        db.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, author, content, approved, created_at)
               VALUES ('curated_x', 'tw-1', 'karpathy', 'Recent post', 1,
                       datetime('now', '-1 hours'))"""
        )
        db.conn.commit()

        items = store.get_recent_by_source_type("curated_x", max_age_hours=24)
        assert len(items) == 1
        assert items[0].author == "karpathy"
        assert items[0].content == "Recent post"

    def test_excludes_old_items(self, db):
        from knowledge.store import KnowledgeStore
        mock_embedder = MagicMock()
        store = KnowledgeStore(db.conn, mock_embedder)

        db.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, author, content, approved, created_at)
               VALUES ('curated_x', 'tw-old', 'karpathy', 'Old post', 1,
                       datetime('now', '-100 hours'))"""
        )
        db.conn.commit()

        items = store.get_recent_by_source_type("curated_x", max_age_hours=72)
        assert len(items) == 0

    def test_excludes_unapproved(self, db):
        from knowledge.store import KnowledgeStore
        mock_embedder = MagicMock()
        store = KnowledgeStore(db.conn, mock_embedder)

        db.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, author, content, approved, created_at)
               VALUES ('curated_x', 'tw-2', 'swyx', 'Unapproved', 0,
                       datetime('now', '-1 hours'))"""
        )
        db.conn.commit()

        items = store.get_recent_by_source_type("curated_x", max_age_hours=24)
        assert len(items) == 0

    def test_filters_by_source_type(self, db):
        from knowledge.store import KnowledgeStore
        mock_embedder = MagicMock()
        store = KnowledgeStore(db.conn, mock_embedder)

        db.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, author, content, approved, created_at)
               VALUES ('own_post', 'own-1', 'me', 'My post', 1,
                       datetime('now', '-1 hours'))"""
        )
        db.conn.commit()

        items = store.get_recent_by_source_type("curated_x", max_age_hours=24)
        assert len(items) == 0


class TestXClientGetUserId:
    """Tests for XClient.get_user_id()."""

    def test_returns_user_id(self):
        from output.x_client import XClient
        client = XClient.__new__(XClient)
        client.client = MagicMock()

        mock_user = MagicMock()
        mock_user.data.id = 12345
        client.client.get_user.return_value = mock_user

        result = client.get_user_id("karpathy")
        assert result == "12345"
        client.client.get_user.assert_called_once_with(username="karpathy")

    def test_returns_none_when_not_found(self):
        from output.x_client import XClient
        client = XClient.__new__(XClient)
        client.client = MagicMock()

        mock_user = MagicMock()
        mock_user.data = None
        client.client.get_user.return_value = mock_user

        result = client.get_user_id("nonexistent")
        assert result is None

    def test_returns_none_on_error(self):
        import tweepy
        from output.x_client import XClient
        client = XClient.__new__(XClient)
        client.client = MagicMock()
        client.client.get_user.side_effect = tweepy.TweepyException("API error")

        result = client.get_user_id("karpathy")
        assert result is None
