"""Tests for knowledge/ingest.py."""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from knowledge.ingest import (
    InsightExtractor,
    ingest_own_post,
    ingest_own_conversation,
    ingest_curated_post,
    ingest_curated_article,
)
from knowledge.store import KnowledgeStore
from knowledge.embeddings import EmbeddingProvider


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class MockEmbedder(EmbeddingProvider):
    """Deterministic embedder that returns a fixed-length vector."""

    def embed(self, text: str) -> list[float]:
        return [0.1, 0.2, 0.3]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self.embed(t) for t in texts]


def _make_mock_response(text: str) -> MagicMock:
    """Build a mock Anthropic response with a single TextBlock."""
    response = MagicMock()
    response.content = [MagicMock(text=text)]
    return response


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def schema_path():
    return str(Path(__file__).parent.parent / "schema.sql")


@pytest.fixture
def store(schema_path):
    """KnowledgeStore backed by in-memory SQLite with schema applied."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    with open(schema_path) as f:
        conn.executescript(f.read())
    return KnowledgeStore(conn, MockEmbedder())


@pytest.fixture
def mock_client():
    """Return a mock anthropic.Anthropic client instance."""
    client = MagicMock()
    client.messages.create.return_value = _make_mock_response("  extracted insight  ")
    return client


@pytest.fixture
def extractor(mock_client):
    """InsightExtractor with a mocked Anthropic client."""
    with patch("knowledge.ingest.anthropic.Anthropic", return_value=mock_client):
        ext = InsightExtractor(api_key="test-key")
    return ext


@pytest.fixture
def extractor_with_client(mock_client):
    """Return (extractor, mock_client) for assertions on API calls."""
    with patch("knowledge.ingest.anthropic.Anthropic", return_value=mock_client):
        ext = InsightExtractor(api_key="test-key")
    return ext, mock_client


# ---------------------------------------------------------------------------
# InsightExtractor
# ---------------------------------------------------------------------------

class TestInsightExtractor:
    def test_extract_insight_calls_claude_and_returns_stripped(self, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("  the insight  ")

        result = ext.extract_insight("some content")

        assert result == "the insight"
        client.messages.create.assert_called_once()
        call_kwargs = client.messages.create.call_args.kwargs
        assert call_kwargs["model"] == ext.model
        assert call_kwargs["max_tokens"] == 200
        prompt = call_kwargs["messages"][0]["content"]
        assert "some content" in prompt

    def test_extract_insight_with_context_includes_context_in_prompt(self, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("insight")

        ext.extract_insight("content here", context="Project: relay")

        prompt = client.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "Context: Project: relay" in prompt

    def test_constructor_passes_api_key_and_timeout(self):
        with patch("knowledge.ingest.anthropic.Anthropic") as mock_anthropic:
            InsightExtractor(api_key="sk-test")
            mock_anthropic.assert_called_once_with(api_key="sk-test", timeout=300.0)

    def test_default_model(self):
        with patch("knowledge.ingest.anthropic.Anthropic"):
            ext = InsightExtractor(api_key="test")
            assert ext.model == "claude-sonnet-4-20250514"


# ---------------------------------------------------------------------------
# ingest_own_post
# ---------------------------------------------------------------------------

class TestIngestOwnPost:
    def test_new_post_extracts_insight_and_stores(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("great insight")

        result = ingest_own_post(
            store=store,
            extractor=ext,
            post_id="post-123",
            content="This is my post about AI agents",
            url="https://x.com/me/status/123",
            author="me",
        )

        assert result is not None
        client.messages.create.assert_called_once()

        item = store.get_by_source("own_post", "post-123")
        assert item is not None
        assert item.source_type == "own_post"
        assert item.approved is True
        assert item.attribution_required is False
        assert item.insight == "great insight"
        assert item.content == "This is my post about AI agents"
        assert item.author == "me"
        assert item.source_url == "https://x.com/me/status/123"

    def test_duplicate_post_returns_none_without_extraction(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("insight")

        ingest_own_post(store, ext, "post-dup", "content", "url", "me")
        client.messages.create.reset_mock()

        result = ingest_own_post(store, ext, "post-dup", "content", "url", "me")

        assert result is None
        client.messages.create.assert_not_called()


# ---------------------------------------------------------------------------
# ingest_own_conversation
# ---------------------------------------------------------------------------

class TestIngestOwnConversation:
    def test_substantial_prompt_extracts_with_project_context(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("conversation insight")

        long_prompt = "Implement a retry mechanism for the pipeline runner with exponential backoff"
        result = ingest_own_conversation(
            store=store,
            extractor=ext,
            message_uuid="uuid-001",
            prompt=long_prompt,
            project_path="/home/user/relay",
        )

        assert result is not None
        client.messages.create.assert_called_once()
        prompt_text = client.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "Project: /home/user/relay" in prompt_text

        item = store.get_by_source("own_conversation", "uuid-001")
        assert item is not None
        assert item.source_type == "own_conversation"
        assert item.author == "self"
        assert item.approved is True
        assert item.attribution_required is False

    def test_short_prompt_returns_none(self, store, extractor_with_client):
        ext, client = extractor_with_client

        result = ingest_own_conversation(
            store=store,
            extractor=ext,
            message_uuid="uuid-short",
            prompt="fix bug",
            project_path="/home/user/relay",
        )

        assert result is None
        client.messages.create.assert_not_called()

    def test_duplicate_message_returns_none(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("insight")

        long_prompt = "A" * 60
        ingest_own_conversation(store, ext, "uuid-dup", long_prompt, "/path")
        client.messages.create.reset_mock()

        result = ingest_own_conversation(store, ext, "uuid-dup", long_prompt, "/path")

        assert result is None
        client.messages.create.assert_not_called()


# ---------------------------------------------------------------------------
# ingest_curated_post
# ---------------------------------------------------------------------------

class TestIngestCuratedPost:
    def test_stores_with_correct_source_type_and_attribution(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("curated insight")

        result = ingest_curated_post(
            store=store,
            extractor=ext,
            post_id="ext-456",
            content="External post about RAG patterns",
            url="https://x.com/expert/status/456",
            author="expert",
            license_type="attribution_required",
        )

        assert result is not None
        item = store.get_by_source("curated_x", "ext-456")
        assert item is not None
        assert item.source_type == "curated_x"
        assert item.attribution_required is True
        assert item.approved is True

    def test_open_license_sets_attribution_false(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("insight")

        ingest_curated_post(
            store=store,
            extractor=ext,
            post_id="ext-open",
            content="Open-licensed content about AI",
            url="https://x.com/oss/status/789",
            author="oss_author",
            license_type="open",
        )

        item = store.get_by_source("curated_x", "ext-open")
        assert item.attribution_required is False

    def test_deduplication(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("insight")

        ingest_curated_post(store, ext, "ext-dup", "content", "url", "author")
        client.messages.create.reset_mock()

        result = ingest_curated_post(store, ext, "ext-dup", "content", "url", "author")

        assert result is None
        client.messages.create.assert_not_called()


# ---------------------------------------------------------------------------
# ingest_curated_article
# ---------------------------------------------------------------------------

class TestIngestCuratedArticle:
    def test_content_truncation_for_insight_and_storage(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("article insight")

        long_content = "x" * 10000

        ingest_curated_article(
            store=store,
            extractor=ext,
            url="https://blog.example.com/post",
            content=long_content,
            title="Great Article",
            author="blogger",
        )

        # Verify insight extraction used content[:2000]
        call_kwargs = client.messages.create.call_args.kwargs
        extract_prompt = call_kwargs["messages"][0]["content"]
        # The prompt should contain exactly 2000 'x' chars, not 10000
        assert "x" * 2000 in extract_prompt
        assert "x" * 2001 not in extract_prompt

        # Verify stored content is truncated to 5000
        item = store.get_by_source("curated_article", "https://blog.example.com/post")
        assert len(item.content) == 5000

    def test_source_type_is_curated_article(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("insight")

        ingest_curated_article(
            store=store,
            extractor=ext,
            url="https://example.com/article",
            content="Article content here",
            title="Title",
            author="author",
        )

        item = store.get_by_source("curated_article", "https://example.com/article")
        assert item is not None
        assert item.source_type == "curated_article"
        assert item.source_id == "https://example.com/article"
        assert item.source_url == "https://example.com/article"

    def test_deduplication_by_url(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("insight")

        url = "https://example.com/dup-article"
        ingest_curated_article(store, ext, url, "content", "Title", "author")
        client.messages.create.reset_mock()

        result = ingest_curated_article(store, ext, url, "content", "Title", "author")

        assert result is None
        client.messages.create.assert_not_called()

    def test_attribution_required_by_default(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("insight")

        ingest_curated_article(
            store=store,
            extractor=ext,
            url="https://example.com/attr",
            content="content",
            title="Title",
            author="author",
        )

        item = store.get_by_source("curated_article", "https://example.com/attr")
        assert item.attribution_required is True

    def test_open_license_no_attribution(self, store, extractor_with_client):
        ext, client = extractor_with_client
        client.messages.create.return_value = _make_mock_response("insight")

        ingest_curated_article(
            store=store,
            extractor=ext,
            url="https://example.com/open",
            content="content",
            title="Title",
            author="author",
            license_type="open",
        )

        item = store.get_by_source("curated_article", "https://example.com/open")
        assert item.attribution_required is False
