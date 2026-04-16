"""Tests for knowledge store error handling."""

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import Mock

import pytest

from src.knowledge.store import KnowledgeStore, KnowledgeItem
from src.knowledge.embeddings import (
    EmbeddingProvider,
    EmbeddingError,
    EmbeddingGenerationError,
    EmbeddingProviderUnavailableError,
)


class MockFailingEmbedder(EmbeddingProvider):
    """Mock embedder that simulates various failure modes."""

    def __init__(self, failure_type: str = "generation"):
        self.failure_type = failure_type
        self.call_count = 0

    def embed(self, text: str) -> list[float]:
        self.call_count += 1
        if self.failure_type == "connection":
            raise ConnectionError("Network unreachable")
        elif self.failure_type == "api_error":
            raise Exception("API rate limit exceeded")
        elif self.failure_type == "empty_response":
            # This should be caught by the provider wrapper
            return []
        elif self.failure_type == "generation":
            raise EmbeddingGenerationError("Model inference failed")
        elif self.failure_type == "unavailable":
            raise EmbeddingProviderUnavailableError("Provider service down")
        else:
            # Normal operation
            return [0.1, 0.2, 0.3]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        # Just call embed for each text in tests
        return [self.embed(t) for t in texts]


def test_add_item_embedding_generation_error():
    """Test that embedding generation failure surfaces as EmbeddingGenerationError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Create minimal schema
        conn.execute("""
            CREATE TABLE knowledge (
                id INTEGER PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_url TEXT,
                author TEXT NOT NULL,
                content TEXT NOT NULL,
                insight TEXT,
                embedding BLOB,
                attribution_required INTEGER DEFAULT 0,
                approved INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_type, source_id)
            )
        """)
        conn.commit()

        embedder = MockFailingEmbedder(failure_type="generation")
        store = KnowledgeStore(conn, embedder)

        item = KnowledgeItem(
            id=None,
            source_type="test",
            source_id="test-1",
            source_url=None,
            author="test_author",
            content="test content",
            insight=None,
            embedding=None,
            attribution_required=False,
            approved=True,
            created_at=None
        )

        with pytest.raises(EmbeddingGenerationError) as exc_info:
            store.add_item(item)

        # Verify the error message
        assert "Model inference failed" in str(exc_info.value)

        conn.close()


def test_add_item_provider_unavailable():
    """Test that provider unavailability surfaces as EmbeddingProviderUnavailableError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Create minimal schema
        conn.execute("""
            CREATE TABLE knowledge (
                id INTEGER PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_url TEXT,
                author TEXT NOT NULL,
                content TEXT NOT NULL,
                insight TEXT,
                embedding BLOB,
                attribution_required INTEGER DEFAULT 0,
                approved INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_type, source_id)
            )
        """)
        conn.commit()

        embedder = MockFailingEmbedder(failure_type="unavailable")
        store = KnowledgeStore(conn, embedder)

        item = KnowledgeItem(
            id=None,
            source_type="test",
            source_id="test-2",
            source_url=None,
            author="test_author",
            content="test content",
            insight=None,
            embedding=None,
            attribution_required=False,
            approved=True,
            created_at=None
        )

        with pytest.raises(EmbeddingProviderUnavailableError) as exc_info:
            store.add_item(item)

        assert "Provider service down" in str(exc_info.value)

        conn.close()


def test_search_similar_embedding_generation_error():
    """Test that search query embedding failure surfaces as EmbeddingGenerationError."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Create minimal schema
        conn.execute("""
            CREATE TABLE knowledge (
                id INTEGER PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_url TEXT,
                author TEXT NOT NULL,
                content TEXT NOT NULL,
                insight TEXT,
                embedding BLOB,
                attribution_required INTEGER DEFAULT 0,
                approved INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_type, source_id)
            )
        """)
        conn.commit()

        embedder = MockFailingEmbedder(failure_type="generation")
        store = KnowledgeStore(conn, embedder)

        with pytest.raises(EmbeddingGenerationError) as exc_info:
            store.search_similar("test query")

        assert "Model inference failed" in str(exc_info.value)

        conn.close()


def test_error_inheritance():
    """Test that exception hierarchy is correct."""
    assert issubclass(EmbeddingGenerationError, EmbeddingError)
    assert issubclass(EmbeddingProviderUnavailableError, EmbeddingError)
    assert issubclass(EmbeddingError, Exception)


def test_exception_chaining_from_connection_error():
    """Test that ConnectionError from provider is properly chained."""
    # We need to test at the provider level since that's where chaining happens
    from src.knowledge.embeddings import VoyageEmbeddings

    # Mock the client to raise ConnectionError
    mock_client = Mock()
    mock_client.embed.side_effect = ConnectionError("Network unreachable")

    embedder = VoyageEmbeddings(api_key="test", model="voyage-3-lite")
    embedder.client = mock_client

    try:
        embedder.embed("test text")
        pytest.fail("Expected EmbeddingProviderUnavailableError")
    except EmbeddingProviderUnavailableError as e:
        # Verify exception chaining
        assert e.__cause__ is not None
        assert isinstance(e.__cause__, ConnectionError)
        assert "Network unreachable" in str(e.__cause__)
        # Verify our error has context
        assert "Failed to connect to Voyage API" in str(e)


def test_exception_chaining_from_api_error():
    """Test that generic API errors are properly chained."""
    from src.knowledge.embeddings import OpenAIEmbeddings

    # Mock the client to raise a generic exception
    mock_client = Mock()
    mock_response = Mock()
    mock_client.embeddings.create.side_effect = ValueError("Invalid input")

    embedder = OpenAIEmbeddings(api_key="test", model="text-embedding-3-small")
    embedder.client = mock_client

    try:
        embedder.embed("test text")
        pytest.fail("Expected EmbeddingGenerationError")
    except EmbeddingGenerationError as e:
        # Verify exception chaining
        assert e.__cause__ is not None
        assert isinstance(e.__cause__, ValueError)
        assert "Invalid input" in str(e.__cause__)
        # Verify our error has context
        assert "OpenAI embedding generation failed" in str(e)
        assert "ValueError" in str(e)


def test_malformed_response_handling_voyage():
    """Test that malformed/empty responses from Voyage are caught."""
    from src.knowledge.embeddings import VoyageEmbeddings

    mock_client = Mock()
    # Simulate empty embeddings response
    mock_result = Mock()
    mock_result.embeddings = []
    mock_client.embed.return_value = mock_result

    embedder = VoyageEmbeddings(api_key="test", model="voyage-3-lite")
    embedder.client = mock_client

    with pytest.raises(EmbeddingGenerationError) as exc_info:
        embedder.embed("test text")

    assert "Voyage API returned empty embeddings" in str(exc_info.value)


def test_malformed_response_handling_openai():
    """Test that malformed/empty responses from OpenAI are caught."""
    from src.knowledge.embeddings import OpenAIEmbeddings

    mock_client = Mock()
    # Simulate empty data response
    mock_response = Mock()
    mock_response.data = []
    mock_client.embeddings.create.return_value = mock_response

    embedder = OpenAIEmbeddings(api_key="test", model="text-embedding-3-small")
    embedder.client = mock_client

    with pytest.raises(EmbeddingGenerationError) as exc_info:
        embedder.embed("test text")

    assert "OpenAI API returned empty response" in str(exc_info.value)


def test_batch_size_mismatch_voyage():
    """Test that batch size mismatches are caught for Voyage."""
    from src.knowledge.embeddings import VoyageEmbeddings

    mock_client = Mock()
    # Return wrong number of embeddings
    mock_result = Mock()
    mock_result.embeddings = [[0.1, 0.2], [0.3, 0.4]]  # 2 embeddings
    mock_client.embed.return_value = mock_result

    embedder = VoyageEmbeddings(api_key="test", model="voyage-3-lite")
    embedder.client = mock_client

    with pytest.raises(EmbeddingGenerationError) as exc_info:
        embedder.embed_batch(["text1", "text2", "text3"])  # 3 texts

    assert "returned 2 embeddings for 3 texts" in str(exc_info.value)


def test_batch_size_mismatch_openai():
    """Test that batch size mismatches are caught for OpenAI."""
    from src.knowledge.embeddings import OpenAIEmbeddings

    mock_client = Mock()
    # Return wrong number of embeddings
    mock_response = Mock()
    mock_item1 = Mock()
    mock_item1.embedding = [0.1, 0.2]
    mock_response.data = [mock_item1]  # 1 embedding
    mock_client.embeddings.create.return_value = mock_response

    embedder = OpenAIEmbeddings(api_key="test", model="text-embedding-3-small")
    embedder.client = mock_client

    with pytest.raises(EmbeddingGenerationError) as exc_info:
        embedder.embed_batch(["text1", "text2"])  # 2 texts

    assert "returned 1 embeddings for 2 texts" in str(exc_info.value)


def test_successful_operation_still_works():
    """Test that normal operations still work with error handling in place."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row

        # Create minimal schema
        conn.execute("""
            CREATE TABLE knowledge (
                id INTEGER PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                source_url TEXT,
                author TEXT NOT NULL,
                content TEXT NOT NULL,
                insight TEXT,
                embedding BLOB,
                attribution_required INTEGER DEFAULT 0,
                approved INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_type, source_id)
            )
        """)
        conn.commit()

        # Use successful embedder
        embedder = MockFailingEmbedder(failure_type="success")
        store = KnowledgeStore(conn, embedder)

        item = KnowledgeItem(
            id=None,
            source_type="test",
            source_id="test-success",
            source_url=None,
            author="test_author",
            content="test content",
            insight=None,
            embedding=None,
            attribution_required=False,
            approved=True,
            created_at=None
        )

        # Should succeed without raising
        row_id = store.add_item(item)
        assert row_id is not None
        assert embedder.call_count == 1

        # Search should also work
        results = store.search_similar("test query", limit=5)
        assert isinstance(results, list)
        assert embedder.call_count == 2  # One for add, one for search

        conn.close()
