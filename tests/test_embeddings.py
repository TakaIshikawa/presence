"""Tests for src/knowledge/embeddings.py."""

import math
from unittest.mock import MagicMock, patch

import pytest

from knowledge.embeddings import (
    AnthropicEmbeddings,
    OpenAIEmbeddings,
    VoyageEmbeddings,
    cosine_similarity,
    deserialize_embedding,
    get_embedding_provider,
    serialize_embedding,
)


# ---------------------------------------------------------------------------
# serialize_embedding / deserialize_embedding roundtrip
# ---------------------------------------------------------------------------

class TestSerializeDeserialize:
    @pytest.mark.parametrize("size", [0, 1, 128, 1024])
    def test_roundtrip(self, size):
        original = [float(i) / max(size, 1) for i in range(size)]
        data = serialize_embedding(original)
        restored = deserialize_embedding(data)
        assert len(restored) == size
        for a, b in zip(original, restored):
            assert math.isclose(a, b, rel_tol=1e-6)

    @pytest.mark.parametrize("size", [0, 1, 128, 1024])
    def test_byte_length(self, size):
        embedding = [1.0] * size
        data = serialize_embedding(embedding)
        assert len(data) == 4 * size

    def test_known_values(self):
        original = [1.0, -1.0, 0.0, 3.14]
        restored = deserialize_embedding(serialize_embedding(original))
        for a, b in zip(original, restored):
            assert math.isclose(a, b, rel_tol=1e-6)


# ---------------------------------------------------------------------------
# cosine_similarity
# ---------------------------------------------------------------------------

class TestCosineSimilarity:
    def test_identical_vectors(self):
        v = [1.0, 2.0, 3.0]
        assert math.isclose(cosine_similarity(v, v), 1.0, rel_tol=1e-9)

    def test_orthogonal_vectors(self):
        a = [1.0, 0.0]
        b = [0.0, 1.0]
        assert math.isclose(cosine_similarity(a, b), 0.0, abs_tol=1e-9)

    def test_opposite_vectors(self):
        a = [1.0, 2.0, 3.0]
        b = [-1.0, -2.0, -3.0]
        assert math.isclose(cosine_similarity(a, b), -1.0, rel_tol=1e-9)

    def test_zero_vector_returns_zero(self):
        zero = [0.0, 0.0, 0.0]
        v = [1.0, 2.0, 3.0]
        assert cosine_similarity(zero, v) == 0.0
        assert cosine_similarity(v, zero) == 0.0
        assert cosine_similarity(zero, zero) == 0.0

    def test_known_similarity(self):
        a = [1.0, 0.0]
        b = [1.0, 1.0]
        # cos(45°) = 1/sqrt(2)
        expected = 1.0 / math.sqrt(2)
        assert math.isclose(cosine_similarity(a, b), expected, rel_tol=1e-9)


# ---------------------------------------------------------------------------
# get_embedding_provider factory
# ---------------------------------------------------------------------------

class TestGetEmbeddingProvider:
    def test_voyage_returns_voyage_instance(self):
        mock_voyageai = MagicMock()
        with patch.dict("sys.modules", {"voyageai": mock_voyageai}):
            provider = get_embedding_provider("voyage", api_key="test-key")
            assert isinstance(provider, VoyageEmbeddings)

    def test_openai_returns_openai_instance(self):
        mock_openai = MagicMock()
        with patch.dict("sys.modules", {"openai": mock_openai}):
            provider = get_embedding_provider("openai", api_key="test-key")
            assert isinstance(provider, OpenAIEmbeddings)

    def test_unknown_provider_raises_valueerror(self):
        with pytest.raises(ValueError, match="Unknown embedding provider"):
            get_embedding_provider("unknown", api_key="test-key")


# ---------------------------------------------------------------------------
# VoyageEmbeddings
# ---------------------------------------------------------------------------

class TestVoyageEmbeddings:
    def _make_provider(self):
        mock_voyageai = MagicMock()
        with patch.dict("sys.modules", {"voyageai": mock_voyageai}):
            provider = VoyageEmbeddings(api_key="vk-test")
        return provider, mock_voyageai

    def test_embed_calls_client_correctly(self):
        provider, mock_voyageai = self._make_provider()
        mock_result = MagicMock()
        mock_result.embeddings = [[0.1, 0.2, 0.3]]
        provider.client.embed.return_value = mock_result

        result = provider.embed("hello")

        provider.client.embed.assert_called_once_with(["hello"], model="voyage-3-lite")
        assert result == [0.1, 0.2, 0.3]

    def test_embed_batch_calls_client_correctly(self):
        provider, mock_voyageai = self._make_provider()
        mock_result = MagicMock()
        mock_result.embeddings = [[0.1, 0.2], [0.3, 0.4]]
        provider.client.embed.return_value = mock_result

        texts = ["hello", "world"]
        result = provider.embed_batch(texts)

        provider.client.embed.assert_called_once_with(texts, model="voyage-3-lite")
        assert result == [[0.1, 0.2], [0.3, 0.4]]

    def test_default_model(self):
        provider, _ = self._make_provider()
        assert provider.model == "voyage-3-lite"

    def test_custom_model(self):
        mock_voyageai = MagicMock()
        with patch.dict("sys.modules", {"voyageai": mock_voyageai}):
            provider = VoyageEmbeddings(api_key="vk-test", model="voyage-3")
        assert provider.model == "voyage-3"


# ---------------------------------------------------------------------------
# OpenAIEmbeddings
# ---------------------------------------------------------------------------

class TestOpenAIEmbeddings:
    def _make_provider(self):
        mock_openai_module = MagicMock()
        mock_client = MagicMock()
        mock_openai_module.OpenAI.return_value = mock_client
        with patch.dict("sys.modules", {"openai": mock_openai_module}):
            provider = OpenAIEmbeddings(api_key="sk-test")
        return provider, mock_client

    def test_embed_calls_client_correctly(self):
        provider, mock_client = self._make_provider()
        mock_item = MagicMock()
        mock_item.embedding = [0.1, 0.2, 0.3]
        mock_response = MagicMock()
        mock_response.data = [mock_item]
        mock_client.embeddings.create.return_value = mock_response

        result = provider.embed("hello")

        mock_client.embeddings.create.assert_called_once_with(
            input="hello", model="text-embedding-3-small"
        )
        assert result == [0.1, 0.2, 0.3]

    def test_embed_batch_calls_client_correctly(self):
        provider, mock_client = self._make_provider()
        mock_item_1 = MagicMock()
        mock_item_1.embedding = [0.1, 0.2]
        mock_item_2 = MagicMock()
        mock_item_2.embedding = [0.3, 0.4]
        mock_response = MagicMock()
        mock_response.data = [mock_item_1, mock_item_2]
        mock_client.embeddings.create.return_value = mock_response

        texts = ["hello", "world"]
        result = provider.embed_batch(texts)

        mock_client.embeddings.create.assert_called_once_with(
            input=texts, model="text-embedding-3-small"
        )
        assert result == [[0.1, 0.2], [0.3, 0.4]]

    def test_default_model(self):
        provider, _ = self._make_provider()
        assert provider.model == "text-embedding-3-small"

    def test_custom_model(self):
        mock_openai_module = MagicMock()
        with patch.dict("sys.modules", {"openai": mock_openai_module}):
            provider = OpenAIEmbeddings(api_key="sk-test", model="text-embedding-ada-002")
        assert provider.model == "text-embedding-ada-002"


# ---------------------------------------------------------------------------
# AnthropicEmbeddings
# ---------------------------------------------------------------------------

class TestAnthropicEmbeddings:
    def test_instantiation_raises(self):
        # Raises NotImplementedError from __init__, but Python may raise
        # TypeError first because abstract methods are unimplemented.
        with pytest.raises((NotImplementedError, TypeError)):
            AnthropicEmbeddings(api_key="sk-ant-test")
