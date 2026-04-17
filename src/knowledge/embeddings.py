"""Embedding generation for semantic search."""

import struct
from typing import Optional
from abc import ABC, abstractmethod


class EmbeddingError(Exception):
    """Base exception for embedding-related errors."""
    pass


class EmbeddingGenerationError(EmbeddingError):
    """Raised when embedding generation fails."""
    pass


class EmbeddingProviderUnavailableError(EmbeddingError):
    """Raised when the embedding provider is unreachable or unavailable."""
    pass


class EmbeddingRateLimitError(EmbeddingError):
    """Raised when the embedding provider rate limit is exceeded."""
    pass


class EmbeddingProvider(ABC):
    @abstractmethod
    def embed(self, text: str) -> list[float]:
        """Generate embedding for a single text."""
        pass

    @abstractmethod
    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings for multiple texts."""
        pass


class VoyageEmbeddings(EmbeddingProvider):
    """Voyage AI embeddings."""

    def __init__(self, api_key: str, model: str = "voyage-3-lite"):
        import voyageai
        self.client = voyageai.Client(api_key=api_key)
        self.model = model

    def embed(self, text: str) -> list[float]:
        from voyageai.error import (
            RateLimitError,
            AuthenticationError,
            APIConnectionError,
            Timeout,
            ServiceUnavailableError,
            APIError,
        )

        try:
            result = self.client.embed([text], model=self.model)
            if not result or not result.embeddings:
                raise EmbeddingGenerationError(
                    f"Voyage API returned empty embeddings for text (length={len(text)})"
                )
            return result.embeddings[0]
        except EmbeddingError:
            # Re-raise our own exceptions
            raise
        except RateLimitError as e:
            raise EmbeddingRateLimitError(
                f"Voyage API rate limit exceeded: {e}"
            ) from e
        except AuthenticationError as e:
            raise EmbeddingProviderUnavailableError(
                f"Voyage API authentication failed: {e}"
            ) from e
        except (APIConnectionError, Timeout, ServiceUnavailableError) as e:
            raise EmbeddingProviderUnavailableError(
                f"Voyage API unavailable: {e}"
            ) from e
        except APIError as e:
            raise EmbeddingGenerationError(
                f"Voyage API error: {e}"
            ) from e

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        from voyageai.error import (
            RateLimitError,
            AuthenticationError,
            APIConnectionError,
            Timeout,
            ServiceUnavailableError,
            APIError,
        )

        try:
            result = self.client.embed(texts, model=self.model)
            if not result or not result.embeddings:
                raise EmbeddingGenerationError(
                    f"Voyage API returned empty embeddings for batch (size={len(texts)})"
                )
            if len(result.embeddings) != len(texts):
                raise EmbeddingGenerationError(
                    f"Voyage API returned {len(result.embeddings)} embeddings for {len(texts)} texts"
                )
            return result.embeddings
        except EmbeddingError:
            # Re-raise our own exceptions
            raise
        except RateLimitError as e:
            raise EmbeddingRateLimitError(
                f"Voyage API rate limit exceeded: {e}"
            ) from e
        except AuthenticationError as e:
            raise EmbeddingProviderUnavailableError(
                f"Voyage API authentication failed: {e}"
            ) from e
        except (APIConnectionError, Timeout, ServiceUnavailableError) as e:
            raise EmbeddingProviderUnavailableError(
                f"Voyage API unavailable: {e}"
            ) from e
        except APIError as e:
            raise EmbeddingGenerationError(
                f"Voyage API error: {e}"
            ) from e


class OpenAIEmbeddings(EmbeddingProvider):
    """OpenAI embeddings."""

    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        from openai import OpenAI
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def embed(self, text: str) -> list[float]:
        from openai import (
            APIError,
            RateLimitError,
            AuthenticationError,
            APITimeoutError,
            APIConnectionError,
        )

        try:
            response = self.client.embeddings.create(
                input=text,
                model=self.model
            )
            if not response or not response.data:
                raise EmbeddingGenerationError(
                    f"OpenAI API returned empty response for text (length={len(text)})"
                )
            return response.data[0].embedding
        except EmbeddingError:
            # Re-raise our own exceptions
            raise
        except RateLimitError as e:
            raise EmbeddingRateLimitError(
                f"OpenAI API rate limit exceeded: {e}"
            ) from e
        except AuthenticationError as e:
            raise EmbeddingProviderUnavailableError(
                f"OpenAI API authentication failed: {e}"
            ) from e
        except (APIConnectionError, APITimeoutError) as e:
            raise EmbeddingProviderUnavailableError(
                f"OpenAI API unavailable: {e}"
            ) from e
        except APIError as e:
            raise EmbeddingGenerationError(
                f"OpenAI API error: {e}"
            ) from e

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        from openai import (
            APIError,
            RateLimitError,
            AuthenticationError,
            APITimeoutError,
            APIConnectionError,
        )

        try:
            response = self.client.embeddings.create(
                input=texts,
                model=self.model
            )
            if not response or not response.data:
                raise EmbeddingGenerationError(
                    f"OpenAI API returned empty response for batch (size={len(texts)})"
                )
            if len(response.data) != len(texts):
                raise EmbeddingGenerationError(
                    f"OpenAI API returned {len(response.data)} embeddings for {len(texts)} texts"
                )
            return [item.embedding for item in response.data]
        except EmbeddingError:
            # Re-raise our own exceptions
            raise
        except RateLimitError as e:
            raise EmbeddingRateLimitError(
                f"OpenAI API rate limit exceeded: {e}"
            ) from e
        except AuthenticationError as e:
            raise EmbeddingProviderUnavailableError(
                f"OpenAI API authentication failed: {e}"
            ) from e
        except (APIConnectionError, APITimeoutError) as e:
            raise EmbeddingProviderUnavailableError(
                f"OpenAI API unavailable: {e}"
            ) from e
        except APIError as e:
            raise EmbeddingGenerationError(
                f"OpenAI API error: {e}"
            ) from e


class AnthropicEmbeddings(EmbeddingProvider):
    """Use Claude to generate pseudo-embeddings via summary."""

    def __init__(self, api_key: str):
        # Anthropic doesn't have embeddings API, fall back to Voyage
        raise NotImplementedError("Anthropic doesn't provide embeddings. Use Voyage or OpenAI.")


def get_embedding_provider(
    provider: str,
    api_key: str,
    model: Optional[str] = None
) -> EmbeddingProvider:
    """Factory for embedding providers."""
    if provider == "voyage":
        return VoyageEmbeddings(api_key, model or "voyage-3-lite")
    elif provider == "openai":
        return OpenAIEmbeddings(api_key, model or "text-embedding-3-small")
    else:
        raise ValueError(f"Unknown embedding provider: {provider}")


def serialize_embedding(embedding: list[float]) -> bytes:
    """Serialize embedding to bytes for SQLite storage."""
    return struct.pack(f'{len(embedding)}f', *embedding)


def deserialize_embedding(data: bytes) -> list[float]:
    """Deserialize embedding from bytes."""
    num_floats = len(data) // 4
    return list(struct.unpack(f'{num_floats}f', data))


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors."""
    dot_product = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(x * x for x in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot_product / (norm_a * norm_b)
