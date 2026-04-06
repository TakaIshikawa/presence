"""Tests for knowledge store and embedding utilities."""

import math
from unittest.mock import MagicMock, patch

import pytest

from knowledge.embeddings import (
    EmbeddingProvider,
    VoyageEmbeddings,
    OpenAIEmbeddings,
    serialize_embedding,
    deserialize_embedding,
    cosine_similarity,
    get_embedding_provider,
)
from knowledge.store import KnowledgeItem, KnowledgeStore


# --- Helpers ---


class FakeEmbedder(EmbeddingProvider):
    """Deterministic embedder that returns normalized 8-d vectors based on text hash."""

    DIM = 8

    def embed(self, text: str) -> list[float]:
        h = hash(text)
        raw = [(h >> (i * 4) & 0xF) - 7.5 for i in range(self.DIM)]
        norm = math.sqrt(sum(x * x for x in raw))
        return [x / norm for x in raw] if norm else raw

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self.embed(t) for t in texts]


def _make_item(
    source_type="curated_x",
    source_id="src-1",
    content="test content",
    insight=None,
    embedding=None,
    approved=True,
    attribution_required=False,
    source_url=None,
    author="tester",
) -> KnowledgeItem:
    return KnowledgeItem(
        id=None,
        source_type=source_type,
        source_id=source_id,
        source_url=source_url,
        author=author,
        content=content,
        insight=insight,
        embedding=embedding,
        attribution_required=attribution_required,
        approved=approved,
        created_at=None,
    )


@pytest.fixture
def embedder():
    return FakeEmbedder()


@pytest.fixture
def store(db, embedder):
    return KnowledgeStore(db.conn, embedder)


# --- Embedding utilities ---


class TestSerializeDeserialize:
    def test_round_trip(self):
        original = [0.1, -0.5, 3.14, 0.0, 1e-6, -1e6, 42.0, 0.999]
        blob = serialize_embedding(original)
        restored = deserialize_embedding(blob)
        assert len(restored) == len(original)
        for a, b in zip(original, restored):
            assert a == pytest.approx(b, rel=1e-6)

    def test_empty_vector(self):
        blob = serialize_embedding([])
        assert deserialize_embedding(blob) == []

    def test_single_element(self):
        blob = serialize_embedding([1.5])
        assert deserialize_embedding(blob) == pytest.approx([1.5])


class TestCosineSimilarity:
    def test_identical_vectors(self):
        v = [1.0, 2.0, 3.0, 4.0]
        assert cosine_similarity(v, v) == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        a = [1.0, 0.0]
        b = [0.0, 1.0]
        assert cosine_similarity(a, b) == pytest.approx(0.0)

    def test_zero_vector(self):
        a = [1.0, 2.0, 3.0]
        z = [0.0, 0.0, 0.0]
        assert cosine_similarity(a, z) == pytest.approx(0.0)
        assert cosine_similarity(z, a) == pytest.approx(0.0)
        assert cosine_similarity(z, z) == pytest.approx(0.0)

    def test_opposite_vectors(self):
        a = [1.0, 2.0, 3.0]
        b = [-1.0, -2.0, -3.0]
        assert cosine_similarity(a, b) == pytest.approx(-1.0)

    def test_known_angle(self):
        # 45-degree angle in 2D: cos(45°) ≈ 0.7071
        a = [1.0, 0.0]
        b = [1.0, 1.0]
        assert cosine_similarity(a, b) == pytest.approx(1.0 / math.sqrt(2))


class TestGetEmbeddingProvider:
    def test_voyage_returns_correct_type(self):
        mock_voyageai = MagicMock()
        with patch.dict("sys.modules", {"voyageai": mock_voyageai}):
            provider = get_embedding_provider("voyage", "fake-key")
            assert isinstance(provider, VoyageEmbeddings)
            mock_voyageai.Client.assert_called_once_with(api_key="fake-key")

    def test_openai_returns_correct_type(self):
        mock_openai_mod = MagicMock()
        with patch.dict("sys.modules", {"openai": mock_openai_mod}):
            provider = get_embedding_provider("openai", "fake-key")
            assert isinstance(provider, OpenAIEmbeddings)
            mock_openai_mod.OpenAI.assert_called_once_with(api_key="fake-key")

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown embedding provider"):
            get_embedding_provider("nonexistent", "fake-key")


# --- KnowledgeStore ---


class TestAddItem:
    def test_add_item_generates_embedding(self, store, embedder):
        spy = MagicMock(wraps=embedder.embed)
        embedder.embed = spy

        item = _make_item(content="hello world")
        row_id = store.add_item(item)

        assert row_id > 0
        spy.assert_called_once_with("hello world")

        # Verify stored in DB
        stored = store.get_by_source("curated_x", "src-1")
        assert stored is not None
        assert stored.content == "hello world"
        assert stored.embedding is not None

    def test_add_item_uses_insight_for_embedding(self, store, embedder):
        spy = MagicMock(wraps=embedder.embed)
        embedder.embed = spy

        item = _make_item(content="raw content", insight="key insight")
        store.add_item(item)

        spy.assert_called_once_with("key insight")

    def test_add_item_with_precomputed_embedding(self, store, embedder):
        spy = MagicMock(wraps=embedder.embed)
        embedder.embed = spy

        precomputed = [0.1] * FakeEmbedder.DIM
        item = _make_item(embedding=precomputed)
        store.add_item(item)

        spy.assert_not_called()

        stored = store.get_by_source("curated_x", "src-1")
        assert stored.embedding == pytest.approx(precomputed, rel=1e-6)

    def test_add_item_upsert(self, store):
        item1 = _make_item(content="original")
        store.add_item(item1)

        item2 = _make_item(content="updated")
        store.add_item(item2)

        # Should have exactly one row, not two
        count = store.conn.execute(
            "SELECT COUNT(*) FROM knowledge WHERE source_type = ? AND source_id = ?",
            ("curated_x", "src-1"),
        ).fetchone()[0]
        assert count == 1

        stored = store.get_by_source("curated_x", "src-1")
        assert stored.content == "updated"


class TestSearchSimilar:
    def _add_items(self, store):
        """Add 3 items with distinct embeddings."""
        items = [
            _make_item(source_id="a", content="machine learning optimization"),
            _make_item(source_id="b", content="cooking pasta recipes"),
            _make_item(source_id="c", content="machine learning neural networks"),
        ]
        for item in items:
            store.add_item(item)

    def test_results_sorted_by_similarity(self, store):
        self._add_items(store)
        results = store.search_similar("machine learning", min_similarity=-1.0)

        assert len(results) >= 1
        # Verify descending similarity order
        sims = [sim for _, sim in results]
        assert sims == sorted(sims, reverse=True)

    def test_min_similarity_filter(self, store):
        self._add_items(store)

        # With very high threshold, should filter out most/all results
        results_strict = store.search_similar("machine learning", min_similarity=0.99)
        results_lax = store.search_similar("machine learning", min_similarity=-1.0)

        assert len(results_strict) <= len(results_lax)

    def test_source_types_filter(self, store):
        store.add_item(_make_item(source_type="own_post", source_id="own-1", content="my post"))
        store.add_item(_make_item(source_type="curated_x", source_id="cur-1", content="curated item"))

        results = store.search_similar(
            "content", source_types=["own_post"], min_similarity=-1.0,
        )
        source_types = {item.source_type for item, _ in results}
        assert source_types == {"own_post"}

    def test_approved_only_filter(self, store):
        store.add_item(_make_item(source_id="approved-1", content="approved item", approved=True))
        store.add_item(_make_item(source_id="unapproved-1", content="unapproved item", approved=False))

        results_approved = store.search_similar("item", approved_only=True, min_similarity=-1.0)
        results_all = store.search_similar("item", approved_only=False, min_similarity=-1.0)

        approved_ids = {item.source_id for item, _ in results_approved}
        all_ids = {item.source_id for item, _ in results_all}

        assert "approved-1" in approved_ids
        assert "unapproved-1" not in approved_ids
        assert "unapproved-1" in all_ids

    def test_limit(self, store):
        for i in range(5):
            store.add_item(_make_item(source_id=f"item-{i}", content=f"content {i}"))

        results = store.search_similar("content", limit=2, min_similarity=-1.0)
        assert len(results) <= 2


class TestGetBySource:
    def test_found(self, store):
        store.add_item(_make_item(source_type="own_post", source_id="p1", content="my post"))
        result = store.get_by_source("own_post", "p1")
        assert result is not None
        assert result.content == "my post"
        assert result.source_type == "own_post"

    def test_not_found(self, store):
        assert store.get_by_source("own_post", "nonexistent") is None


class TestExists:
    def test_exists_true(self, store):
        store.add_item(_make_item(source_type="curated_x", source_id="e1"))
        assert store.exists("curated_x", "e1") is True

    def test_exists_false(self, store):
        assert store.exists("curated_x", "nonexistent") is False


class TestGetOwnInsights:
    def test_returns_only_own_types(self, store):
        store.add_item(_make_item(source_type="own_post", source_id="own-1", content="my post"))
        store.add_item(_make_item(source_type="own_conversation", source_id="own-2", content="my convo"))
        store.add_item(_make_item(source_type="curated_x", source_id="cur-1", content="curated"))
        store.add_item(_make_item(source_type="curated_article", source_id="cur-2", content="article"))

        results = store.get_own_insights()

        source_types = {item.source_type for item in results}
        assert source_types == {"own_post", "own_conversation"}
        assert len(results) == 2

    def test_ordered_by_created_at_desc(self, store):
        # Insert with explicit created_at to control ordering
        for i, ts in enumerate(["2026-01-01T10:00:00", "2026-01-03T10:00:00", "2026-01-02T10:00:00"]):
            item = _make_item(source_type="own_post", source_id=f"own-{i}", content=f"post {i}")
            store.add_item(item)
            store.conn.execute(
                "UPDATE knowledge SET created_at = ? WHERE source_id = ?", (ts, f"own-{i}")
            )
        store.conn.commit()

        results = store.get_own_insights()
        # Expect order: own-1 (Jan 3), own-2 (Jan 2), own-0 (Jan 1)
        assert results[0].source_id == "own-1"
        assert results[1].source_id == "own-2"
        assert results[2].source_id == "own-0"

    def test_respects_limit(self, store):
        for i in range(5):
            store.add_item(_make_item(source_type="own_post", source_id=f"own-{i}", content=f"post {i}"))

        results = store.get_own_insights(limit=2)
        assert len(results) == 2

    def test_empty(self, store):
        assert store.get_own_insights() == []


class TestLinkToContent:
    def test_creates_link_row(self, store, db):
        # Create a generated_content row to satisfy the FK
        content_id = db.insert_generated_content(
            "x_post", ["sha"], ["uuid"], "post", 8.0, "ok"
        )
        item = _make_item(source_id="link-1")
        knowledge_id = store.add_item(item)

        store.link_to_content(content_id, knowledge_id, relevance=0.85)

        row = store.conn.execute(
            "SELECT content_id, knowledge_id, relevance_score FROM content_knowledge_links "
            "WHERE content_id = ? AND knowledge_id = ?",
            (content_id, knowledge_id),
        ).fetchone()
        assert row is not None
        assert row["content_id"] == content_id
        assert row["knowledge_id"] == knowledge_id
        assert row["relevance_score"] == pytest.approx(0.85)
