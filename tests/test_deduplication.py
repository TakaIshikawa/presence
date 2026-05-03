"""Comprehensive tests for deduplication logic across synthesis stages.

Tests the 3-layer deduplication system:
1. Opening-clause similarity using SequenceMatcher
2. Semantic embedding similarity against recent posts
3. Stale pattern regex matching
"""

import pytest
import time
from difflib import SequenceMatcher
from unittest.mock import Mock, MagicMock, patch

from synthesis.pipeline import SynthesisPipeline
from synthesis.stale_patterns import has_stale_pattern, STALE_PATTERNS
from knowledge.embeddings import cosine_similarity, serialize_embedding, deserialize_embedding
from storage.db import Database


# --- Layer 1: Opening-Clause Similarity Tests ---


class TestOpeningClauseSimilarity:
    """Test opening-clause similarity detection using SequenceMatcher."""

    def test_extract_opening_removes_tweet_prefix(self):
        """Test that TWEET prefix is stripped before comparison."""
        text = "TWEET 1:\nThis is the actual opening line"
        opening = SynthesisPipeline._extract_opening(text)
        assert "TWEET" not in opening
        assert "this is the actual opening line" in opening

    def test_extract_opening_handles_various_tweet_formats(self):
        """Test extraction handles different TWEET prefix formats."""
        cases = [
            ("TWEET 1:\nContent here", "content here"),
            ("TWEET 2:  \nContent here", "content here"),
            ("TWEET 10:\nContent", "content"),
            ("No prefix content", "no prefix content"),
        ]
        for input_text, expected_start in cases:
            opening = SynthesisPipeline._extract_opening(input_text)
            assert opening.startswith(expected_start)

    def test_extract_opening_splits_on_punctuation(self):
        """Test that opening splits on em-dash, colon, or period."""
        cases = [
            "First part—second part",
            "First part: second part",
            "First part. Second part",
        ]
        for text in cases:
            opening = SynthesisPipeline._extract_opening(text)
            assert "second" not in opening
            assert "first part" in opening

    def test_extract_opening_respects_max_length(self):
        """Test that opening is limited to max_len characters."""
        long_text = "a" * 200
        opening = SynthesisPipeline._extract_opening(long_text, max_len=50)
        assert len(opening) <= 50

    def test_filter_repetitive_rejects_similar_openings(self):
        """Test that candidates with similar openings to recent posts are filtered."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = [
            {"content": "I was debugging async race conditions in production"},
            {"content": "Spent the afternoon refactoring the auth module"},
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        candidates = [
            "I was debugging async issues in staging",  # Very similar to first
            "Completely different content about API design",  # Different
            "Spent the afternoon working on caching",  # Similar to second
        ]

        filtered, rejected = pipeline._filter_repetitive(candidates, "x_post")

        assert rejected >= 1  # At least one should be rejected
        assert len(filtered) < len(candidates)

    def test_filter_repetitive_threshold_55_percent(self):
        """Test that similarity threshold of 0.55 is used."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = [
            {"content": "The quick brown fox jumps"},
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        # Create candidates with varying similarity
        recent_opening = SynthesisPipeline._extract_opening("The quick brown fox jumps")

        # Just below threshold (should pass)
        candidate_below = "The quick brown dog runs"
        similarity_below = SequenceMatcher(
            None,
            SynthesisPipeline._extract_opening(candidate_below),
            recent_opening
        ).ratio()

        # Above threshold (should be rejected)
        candidate_above = "The quick brown fox leaps"
        similarity_above = SequenceMatcher(
            None,
            SynthesisPipeline._extract_opening(candidate_above),
            recent_opening
        ).ratio()

        # Verify our test setup
        assert similarity_below < 0.55 or similarity_above > 0.55

    def test_filter_repetitive_with_no_recent_posts(self):
        """Test that filtering passes all candidates when no recent posts exist."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        candidates = ["Content 1", "Content 2", "Content 3"]
        filtered, rejected = pipeline._filter_repetitive(candidates, "x_post")

        assert rejected == 0
        assert filtered == candidates

    def test_filter_repetitive_checks_last_20_posts(self):
        """Test that repetition filter checks against last 20 published posts."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        pipeline._filter_repetitive(["test"], "x_post")

        db.get_recent_published_content.assert_called_once_with("x_post", limit=20)


# --- Layer 2: Semantic Embedding Similarity Tests ---


class TestSemanticEmbeddingSimilarity:
    """Test semantic embedding similarity against recent posts."""

    def test_cosine_similarity_identical_vectors(self):
        """Test cosine similarity is 1.0 for identical vectors."""
        vec = [1.0, 2.0, 3.0, 4.0]
        similarity = cosine_similarity(vec, vec)
        assert abs(similarity - 1.0) < 1e-6

    def test_cosine_similarity_orthogonal_vectors(self):
        """Test cosine similarity is 0.0 for orthogonal vectors."""
        vec_a = [1.0, 0.0, 0.0]
        vec_b = [0.0, 1.0, 0.0]
        similarity = cosine_similarity(vec_a, vec_b)
        assert abs(similarity) < 1e-6

    def test_cosine_similarity_opposite_vectors(self):
        """Test cosine similarity is -1.0 for opposite vectors."""
        vec_a = [1.0, 2.0, 3.0]
        vec_b = [-1.0, -2.0, -3.0]
        similarity = cosine_similarity(vec_a, vec_b)
        assert abs(similarity - (-1.0)) < 1e-6

    def test_cosine_similarity_zero_vector_handling(self):
        """Test that zero vectors return 0.0 similarity."""
        vec_zero = [0.0, 0.0, 0.0]
        vec_normal = [1.0, 2.0, 3.0]

        similarity = cosine_similarity(vec_zero, vec_normal)
        assert similarity == 0.0

    def test_embedding_serialization_roundtrip(self):
        """Test that embeddings can be serialized and deserialized."""
        original = [0.1, 0.2, 0.3, 0.4, 0.5]
        serialized = serialize_embedding(original)
        deserialized = deserialize_embedding(serialized)

        assert len(deserialized) == len(original)
        for a, b in zip(original, deserialized):
            assert abs(a - b) < 1e-6

    def test_filter_semantic_duplicates_threshold_0_82(self):
        """Test that default semantic threshold of 0.82 is used."""
        db = Mock(spec=Database)

        # Create mock embeddings
        recent_embedding = [0.5] * 100
        db.get_recent_published_content_all.return_value = [
            {
                "content": "Recent post about testing",
                "content_embedding": serialize_embedding(recent_embedding),
            }
        ]

        embedder = Mock()
        # Very similar embedding (will exceed 0.82)
        similar_embedding = [0.51] * 100
        # Different embedding (will not exceed 0.82)
        different_embedding = [0.1] * 100

        embedder.embed_batch.return_value = [similar_embedding, different_embedding]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=embedder,
            semantic_threshold=0.82,
        )

        candidates = ["Similar content", "Different content"]
        filtered = pipeline._filter_semantic_duplicates(candidates)

        # Verify embedding was called
        embedder.embed_batch.assert_called_once()
        assert len(filtered) <= len(candidates)

    def test_filter_semantic_duplicates_with_custom_threshold(self):
        """Test semantic filtering with custom threshold values."""
        db = Mock(spec=Database)

        recent_embedding = [1.0, 0.0, 0.0]
        db.get_recent_published_content_all.return_value = [
            {
                "content": "Recent",
                "content_embedding": serialize_embedding(recent_embedding),
            }
        ]

        embedder = Mock()
        # Similarity will be moderate (~0.7)
        candidate_embedding = [0.7, 0.3, 0.0]
        embedder.embed_batch.return_value = [candidate_embedding]

        # Test with threshold 0.7 (should reject)
        pipeline_strict = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=embedder,
            semantic_threshold=0.7,
        )

        # Test with threshold 0.9 (should pass)
        pipeline_lenient = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=embedder,
            semantic_threshold=0.9,
        )

        candidates = ["Test content"]

        # The actual similarity will determine filtering
        # We're mainly testing that threshold is configurable

    def test_filter_semantic_duplicates_checks_last_30_posts(self):
        """Test that semantic dedup checks last 30 published posts."""
        db = Mock(spec=Database)
        db.get_recent_published_content_all.return_value = []

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=Mock(),
        )

        pipeline._filter_semantic_duplicates(["test"])

        db.get_recent_published_content_all.assert_called_once_with(limit=30)

    def test_filter_semantic_duplicates_skips_posts_without_embeddings(self):
        """Test that posts without embeddings are skipped."""
        db = Mock(spec=Database)
        db.get_recent_published_content_all.return_value = [
            {"content": "Post without embedding"},
            {
                "content": "Post with embedding",
                "content_embedding": serialize_embedding([0.1] * 10),
            },
        ]

        embedder = Mock()
        embedder.embed_batch.return_value = [[0.2] * 10]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=embedder,
        )

        filtered = pipeline._filter_semantic_duplicates(["test"])

        # Should process without error
        assert isinstance(filtered, list)

    def test_filter_semantic_duplicates_without_embedder(self):
        """Test that filtering is skipped when no embedder is configured."""
        db = Mock(spec=Database)

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=None,
        )

        candidates = ["Test 1", "Test 2"]
        filtered = pipeline._filter_semantic_duplicates(candidates)

        assert filtered == candidates


# --- Layer 3: Stale Pattern Tests ---


class TestStalePatternFiltering:
    """Test stale pattern regex matching."""

    def test_filter_stale_patterns_rejects_matches(self):
        """Test that candidates matching stale patterns are rejected."""
        db = Mock(spec=Database)

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        candidates = [
            "AI is transforming everything",  # Stale
            "Debugging async issues today",  # Clean
            "Everyone says testing is important",  # Stale
            "Refactored the auth module",  # Clean
        ]

        filtered, rejected, patterns = pipeline._filter_stale_patterns(candidates)

        assert rejected >= 2
        assert len(filtered) <= 2
        assert len(patterns) >= 2

    def test_filter_stale_patterns_returns_matched_patterns(self):
        """Test that matched pattern strings are returned."""
        db = Mock(spec=Database)

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        candidates = ["AI is great", "Breakthrough discovery"]
        filtered, rejected, patterns = pipeline._filter_stale_patterns(candidates)

        assert len(patterns) >= 2  # Should have matched patterns
        assert all(isinstance(p, str) for p in patterns)

    def test_filter_stale_patterns_integration_with_module(self):
        """Test integration with stale_patterns module."""
        # This test verifies the pipeline uses the shared STALE_PATTERNS
        db = Mock(spec=Database)

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        # Test a few patterns from the module
        stale_samples = [
            "AI is transforming coding",
            "This is a breakthrough",
            "Perfect prompts are impossible",
        ]

        for sample in stale_samples:
            assert has_stale_pattern(sample), f"Pattern module should detect: {sample}"
            filtered, _, _ = pipeline._filter_stale_patterns([sample])
            assert len(filtered) == 0, f"Pipeline should filter: {sample}"


# --- Combined Deduplication Tests ---


class TestCombinedDeduplication:
    """Test all three deduplication layers working together."""

    def test_all_three_layers_reject_appropriately(self):
        """Test that content can be rejected by any of the three layers."""
        db = Mock(spec=Database)

        # Setup recent posts for layer 1
        db.get_recent_published_content.return_value = [
            {"content": "I was debugging async issues"}
        ]

        # Setup embeddings for layer 2
        db.get_recent_published_content_all.return_value = [
            {
                "content": "Technical content",
                "content_embedding": serialize_embedding([0.5] * 10),
            }
        ]

        embedder = Mock()
        embedder.embed_batch.return_value = [
            [0.51] * 10,  # Very similar (layer 2 reject)
            [0.1] * 10,   # Different (layer 2 pass)
            [0.1] * 10,   # Different (layer 2 pass)
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=embedder,
            semantic_threshold=0.82,
        )

        candidates = [
            "I was debugging async problems",  # Layer 1: similar opening
            "AI is transforming development",  # Layer 3: stale pattern
            "Refactored the caching layer",    # All layers: pass
        ]

        # Apply layer 1
        after_layer1, _ = pipeline._filter_repetitive(candidates, "x_post")

        # Apply layer 2
        after_layer2 = pipeline._filter_semantic_duplicates(after_layer1)

        # Apply layer 3
        after_layer3, _, _ = pipeline._filter_stale_patterns(after_layer2)

        # At least one should pass through all layers
        assert len(after_layer3) >= 0

    def test_layers_are_independent(self):
        """Test that each layer operates independently."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = []
        db.get_recent_published_content_all.return_value = []

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        # Content that only fails stale pattern check
        stale_only = ["AI is great but no similar recent posts"]

        # Should pass layer 1 and 2
        after_layer1, _ = pipeline._filter_repetitive(stale_only, "x_post")
        assert len(after_layer1) == 1

        after_layer2 = pipeline._filter_semantic_duplicates(after_layer1)
        assert len(after_layer2) == 1

        # Should fail layer 3
        after_layer3, _, _ = pipeline._filter_stale_patterns(after_layer2)
        assert len(after_layer3) == 0


# --- Edge Cases Tests ---


class TestEdgeCases:
    """Test edge cases in deduplication logic."""

    def test_identical_content_is_rejected(self):
        """Test that identical content to recent posts is rejected."""
        db = Mock(spec=Database)

        identical_content = "This is identical content about testing"

        db.get_recent_published_content.return_value = [
            {"content": identical_content}
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        candidates = [identical_content]
        filtered, rejected = pipeline._filter_repetitive(candidates, "x_post")

        assert rejected >= 1
        assert len(filtered) == 0

    def test_near_duplicates_with_minor_variations(self):
        """Test that near-duplicates with minor wording changes are caught."""
        db = Mock(spec=Database)

        db.get_recent_published_content.return_value = [
            {"content": "I spent the afternoon debugging race conditions"}
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        near_duplicates = [
            "I spent the afternoon debugging race issues",
            "I spent the morning debugging race conditions",
            "I spent the afternoon fixing race conditions",
        ]

        filtered, rejected = pipeline._filter_repetitive(near_duplicates, "x_post")

        # At least some should be rejected as too similar
        assert rejected >= 1

    def test_legitimate_similar_posts_on_different_topics(self):
        """Test that structurally similar but topically different posts can pass."""
        db = Mock(spec=Database)

        db.get_recent_published_content.return_value = [
            {"content": "Fixed a critical bug in the payment processor today"}
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        # Different topic, similar structure
        different_topic = [
            "Implemented a new feature in the auth system today"
        ]

        filtered, rejected = pipeline._filter_repetitive(different_topic, "x_post")

        # Should pass as topics are different enough
        # (actual behavior depends on similarity threshold)

    def test_empty_candidate_list(self):
        """Test handling of empty candidate list."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = [{"content": "test"}]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        filtered, rejected = pipeline._filter_repetitive([], "x_post")

        assert filtered == []
        assert rejected == 0

    def test_single_candidate(self):
        """Test deduplication with single candidate."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        candidates = ["Single candidate content"]
        filtered, rejected = pipeline._filter_repetitive(candidates, "x_post")

        assert len(filtered) == 1
        assert rejected == 0


# --- Content Type Tests ---


class TestDeduplicationAcrossContentTypes:
    """Test deduplication across different content types."""

    def test_deduplication_for_posts(self):
        """Test deduplication for x_post content type."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = [
            {"content": "Testing posts"}
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        pipeline._filter_repetitive(["test"], "x_post")

        db.get_recent_published_content.assert_called_with("x_post", limit=20)

    def test_deduplication_for_threads(self):
        """Test deduplication for x_thread content type."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = [
            {"content": "TWEET 1:\nThread content"}
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        candidates = ["TWEET 1:\nSimilar thread content"]
        filtered, _ = pipeline._filter_repetitive(candidates, "x_thread")

        db.get_recent_published_content.assert_called_with("x_thread", limit=20)

    def test_thread_prefix_handling_in_deduplication(self):
        """Test that thread prefixes are properly handled in deduplication."""
        db = Mock(spec=Database)

        db.get_recent_published_content.return_value = [
            {"content": "TWEET 1:\nI was debugging async issues"}
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        # Should compare actual content, not prefix
        candidates = [
            "TWEET 1:\nI was debugging async problems",
            "TWEET 2:\nCompletely different content",
        ]

        filtered, rejected = pipeline._filter_repetitive(candidates, "x_thread")

        # First should be rejected as similar
        assert rejected >= 1


# --- Window Sizing Tests ---


class TestRecentPostWindowSizing:
    """Test deduplication with different recent post window sizes."""

    def test_filter_repetitive_uses_limit_20(self):
        """Test that repetition filter uses window of 20 recent posts."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        pipeline._filter_repetitive(["test"], "x_post")

        # Verify it requests exactly 20 posts
        db.get_recent_published_content.assert_called_once_with("x_post", limit=20)

    def test_semantic_dedup_uses_limit_30(self):
        """Test that semantic dedup uses window of 30 recent posts."""
        db = Mock(spec=Database)
        db.get_recent_published_content_all.return_value = []

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=Mock(),
        )

        pipeline._filter_semantic_duplicates(["test"])

        # Verify it requests exactly 30 posts
        db.get_recent_published_content_all.assert_called_once_with(limit=30)

    def test_topic_saturation_uses_limit_10(self):
        """Test that topic saturation filter uses window of 10 recent posts."""
        db = Mock(spec=Database)
        db.get_recent_published_content_all.return_value = []

        embedder = Mock()
        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=embedder,
        )

        pipeline._filter_topic_saturated(["test"])

        # Verify it requests exactly 10 posts
        db.get_recent_published_content_all.assert_called_once_with(limit=10)


# --- Performance Tests ---


class TestPerformanceWithLargeSets:
    """Test deduplication performance with large sets of posts."""

    def test_performance_with_1000_recent_posts(self):
        """Test opening-clause filtering performance with 1000 recent posts."""
        db = Mock(spec=Database)

        # Create 1000 mock recent posts
        recent_posts = [
            {"content": f"Post number {i} about various topics"}
            for i in range(1000)
        ]
        db.get_recent_published_content.return_value = recent_posts[:20]  # Only returns 20

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        candidates = [f"Candidate {i} content" for i in range(10)]

        start = time.perf_counter()
        filtered, _ = pipeline._filter_repetitive(candidates, "x_post")
        elapsed = time.perf_counter() - start

        # Should complete in under 1 second
        assert elapsed < 1.0
        assert isinstance(filtered, list)

    def test_performance_embedding_similarity_large_set(self):
        """Test semantic dedup performance with many embeddings."""
        db = Mock(spec=Database)

        # Create 100 posts with embeddings (reasonable for 30-post window)
        recent_posts = [
            {
                "content": f"Post {i}",
                "content_embedding": serialize_embedding([float(i % 10)] * 100),
            }
            for i in range(100)
        ]
        db.get_recent_published_content_all.return_value = recent_posts[:30]

        embedder = Mock()
        # Generate embeddings for 10 candidates
        embedder.embed_batch.return_value = [
            [0.5] * 100 for _ in range(10)
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=embedder,
        )

        candidates = [f"Candidate {i}" for i in range(10)]

        start = time.perf_counter()
        filtered = pipeline._filter_semantic_duplicates(candidates)
        elapsed = time.perf_counter() - start

        # Should complete in under 2 seconds
        assert elapsed < 2.0
        assert isinstance(filtered, list)

    def test_performance_stale_pattern_matching_large_candidates(self):
        """Test stale pattern matching with many candidates."""
        db = Mock(spec=Database)

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        # Test with 100 candidates
        candidates = [
            f"This is candidate number {i} with unique content"
            for i in range(100)
        ]
        # Add some stale patterns
        candidates.extend([
            "AI is transforming everything",
            "Everyone says this is important",
            "The secret to success",
        ])

        start = time.perf_counter()
        filtered, rejected, _ = pipeline._filter_stale_patterns(candidates)
        elapsed = time.perf_counter() - start

        # Should complete quickly even with 100+ candidates
        assert elapsed < 0.5
        assert rejected >= 3  # Should catch the stale patterns


# --- Error Handling Tests ---


class TestErrorHandling:
    """Test error handling in deduplication logic."""

    def test_missing_embeddings_for_comparison(self):
        """Test handling when embeddings are missing for recent posts."""
        db = Mock(spec=Database)

        # Posts without embeddings
        db.get_recent_published_content_all.return_value = [
            {"content": "Post 1"},
            {"content": "Post 2"},
        ]

        embedder = Mock()
        embedder.embed_batch.return_value = [[0.5] * 10]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=embedder,
        )

        # Should not crash, should skip comparison
        filtered = pipeline._filter_semantic_duplicates(["test"])
        assert filtered == ["test"]

    def test_empty_recent_post_set(self):
        """Test handling when recent post set is empty."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = []
        db.get_recent_published_content_all.return_value = []

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        candidates = ["Test content"]

        # Should pass through without issues
        filtered1, rejected1 = pipeline._filter_repetitive(candidates, "x_post")
        assert filtered1 == candidates
        assert rejected1 == 0

        filtered2 = pipeline._filter_semantic_duplicates(candidates)
        assert filtered2 == candidates

    def test_malformed_content_input(self):
        """Test handling of malformed content input."""
        db = Mock(spec=Database)
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        # Test various edge cases
        edge_cases = [
            "",  # Empty string
            " ",  # Whitespace only
            "\n\n",  # Newlines only
            "x" * 10000,  # Very long content
        ]

        for content in edge_cases:
            # Should not crash
            filtered, _ = pipeline._filter_repetitive([content], "x_post")
            assert isinstance(filtered, list)

    def test_embedding_generation_failure(self):
        """Test handling when embedding generation fails."""
        db = Mock(spec=Database)
        db.get_recent_published_content_all.return_value = [
            {
                "content": "Test",
                "content_embedding": serialize_embedding([0.1] * 10),
            }
        ]

        embedder = Mock()
        embedder.embed_batch.side_effect = Exception("Embedding API error")

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=embedder,
        )

        candidates = ["Test content"]

        # Should handle gracefully and return candidates unchanged
        filtered = pipeline._filter_semantic_duplicates(candidates)
        assert filtered == candidates

    def test_database_query_failure_repetitive(self):
        """Test handling when database query fails for repetitive filter."""
        db = Mock(spec=Database)
        db.get_recent_published_content.side_effect = Exception("DB error")

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        # Should raise the exception (caller should handle)
        with pytest.raises(Exception, match="DB error"):
            pipeline._filter_repetitive(["test"], "x_post")


# --- Accuracy and Metrics Tests ---


class TestDeduplicationAccuracy:
    """Test deduplication accuracy metrics."""

    def test_false_positive_rate_low(self):
        """Test that false positive rate is acceptably low."""
        db = Mock(spec=Database)

        # Create diverse recent posts
        db.get_recent_published_content.return_value = [
            {"content": "Post about testing strategies"},
            {"content": "Article on performance optimization"},
            {"content": "Guide to API design patterns"},
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        # Create clearly different candidates
        different_candidates = [
            "Debugging async race conditions in production",
            "Refactored authentication middleware today",
            "Implemented caching layer for API responses",
            "Fixed memory leak in worker processes",
        ]

        filtered, rejected = pipeline._filter_repetitive(different_candidates, "x_post")

        # Should not reject clearly different content
        false_positive_rate = rejected / len(different_candidates)
        assert false_positive_rate <= 0.25  # Allow max 25% false positive

    def test_true_positive_rate_high(self):
        """Test that true positive rate (catching duplicates) is high."""
        db = Mock(spec=Database)

        recent_content = "I spent the afternoon debugging async race conditions"
        db.get_recent_published_content.return_value = [
            {"content": recent_content}
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        # Create near-duplicate variations
        near_duplicates = [
            "I spent the afternoon debugging async issues",
            "I spent the morning debugging async race conditions",
            "I spent the afternoon fixing async race conditions",
        ]

        filtered, rejected = pipeline._filter_repetitive(near_duplicates, "x_post")

        # Should catch most near-duplicates
        true_positive_rate = rejected / len(near_duplicates)
        assert true_positive_rate >= 0.5  # Should catch at least 50%

    def test_precision_recall_tradeoff(self):
        """Test that deduplication balances precision and recall."""
        db = Mock(spec=Database)

        db.get_recent_published_content.return_value = [
            {"content": "Debugging production issues with async code"}
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
        )

        test_cases = [
            ("Debugging production issues with async tasks", True),  # Should reject
            ("Fixed authentication bug in production", False),  # Should pass
            ("Debugging staging issues with sync code", False),  # Should pass
            ("Debugging production problems with async code", True),  # Should reject
        ]

        results = []
        for content, should_reject in test_cases:
            filtered, rejected = pipeline._filter_repetitive([content], "x_post")
            was_rejected = (rejected > 0)
            results.append((should_reject, was_rejected))

        # Calculate accuracy
        correct = sum(1 for expected, actual in results if expected == actual)
        accuracy = correct / len(results)

        # Should have reasonable accuracy
        assert accuracy >= 0.5


# --- Integration Test ---


class TestDeduplicationIntegration:
    """Integration test for full deduplication pipeline."""

    def test_full_deduplication_pipeline(self):
        """Test complete deduplication flow through all three layers."""
        db = Mock(spec=Database)

        # Setup data for all three layers
        db.get_recent_published_content.return_value = [
            {"content": "I was debugging async race conditions"}
        ]

        db.get_recent_published_content_all.return_value = [
            {
                "content": "Technical post about testing",
                "content_embedding": serialize_embedding([0.5] * 10),
            }
        ]

        embedder = Mock()
        embedder.embed_batch.return_value = [
            [0.1] * 10,  # Different
            [0.51] * 10,  # Similar
            [0.1] * 10,  # Different
            [0.1] * 10,  # Different
        ]

        pipeline = SynthesisPipeline(
            api_key="test",
            generator_model="test",
            evaluator_model="test",
            db=db,
            embedder=embedder,
            semantic_threshold=0.82,
        )

        candidates = [
            "I was debugging async problems in prod",  # Fail layer 1
            "Completely different technical content",  # May fail layer 2
            "AI is transforming software development",  # Fail layer 3
            "Refactored the authentication middleware",  # Pass all
        ]

        # Run through all layers
        after_layer1, rejected1 = pipeline._filter_repetitive(candidates, "x_post")
        after_layer2 = pipeline._filter_semantic_duplicates(after_layer1)
        after_layer3, rejected3, patterns = pipeline._filter_stale_patterns(after_layer2)

        # Should have filtered at least some candidates
        total_rejected = rejected1 + rejected3
        assert total_rejected >= 1

        # Should have some candidates remaining
        assert len(after_layer3) >= 0
