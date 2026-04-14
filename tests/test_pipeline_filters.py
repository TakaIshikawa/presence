"""Unit tests for SynthesisPipeline private filter and helper methods."""

from unittest.mock import MagicMock, patch

import pytest

from synthesis.pipeline import SynthesisPipeline


# --- Test fixtures ---


@pytest.fixture
def mock_db():
    """Database mock for pipeline construction."""
    db = MagicMock()
    db.get_recent_published_content.return_value = []
    db.get_curated_posts.return_value = []
    db.get_auto_classified_posts.return_value = []
    return db


@pytest.fixture
def mock_generator():
    """ContentGenerator mock."""
    return MagicMock()


@pytest.fixture
def pipeline(mock_db, mock_generator):
    """Construct a pipeline with mocked dependencies."""
    with patch("synthesis.pipeline.ContentRefiner"), \
         patch("synthesis.pipeline.CrossModelEvaluator"), \
         patch("synthesis.pipeline.ContentGenerator", return_value=mock_generator), \
         patch("synthesis.pipeline.FewShotSelector"):
        pipeline = SynthesisPipeline(
            api_key="test-key",
            generator_model="claude-sonnet-4-20250514",
            evaluator_model="claude-opus-4-20250514",
            db=mock_db,
            num_candidates=3,
        )
    return pipeline


# --- _extract_opening tests ---


class TestExtractOpening:
    """Test the _extract_opening static method."""

    def test_splits_on_em_dash(self):
        text = "Debugging is about context—not just reading error messages."
        opening = SynthesisPipeline._extract_opening(text)
        assert opening == "debugging is about context"

    def test_splits_on_colon(self):
        text = "The lesson: always validate inputs before processing."
        opening = SynthesisPipeline._extract_opening(text)
        assert opening == "the lesson"

    def test_splits_on_period(self):
        text = "Error handling matters. Most devs ignore it."
        opening = SynthesisPipeline._extract_opening(text)
        assert opening == "error handling matters"

    def test_strips_thread_prefix(self):
        """Thread format: 'TWEET 1:\n' should be stripped before extraction."""
        text = "TWEET 1:\nDebugging is about context—not just reading errors."
        opening = SynthesisPipeline._extract_opening(text)
        assert opening == "debugging is about context"

    def test_strips_thread_prefix_with_whitespace(self):
        text = "TWEET 1:  \n  Debugging is about context—not errors."
        opening = SynthesisPipeline._extract_opening(text)
        assert opening == "debugging is about context"

    def test_truncates_to_max_len(self):
        text = "a" * 150
        opening = SynthesisPipeline._extract_opening(text, max_len=50)
        assert len(opening) == 50

    def test_returns_lowercase(self):
        text = "THIS IS ALL UPPERCASE TEXT"
        opening = SynthesisPipeline._extract_opening(text, max_len=100)
        assert opening == "this is all uppercase text"

    def test_handles_no_delimiters(self):
        text = "short text with no delimiters"
        opening = SynthesisPipeline._extract_opening(text, max_len=100)
        assert opening == text.lower()

    def test_handles_empty_after_prefix(self):
        text = "TWEET 1:\n"
        opening = SynthesisPipeline._extract_opening(text, max_len=100)
        assert opening == ""


# --- _filter_repetitive tests ---


class TestFilterRepetitive:
    """Test the _filter_repetitive method."""

    def test_filters_similar_opening(self, pipeline, mock_db):
        """Candidates with similar openings (ratio > 0.55) should be filtered."""
        mock_db.get_recent_published_content.return_value = [
            {"content": "Debugging is about context—the error message is just the start."}
        ]

        candidates = [
            "Debugging is about context—you need to understand the system.",
            "Debugging is about context: read the logs carefully.",
            "A completely different approach—test-driven development changes everything.",
        ]

        filtered, rejected = pipeline._filter_repetitive(candidates, "x_post")

        # Only the dissimilar candidate should remain
        assert len(filtered) == 1
        assert "test-driven" in filtered[0]

    def test_passes_dissimilar_candidates(self, pipeline, mock_db):
        mock_db.get_recent_published_content.return_value = [
            {"content": "Error handling matters. Most devs ignore it."}
        ]

        candidates = [
            "Context switching costs more than you think.",
            "Type safety catches bugs before production.",
            "Refactoring is a discipline, not a chore.",
        ]

        filtered, rejected = pipeline._filter_repetitive(candidates, "x_post")

        # All should pass through
        assert len(filtered) == 3
        assert filtered == candidates

    def test_empty_recent_history_returns_all(self, pipeline, mock_db):
        mock_db.get_recent_published_content.return_value = []

        candidates = [
            "Debugging is about context—version A.",
            "Debugging is about context—version B.",
            "Debugging is about context—version C.",
        ]

        filtered, rejected = pipeline._filter_repetitive(candidates, "x_post")

        # All candidates pass through when no recent history
        assert len(filtered) == 3
        assert filtered == candidates

    def test_compares_thread_content_not_prefix(self, pipeline, mock_db):
        """Thread prefix should be stripped before comparison."""
        mock_db.get_recent_published_content.return_value = [
            {"content": "TWEET 1:\nDebugging is about context—you need logs."}
        ]

        candidates = [
            "TWEET 1:\nDebugging is about context—you need to read carefully.",
            "TWEET 1:\nA different approach works better.",
        ]

        filtered, rejected = pipeline._filter_repetitive(candidates, "x_thread")

        # First should be filtered, second should pass
        assert len(filtered) == 1
        assert "different approach" in filtered[0]

    def test_filters_all_if_all_repetitive(self, pipeline, mock_db):
        mock_db.get_recent_published_content.return_value = [
            {"content": "Same opening everywhere—this is the pattern."}
        ]

        candidates = [
            "Same opening everywhere—version A.",
            "Same opening everywhere—version B.",
            "Same opening everywhere—version C.",
        ]

        filtered, rejected = pipeline._filter_repetitive(candidates, "x_post")

        # All filtered out
        assert len(filtered) == 0


# --- _filter_stale_patterns tests ---


class TestFilterStalePatterns:
    """Test the _filter_stale_patterns method."""

    def test_rejects_ai_prefix(self, pipeline):
        candidates = [
            "AI isn't about perfect prompts—it's about iteration.",
            "This is a clean post without stale patterns.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert filtered[0] == "This is a clean post without stale patterns."

    def test_allows_isnt_about_pattern(self, pipeline):
        """The 'isn't about X—it's about Y' pattern was removed from stale filters.
        Evaluator handles this with nuance instead of hard-filtering."""
        candidates = [
            "Debugging isn't about the tools—it's about the mindset.",
            "Error handling requires discipline.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 2
        assert rejected == 0

    def test_rejects_breakthrough(self, pipeline):
        candidates = [
            "Today's breakthrough: agents can handle interrupts.",
            "Incremental progress compounds over time.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert "Incremental" in filtered[0]

    def test_rejects_perfect_pattern(self, pipeline):
        candidates = [
            "Perfect prompts aren't the goal—useful output is.",
            "Good enough ships. Perfect stays local.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert "Good enough" in filtered[0]

    def test_rejects_commits_across_pattern(self, pipeline):
        candidates = [
            "120 commits across 6 repos taught me one thing.",
            "Small changes compound into big wins.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert "Small changes" in filtered[0]

    def test_rejects_todays_insight_pattern(self, pipeline):
        candidates = [
            "Today's insight: context matters more than code.",
            "TWEET 1:\nToday's breakthrough with agent handoffs.",
            "Yesterday I learned something valuable.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert "Yesterday" in filtered[0]

    def test_rejects_engagement_bait_unpopular_opinion(self, pipeline):
        candidates = [
            "Unpopular opinion: AI agents don't need perfect memory.",
            "Controversial take—context beats prompts every time.",
            "A measured take on agent reliability.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert "measured take" in filtered[0]

    def test_rejects_nobody_talks_about(self, pipeline):
        candidates = [
            "Nobody is talking about agent error recovery.",
            "Nobody mentions this critical detail.",
            "This pattern is worth discussing.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        # First two match the nobody pattern, third is clean
        assert len(filtered) == 1
        assert "worth discussing" in filtered[0]

    def test_rejects_secret_trick_pattern(self, pipeline):
        candidates = [
            "The secret to better agents: clear boundaries.",
            "The trick to debugging: read the logs.",
            "One simple approach works consistently.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert "simple approach" in filtered[0]

    def test_rejects_stop_start_pattern(self, pipeline):
        candidates = [
            "Stop writing perfect code. Start shipping.",
            "Stop optimizing. Start measuring.",
            "Focus on what matters most.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert "Focus on" in filtered[0]

    def test_rejects_is_dead_pattern(self, pipeline):
        candidates = [
            "Waterfall is dead. Long live iterative development.",
            "Manual testing is dead. Long live automation.",
            "Old approaches fade. New patterns emerge.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert "Old approaches" in filtered[0]

    def test_allows_i_spent_time_pattern(self, pipeline):
        """The 'I spent N hours' pattern was removed from stale filters.
        Resonated posts often use this structure."""
        candidates = [
            "I spent 3 hours debugging this one issue.",
            "I spent 2 weeks building this feature.",
            "After a few iterations, I found the pattern.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 3
        assert rejected == 0

    def test_rejects_most_people_dont_pattern(self, pipeline):
        candidates = [
            "Most developers don't realize this matters.",
            "Most people don't understand agent boundaries.",
            "Many engineers overlook this detail.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert "Many engineers" in filtered[0]

    def test_rejects_everyone_says_pattern(self, pipeline):
        candidates = [
            "Everyone says AI is perfect. I found the opposite.",
            "Everyone preaches best practices. Reality differs.",
            "Common wisdom suggests one approach.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 1
        assert "Common wisdom" in filtered[0]

    def test_clean_candidates_pass_through(self, pipeline):
        candidates = [
            "Context switching costs more than you think.",
            "Error handling requires discipline and practice.",
            "Small improvements compound over time.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        assert len(filtered) == 3
        assert filtered == candidates

    def test_multiple_matches_still_single_rejection(self, pipeline):
        """A candidate matching multiple patterns should still be rejected once."""
        candidates = [
            "AI breakthrough: perfect prompts aren't about the tools—they're about the mindset.",
            "Clean post without patterns.",
        ]

        filtered, rejected, matched_patterns = pipeline._filter_stale_patterns(candidates)

        # First has multiple pattern matches, but only rejected once
        assert len(filtered) == 1
        assert filtered[0] == "Clean post without patterns."


# --- _select_format_directives tests ---


class TestSelectFormatDirectives:
    """Test the _select_format_directives method."""

    def test_returns_correct_number(self, pipeline):
        directives = pipeline._select_format_directives(3, "x_post")
        assert len(directives) == 3

    def test_uses_post_formats_for_x_post(self, pipeline):
        directives = pipeline._select_format_directives(3, "x_post")

        # All directives should start with "FORMAT:"
        for directive in directives:
            assert directive.startswith("FORMAT:")

    def test_uses_thread_formats_for_x_thread(self, pipeline):
        directives = pipeline._select_format_directives(3, "x_thread")

        # All directives should start with "THREAD HOOK:"
        for directive in directives:
            assert directive.startswith("THREAD HOOK:")

    def test_returns_only_directive_strings(self, pipeline):
        """Should return directive strings, not (name, directive) tuples."""
        directives = pipeline._select_format_directives(2, "x_post")

        # All should be strings, not tuples
        for directive in directives:
            assert isinstance(directive, str)
            assert not isinstance(directive, tuple)

    def test_limits_to_available_formats(self, pipeline):
        """Requesting more than available should return all available."""
        # POST_FORMATS has 5 formats, THREAD_FORMATS has 5 formats
        post_directives = pipeline._select_format_directives(10, "x_post")
        assert len(post_directives) == 5

        thread_directives = pipeline._select_format_directives(10, "x_thread")
        assert len(thread_directives) == 5

    def test_randomness_produces_variety(self, pipeline):
        """Multiple calls should produce different selections (probabilistic)."""
        selections = [
            pipeline._select_format_directives(3, "x_post")
            for _ in range(10)
        ]

        # At least one selection should differ (very high probability)
        unique_selections = [tuple(s) for s in selections]
        assert len(set(unique_selections)) > 1


# --- _enforce_char_limit tests ---


class TestEnforceCharLimit:
    """Test the _enforce_char_limit method."""

    def test_candidates_under_limit_pass_through(self, pipeline, mock_generator):
        candidates = ["Short post A", "Short post B", "Short post C"]

        filtered, rejected = pipeline._enforce_char_limit(candidates, 280)

        assert filtered == candidates
        mock_generator.condense.assert_not_called()

    def test_condenses_over_limit_candidate(self, pipeline, mock_generator):
        candidates = [
            "Short post",
            "x" * 300,  # Over 280 limit
            "Another short one",
        ]
        mock_generator.condense.return_value = "x" * 250  # Within limit after condense

        filtered, rejected = pipeline._enforce_char_limit(candidates, 280)

        # condense should be called once for the over-limit candidate
        assert mock_generator.condense.call_count == 1
        assert len(filtered) == 3
        assert all(len(c) <= 280 for c in filtered)

    def test_attempts_condense_twice(self, pipeline, mock_generator):
        candidates = ["x" * 350]
        # First attempt still over, second attempt succeeds
        mock_generator.condense.side_effect = ["x" * 290, "x" * 270]

        filtered, rejected = pipeline._enforce_char_limit(candidates, 280)

        # Should attempt twice
        assert mock_generator.condense.call_count == 2
        assert len(filtered) == 1
        assert len(filtered[0]) <= 280

    def test_discards_after_two_failed_condense_attempts(self, pipeline, mock_generator):
        candidates = [
            "x" * 350,
            "Short post within limit",
            "y" * 320,
        ]
        # All condense attempts fail
        mock_generator.condense.return_value = "x" * 290

        filtered, rejected = pipeline._enforce_char_limit(candidates, 280)

        # Only the short one survives
        assert len(filtered) == 1
        assert filtered[0] == "Short post within limit"

    def test_fallback_truncates_shortest_candidate(self, pipeline, mock_generator):
        """If all candidates over limit, truncate shortest at sentence boundary."""
        candidates = [
            "First sentence. Second sentence. Third sentence." + "x" * 250,
            "y" * 400,
            "z" * 500,
        ]
        # condense fails
        mock_generator.condense.return_value = "x" * 290

        filtered, rejected = pipeline._enforce_char_limit(candidates, 280)

        # Should have one fallback candidate
        assert len(filtered) == 1
        # Should be truncated at sentence boundary
        assert filtered[0].startswith("First sentence.")
        assert len(filtered[0]) <= 280

    def test_fallback_handles_no_sentence_boundaries(self, pipeline, mock_generator):
        """If shortest has no sentences, hard truncate."""
        candidates = ["x" * 350, "y" * 400]
        mock_generator.condense.return_value = "x" * 290

        filtered, rejected = pipeline._enforce_char_limit(candidates, 280)

        # Should hard truncate
        assert len(filtered) == 1
        assert len(filtered[0]) == 280

    def test_mixed_valid_and_invalid_candidates(self, pipeline, mock_generator):
        candidates = [
            "Valid short post",
            "x" * 300,  # Over limit
            "Another valid one",
            "y" * 320,  # Over limit
        ]
        mock_generator.condense.return_value = "x" * 250  # Success

        filtered, rejected = pipeline._enforce_char_limit(candidates, 280)

        # Valid ones pass through, invalid ones condensed
        assert len(filtered) == 4
        assert "Valid short post" in filtered
        assert "Another valid one" in filtered


# --- Topic saturation filter tests ---


class TestFilterTopicSaturated:
    """Test the _filter_topic_saturated method."""

    def test_passes_all_without_embedder(self, pipeline, mock_db):
        """Without embedder, all candidates pass through."""
        pipeline.embedder = None
        candidates = ["post about agents", "post about testing"]
        filtered, rejected = pipeline._filter_topic_saturated(candidates)
        assert filtered == candidates
        assert rejected == 0

    def test_passes_with_insufficient_history(self, pipeline, mock_db):
        """With fewer than 3 embedded recent posts, skip filter."""
        pipeline.embedder = MagicMock()
        mock_db.get_recent_published_content_all.return_value = [
            {"content": "post1", "content_embedding": b"\x00"},
            {"content": "post2", "content_embedding": b"\x00"},
        ]
        candidates = ["candidate"]
        filtered, rejected = pipeline._filter_topic_saturated(candidates)
        assert filtered == candidates
        assert rejected == 0

    @patch("knowledge.embeddings.cosine_similarity")
    @patch("knowledge.embeddings.deserialize_embedding")
    def test_rejects_topic_saturated(self, mock_deser, mock_cos, pipeline, mock_db):
        """Candidate with avg similarity > 0.65 to recent posts is rejected."""
        pipeline.embedder = MagicMock()
        pipeline.embedder.embed_batch.return_value = [[0.1, 0.2]]

        mock_db.get_recent_published_content_all.return_value = [
            {"content": f"post {i}", "content_embedding": b"\x00"}
            for i in range(5)
        ]
        mock_deser.return_value = [0.1, 0.2]
        mock_cos.return_value = 0.75  # Above 0.65 threshold

        candidates = ["yet another agent post"]
        filtered, rejected = pipeline._filter_topic_saturated(candidates)
        assert filtered == []
        assert rejected == 1

    @patch("knowledge.embeddings.cosine_similarity")
    @patch("knowledge.embeddings.deserialize_embedding")
    def test_passes_diverse_candidate(self, mock_deser, mock_cos, pipeline, mock_db):
        """Candidate with avg similarity < 0.65 passes through."""
        pipeline.embedder = MagicMock()
        pipeline.embedder.embed_batch.return_value = [[0.5, 0.6]]

        mock_db.get_recent_published_content_all.return_value = [
            {"content": f"post {i}", "content_embedding": b"\x00"}
            for i in range(5)
        ]
        mock_deser.return_value = [0.1, 0.2]
        mock_cos.return_value = 0.45  # Below 0.65 threshold

        candidates = ["a post about database design"]
        filtered, rejected = pipeline._filter_topic_saturated(candidates)
        assert filtered == ["a post about database design"]
        assert rejected == 0
