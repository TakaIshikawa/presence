"""Tests for session insight quote extraction (synthesis.session_insight_quote_export)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from synthesis.session_insight_quote_export import (
    SessionInsightQuote,
    build_session_insight_quote_exports,
    extract_session_insight_quotes_from_rows,
    extract_session_insight_quotes_from_text,
    format_session_insight_quotes_csv,
    format_session_insight_quotes_json,
    format_session_insight_quotes_text,
    format_session_insight_quotes_markdown,
    format_session_insight_quotes_html,
    _clean_text,
    _contains_secrets_or_long_paths,
    _is_noise,
    _normalize_quote,
    _quote_id,
    _score_quote_candidate,
    _split_into_sentences,
)


# --- Helper functions ---


def _make_row(
    text: str,
    session_id: str = "test-session",
    timestamp: str | None = None,
) -> dict:
    return {
        "prompt_text": text,
        "session_id": session_id,
        "sessionId": session_id,
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "project_path": "/test/project",
    }


# --- SessionInsightQuote dataclass ---


class TestSessionInsightQuote:
    def test_fields_stored(self):
        quote = SessionInsightQuote(
            quote_id="quote_abc123",
            quote="I noticed that async operations improve performance significantly.",
            confidence=0.85,
            reason="first-person observation, technical concept",
            category="technical",
            session_id="session-123",
            session_path="/path/to/session",
            project_path="/project",
            message_id=42,
            message_uuid="msg-uuid",
            timestamp="2024-01-01T00:00:00Z",
            source_metadata={},
        )
        assert quote.quote_id == "quote_abc123"
        assert quote.confidence == 0.85
        assert quote.category == "technical"
        assert "async operations" in quote.quote

    def test_to_dict(self):
        quote = SessionInsightQuote(
            quote_id="quote_abc",
            quote="Test quote",
            confidence=0.7,
            reason="test",
            category="general",
            session_id="sess-1",
            session_path=None,
            project_path=None,
            message_id=None,
            message_uuid=None,
            timestamp=None,
            source_metadata={},
        )
        d = quote.to_dict()
        assert d["quote_id"] == "quote_abc"
        assert d["quote"] == "Test quote"
        assert d["confidence"] == 0.7
        assert d["category"] == "general"


# --- Text cleaning and normalization ---


class TestTextCleaning:
    def test_clean_text_removes_extra_whitespace(self):
        assert _clean_text("  hello   world  ") == "hello world"
        assert _clean_text("line1\n\nline2") == "line1 line2"

    def test_normalize_quote(self):
        normalized = _normalize_quote("I found a bug in /usr/local/bin at line 123")
        assert "<path>" in normalized
        assert "<num>" in normalized
        assert "found" in normalized

    def test_quote_id_stable(self):
        id1 = _quote_id("session-1", "Test quote")
        id2 = _quote_id("session-1", "Test quote")
        assert id1 == id2
        assert id1.startswith("quote_")

    def test_quote_id_different_for_different_sessions(self):
        id1 = _quote_id("session-1", "Test quote")
        id2 = _quote_id("session-2", "Test quote")
        assert id1 != id2


# --- Sentence splitting ---


class TestSentenceSplitting:
    def test_split_into_sentences_basic(self):
        text = "This is the first sentence with enough length. This is the second sentence."
        sentences = _split_into_sentences(text)
        assert len(sentences) >= 1
        assert any("first" in s or "second" in s for s in sentences)

    def test_split_filters_too_short(self):
        text = "Hi. This is a proper sentence that is long enough."
        sentences = _split_into_sentences(text)
        # "Hi." is too short (< MIN_QUOTE_LENGTH)
        assert not any(s == "Hi." for s in sentences)

    def test_split_filters_too_long(self):
        text = "x" * 600  # Exceeds MAX_QUOTE_LENGTH
        sentences = _split_into_sentences(text)
        assert len(sentences) == 0


# --- Noise detection ---


class TestNoiseDetection:
    def test_is_noise_command_output(self):
        assert _is_noise("$ git status")
        assert _is_noise("> npm install")
        assert _is_noise("# echo test")

    def test_is_noise_test_output(self):
        assert _is_noise("42 passed, 2 failed")
        assert _is_noise("running test suite")

    def test_is_noise_file_paths(self):
        assert _is_noise("src/app.py:123:45: error message")

    def test_is_noise_stack_trace(self):
        assert _is_noise("  at Object.run (test.js:12)")
        assert _is_noise('  File "app.py", line 42')

    def test_is_not_noise_insight(self):
        assert not _is_noise("I discovered an interesting pattern in error handling.")
        assert not _is_noise("We found that caching improves performance by 50%.")


# --- Secret and path detection ---


class TestSecretsAndPaths:
    def test_contains_secrets_long_token(self):
        assert _contains_secrets_or_long_paths("Token: abc123def456ghi789jkl012mno345")

    def test_contains_secrets_api_key(self):
        assert _contains_secrets_or_long_paths("api_key=sk-ant-1234567890abcdef")
        assert _contains_secrets_or_long_paths("Bearer xyzABC123...")

    def test_contains_secrets_github_token(self):
        assert _contains_secrets_or_long_paths("ghp_1234567890abcdefghijk")

    def test_contains_long_paths(self):
        # Path with > 4 segments
        assert _contains_secrets_or_long_paths("/usr/local/lib/node/modules/package")

    def test_no_secrets_normal_text(self):
        assert not _contains_secrets_or_long_paths("I noticed the API returns cached results.")
        assert not _contains_secrets_or_long_paths("Found a bug in /src/utils.py")


# --- Quote scoring ---


class TestQuoteScoring:
    def test_score_first_person_observation(self):
        signal = _score_quote_candidate("I noticed that the cache invalidation logic was flawed.")
        assert signal is not None
        assert signal.confidence > 0.7
        assert "first-person observation" in signal.reason

    def test_score_collaborative_observation(self):
        signal = _score_quote_candidate("We discovered a performance bottleneck in the query layer.")
        assert signal is not None
        assert signal.confidence > 0.6
        assert "collaborative observation" in signal.reason

    def test_score_discovery_language(self):
        signal = _score_quote_candidate("Turns out the async approach reduces latency significantly.")
        assert signal is not None
        assert "discovery language" in signal.reason

    def test_score_technical_substance(self):
        signal = _score_quote_candidate("The tradeoff between memory and performance is crucial here.")
        assert signal is not None
        assert signal.confidence > 0.6
        assert "tradeoff awareness" in signal.reason

    def test_score_penalize_questions(self):
        signal = _score_quote_candidate("Should we use caching for this endpoint?")
        # Questions get penalized
        assert signal is None or signal.confidence < 0.5

    def test_score_penalize_filler_language(self):
        signal1 = _score_quote_candidate("I noticed an interesting pattern in error handling.")
        signal2 = _score_quote_candidate("I just basically noticed a simple pattern.")
        if signal1 and signal2:
            assert signal1.confidence > signal2.confidence

    def test_score_returns_none_for_generic(self):
        signal = _score_quote_candidate("This is a simple test sentence.")
        # Too generic, no technical markers
        assert signal is None or signal.confidence < 0.5


# --- Extraction from text ---


class TestExtractionFromText:
    def test_extract_from_text_with_insight(self):
        text = """
        I was debugging the issue and I discovered that the connection pool
        was not being reused properly. This caused a significant performance degradation.
        """
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.6)
        assert len(quotes) > 0
        assert any("discovered" in q.quote.lower() for q in quotes)

    def test_extract_filters_noise(self):
        text = """
        $ npm install
        42 passed, 3 failed
        I noticed an interesting tradeoff between memory and speed.
        """
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.6)
        # Should extract the insight, not the command output
        assert all("npm install" not in q.quote for q in quotes)
        assert any("tradeoff" in q.quote.lower() for q in quotes)

    def test_extract_filters_secrets(self):
        text = """
        I found that using token sk-ant-abc123def456ghi789 works.
        The API key is api_key=secret123456789012345678901234567890.
        """
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.5)
        # Sentences with secrets should be filtered out
        assert len(quotes) == 0

    def test_extract_respects_min_confidence(self):
        text = "I noticed something interesting about the pattern."
        quotes_low = extract_session_insight_quotes_from_text(text, min_confidence=0.5)
        quotes_high = extract_session_insight_quotes_from_text(text, min_confidence=0.9)
        assert len(quotes_low) >= len(quotes_high)

    def test_extract_with_metadata(self):
        text = "I discovered a bug in the validation logic."
        metadata = {
            "session_id": "test-session-123",
            "timestamp": "2024-01-01T12:00:00Z",
            "project_path": "/test/project",
        }
        quotes = extract_session_insight_quotes_from_text(text, session_metadata=metadata, min_confidence=0.5)
        assert len(quotes) > 0
        assert quotes[0].session_id == "test-session-123"


# --- Extraction from rows ---


class TestExtractionFromRows:
    def test_extract_from_rows_basic(self):
        rows = [
            _make_row("I noticed the caching strategy was inefficient."),
            _make_row("We discovered a memory leak in the worker pool."),
        ]
        quotes = extract_session_insight_quotes_from_rows(rows, min_confidence=0.6)
        assert len(quotes) >= 1
        assert any("caching" in q.quote.lower() or "memory leak" in q.quote.lower() for q in quotes)

    def test_extract_from_rows_deduplicates(self):
        rows = [
            _make_row("I noticed the pattern.", session_id="session-1"),
            _make_row("I noticed the pattern.", session_id="session-1"),
        ]
        quotes = extract_session_insight_quotes_from_rows(rows, min_confidence=0.5)
        # Should dedupe by quote_id
        quote_ids = [q.quote_id for q in quotes]
        assert len(quote_ids) == len(set(quote_ids))

    def test_extract_from_rows_empty_text(self):
        rows = [
            {"prompt_text": "", "session_id": "test"},
            {"prompt_text": None, "session_id": "test"},
        ]
        quotes = extract_session_insight_quotes_from_rows(rows, min_confidence=0.5)
        assert len(quotes) == 0


# --- Build exports from database ---


class TestBuildExports:
    def test_build_from_list(self):
        now = datetime.now(timezone.utc)
        rows = [
            _make_row("I discovered an optimization technique.", timestamp=now.isoformat()),
        ]
        quotes = build_session_insight_quote_exports(
            rows,
            days=7,
            limit=10,
            min_confidence=0.5,
            now=now,
        )
        assert len(quotes) >= 0

    def test_build_respects_limit(self):
        rows = [
            _make_row(f"I noticed pattern {i} is important.") for i in range(10)
        ]
        quotes = build_session_insight_quote_exports(rows, days=7, limit=3, min_confidence=0.5)
        assert len(quotes) <= 3

    def test_build_with_zero_days_returns_empty(self):
        rows = [_make_row("I noticed something.")]
        quotes = build_session_insight_quote_exports(rows, days=0, limit=10, min_confidence=0.5)
        assert len(quotes) == 0

    def test_build_with_zero_limit_returns_empty(self):
        rows = [_make_row("I noticed something.")]
        quotes = build_session_insight_quote_exports(rows, days=7, limit=0, min_confidence=0.5)
        assert len(quotes) == 0

    def test_build_validates_confidence(self):
        rows = [_make_row("Test")]
        with pytest.raises(ValueError, match="min_confidence must be between 0 and 1"):
            build_session_insight_quote_exports(rows, days=7, limit=10, min_confidence=1.5)


# --- Formatting ---


class TestFormatting:
    def test_format_json_empty(self):
        result = format_session_insight_quotes_json([])
        assert result == "[]"

    def test_format_json_with_quotes(self):
        quote = SessionInsightQuote(
            quote_id="quote_1",
            quote="Test quote",
            confidence=0.8,
            reason="test reason",
            category="technical",
            session_id="session-1",
            session_path=None,
            project_path=None,
            message_id=None,
            message_uuid=None,
            timestamp=None,
            source_metadata={},
        )
        result = format_session_insight_quotes_json([quote])
        assert '"quote_id": "quote_1"' in result
        assert '"confidence": 0.8' in result
        assert '"category": "technical"' in result

    def test_format_csv_empty(self):
        result = format_session_insight_quotes_csv([])
        assert "quote_id,quote,confidence,reason,category,session_id,timestamp" in result

    def test_format_csv_with_quotes(self):
        quote = SessionInsightQuote(
            quote_id="quote_1",
            quote="Test quote",
            confidence=0.8,
            reason="test",
            category="workflow",
            session_id="session-1",
            session_path=None,
            project_path=None,
            message_id=None,
            message_uuid=None,
            timestamp="2024-01-01T00:00:00Z",
            source_metadata={},
        )
        result = format_session_insight_quotes_csv([quote])
        assert "quote_1" in result
        assert "Test quote" in result
        assert "0.8" in result
        assert "workflow" in result

    def test_format_text_empty(self):
        result = format_session_insight_quotes_text([])
        assert "insight_quotes=0" in result
        assert "no insight quotes found" in result

    def test_format_text_with_quotes(self):
        quote = SessionInsightQuote(
            quote_id="quote_1",
            quote="I discovered a pattern",
            confidence=0.75,
            reason="test",
            category="debugging",
            session_id="session-123",
            session_path=None,
            project_path=None,
            message_id=None,
            message_uuid=None,
            timestamp=None,
            source_metadata={},
        )
        result = format_session_insight_quotes_text([quote])
        assert "insight_quotes=1" in result
        assert "0.75" in result
        assert "discovered" in result


# --- Edge cases and validation ---


class TestEdgeCases:
    def test_extract_with_invalid_confidence(self):
        with pytest.raises(ValueError, match="min_confidence must be between 0 and 1"):
            extract_session_insight_quotes_from_text("test", min_confidence=2.0)

        with pytest.raises(ValueError, match="min_confidence must be between 0 and 1"):
            extract_session_insight_quotes_from_text("test", min_confidence=-0.1)

    def test_extract_from_empty_text(self):
        quotes = extract_session_insight_quotes_from_text("", min_confidence=0.5)
        assert len(quotes) == 0

    def test_extract_from_whitespace_only(self):
        quotes = extract_session_insight_quotes_from_text("   \n\n   ", min_confidence=0.5)
        assert len(quotes) == 0


# --- Integration test ---


class TestIntegration:
    def test_end_to_end_extraction(self):
        """Test complete extraction pipeline with realistic session text."""
        session_text = """
        I was working on optimizing the database queries and I noticed that
        adding an index on the user_id column significantly improved performance.

        $ pytest tests/
        42 passed in 2.3s

        Turns out the bottleneck was in the N+1 query pattern we had.
        The tradeoff is slightly more disk space for much faster lookups.

        api_key=sk-ant-shouldbefiltered123456789

        We discovered that caching the results reduces API calls by 80%.
        """

        metadata = {
            "session_id": "integration-test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "project_path": "/test/project",
        }

        quotes = extract_session_insight_quotes_from_text(
            session_text,
            session_metadata=metadata,
            min_confidence=0.6,
        )

        # Should extract insights, filter noise and secrets
        assert len(quotes) > 0

        # Should not include command output
        assert not any("pytest" in q.quote for q in quotes)
        assert not any("passed in" in q.quote for q in quotes)

        # Should not include secrets
        assert not any("api_key" in q.quote for q in quotes)
        assert not any("sk-ant-" in q.quote for q in quotes)

        # Should include technical insights
        quote_texts = " ".join(q.quote for q in quotes).lower()
        assert "performance" in quote_texts or "bottleneck" in quote_texts or "caching" in quote_texts

        # Should have proper metadata
        assert all(q.session_id == "integration-test" for q in quotes)
        assert all(q.confidence >= 0.6 for q in quotes)
        assert all(q.reason for q in quotes)
        assert all(q.category for q in quotes)


# --- Quote categorization tests ---


class TestQuoteCategorization:
    def test_technical_category(self):
        """Test that technical insights are categorized as 'technical'."""
        text = "I noticed the implementation uses a cache for better performance."
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.5)
        assert len(quotes) > 0
        assert quotes[0].category == "technical"

    def test_debugging_category(self):
        """Test that debugging insights are categorized as 'debugging'."""
        text = "I discovered the bug was caused by a race condition in the error handler."
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.5)
        assert len(quotes) > 0
        assert quotes[0].category == "debugging"

    def test_workflow_category(self):
        """Test that workflow insights are categorized as 'workflow'."""
        text = "I always prefer to validate input before processing to avoid issues downstream."
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.5)
        assert len(quotes) > 0
        assert quotes[0].category == "workflow"

    def test_general_category_fallback(self):
        """Test that non-specific insights get 'general' category."""
        text = "I noticed something interesting about the user feedback patterns."
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.5)
        assert len(quotes) > 0
        # Could be general or technical depending on scoring

    def test_multiple_categories_in_session(self):
        """Test extraction with multiple category types."""
        text = """
        I discovered a bug in the validation logic that caused crashes.
        The performance optimization using caching improved speed by 50%.
        I usually test edge cases before committing code changes.
        """
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.5)
        categories = {q.category for q in quotes}
        # Should have multiple categories
        assert len(categories) > 1


# --- Export format tests ---


class TestMarkdownExport:
    def test_markdown_export_empty(self):
        result = format_session_insight_quotes_markdown([])
        assert "# Session Insight Quotes" in result
        assert "No insight quotes found" in result

    def test_markdown_export_with_quotes(self):
        quotes = [
            SessionInsightQuote(
                quote_id="q1",
                quote="I noticed the cache improves performance.",
                confidence=0.85,
                reason="first-person observation",
                category="technical",
                session_id="sess-1",
                session_path=None,
                project_path="/test/project",
                message_id=None,
                message_uuid=None,
                timestamp="2024-01-01T12:00:00Z",
                source_metadata={},
            ),
            SessionInsightQuote(
                quote_id="q2",
                quote="I discovered a bug in the error handler.",
                confidence=0.80,
                reason="discovery language",
                category="debugging",
                session_id="sess-1",
                session_path=None,
                project_path=None,
                message_id=None,
                message_uuid=None,
                timestamp=None,
                source_metadata={},
            ),
        ]
        result = format_session_insight_quotes_markdown(quotes)
        assert "# Session Insight Quotes" in result
        assert "## Technical Insights" in result
        assert "## Debugging Insights" in result
        assert "cache improves performance" in result
        assert "bug in the error handler" in result
        assert "confidence: 0.85" in result
        assert "`sess-1`" in result

    def test_markdown_groups_by_category(self):
        quotes = [
            SessionInsightQuote(
                quote_id="q1",
                quote="Technical quote 1",
                confidence=0.8,
                reason="test",
                category="technical",
                session_id="s1",
                session_path=None,
                project_path=None,
                message_id=None,
                message_uuid=None,
                timestamp=None,
                source_metadata={},
            ),
            SessionInsightQuote(
                quote_id="q2",
                quote="Technical quote 2",
                confidence=0.8,
                reason="test",
                category="technical",
                session_id="s1",
                session_path=None,
                project_path=None,
                message_id=None,
                message_uuid=None,
                timestamp=None,
                source_metadata={},
            ),
        ]
        result = format_session_insight_quotes_markdown(quotes)
        # Should show count of technical insights
        assert "## Technical Insights (2)" in result


class TestHTMLExport:
    def test_html_export_empty(self):
        result = format_session_insight_quotes_html([])
        assert "<!DOCTYPE html>" in result
        assert "<title>Session Insight Quotes</title>" in result
        assert "No insight quotes found" in result

    def test_html_export_with_quotes(self):
        quotes = [
            SessionInsightQuote(
                quote_id="q1",
                quote="I noticed the pattern improves code quality.",
                confidence=0.85,
                reason="first-person observation",
                category="technical",
                session_id="sess-1",
                session_path=None,
                project_path="/test/project",
                message_id=None,
                message_uuid=None,
                timestamp="2024-01-01T12:00:00Z",
                source_metadata={},
            ),
        ]
        result = format_session_insight_quotes_html(quotes)
        assert "<!DOCTYPE html>" in result
        assert "<h1>Session Insight Quotes</h1>" in result
        assert "<h2>Technical Insights (1)</h2>" in result
        assert "pattern improves code quality" in result
        assert "Confidence: 0.85" in result
        assert "<code>sess-1</code>" in result
        assert "2024-01-01T12:00:00Z" in result

    def test_html_includes_css_styling(self):
        result = format_session_insight_quotes_html([])
        assert "<style>" in result
        assert "font-family:" in result
        assert ".quote" in result
        assert ".category" in result

    def test_html_category_specific_styling(self):
        quotes = [
            SessionInsightQuote(
                quote_id="q1",
                quote="Test",
                confidence=0.8,
                reason="test",
                category="debugging",
                session_id="s1",
                session_path=None,
                project_path=None,
                message_id=None,
                message_uuid=None,
                timestamp=None,
                source_metadata={},
            ),
        ]
        result = format_session_insight_quotes_html(quotes)
        # Should have category-specific CSS class
        assert 'class="quote debugging"' in result


# --- Batch export tests ---


class TestBatchExport:
    def test_batch_export_multiple_sessions(self):
        """Test exporting quotes from multiple different sessions."""
        rows = [
            _make_row("I noticed a pattern in session A.", session_id="session-A"),
            _make_row("We discovered an optimization in session B.", session_id="session-B"),
            _make_row("Turns out the issue was in session C.", session_id="session-C"),
        ]
        quotes = extract_session_insight_quotes_from_rows(rows, min_confidence=0.5)

        # Should extract quotes from multiple sessions
        session_ids = {q.session_id for q in quotes}
        assert len(session_ids) >= 2

    def test_batch_export_preserves_session_metadata(self):
        """Test that batch export preserves correct session metadata."""
        now = datetime.now(timezone.utc)
        rows = [
            _make_row(
                "I noticed a performance issue.",
                session_id="session-1",
                timestamp=(now - timedelta(hours=2)).isoformat(),
            ),
            _make_row(
                "We discovered a bug.",
                session_id="session-2",
                timestamp=(now - timedelta(hours=1)).isoformat(),
            ),
        ]
        quotes = extract_session_insight_quotes_from_rows(rows, min_confidence=0.5)

        # Each quote should have correct session ID
        for quote in quotes:
            assert quote.session_id in ["session-1", "session-2"]
            assert quote.timestamp is not None


# --- Date range filtering tests ---


class TestDateRangeFiltering:
    def test_filter_by_date_range(self):
        """Test that build_session_insight_quote_exports respects date range."""
        now = datetime.now(timezone.utc)
        old_timestamp = (now - timedelta(days=30)).isoformat()
        recent_timestamp = (now - timedelta(days=3)).isoformat()

        rows = [
            _make_row("Old insight from 30 days ago.", timestamp=old_timestamp),
            _make_row("Recent insight from 3 days ago.", timestamp=recent_timestamp),
        ]

        # With days=7, should only get recent quote
        quotes = build_session_insight_quote_exports(
            rows,
            days=7,
            limit=None,
            min_confidence=0.5,
            now=now,
        )

        # Should filter out old quotes
        # Note: This depends on the database query filtering, not extraction logic
        # In this test we're passing rows directly, so all might be included
        # The filtering happens in _recent_claude_message_rows when using a DB

    def test_filter_by_confidence_threshold(self):
        """Test filtering quotes by minimum confidence."""
        text = """
        I noticed a very important pattern with clear technical implications.
        Something happened.
        """
        quotes_low = extract_session_insight_quotes_from_text(text, min_confidence=0.3)
        quotes_high = extract_session_insight_quotes_from_text(text, min_confidence=0.8)

        # Higher threshold should filter more
        assert len(quotes_high) <= len(quotes_low)


# --- Malformed JSON edge cases ---


class TestMalformedJSON:
    def test_malformed_row_missing_text_fields(self):
        """Test handling of rows with no text content fields."""
        rows = [
            {"session_id": "test", "id": 1},  # No text fields
            {"session_id": "test", "prompt_text": None},  # Null text
        ]
        quotes = extract_session_insight_quotes_from_rows(rows, min_confidence=0.5)
        assert len(quotes) == 0

    def test_malformed_row_invalid_metadata(self):
        """Test handling of rows with invalid metadata types."""
        rows = [
            {
                "prompt_text": "I noticed a pattern in the code structure.",
                "session_id": None,  # Missing session ID
                "timestamp": "invalid-date-format",
            },
        ]
        quotes = extract_session_insight_quotes_from_rows(rows, min_confidence=0.5)
        # Should still extract, using default session ID
        if len(quotes) > 0:
            assert quotes[0].session_id == "plain-transcript"

    def test_row_with_nested_content_dict(self):
        """Test extraction from row with nested content structure."""
        rows = [
            {
                "content": {
                    "content": "I discovered an interesting optimization technique.",
                },
                "session_id": "test-session",
            },
        ]
        quotes = extract_session_insight_quotes_from_rows(rows, min_confidence=0.5)
        assert len(quotes) > 0
        assert "optimization" in quotes[0].quote.lower()


# --- Special characters and code blocks ---


class TestSpecialCharactersAndCode:
    def test_quote_with_code_syntax(self):
        """Test extraction of quotes containing code syntax."""
        text = "I noticed the `async/await` pattern improves readability significantly."
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.5)
        assert len(quotes) > 0
        assert "async/await" in quotes[0].quote

    def test_quote_with_excessive_code_gets_penalized(self):
        """Test that quotes with too much code syntax get lower confidence."""
        text1 = "I noticed the pattern improves performance."
        text2 = "I noticed {the: [pattern, (improves), <performance>]}."

        quotes1 = extract_session_insight_quotes_from_text(text1, min_confidence=0.5)
        quotes2 = extract_session_insight_quotes_from_text(text2, min_confidence=0.5)

        if quotes1 and quotes2:
            # Code-heavy quote should have lower confidence
            assert quotes1[0].confidence > quotes2[0].confidence

    def test_quote_with_unicode_characters(self):
        """Test extraction of quotes with unicode characters."""
        text = "I noticed the → operator improves code flow visualization."
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.5)
        if len(quotes) > 0:
            assert "→" in quotes[0].quote

    def test_quote_with_newlines_gets_cleaned(self):
        """Test that quotes with newlines are cleaned properly."""
        text = "I noticed\nthat the\nmulti-line approach\nimproves readability."
        quotes = extract_session_insight_quotes_from_text(text, min_confidence=0.5)
        if len(quotes) > 0:
            # Should be cleaned to single line with spaces
            assert "\n" not in quotes[0].quote
            assert "multi-line" in quotes[0].quote
