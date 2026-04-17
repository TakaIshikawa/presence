"""Tests for topic extraction functionality."""

import json
from unittest.mock import Mock, patch

import pytest

from evaluation.topic_extractor import TopicExtractor, TOPIC_TAXONOMY


# Sample text fixtures
TESTING_ARTICLE = """
Writing effective integration tests requires careful attention to test isolation
and state management. This article explores patterns for testing database interactions
and API endpoints, with examples of mocking external dependencies.
"""

ARCHITECTURE_ARTICLE = """
State management is a critical concern in modern web applications. This post
compares different approaches: Redux for predictable state updates, Context API
for simpler use cases, and custom event-driven architectures for complex systems.
"""

MIXED_TOPIC_ARTICLE = """
Debugging production performance issues often reveals architectural problems.
We'll explore profiling tools, monitoring strategies, and how to refactor
hot paths in your application for better throughput.
"""

SHORT_TEXT = "Quick tip about testing."

VERY_SHORT_TEXT = "Test"

REPEATED_TERMS_TEXT = """
Testing testing testing. Integration tests and unit tests. Testing patterns
for better test coverage. Test-driven development improves testing practices.
"""


class TestTopicExtractor:
    """Tests for TopicExtractor class."""

    @pytest.fixture
    def mock_anthropic_client(self):
        """Mock Anthropic client for testing without API calls."""
        with patch("evaluation.topic_extractor.Anthropic") as mock_class:
            mock_client = Mock()
            mock_class.return_value = mock_client
            yield mock_client

    @pytest.fixture
    def extractor(self, mock_anthropic_client):
        """TopicExtractor instance with mocked client."""
        return TopicExtractor(api_key="test-key")

    def test_init_sets_model_default(self, mock_anthropic_client):
        """Test that default model is set correctly."""
        extractor = TopicExtractor(api_key="test-key")
        assert extractor.model == "claude-haiku-4-5-20251001"

    def test_init_accepts_custom_model(self, mock_anthropic_client):
        """Test that custom model can be specified."""
        extractor = TopicExtractor(api_key="test-key", model="custom-model")
        assert extractor.model == "custom-model"

    def test_extract_topics_returns_correct_shape(self, extractor, mock_anthropic_client):
        """Test that extract_topics returns list of (topic, subtopic, confidence) tuples."""
        # Mock successful API response
        mock_response = Mock()
        mock_response.content = [Mock(text='[{"topic": "testing", "subtopic": "integration patterns", "confidence": 0.9}]')]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert isinstance(result, list)
        assert len(result) == 1
        assert len(result[0]) == 3
        topic, subtopic, confidence = result[0]
        assert isinstance(topic, str)
        assert isinstance(subtopic, str)
        assert isinstance(confidence, float)

    def test_extract_topics_with_valid_json(self, extractor, mock_anthropic_client):
        """Test parsing valid JSON response."""
        mock_response = Mock()
        mock_response.content = [Mock(text=json.dumps([
            {"topic": "testing", "subtopic": "integration tests", "confidence": 0.9},
            {"topic": "architecture", "subtopic": "state management", "confidence": 0.7}
        ]))]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert len(result) == 2
        assert result[0] == ("testing", "integration tests", 0.9)
        assert result[1] == ("architecture", "state management", 0.7)

    def test_extract_topics_with_markdown_json(self, extractor, mock_anthropic_client):
        """Test parsing JSON wrapped in markdown code blocks."""
        mock_response = Mock()
        mock_response.content = [Mock(text="""```json
[{"topic": "debugging", "subtopic": "profiling", "confidence": 0.85}]
```""")]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert len(result) == 1
        assert result[0] == ("debugging", "profiling", 0.85)

    def test_extract_topics_with_generic_code_block(self, extractor, mock_anthropic_client):
        """Test parsing JSON in generic code block (no language specified)."""
        mock_response = Mock()
        mock_response.content = [Mock(text="""```
[{"topic": "performance", "subtopic": "optimization", "confidence": 0.75}]
```""")]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert len(result) == 1
        assert result[0] == ("performance", "optimization", 0.75)

    def test_extract_topics_invalid_topic_defaults_to_other(self, extractor, mock_anthropic_client):
        """Test that invalid topics are replaced with 'other'."""
        mock_response = Mock()
        mock_response.content = [Mock(text=json.dumps([
            {"topic": "invalid-topic", "subtopic": "something", "confidence": 0.8}
        ]))]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert len(result) == 1
        assert result[0][0] == "other"  # topic should be replaced
        assert result[0][1] == "something"  # subtopic preserved
        assert result[0][2] == 0.8  # confidence preserved

    def test_extract_topics_confidence_clamped_to_range(self, extractor, mock_anthropic_client):
        """Test that confidence values are clamped to [0.0, 1.0]."""
        mock_response = Mock()
        mock_response.content = [Mock(text=json.dumps([
            {"topic": "testing", "subtopic": "unit tests", "confidence": 1.5},
            {"topic": "debugging", "subtopic": "logging", "confidence": -0.2}
        ]))]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert len(result) == 2
        assert result[0][2] == 1.0  # clamped from 1.5
        assert result[1][2] == 0.0  # clamped from -0.2

    def test_extract_topics_missing_fields_use_defaults(self, extractor, mock_anthropic_client):
        """Test that missing fields use sensible defaults."""
        mock_response = Mock()
        mock_response.content = [Mock(text=json.dumps([
            {"confidence": 0.8},  # missing topic and subtopic
            {"topic": "testing"}  # missing subtopic and confidence
        ]))]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert len(result) == 2
        assert result[0][0] == "other"  # default topic
        assert result[0][1] == ""  # default subtopic
        assert result[0][2] == 0.8
        assert result[1][0] == "testing"
        assert result[1][1] == ""  # default subtopic
        assert result[1][2] == 0.5  # default confidence

    def test_extract_topics_empty_list_returns_default(self, extractor, mock_anthropic_client):
        """Test that empty JSON list returns default 'other' topic."""
        mock_response = Mock()
        mock_response.content = [Mock(text="[]")]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert len(result) == 1
        assert result[0] == ("other", "", 0.5)

    def test_extract_topics_invalid_json_returns_default(self, extractor, mock_anthropic_client):
        """Test that invalid JSON returns default 'other' topic."""
        mock_response = Mock()
        mock_response.content = [Mock(text="not valid json {]")]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert len(result) == 1
        assert result[0] == ("other", "", 0.5)

    def test_extract_topics_non_list_json_returns_default(self, extractor, mock_anthropic_client):
        """Test that JSON object (not list) returns default 'other' topic."""
        mock_response = Mock()
        mock_response.content = [Mock(text='{"topic": "testing", "confidence": 0.9}')]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert len(result) == 1
        assert result[0] == ("other", "", 0.5)

    def test_extract_topics_api_connection_error_raises_structured_exception(self, extractor, mock_anthropic_client):
        """Test that APIConnectionError raises TopicExtractionAPIError."""
        from evaluation.topic_extractor import TopicExtractionAPIError
        import anthropic

        # Create a mock request for APIConnectionError
        mock_request = Mock()
        api_error = anthropic.APIConnectionError(message="Connection failed", request=mock_request)
        mock_anthropic_client.messages.create.side_effect = api_error

        with pytest.raises(TopicExtractionAPIError) as exc_info:
            extractor.extract_topics(TESTING_ARTICLE)

        # Verify error message and chaining
        assert "APIConnectionError" in str(exc_info.value)
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, anthropic.APIConnectionError)

    def test_extract_topics_api_status_error_raises_structured_exception(self, extractor, mock_anthropic_client):
        """Test that APIStatusError raises TopicExtractionAPIError."""
        from evaluation.topic_extractor import TopicExtractionAPIError
        import anthropic

        # Create a minimal mock response for APIStatusError
        mock_response = Mock()
        mock_response.status_code = 500
        api_error = anthropic.APIStatusError(
            message="Server error",
            response=mock_response,
            body=None
        )
        mock_anthropic_client.messages.create.side_effect = api_error

        with pytest.raises(TopicExtractionAPIError) as exc_info:
            extractor.extract_topics(TESTING_ARTICLE)

        # Verify error message and chaining
        assert "APIStatusError" in str(exc_info.value)
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, anthropic.APIStatusError)

    def test_extract_topics_generic_exception_raises_structured_exception(self, extractor, mock_anthropic_client):
        """Test that generic exceptions are wrapped in TopicExtractionAPIError."""
        from evaluation.topic_extractor import TopicExtractionAPIError

        mock_anthropic_client.messages.create.side_effect = ValueError("Unexpected error")

        with pytest.raises(TopicExtractionAPIError) as exc_info:
            extractor.extract_topics(TESTING_ARTICLE)

        # Verify error message and chaining
        assert "ValueError" in str(exc_info.value)
        assert exc_info.value.__cause__ is not None
        assert isinstance(exc_info.value.__cause__, ValueError)

    def test_extract_topics_empty_string_input(self, extractor, mock_anthropic_client):
        """Test handling of empty string input."""
        mock_response = Mock()
        mock_response.content = [Mock(text='[{"topic": "other", "subtopic": "", "confidence": 0.3}]')]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics("")

        # The function doesn't reject empty input - it passes it to LLM
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_extract_topics_whitespace_only_input(self, extractor, mock_anthropic_client):
        """Test handling of whitespace-only input."""
        mock_response = Mock()
        mock_response.content = [Mock(text='[{"topic": "other", "subtopic": "", "confidence": 0.3}]')]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics("   \n\t  ")

        # The function doesn't reject whitespace - it passes it to LLM
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_extract_topics_short_text(self, extractor, mock_anthropic_client):
        """Test handling of short text input."""
        mock_response = Mock()
        mock_response.content = [Mock(text='[{"topic": "testing", "subtopic": "tips", "confidence": 0.6}]')]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(SHORT_TEXT)

        # Short text is processed normally
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_extract_topics_very_short_text(self, extractor, mock_anthropic_client):
        """Test handling of very short text (single word)."""
        mock_response = Mock()
        mock_response.content = [Mock(text='[{"topic": "testing", "subtopic": "", "confidence": 0.5}]')]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(VERY_SHORT_TEXT)

        # Very short text is processed normally
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_extract_topics_repeated_terms(self, extractor, mock_anthropic_client):
        """Test that repeated terms don't cause duplicate topics in response parsing."""
        mock_response = Mock()
        # LLM might return duplicates, but parsing should handle them
        mock_response.content = [Mock(text=json.dumps([
            {"topic": "testing", "subtopic": "test patterns", "confidence": 0.9},
            {"topic": "testing", "subtopic": "test coverage", "confidence": 0.8}
        ]))]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(REPEATED_TERMS_TEXT)

        # The parser doesn't deduplicate - it returns what the LLM provides
        # This tests that duplicate topics are handled without errors
        assert isinstance(result, list)
        assert all(len(item) == 3 for item in result)

    def test_batch_extract_returns_list_of_lists(self, extractor, mock_anthropic_client):
        """Test that batch_extract returns correct shape."""
        mock_response = Mock()
        mock_response.content = [Mock(text='[{"topic": "testing", "subtopic": "patterns", "confidence": 0.9}]')]
        mock_anthropic_client.messages.create.return_value = mock_response

        contents = [TESTING_ARTICLE, ARCHITECTURE_ARTICLE]
        results = extractor.batch_extract(contents)

        assert isinstance(results, list)
        assert len(results) == 2
        assert all(isinstance(r, list) for r in results)
        assert all(all(len(item) == 3 for item in r) for r in results)

    def test_batch_extract_empty_list(self, extractor, mock_anthropic_client):
        """Test batch_extract with empty input list."""
        results = extractor.batch_extract([])

        assert isinstance(results, list)
        assert len(results) == 0

    def test_batch_extract_single_item(self, extractor, mock_anthropic_client):
        """Test batch_extract with single content."""
        mock_response = Mock()
        mock_response.content = [Mock(text='[{"topic": "testing", "subtopic": "patterns", "confidence": 0.9}]')]
        mock_anthropic_client.messages.create.return_value = mock_response

        results = extractor.batch_extract([TESTING_ARTICLE])

        assert len(results) == 1
        assert isinstance(results[0], list)

    def test_batch_extract_calls_extract_for_each_content(self, extractor, mock_anthropic_client):
        """Test that batch_extract calls extract_topics for each content."""
        mock_response = Mock()
        mock_response.content = [Mock(text='[{"topic": "testing", "subtopic": "patterns", "confidence": 0.9}]')]
        mock_anthropic_client.messages.create.return_value = mock_response

        contents = [TESTING_ARTICLE, ARCHITECTURE_ARTICLE, MIXED_TOPIC_ARTICLE]
        results = extractor.batch_extract(contents)

        # Should have called the API once per content
        assert mock_anthropic_client.messages.create.call_count == 3
        assert len(results) == 3

    def test_build_extraction_prompt_includes_taxonomy(self, extractor):
        """Test that extraction prompt includes all taxonomy topics."""
        prompt = extractor._build_extraction_prompt(TESTING_ARTICLE)

        assert "taxonomy:" in prompt.lower()
        for topic in TOPIC_TAXONOMY:
            assert topic in prompt

    def test_build_extraction_prompt_includes_content(self, extractor):
        """Test that extraction prompt includes the input content."""
        prompt = extractor._build_extraction_prompt(TESTING_ARTICLE)

        assert TESTING_ARTICLE in prompt

    def test_build_extraction_prompt_requests_json_format(self, extractor):
        """Test that extraction prompt requests JSON output."""
        prompt = extractor._build_extraction_prompt(TESTING_ARTICLE)

        assert "JSON" in prompt or "json" in prompt
        assert "topic" in prompt
        assert "subtopic" in prompt
        assert "confidence" in prompt

    def test_all_taxonomy_topics_are_valid(self):
        """Test that all topics in TOPIC_TAXONOMY are strings."""
        assert all(isinstance(topic, str) for topic in TOPIC_TAXONOMY)
        assert len(TOPIC_TAXONOMY) > 0

    def test_topic_taxonomy_includes_other(self):
        """Test that taxonomy includes 'other' as fallback."""
        assert "other" in TOPIC_TAXONOMY

    def test_extract_topics_with_mixed_case_normalization(self, extractor, mock_anthropic_client):
        """Test that topics returned are handled regardless of case."""
        # LLM might return topics in different cases
        mock_response = Mock()
        mock_response.content = [Mock(text=json.dumps([
            {"topic": "Testing", "subtopic": "Unit Tests", "confidence": 0.9},
        ]))]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        # Invalid case means it won't match taxonomy and becomes "other"
        # This is the current behavior - testing it as-is
        assert len(result) == 1
        assert result[0][0] == "other"  # "Testing" != "testing"

    def test_extract_topics_with_punctuation_in_subtopic(self, extractor, mock_anthropic_client):
        """Test that punctuation in subtopics is preserved."""
        mock_response = Mock()
        mock_response.content = [Mock(text=json.dumps([
            {"topic": "testing", "subtopic": "unit-tests & mocking", "confidence": 0.9},
        ]))]
        mock_anthropic_client.messages.create.return_value = mock_response

        result = extractor.extract_topics(TESTING_ARTICLE)

        assert len(result) == 1
        assert result[0][1] == "unit-tests & mocking"

    def test_extract_topics_model_parameter_used(self, extractor, mock_anthropic_client):
        """Test that the configured model is used in API calls."""
        mock_response = Mock()
        mock_response.content = [Mock(text='[{"topic": "testing", "subtopic": "patterns", "confidence": 0.9}]')]
        mock_anthropic_client.messages.create.return_value = mock_response

        extractor.extract_topics(TESTING_ARTICLE)

        # Verify the model parameter was passed
        call_kwargs = mock_anthropic_client.messages.create.call_args[1]
        assert call_kwargs["model"] == "claude-haiku-4-5-20251001"

    def test_extract_topics_max_tokens_set(self, extractor, mock_anthropic_client):
        """Test that max_tokens is set in API calls."""
        mock_response = Mock()
        mock_response.content = [Mock(text='[{"topic": "testing", "subtopic": "patterns", "confidence": 0.9}]')]
        mock_anthropic_client.messages.create.return_value = mock_response

        extractor.extract_topics(TESTING_ARTICLE)

        call_kwargs = mock_anthropic_client.messages.create.call_args[1]
        assert "max_tokens" in call_kwargs
        assert call_kwargs["max_tokens"] == 500
