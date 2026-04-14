"""Tests for pattern analyzer module (Feature 2)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from synthesis.pattern_analyzer import PatternAnalyzer, PatternAnalysis


class TestPatternAnalyzer:
    """Tests for PatternAnalyzer.analyze()."""

    def test_returns_none_with_insufficient_data(self):
        analyzer = PatternAnalyzer(api_key="test-key")
        result = analyzer.analyze(
            resonated=[{"content": "post1"}, {"content": "post2"}],
            low_resonance=[{"content": "bad1"}],
        )
        assert result is None

    @patch("synthesis.pattern_analyzer.anthropic.Anthropic")
    def test_analyze_returns_pattern_analysis(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = """
<json>
{
    "positive_patterns": ["Opens with specific moment", "Shows vulnerability"],
    "negative_patterns": ["Abstract statements", "Lists features"],
    "key_differences": ["Resonated shows process; low_resonance states outcomes"],
    "actionable_rules": ["Always open with what surprised you"]
}
</json>
"""
        mock_client.messages.create.return_value = mock_response

        analyzer = PatternAnalyzer(api_key="test-key")
        result = analyzer.analyze(
            resonated=[
                {"content": f"resonated post {i}", "engagement_score": 5.0}
                for i in range(5)
            ],
            low_resonance=[
                {"content": f"low resonance post {i}"}
                for i in range(10)
            ],
        )

        assert result is not None
        assert isinstance(result, PatternAnalysis)
        assert len(result.positive_patterns) == 2
        assert len(result.negative_patterns) == 2
        assert len(result.actionable_rules) == 1
        assert result.analyzed_at is not None

    @patch("synthesis.pattern_analyzer.anthropic.Anthropic")
    def test_handles_malformed_json(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = "This is not valid JSON at all"
        mock_client.messages.create.return_value = mock_response

        analyzer = PatternAnalyzer(api_key="test-key")
        result = analyzer.analyze(
            resonated=[{"content": f"post {i}"} for i in range(3)],
            low_resonance=[{"content": "bad"}],
        )

        assert result is not None
        assert result.positive_patterns == []
        assert result.actionable_rules == []

    @patch("synthesis.pattern_analyzer.anthropic.Anthropic")
    def test_handles_json_without_tags(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = json.dumps({
            "positive_patterns": ["pattern1"],
            "negative_patterns": ["antipattern1"],
            "key_differences": ["diff1"],
            "actionable_rules": ["rule1"],
        })
        mock_client.messages.create.return_value = mock_response

        analyzer = PatternAnalyzer(api_key="test-key")
        result = analyzer.analyze(
            resonated=[{"content": f"post {i}"} for i in range(3)],
            low_resonance=[],
        )

        assert result is not None
        assert result.positive_patterns == ["pattern1"]
        assert result.actionable_rules == ["rule1"]


class TestBuildPrompt:
    """Tests for _build_prompt."""

    def test_includes_resonated_content(self):
        analyzer = PatternAnalyzer(api_key="test-key")
        prompt = analyzer._build_prompt(
            resonated=[
                {"content": "great post", "engagement_score": 10.0},
            ],
            low_resonance=[{"content": "boring post"}],
        )
        assert "great post" in prompt
        assert "boring post" in prompt
        assert "Score: 10.0" in prompt

    def test_caps_low_resonance_at_20(self):
        analyzer = PatternAnalyzer(api_key="test-key")
        prompt = analyzer._build_prompt(
            resonated=[{"content": "good", "engagement_score": 5.0}],
            low_resonance=[{"content": f"bad {i}"} for i in range(30)],
        )
        # Should only include up to 20 low_resonance posts
        assert "bad 19" in prompt
        assert "bad 20" not in prompt


class TestConfidence:
    """Tests for confidence level based on sample size."""

    @patch("synthesis.pattern_analyzer.anthropic.Anthropic")
    def test_low_confidence_with_few_resonated(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = '<json>{"positive_patterns":[],"negative_patterns":[],"key_differences":[],"actionable_rules":["rule1"]}</json>'
        mock_client.messages.create.return_value = mock_response

        analyzer = PatternAnalyzer(api_key="test-key")
        result = analyzer.analyze(
            resonated=[{"content": f"post {i}"} for i in range(5)],
            low_resonance=[{"content": "bad"}],
        )
        assert result.confidence == "low"

    @patch("synthesis.pattern_analyzer.anthropic.Anthropic")
    def test_medium_confidence(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = '<json>{"positive_patterns":[],"negative_patterns":[],"key_differences":[],"actionable_rules":["rule1"]}</json>'
        mock_client.messages.create.return_value = mock_response

        analyzer = PatternAnalyzer(api_key="test-key")
        result = analyzer.analyze(
            resonated=[{"content": f"post {i}"} for i in range(15)],
            low_resonance=[{"content": "bad"}],
        )
        assert result.confidence == "medium"

    @patch("synthesis.pattern_analyzer.anthropic.Anthropic")
    def test_high_confidence(self, MockAnthropic):
        mock_client = MagicMock()
        MockAnthropic.return_value = mock_client

        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = '<json>{"positive_patterns":[],"negative_patterns":[],"key_differences":[],"actionable_rules":["rule1"]}</json>'
        mock_client.messages.create.return_value = mock_response

        analyzer = PatternAnalyzer(api_key="test-key")
        result = analyzer.analyze(
            resonated=[{"content": f"post {i}"} for i in range(30)],
            low_resonance=[{"content": "bad"}],
        )
        assert result.confidence == "high"

    def test_prompt_uses_soft_language_for_small_sample(self):
        analyzer = PatternAnalyzer(api_key="test-key")
        prompt = analyzer._build_prompt(
            resonated=[{"content": "post", "engagement_score": 5.0}] * 5,
            low_resonance=[{"content": "bad"}],
        )
        assert "Consider" in prompt
        assert "sample size is small" in prompt
        assert 'Avoid "Always..."' in prompt

    def test_prompt_uses_imperative_for_large_sample(self):
        analyzer = PatternAnalyzer(api_key="test-key")
        prompt = analyzer._build_prompt(
            resonated=[{"content": "post", "engagement_score": 5.0}] * 15,
            low_resonance=[{"content": "bad"}],
        )
        assert "imperative" in prompt.lower()


class TestParseResponse:
    """Tests for _parse_response."""

    def test_parses_json_in_tags(self):
        analyzer = PatternAnalyzer(api_key="test-key")
        response = """Here's my analysis:
<json>
{
    "positive_patterns": ["p1", "p2"],
    "negative_patterns": ["n1"],
    "key_differences": ["d1"],
    "actionable_rules": ["r1", "r2", "r3"]
}
</json>
Some trailing text."""
        result = analyzer._parse_response(response)
        assert result.positive_patterns == ["p1", "p2"]
        assert result.actionable_rules == ["r1", "r2", "r3"]
        assert result.analyzed_at is not None

    def test_fallback_to_raw_json(self):
        analyzer = PatternAnalyzer(api_key="test-key")
        response = '{"positive_patterns": ["x"], "negative_patterns": [], "key_differences": [], "actionable_rules": ["y"]}'
        result = analyzer._parse_response(response)
        assert result.positive_patterns == ["x"]
        assert result.actionable_rules == ["y"]

    def test_graceful_on_garbage(self):
        analyzer = PatternAnalyzer(api_key="test-key")
        result = analyzer._parse_response("completely unparseable garbage")
        assert result.positive_patterns == []
        assert result.actionable_rules == []


class TestPipelinePatternContext:
    """Tests for pipeline._build_pattern_context()."""

    def test_empty_when_no_meta(self, db):
        from synthesis.pipeline import SynthesisPipeline
        pipeline = SynthesisPipeline.__new__(SynthesisPipeline)
        pipeline.db = db
        assert pipeline._build_pattern_context() == ""

    def test_formats_actionable_rules_low_confidence(self, db):
        from synthesis.pipeline import SynthesisPipeline
        pipeline = SynthesisPipeline.__new__(SynthesisPipeline)
        pipeline.db = db

        db.set_meta("pattern_analysis", json.dumps({
            "actionable_rules": [
                "Open with what surprised you",
                "Show the process, not just the outcome",
            ],
            "positive_patterns": [],
            "negative_patterns": [],
            "confidence": "low",
            "resonated_count": 5,
        }))

        result = pipeline._build_pattern_context()
        assert "ENGAGEMENT PATTERNS" in result
        assert "limited data" in result
        assert "suggestions, not hard rules" in result
        assert "Open with what surprised you" in result
        assert "Show the process" in result

    def test_formats_actionable_rules_high_confidence(self, db):
        from synthesis.pipeline import SynthesisPipeline
        pipeline = SynthesisPipeline.__new__(SynthesisPipeline)
        pipeline.db = db

        db.set_meta("pattern_analysis", json.dumps({
            "actionable_rules": ["Rule 1"],
            "confidence": "high",
        }))

        result = pipeline._build_pattern_context()
        assert "follow these):" in result
        assert "limited data" not in result

    def test_formats_actionable_rules_medium_confidence(self, db):
        from synthesis.pipeline import SynthesisPipeline
        pipeline = SynthesisPipeline.__new__(SynthesisPipeline)
        pipeline.db = db

        db.set_meta("pattern_analysis", json.dumps({
            "actionable_rules": ["Rule 1"],
            "confidence": "medium",
        }))

        result = pipeline._build_pattern_context()
        assert "follow these when relevant" in result

    def test_empty_when_no_rules(self, db):
        from synthesis.pipeline import SynthesisPipeline
        pipeline = SynthesisPipeline.__new__(SynthesisPipeline)
        pipeline.db = db

        db.set_meta("pattern_analysis", json.dumps({
            "actionable_rules": [],
        }))

        assert pipeline._build_pattern_context() == ""

    def test_handles_invalid_json(self, db):
        from synthesis.pipeline import SynthesisPipeline
        pipeline = SynthesisPipeline.__new__(SynthesisPipeline)
        pipeline.db = db

        db.set_meta("pattern_analysis", "not valid json")
        assert pipeline._build_pattern_context() == ""
