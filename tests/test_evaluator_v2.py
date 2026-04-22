"""Tests for CrossModelEvaluator response parsing and negative section building."""

from unittest.mock import patch
import pytest

from synthesis.evaluator_v2 import CrossModelEvaluator, ComparisonResult


class TestComparisonResult:
    def test_passes_threshold_at_boundary(self):
        result = ComparisonResult(
            ranking=[0, 1],
            best_score=7.0,  # exactly 0.7 * 10
            groundedness=7.0,
            rawness=7.0,
            narrative_specificity=7.0,
            voice=7.0,
            engagement_potential=7.0,
            best_feedback="Good",
            improvement="None",
            reject_reason=None,
            raw_response="...",
        )
        assert result.passes_threshold(0.7) is True

    def test_passes_threshold_above(self):
        result = ComparisonResult(
            ranking=[0],
            best_score=8.5,
            groundedness=8.0,
            rawness=8.0,
            narrative_specificity=8.0,
            voice=8.0,
            engagement_potential=9.0,
            best_feedback="Excellent",
            improvement="None",
            reject_reason=None,
            raw_response="...",
        )
        assert result.passes_threshold(0.7) is True
        assert result.passes_threshold(0.8) is True

    def test_passes_threshold_below(self):
        result = ComparisonResult(
            ranking=[0],
            best_score=5.0,
            groundedness=5.0,
            rawness=5.0,
            narrative_specificity=5.0,
            voice=5.0,
            engagement_potential=5.0,
            best_feedback="Weak",
            improvement="Add specifics",
            reject_reason=None,
            raw_response="...",
        )
        assert result.passes_threshold(0.7) is False
        assert result.passes_threshold(0.5) is True

    def test_passes_threshold_default_is_0_7(self):
        result = ComparisonResult(
            ranking=[0],
            best_score=6.9,
            groundedness=7.0,
            rawness=7.0,
            narrative_specificity=7.0,
            voice=7.0,
            engagement_potential=7.0,
            best_feedback="Good",
            improvement="None",
            reject_reason=None,
            raw_response="...",
        )
        assert result.passes_threshold() is False


class TestParseCriterionScore:
    @pytest.fixture
    def evaluator(self):
        with patch("synthesis.evaluator_v2.anthropic.Anthropic"):
            return CrossModelEvaluator(api_key="test-key")

    def test_exact_match(self, evaluator):
        response = "ENGAGEMENT_POTENTIAL: 8"
        score = evaluator._parse_criterion_score(response, "ENGAGEMENT_POTENTIAL")
        assert score == 8.0

    def test_decimal_score(self, evaluator):
        response = "GROUNDEDNESS: 7.5"
        score = evaluator._parse_criterion_score(response, "GROUNDEDNESS")
        assert score == 7.5

    def test_missing_criterion_defaults_to_5(self, evaluator):
        response = "ENGAGEMENT_POTENTIAL: 8\nVOICE: 7"
        score = evaluator._parse_criterion_score(response, "GROUNDEDNESS")
        assert score == 5.0

    def test_multiple_criteria(self, evaluator):
        response = """
ENGAGEMENT_POTENTIAL: 8
GROUNDEDNESS: 7
NARRATIVE_SPECIFICITY: 9
RAWNESS: 6
VOICE: 8
"""
        assert evaluator._parse_criterion_score(response, "ENGAGEMENT_POTENTIAL") == 8.0
        assert evaluator._parse_criterion_score(response, "GROUNDEDNESS") == 7.0
        assert evaluator._parse_criterion_score(response, "NARRATIVE_SPECIFICITY") == 9.0
        assert evaluator._parse_criterion_score(response, "RAWNESS") == 6.0
        assert evaluator._parse_criterion_score(response, "VOICE") == 8.0

    def test_whitespace_variations(self, evaluator):
        response = "ENGAGEMENT_POTENTIAL:    8.5"
        score = evaluator._parse_criterion_score(response, "ENGAGEMENT_POTENTIAL")
        assert score == 8.5


class TestParseResponse:
    @pytest.fixture
    def evaluator(self):
        with patch("synthesis.evaluator_v2.anthropic.Anthropic"):
            return CrossModelEvaluator(api_key="test-key")

    def test_full_response_all_fields(self, evaluator):
        response = """
RANKING: B > A > C

ENGAGEMENT_POTENTIAL: 8.0
GROUNDEDNESS: 7.5
NARRATIVE_SPECIFICITY: 8.5
RAWNESS: 7.0
VOICE: 8.0

BEST_FEEDBACK: Strong hook with concrete examples.

IMPROVEMENT: Could add more emotional resonance.

REJECT_REASON: none
"""
        result = evaluator._parse_response(response, num_candidates=3)

        assert result.ranking == [1, 0, 2]  # B, A, C
        assert result.engagement_potential == 8.0
        assert result.groundedness == 7.5
        assert result.narrative_specificity == 8.5
        assert result.rawness == 7.0
        assert result.voice == 8.0
        # Weighted average: (8*2 + 7.5 + 8.5 + 7 + 8) / 6 = 47/6 ≈ 7.833
        assert abs(result.best_score - 7.833) < 0.01
        assert result.best_feedback == "Strong hook with concrete examples."
        assert result.improvement == "Could add more emotional resonance."
        assert result.reject_reason is None
        assert result.raw_response == response

    def test_partial_response_missing_fields(self, evaluator):
        response = """
RANKING: A

ENGAGEMENT_POTENTIAL: 6.0

BEST_FEEDBACK: Decent effort.
"""
        result = evaluator._parse_response(response, num_candidates=2)

        assert result.ranking == [0]
        assert result.engagement_potential == 6.0
        assert result.groundedness == 5.0  # default
        assert result.narrative_specificity == 5.0  # default
        assert result.rawness == 5.0  # default
        assert result.voice == 5.0  # default
        # Weighted average: (6*2 + 5 + 5 + 5 + 5) / 6 = 32/6 ≈ 5.333
        assert abs(result.best_score - 5.333) < 0.01
        assert result.best_feedback == "Decent effort."
        assert result.improvement == ""
        assert result.reject_reason is None

    def test_ranking_format_arrow(self, evaluator):
        response = "RANKING: C > B > A"
        result = evaluator._parse_response(response, num_candidates=3)
        assert result.ranking == [2, 1, 0]

    def test_ranking_format_comma(self, evaluator):
        response = "RANKING: A, B, C"
        result = evaluator._parse_response(response, num_candidates=3)
        assert result.ranking == [0, 1, 2]

    def test_ranking_format_mixed(self, evaluator):
        response = "RANKING: The best is B, then A, then C"
        result = evaluator._parse_response(response, num_candidates=3)
        assert result.ranking == [1, 0, 2]

    def test_fallback_ranking_when_parsing_fails(self, evaluator):
        response = "No ranking provided."
        result = evaluator._parse_response(response, num_candidates=3)
        assert result.ranking == [0, 1, 2]  # fallback

    def test_auto_reject_on_low_groundedness(self, evaluator):
        response = """
RANKING: A

ENGAGEMENT_POTENTIAL: 8.0
GROUNDEDNESS: 3.0
NARRATIVE_SPECIFICITY: 7.0
RAWNESS: 7.0
VOICE: 7.0

BEST_FEEDBACK: Good writing but claims are questionable.

REJECT_REASON: none
"""
        result = evaluator._parse_response(response, num_candidates=1)

        assert result.groundedness == 3.0
        assert result.reject_reason == "Groundedness score too low (3.0/10) — likely contains fabricated claims"

    def test_auto_reject_not_triggered_on_boundary(self, evaluator):
        response = """
RANKING: A

ENGAGEMENT_POTENTIAL: 7.0
GROUNDEDNESS: 3.1
NARRATIVE_SPECIFICITY: 7.0
RAWNESS: 7.0
VOICE: 7.0

BEST_FEEDBACK: Acceptable.

REJECT_REASON: none
"""
        result = evaluator._parse_response(response, num_candidates=1)

        assert result.groundedness == 3.1
        assert result.reject_reason is None

    def test_explicit_reject_reason_preserved(self, evaluator):
        response = """
RANKING: A

ENGAGEMENT_POTENTIAL: 8.0
GROUNDEDNESS: 2.5
NARRATIVE_SPECIFICITY: 7.0
RAWNESS: 7.0
VOICE: 7.0

BEST_FEEDBACK: Good style but factually incorrect.

REJECT_REASON: Contains fabricated statistics.
"""
        result = evaluator._parse_response(response, num_candidates=1)

        # Explicit reject reason takes precedence
        assert result.reject_reason == "Contains fabricated statistics."

    def test_reject_reason_none_becomes_python_none(self, evaluator):
        response = """
RANKING: A
REJECT_REASON: none
"""
        result = evaluator._parse_response(response, num_candidates=1)
        assert result.reject_reason is None

    def test_reject_reason_case_insensitive(self, evaluator):
        response = """
RANKING: A
REJECT_REASON: None
"""
        result = evaluator._parse_response(response, num_candidates=1)
        assert result.reject_reason is None

    def test_multiline_feedback(self, evaluator):
        response = """
RANKING: A

ENGAGEMENT_POTENTIAL: 7.0
GROUNDEDNESS: 7.0
NARRATIVE_SPECIFICITY: 7.0
RAWNESS: 7.0
VOICE: 7.0

BEST_FEEDBACK: Strong opening.
Second line with more detail.

IMPROVEMENT: Add conclusion.

REJECT_REASON: none
"""
        result = evaluator._parse_response(response, num_candidates=1)
        assert "Strong opening." in result.best_feedback
        assert "Second line with more detail." in result.best_feedback

    def test_weighted_average_computation(self, evaluator):
        # Verify ENGAGEMENT_POTENTIAL counts double
        response = """
ENGAGEMENT_POTENTIAL: 10.0
GROUNDEDNESS: 5.0
NARRATIVE_SPECIFICITY: 5.0
RAWNESS: 5.0
VOICE: 5.0
"""
        result = evaluator._parse_response(response, num_candidates=1)
        # (10*2 + 5 + 5 + 5 + 5) / 6 = 40/6 ≈ 6.667
        assert abs(result.best_score - 6.667) < 0.01

    def test_ranking_filters_invalid_letters(self, evaluator):
        # Should ignore letters beyond num_candidates
        response = "RANKING: B > Z > A"
        result = evaluator._parse_response(response, num_candidates=2)
        assert result.ranking == [1, 0]  # Z ignored


class TestBuildNegativeSection:
    @pytest.fixture
    def evaluator(self):
        with patch("synthesis.evaluator_v2.anthropic.Anthropic"):
            return CrossModelEvaluator(api_key="test-key")

    def test_tuple_inputs_with_too_specific(self, evaluator):
        examples = [
            ("This post uses internal jargon.", "too_specific"),
        ]
        result = evaluator._build_negative_section(examples)

        assert "NEGATIVE EXAMPLES" in result
        assert "This post uses internal jargon." in result
        assert "project-specific jargon meaningless to outside readers" in result

    def test_tuple_inputs_with_low_resonance(self, evaluator):
        examples = [
            ("Generic platitude about productivity.", "low_resonance"),
        ]
        result = evaluator._build_negative_section(examples)

        assert "NEGATIVE EXAMPLES" in result
        assert "Generic platitude about productivity." in result
        assert "got zero audience engagement" in result

    def test_plain_string_inputs_default_to_too_specific(self, evaluator):
        examples = [
            "Legacy format without source tag.",
        ]
        result = evaluator._build_negative_section(examples)

        assert "NEGATIVE EXAMPLES" in result
        assert "Legacy format without source tag." in result
        assert "project-specific jargon meaningless to outside readers" in result

    def test_none_input_uses_static_examples(self, evaluator):
        result = evaluator._build_negative_section(None)

        assert "NEGATIVE EXAMPLES" in result
        # Should have 3 static examples
        assert "termination detection with JSON fallback" in result
        assert "consolidation tracking, auto-restart hooks" in result
        assert "Assignment preservation, session boundaries" in result

    def test_empty_input_uses_static_examples(self, evaluator):
        result = evaluator._build_negative_section([])

        assert "NEGATIVE EXAMPLES" in result
        assert "termination detection with JSON fallback" in result

    def test_mix_of_dynamic_and_static_up_to_3_total(self, evaluator):
        examples = [
            ("Dynamic example 1.", "too_specific"),
        ]
        result = evaluator._build_negative_section(examples)

        assert "Dynamic example 1." in result
        # Should have 2 static examples to fill up to 3 total
        assert "termination detection with JSON fallback" in result
        assert "consolidation tracking, auto-restart hooks" in result
        # Third static example should NOT be included
        assert "Assignment preservation" not in result

    def test_more_than_3_dynamic_examples_no_static_fill(self, evaluator):
        examples = [
            ("Example 1", "too_specific"),
            ("Example 2", "low_resonance"),
            ("Example 3", "too_specific"),
        ]
        result = evaluator._build_negative_section(examples)

        assert "Example 1" in result
        assert "Example 2" in result
        assert "Example 3" in result
        # No static examples should be added
        assert "termination detection" not in result

    def test_limits_to_5_dynamic_examples(self, evaluator):
        examples = [
            (f"Example {i}", "too_specific") for i in range(10)
        ]
        result = evaluator._build_negative_section(examples)

        # Should only include first 5
        assert "Example 0" in result
        assert "Example 4" in result
        assert "Example 5" not in result
        # No static examples when 5+ dynamic provided
        assert "termination detection" not in result

    def test_unknown_source_defaults_to_too_specific(self, evaluator):
        examples = [
            ("Unknown source example", "unknown_source_type"),
        ]
        result = evaluator._build_negative_section(examples)

        assert "Unknown source example" in result
        # Should use too_specific annotation as fallback
        assert "project-specific jargon meaningless to outside readers" in result

    def test_mixed_tuple_and_string_inputs(self, evaluator):
        examples = [
            ("Tuple example", "low_resonance"),
            "String example",
        ]
        result = evaluator._build_negative_section(examples)

        assert "Tuple example" in result
        assert "got zero audience engagement" in result
        assert "String example" in result
        assert "project-specific jargon meaningless to outside readers" in result

    def test_formatting_structure(self, evaluator):
        examples = [
            ("Test content", "too_specific"),
        ]
        result = evaluator._build_negative_section(examples)

        # Verify structure
        assert result.startswith("NEGATIVE EXAMPLES — posts that scored well but failed with real audiences.")
        assert "Penalize candidates that follow these patterns:" in result
        assert '- "Test content"' in result
        assert "  Problem:" in result

    def test_empty_result_when_no_examples_and_static_disabled(self, evaluator):
        # This tests the edge case where we might want no examples at all
        # Current implementation always returns static examples for None/[]
        # but tests the empty string path
        evaluator.STATIC_NEGATIVE_EXAMPLES = []
        result = evaluator._build_negative_section([])
        assert result == ""


class TestTopicHistorySection:
    @pytest.fixture
    def evaluator(self):
        with patch("synthesis.evaluator_v2.anthropic.Anthropic"):
            return CrossModelEvaluator(api_key="test-key")

    def test_empty_topic_history_section_is_optional(self, evaluator):
        assert evaluator._build_topic_history_section(None) == ""
        assert evaluator._build_topic_history_section("") == ""

    def test_topic_history_section_strips_context(self, evaluator):
        result = evaluator._build_topic_history_section(
            "\nENGAGEMENT HISTORY BY TOPIC\n"
        )
        assert result == "ENGAGEMENT HISTORY BY TOPIC"

    def test_evaluate_injects_topic_history_context(self):
        response_text = """
RANKING: A
ENGAGEMENT_POTENTIAL: 7
GROUNDEDNESS: 8
NARRATIVE_SPECIFICITY: 7
RAWNESS: 7
VOICE: 7
BEST_FEEDBACK: Strong.
IMPROVEMENT: none
REJECT_REASON: none
"""
        with patch("synthesis.evaluator_v2.anthropic.Anthropic") as MockAnthropic:
            mock_client = MockAnthropic.return_value
            mock_client.messages.create.return_value.content = [
                type("Block", (), {"text": response_text})()
            ]
            evaluator = CrossModelEvaluator(api_key="test-key")

            evaluator.evaluate(
                candidates=["Post A"],
                source_prompts=["prompt"],
                source_commits=["commit"],
                topic_history_context="ENGAGEMENT HISTORY BY TOPIC\n- testing: n=3",
            )

        prompt = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
        assert "ENGAGEMENT HISTORY BY TOPIC" in prompt
        assert "- testing: n=3" in prompt
        assert "calibration only" in prompt
