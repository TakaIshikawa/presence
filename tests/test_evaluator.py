"""Tests for the cross-model comparative evaluator (evaluator_v2)."""

from unittest.mock import MagicMock, patch

import pytest

from synthesis.evaluator_v2 import CrossModelEvaluator, ComparisonResult


# --- ComparisonResult.passes_threshold ---


class TestPassesThreshold:
    def test_passes_at_default_threshold(self):
        result = ComparisonResult(
            ranking=[0], best_score=7.0, groundedness=7.0,
            authenticity=7.0, narrative_specificity=7.0, voice=7.0,
            engagement_potential=7.0, best_feedback="", improvement="",
            reject_reason=None, raw_response="",
        )
        assert result.passes_threshold() is True

    def test_fails_below_default_threshold(self):
        result = ComparisonResult(
            ranking=[0], best_score=6.9, groundedness=6.9,
            authenticity=6.9, narrative_specificity=6.9, voice=6.9,
            engagement_potential=6.9, best_feedback="", improvement="",
            reject_reason=None, raw_response="",
        )
        assert result.passes_threshold() is False

    def test_custom_threshold(self):
        result = ComparisonResult(
            ranking=[0], best_score=8.0, groundedness=8.0,
            authenticity=8.0, narrative_specificity=8.0, voice=8.0,
            engagement_potential=8.0, best_feedback="", improvement="",
            reject_reason=None, raw_response="",
        )
        # 0.9 * 10 = 9.0 — score 8.0 should fail
        assert result.passes_threshold(0.9) is False
        # 0.8 * 10 = 8.0 — score 8.0 should pass (>=)
        assert result.passes_threshold(0.8) is True

    def test_boundary_exact_threshold(self):
        result = ComparisonResult(
            ranking=[0], best_score=7.0, groundedness=7.0,
            authenticity=7.0, narrative_specificity=7.0, voice=7.0,
            engagement_potential=7.0, best_feedback="", improvement="",
            reject_reason=None, raw_response="",
        )
        # Exactly at threshold should pass (>=)
        assert result.passes_threshold(0.7) is True

    def test_zero_threshold_always_passes(self):
        result = ComparisonResult(
            ranking=[0], best_score=0.1, groundedness=1.0,
            authenticity=1.0, narrative_specificity=1.0, voice=1.0,
            engagement_potential=1.0, best_feedback="", improvement="",
            reject_reason=None, raw_response="",
        )
        assert result.passes_threshold(0.0) is True

    def test_max_threshold_requires_perfect_score(self):
        result = ComparisonResult(
            ranking=[0], best_score=10.0, groundedness=10.0,
            authenticity=10.0, narrative_specificity=10.0, voice=10.0,
            engagement_potential=10.0, best_feedback="", improvement="",
            reject_reason=None, raw_response="",
        )
        assert result.passes_threshold(1.0) is True


# --- Response parsing ---


class TestParseResponse:
    """Test _parse_response with various structured response formats."""

    @pytest.fixture
    def evaluator(self):
        with patch("synthesis.evaluator_v2.anthropic.Anthropic"):
            return CrossModelEvaluator(api_key="test-key")

    def test_full_well_formed_response(self, evaluator):
        response = (
            "RANKING: A > B > C\n\n"
            "GROUNDEDNESS: 8\n"
            "AUTHENTICITY: 7\n"
            "NARRATIVE_SPECIFICITY: 9\n"
            "VOICE: 6\n"
            "ENGAGEMENT_POTENTIAL: 8\n\n"
            "BEST_FEEDBACK: Candidate A tells a concrete story\n"
            "IMPROVEMENT: Could add a more specific takeaway\n"
            "REJECT_REASON: none"
        )
        result = evaluator._parse_response(response, num_candidates=3)

        assert result.ranking == [0, 1, 2]
        assert result.groundedness == 8.0
        assert result.authenticity == 7.0
        assert result.narrative_specificity == 9.0
        assert result.voice == 6.0
        assert result.engagement_potential == 8.0
        # Weighted: (8*2 + 7 + 9 + 6 + 8) / 6 = 46/6 ≈ 7.667
        assert abs(result.best_score - 46 / 6) < 0.01
        assert "Candidate A tells a concrete story" in result.best_feedback
        assert "more specific takeaway" in result.improvement
        assert result.reject_reason is None

    def test_weighted_score_groundedness_doubled(self, evaluator):
        """Verify groundedness has 2x weight in the scoring formula."""
        response = (
            "RANKING: A > B\n"
            "GROUNDEDNESS: 10\n"
            "AUTHENTICITY: 5\n"
            "NARRATIVE_SPECIFICITY: 5\n"
            "VOICE: 5\n"
            "ENGAGEMENT_POTENTIAL: 5\n"
            "BEST_FEEDBACK: ok\n"
            "IMPROVEMENT: none\n"
            "REJECT_REASON: none"
        )
        result = evaluator._parse_response(response, num_candidates=2)
        # (10*2 + 5 + 5 + 5 + 5) / 6 = 40/6 ≈ 6.667
        expected = (10 * 2 + 5 + 5 + 5 + 5) / 6
        assert abs(result.best_score - expected) < 0.01

    def test_ranking_reversed(self, evaluator):
        response = (
            "RANKING: C > A > B\n"
            "GROUNDEDNESS: 7\nAUTHENTICITY: 7\n"
            "NARRATIVE_SPECIFICITY: 7\nVOICE: 7\n"
            "ENGAGEMENT_POTENTIAL: 7\n"
            "BEST_FEEDBACK: ok\nIMPROVEMENT: none\n"
            "REJECT_REASON: none"
        )
        result = evaluator._parse_response(response, num_candidates=3)
        assert result.ranking == [2, 0, 1]

    def test_missing_scores_default_to_5(self, evaluator):
        response = (
            "RANKING: A > B\n"
            "GROUNDEDNESS: 8\n"
            "BEST_FEEDBACK: ok\n"
            "IMPROVEMENT: none\n"
            "REJECT_REASON: none"
        )
        result = evaluator._parse_response(response, num_candidates=2)
        assert result.groundedness == 8.0
        assert result.authenticity == 5.0  # default
        assert result.narrative_specificity == 5.0
        assert result.voice == 5.0
        assert result.engagement_potential == 5.0

    def test_missing_ranking_fallback(self, evaluator):
        response = (
            "GROUNDEDNESS: 7\nAUTHENTICITY: 7\n"
            "NARRATIVE_SPECIFICITY: 7\nVOICE: 7\n"
            "ENGAGEMENT_POTENTIAL: 7\n"
            "BEST_FEEDBACK: ok\nIMPROVEMENT: none\n"
            "REJECT_REASON: none"
        )
        result = evaluator._parse_response(response, num_candidates=3)
        # Fallback: [0, 1, 2]
        assert result.ranking == [0, 1, 2]

    def test_low_groundedness_auto_reject(self, evaluator):
        """Groundedness <= 3.0 triggers automatic rejection."""
        response = (
            "RANKING: A > B\n"
            "GROUNDEDNESS: 3\n"
            "AUTHENTICITY: 9\nNARRATIVE_SPECIFICITY: 9\n"
            "VOICE: 9\nENGAGEMENT_POTENTIAL: 9\n"
            "BEST_FEEDBACK: ok\n"
            "IMPROVEMENT: none\n"
            "REJECT_REASON: none"
        )
        result = evaluator._parse_response(response, num_candidates=2)
        assert result.reject_reason is not None
        assert "Groundedness" in result.reject_reason

    def test_groundedness_just_above_auto_reject(self, evaluator):
        response = (
            "RANKING: A\n"
            "GROUNDEDNESS: 3.5\n"
            "AUTHENTICITY: 7\nNARRATIVE_SPECIFICITY: 7\n"
            "VOICE: 7\nENGAGEMENT_POTENTIAL: 7\n"
            "BEST_FEEDBACK: ok\n"
            "IMPROVEMENT: none\n"
            "REJECT_REASON: none"
        )
        result = evaluator._parse_response(response, num_candidates=1)
        assert result.reject_reason is None

    def test_explicit_reject_reason_preserved(self, evaluator):
        response = (
            "RANKING: A\n"
            "GROUNDEDNESS: 5\nAUTHENTICITY: 4\n"
            "NARRATIVE_SPECIFICITY: 4\nVOICE: 4\n"
            "ENGAGEMENT_POTENTIAL: 4\n"
            "BEST_FEEDBACK: weak\n"
            "IMPROVEMENT: everything\n"
            "REJECT_REASON: All candidates are too generic"
        )
        result = evaluator._parse_response(response, num_candidates=1)
        assert result.reject_reason == "All candidates are too generic"

    def test_decimal_scores(self, evaluator):
        response = (
            "RANKING: A\n"
            "GROUNDEDNESS: 7.5\nAUTHENTICITY: 6.5\n"
            "NARRATIVE_SPECIFICITY: 8.5\nVOICE: 7.0\n"
            "ENGAGEMENT_POTENTIAL: 8.0\n"
            "BEST_FEEDBACK: ok\nIMPROVEMENT: none\n"
            "REJECT_REASON: none"
        )
        result = evaluator._parse_response(response, num_candidates=1)
        assert result.groundedness == 7.5
        assert result.authenticity == 6.5
        expected = (7.5 * 2 + 6.5 + 8.5 + 7.0 + 8.0) / 6
        assert abs(result.best_score - expected) < 0.01


# --- Negative examples ---


class TestNegativeExamples:
    @pytest.fixture
    def evaluator(self):
        with patch("synthesis.evaluator_v2.anthropic.Anthropic"):
            return CrossModelEvaluator(api_key="test-key")

    def test_no_negatives_uses_static(self, evaluator):
        section = evaluator._build_negative_section(None)
        assert "NEGATIVE EXAMPLES" in section
        # All 3 static examples included
        assert "JSON fallback parsing" in section
        assert "consolidation tracking" in section
        assert "Assignment preservation" in section

    def test_custom_negatives_replace_static(self, evaluator):
        negatives = [
            ("My custom bad post 1", "too_specific"),
            ("My custom bad post 2", "low_resonance"),
            ("My custom bad post 3", "too_specific"),
        ]
        section = evaluator._build_negative_section(negatives)
        assert "My custom bad post 1" in section
        assert "My custom bad post 2" in section
        assert "My custom bad post 3" in section
        # All 3 slots filled by custom — no static examples
        assert "JSON fallback parsing" not in section

    def test_partial_negatives_fills_with_static(self, evaluator):
        negatives = [("One bad post", "too_specific")]
        section = evaluator._build_negative_section(negatives)
        assert "One bad post" in section
        # Remaining 2 slots filled with static
        assert "JSON fallback parsing" in section
        assert "consolidation tracking" in section
        # Third static not needed (1 custom + 2 static = 3)
        assert "Assignment preservation" not in section

    def test_too_specific_annotation(self, evaluator):
        negatives = [("jargon post", "too_specific")]
        section = evaluator._build_negative_section(negatives)
        assert "project-specific jargon" in section

    def test_low_resonance_annotation(self, evaluator):
        negatives = [("bland post", "low_resonance")]
        section = evaluator._build_negative_section(negatives)
        assert "zero audience engagement" in section

    def test_legacy_string_negatives_treated_as_too_specific(self, evaluator):
        negatives = ["plain string negative"]
        section = evaluator._build_negative_section(negatives)
        assert "plain string negative" in section
        assert "project-specific jargon" in section

    def test_max_five_custom_negatives(self, evaluator):
        negatives = [(f"neg {i}", "too_specific") for i in range(8)]
        section = evaluator._build_negative_section(negatives)
        # Only first 5 custom should appear
        assert "neg 0" in section
        assert "neg 4" in section
        assert "neg 5" not in section

    def test_empty_list_uses_static(self, evaluator):
        section = evaluator._build_negative_section([])
        assert "NEGATIVE EXAMPLES" in section
        assert "JSON fallback parsing" in section


# --- End-to-end evaluate() with mocked Claude API ---


class TestEvaluateE2E:
    def test_evaluate_calls_api_and_parses(self):
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text=(
            "RANKING: B > A\n"
            "GROUNDEDNESS: 9\nAUTHENTICITY: 8\n"
            "NARRATIVE_SPECIFICITY: 7\nVOICE: 8\n"
            "ENGAGEMENT_POTENTIAL: 7\n"
            "BEST_FEEDBACK: Candidate B is grounded in real work\n"
            "IMPROVEMENT: Add a punchier ending\n"
            "REJECT_REASON: none"
        ))]

        with patch("synthesis.evaluator_v2.anthropic.Anthropic") as mock_anthropic:
            mock_client = MagicMock()
            mock_client.messages.create.return_value = mock_response
            mock_anthropic.return_value = mock_client

            evaluator = CrossModelEvaluator(api_key="test-key")
            # Patch prompt loading to avoid filesystem dependency
            evaluator._load_prompt = MagicMock(
                return_value="{candidates}\n{source_prompts}\n{source_commits}\n"
                             "{reference_section}\n{negative_examples_section}"
            )

            result = evaluator.evaluate(
                candidates=["Candidate A text", "Candidate B text"],
                source_prompts=["prompt1"],
                source_commits=["commit1"],
            )

        assert result.ranking == [1, 0]  # B > A
        assert result.groundedness == 9.0
        assert result.best_score == (9 * 2 + 8 + 7 + 8 + 7) / 6
        assert result.passes_threshold(0.7)
        assert result.reject_reason is None
        mock_client.messages.create.assert_called_once()

    def test_evaluate_with_reference_and_negative_examples(self):
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text=(
            "RANKING: A\n"
            "GROUNDEDNESS: 7\nAUTHENTICITY: 7\n"
            "NARRATIVE_SPECIFICITY: 7\nVOICE: 7\n"
            "ENGAGEMENT_POTENTIAL: 7\n"
            "BEST_FEEDBACK: ok\nIMPROVEMENT: none\n"
            "REJECT_REASON: none"
        ))]

        with patch("synthesis.evaluator_v2.anthropic.Anthropic") as mock_anthropic:
            mock_client = MagicMock()
            mock_client.messages.create.return_value = mock_response
            mock_anthropic.return_value = mock_client

            evaluator = CrossModelEvaluator(api_key="test-key")
            evaluator._load_prompt = MagicMock(
                return_value="{candidates}\n{source_prompts}\n{source_commits}\n"
                             "{reference_section}\n{negative_examples_section}"
            )

            result = evaluator.evaluate(
                candidates=["Single candidate"],
                source_prompts=["prompt1"],
                source_commits=["commit1"],
                reference_examples=["A great example post"],
                negative_examples=[("A bad post", "too_specific")],
            )

        assert result.ranking == [0]
        # Verify the prompt was filled with reference and negative sections
        call_args = mock_client.messages.create.call_args
        prompt_text = call_args[1]["messages"][0]["content"]
        assert "REFERENCE EXAMPLES" in prompt_text
        assert "NEGATIVE EXAMPLES" in prompt_text
