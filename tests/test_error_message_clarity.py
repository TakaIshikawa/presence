"""Tests for error message clarity analysis."""

import pytest

from engagement.error_message_clarity import (
    ErrorMessage,
    ClarityMetrics,
    ActionabilityDistribution,
    ContextScores,
    EffectivenessCorrelation,
    analyze_error_message_clarity,
    _calculate_clarity_metrics,
    _calculate_actionability_distribution,
    _calculate_context_scores,
    _calculate_effectiveness_correlation,
    SPECIFICITY_SPECIFIC,
    SPECIFICITY_PARTIAL,
    SPECIFICITY_GENERIC,
    ACTIONABILITY_HIGH,
    ACTIONABILITY_MODERATE,
    ACTIONABILITY_LOW,
    CLARITY_EXCELLENT,
    CLARITY_GOOD,
)


class TestErrorMessage:
    """Test ErrorMessage dataclass."""

    def test_create_error_message(self):
        """Verify error message can be created."""
        error = ErrorMessage(
            error_id="err_1",
            message_text="TypeError at line 42: expected str, got int",
            has_line_number=True,
            has_error_value=True,
            has_stack_trace=True,
            has_file_path=True,
            has_resolution_hint=True,
            specificity_level=SPECIFICITY_SPECIFIC,
            actionability_level=ACTIONABILITY_HIGH,
            was_fixed_successfully=True,
        )
        assert error.error_id == "err_1"
        assert error.has_line_number is True
        assert error.was_fixed_successfully is True

    def test_error_message_frozen(self):
        """Verify error message is immutable."""
        error = ErrorMessage(
            "err_1", "text", False, False, False, False, False,
            SPECIFICITY_GENERIC, ACTIONABILITY_LOW, None
        )
        with pytest.raises(AttributeError):
            error.has_line_number = True


class TestCalculateClarityMetrics:
    """Test clarity metrics calculation."""

    def test_empty_errors(self):
        """Verify empty errors returns zero metrics."""
        metrics = _calculate_clarity_metrics([])
        assert metrics.avg_specificity_score == 0.0
        assert metrics.avg_actionability_score == 0.0
        assert metrics.avg_context_score == 0.0
        assert metrics.guidance_presence_rate == 0.0

    def test_highly_specific_error(self):
        """Verify highly specific error scoring."""
        errors = [
            ErrorMessage(
                "err_1", "text",
                has_line_number=True,
                has_error_value=True,
                has_stack_trace=True,
                has_file_path=True,
                has_resolution_hint=True,
                specificity_level=SPECIFICITY_SPECIFIC,
                actionability_level=ACTIONABILITY_HIGH,
                was_fixed_successfully=True,
            )
        ]
        metrics = _calculate_clarity_metrics(errors)
        assert metrics.avg_specificity_score == 100.0
        assert metrics.avg_actionability_score == 100.0
        assert metrics.avg_context_score == 100.0  # All 4 context elements
        assert metrics.guidance_presence_rate == 1.0

    def test_generic_low_actionability_error(self):
        """Verify generic, low actionability error scoring."""
        errors = [
            ErrorMessage(
                "err_1", "text",
                has_line_number=False,
                has_error_value=False,
                has_stack_trace=False,
                has_file_path=False,
                has_resolution_hint=False,
                specificity_level=SPECIFICITY_GENERIC,
                actionability_level=ACTIONABILITY_LOW,
                was_fixed_successfully=False,
            )
        ]
        metrics = _calculate_clarity_metrics(errors)
        assert metrics.avg_specificity_score == 20.0
        assert metrics.avg_actionability_score == 20.0
        assert metrics.avg_context_score == 0.0
        assert metrics.guidance_presence_rate == 0.0

    def test_partial_specificity_moderate_actionability(self):
        """Verify partial specificity and moderate actionability scoring."""
        errors = [
            ErrorMessage(
                "err_1", "text",
                has_line_number=True,
                has_error_value=True,
                has_stack_trace=False,
                has_file_path=False,
                has_resolution_hint=False,
                specificity_level=SPECIFICITY_PARTIAL,
                actionability_level=ACTIONABILITY_MODERATE,
                was_fixed_successfully=None,
            )
        ]
        metrics = _calculate_clarity_metrics(errors)
        assert metrics.avg_specificity_score == 60.0
        assert metrics.avg_actionability_score == 60.0
        assert metrics.avg_context_score == 50.0  # 2/4 context elements

    def test_mixed_error_levels(self):
        """Verify averaging across mixed error levels."""
        errors = [
            ErrorMessage(
                "err_1", "text",
                True, True, True, True, True,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, True
            ),
            ErrorMessage(
                "err_2", "text",
                False, False, False, False, False,
                SPECIFICITY_GENERIC, ACTIONABILITY_LOW, False
            ),
        ]
        metrics = _calculate_clarity_metrics(errors)
        # Specificity: (100 + 20) / 2 = 60
        assert metrics.avg_specificity_score == 60.0
        # Actionability: (100 + 20) / 2 = 60
        assert metrics.avg_actionability_score == 60.0
        # Context: (100 + 0) / 2 = 50
        assert metrics.avg_context_score == 50.0
        # Guidance: 1/2 = 0.5
        assert metrics.guidance_presence_rate == 0.5


class TestCalculateActionabilityDistribution:
    """Test actionability distribution calculation."""

    def test_empty_errors(self):
        """Verify empty errors returns zero distribution."""
        dist = _calculate_actionability_distribution([])
        assert dist.high_count == 0
        assert dist.moderate_count == 0
        assert dist.low_count == 0
        assert dist.total_count == 0

    def test_all_high_actionability(self):
        """Verify all high actionability distribution."""
        errors = [
            ErrorMessage(
                "err_1", "text", False, False, False, False, False,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, None
            ),
            ErrorMessage(
                "err_2", "text", False, False, False, False, False,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, None
            ),
        ]
        dist = _calculate_actionability_distribution(errors)
        assert dist.high_count == 2
        assert dist.moderate_count == 0
        assert dist.low_count == 0
        assert dist.total_count == 2

    def test_mixed_actionability(self):
        """Verify mixed actionability distribution."""
        errors = [
            ErrorMessage(
                "err_1", "text", False, False, False, False, False,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, None
            ),
            ErrorMessage(
                "err_2", "text", False, False, False, False, False,
                SPECIFICITY_PARTIAL, ACTIONABILITY_MODERATE, None
            ),
            ErrorMessage(
                "err_3", "text", False, False, False, False, False,
                SPECIFICITY_GENERIC, ACTIONABILITY_LOW, None
            ),
            ErrorMessage(
                "err_4", "text", False, False, False, False, False,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, None
            ),
        ]
        dist = _calculate_actionability_distribution(errors)
        assert dist.high_count == 2
        assert dist.moderate_count == 1
        assert dist.low_count == 1
        assert dist.total_count == 4


class TestCalculateContextScores:
    """Test context scores calculation."""

    def test_empty_errors(self):
        """Verify empty errors returns zero scores."""
        scores = _calculate_context_scores([])
        assert scores.avg_context_elements == 0.0
        assert scores.complete_context_rate == 0.0
        assert scores.missing_line_numbers == 0
        assert scores.missing_stack_traces == 0

    def test_complete_context(self):
        """Verify complete context (all 4 elements)."""
        errors = [
            ErrorMessage(
                "err_1", "text", True, True, True, True, False,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, None
            )
        ]
        scores = _calculate_context_scores(errors)
        assert scores.avg_context_elements == 4.0
        assert scores.complete_context_rate == 1.0  # All 4 elements
        assert scores.missing_line_numbers == 0
        assert scores.missing_stack_traces == 0

    def test_incomplete_context(self):
        """Verify incomplete context."""
        errors = [
            ErrorMessage(
                "err_1", "text", True, True, False, False, False,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, None
            )
        ]
        scores = _calculate_context_scores(errors)
        assert scores.avg_context_elements == 2.0
        assert scores.complete_context_rate == 0.0  # Need 4 for complete
        assert scores.missing_line_numbers == 0
        assert scores.missing_stack_traces == 1

    def test_missing_critical_elements(self):
        """Verify tracking of missing critical elements."""
        errors = [
            ErrorMessage(
                "err_1", "text", False, True, False, True, False,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, None
            ),
            ErrorMessage(
                "err_2", "text", False, False, False, False, False,
                SPECIFICITY_GENERIC, ACTIONABILITY_LOW, None
            ),
        ]
        scores = _calculate_context_scores(errors)
        assert scores.missing_line_numbers == 2
        assert scores.missing_stack_traces == 2


class TestCalculateEffectivenessCorrelation:
    """Test effectiveness correlation calculation."""

    def test_empty_errors(self):
        """Verify empty errors returns zero correlation."""
        correlation = _calculate_effectiveness_correlation([])
        assert correlation.high_clarity_fix_rate == 0.0
        assert correlation.moderate_clarity_fix_rate == 0.0
        assert correlation.low_clarity_fix_rate == 0.0
        assert correlation.correlation_strength == 0.0

    def test_perfect_positive_correlation(self):
        """Verify perfect positive correlation (high clarity = high fix rate)."""
        errors = [
            # High clarity, fixed
            ErrorMessage(
                "err_1", "text", True, True, True, True, True,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, True
            ),
            ErrorMessage(
                "err_2", "text", True, True, True, True, True,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, True
            ),
            # Moderate clarity, some fixed
            ErrorMessage(
                "err_3", "text", True, True, False, False, False,
                SPECIFICITY_PARTIAL, ACTIONABILITY_MODERATE, True
            ),
            ErrorMessage(
                "err_4", "text", True, True, False, False, False,
                SPECIFICITY_PARTIAL, ACTIONABILITY_MODERATE, False
            ),
            # Low clarity, not fixed
            ErrorMessage(
                "err_5", "text", False, False, False, False, False,
                SPECIFICITY_GENERIC, ACTIONABILITY_LOW, False
            ),
            ErrorMessage(
                "err_6", "text", False, False, False, False, False,
                SPECIFICITY_GENERIC, ACTIONABILITY_LOW, False
            ),
        ]
        correlation = _calculate_effectiveness_correlation(errors)
        assert correlation.high_clarity_fix_rate == 1.0  # 2/2
        assert correlation.moderate_clarity_fix_rate == 0.5  # 1/2
        assert correlation.low_clarity_fix_rate == 0.0  # 0/2
        assert correlation.correlation_strength == 1.0  # Perfect correlation

    def test_no_correlation(self):
        """Verify no correlation detection."""
        errors = [
            # High clarity, not fixed
            ErrorMessage(
                "err_1", "text", True, True, True, True, True,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, False
            ),
            # Low clarity, fixed
            ErrorMessage(
                "err_2", "text", False, False, False, False, False,
                SPECIFICITY_GENERIC, ACTIONABILITY_LOW, True
            ),
        ]
        correlation = _calculate_effectiveness_correlation(errors)
        # Fix rates should be equal or reverse
        assert correlation.correlation_strength <= 0.6

    def test_unresolved_errors_excluded(self):
        """Verify unresolved errors (None) are excluded from fix rate."""
        errors = [
            # High clarity, fixed
            ErrorMessage(
                "err_1", "text", True, True, True, True, True,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, True
            ),
            # High clarity, unresolved
            ErrorMessage(
                "err_2", "text", True, True, True, True, True,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, None
            ),
        ]
        correlation = _calculate_effectiveness_correlation(errors)
        # Should be 1/1 = 1.0 (excluding unresolved)
        assert correlation.high_clarity_fix_rate == 1.0


class TestAnalyzeErrorMessageClarity:
    """Test complete error message clarity analysis."""

    def test_empty_errors(self):
        """Verify analysis of empty errors."""
        result = analyze_error_message_clarity([])
        assert result["clarity_metrics"]["avg_specificity_score"] == 0.0
        assert result["actionability_distribution"]["total_count"] == 0
        assert result["context_scores"]["avg_context_elements"] == 0.0
        assert result["guidance_presence"] == 0.0

    def test_single_excellent_error(self):
        """Verify analysis of single excellent error."""
        errors = [
            ErrorMessage(
                "err_1", "TypeError at line 42: expected str, got int",
                True, True, True, True, True,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, True
            )
        ]
        result = analyze_error_message_clarity(errors)
        assert result["clarity_metrics"]["avg_specificity_score"] == 100.0
        assert result["clarity_metrics"]["avg_actionability_score"] == 100.0
        assert result["actionability_distribution"]["high_count"] == 1
        assert result["context_scores"]["complete_context_rate"] == 1.0
        assert result["guidance_presence"] == 1.0

    def test_mixed_error_quality(self):
        """Verify analysis of mixed error quality."""
        errors = [
            ErrorMessage(
                "err_1", "text", True, True, True, True, True,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, True
            ),
            ErrorMessage(
                "err_2", "text", True, False, False, False, False,
                SPECIFICITY_PARTIAL, ACTIONABILITY_MODERATE, True
            ),
            ErrorMessage(
                "err_3", "text", False, False, False, False, False,
                SPECIFICITY_GENERIC, ACTIONABILITY_LOW, False
            ),
        ]
        result = analyze_error_message_clarity(errors)
        assert result["actionability_distribution"]["high_count"] == 1
        assert result["actionability_distribution"]["moderate_count"] == 1
        assert result["actionability_distribution"]["low_count"] == 1
        assert result["context_scores"]["missing_line_numbers"] == 1

    def test_effectiveness_correlation_in_result(self):
        """Verify effectiveness correlation is included in result."""
        errors = [
            ErrorMessage(
                "err_1", "text", True, True, True, True, True,
                SPECIFICITY_SPECIFIC, ACTIONABILITY_HIGH, True
            ),
            ErrorMessage(
                "err_2", "text", False, False, False, False, False,
                SPECIFICITY_GENERIC, ACTIONABILITY_LOW, False
            ),
        ]
        result = analyze_error_message_clarity(errors)
        assert "effectiveness_correlation" in result
        assert "high_clarity_fix_rate" in result["effectiveness_correlation"]
        assert "correlation_strength" in result["effectiveness_correlation"]

    def test_invalid_errors_not_sequence(self):
        """Verify error on non-sequence input."""
        with pytest.raises(ValueError, match="errors must be a sequence"):
            analyze_error_message_clarity("not a sequence")

    def test_invalid_errors_wrong_type(self):
        """Verify error on wrong element type."""
        with pytest.raises(ValueError, match="must contain ErrorMessage instances"):
            analyze_error_message_clarity([{"error": "err_1"}])

    def test_invalid_empty_error_id(self):
        """Verify error on empty error_id."""
        errors = [
            ErrorMessage(
                "", "text", False, False, False, False, False,
                SPECIFICITY_GENERIC, ACTIONABILITY_LOW, None
            )
        ]
        with pytest.raises(ValueError, match="error_id cannot be empty"):
            analyze_error_message_clarity(errors)

    def test_invalid_specificity_level(self):
        """Verify error on invalid specificity level."""
        errors = [
            ErrorMessage(
                "err_1", "text", False, False, False, False, False,
                "invalid_level", ACTIONABILITY_LOW, None
            )
        ]
        with pytest.raises(ValueError, match="invalid specificity_level"):
            analyze_error_message_clarity(errors)

    def test_invalid_actionability_level(self):
        """Verify error on invalid actionability level."""
        errors = [
            ErrorMessage(
                "err_1", "text", False, False, False, False, False,
                SPECIFICITY_GENERIC, "invalid_level", None
            )
        ]
        with pytest.raises(ValueError, match="invalid actionability_level"):
            analyze_error_message_clarity(errors)

    def test_result_structure(self):
        """Verify result structure contains all required fields."""
        errors = [
            ErrorMessage(
                "err_1", "text", True, True, False, False, True,
                SPECIFICITY_PARTIAL, ACTIONABILITY_MODERATE, True
            )
        ]
        result = analyze_error_message_clarity(errors)

        # Check top-level keys
        assert "clarity_metrics" in result
        assert "actionability_distribution" in result
        assert "context_scores" in result
        assert "guidance_presence" in result
        assert "effectiveness_correlation" in result

        # Check clarity_metrics structure
        metrics = result["clarity_metrics"]
        assert "avg_specificity_score" in metrics
        assert "avg_actionability_score" in metrics
        assert "avg_context_score" in metrics
        assert "guidance_presence_rate" in metrics

        # Check actionability_distribution structure
        dist = result["actionability_distribution"]
        assert "high_count" in dist
        assert "moderate_count" in dist
        assert "low_count" in dist
        assert "total_count" in dist

        # Check context_scores structure
        context = result["context_scores"]
        assert "avg_context_elements" in context
        assert "complete_context_rate" in context
        assert "missing_line_numbers" in context
        assert "missing_stack_traces" in context

        # Check effectiveness_correlation structure
        correlation = result["effectiveness_correlation"]
        assert "high_clarity_fix_rate" in correlation
        assert "moderate_clarity_fix_rate" in correlation
        assert "low_clarity_fix_rate" in correlation
        assert "correlation_strength" in correlation
