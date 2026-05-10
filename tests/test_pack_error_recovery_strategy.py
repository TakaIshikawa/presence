"""Tests for pack error recovery strategy analyzer."""

import pytest

from synthesis.pack_error_recovery_strategy import analyze_pack_error_recovery_strategy


class TestAnalyzePackErrorRecoveryStrategy:
    """Test main analyzer function."""

    def test_empty_records_returns_zero_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_pack_error_recovery_strategy([])
        assert result["total_sessions"] == 0
        assert result["sessions_with_errors"] == 0
        assert result["total_errors"] == 0
        assert result["recovery_effectiveness_score"] == 0.0

    def test_invalid_input_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="must be a list"):
            analyze_pack_error_recovery_strategy("not a list")

    def test_high_effectiveness_recovery(self):
        """Verify high effectiveness recovery yields high score."""
        records = [
            {
                "total_errors": 10,
                "errors_resolved": 9,
                "errors_abandoned": 1,
                "targeted_recovery_count": 7,
                "blind_retry_count": 1,
                "verification_escalation_count": 2,
                "avg_turns_to_resolution": 2.5,
                "unacknowledged_errors": 0,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["sessions_with_errors"] == 1
        assert result["error_resolution_rate"] == 90.0
        assert result["targeted_recovery_rate"] == 70.0
        assert result["blind_retry_rate"] == 10.0
        assert result["recovery_effectiveness_score"] > 0.80

    def test_error_resolution_rate_calculation(self):
        """Verify error resolution rate is calculated correctly."""
        records = [
            {
                "total_errors": 20,
                "errors_resolved": 17,
                "errors_abandoned": 3,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["error_resolution_rate"] == 85.0

    def test_targeted_recovery_detection(self):
        """Verify targeted recovery pattern is detected."""
        records = [
            {
                "total_errors": 10,
                "targeted_recovery_count": 6,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["targeted_recovery_rate"] == 60.0

    def test_blind_retry_anti_pattern_detection(self):
        """Verify blind retry anti-pattern is detected."""
        records = [
            {
                "total_errors": 10,
                "blind_retry_count": 5,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["blind_retry_rate"] == 50.0

    def test_verification_usage_rate(self):
        """Verify verification escalation tracking."""
        records = [
            {
                "total_errors": 10,
                "verification_escalation_count": 2,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["verification_usage_rate"] == 20.0

    def test_full_reread_after_error_anti_pattern(self):
        """Verify full-file reread after error is tracked."""
        records = [
            {
                "total_errors": 10,
                "full_file_reread_after_error": 3,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["full_reread_after_error_rate"] == 30.0

    def test_avg_turns_to_resolution(self):
        """Verify average turns to resolution calculation."""
        records = [
            {"avg_turns_to_resolution": 3.0},
            {"avg_turns_to_resolution": 5.0},
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["avg_turns_to_resolution"] == 4.0

    def test_error_source_categorization(self):
        """Verify error sources are categorized correctly."""
        records = [
            {
                "test_errors": 5,
                "build_errors": 3,
                "type_errors": 2,
                "runtime_errors": 1,
                "tool_validation_errors": 2,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["test_error_count"] == 5
        assert result["build_error_count"] == 3
        assert result["type_error_count"] == 2
        assert result["runtime_error_count"] == 1
        assert result["tool_validation_error_count"] == 2

    def test_repeated_failure_detection(self):
        """Verify repeated failures are tracked."""
        records = [
            {
                "total_errors": 10,
                "repeated_failures_count": 3,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["repeated_failure_rate"] == 30.0

    def test_unacknowledged_errors_tracking(self):
        """Verify unacknowledged errors are tracked."""
        records = [
            {
                "total_errors": 10,
                "unacknowledged_errors": 2,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["unacknowledged_error_rate"] == 20.0

    def test_multiple_sessions_aggregation(self):
        """Verify metrics aggregate across sessions."""
        records = [
            {
                "total_errors": 5,
                "errors_resolved": 4,
                "targeted_recovery_count": 3,
            },
            {
                "total_errors": 8,
                "errors_resolved": 7,
                "targeted_recovery_count": 5,
            },
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["total_sessions"] == 2
        assert result["sessions_with_errors"] == 2
        assert result["total_errors"] == 13
        # (4+7)/13 = 11/13 = 84.62%
        assert result["error_resolution_rate"] == 84.62
        # (3+5)/13 = 8/13 = 61.54%
        assert result["targeted_recovery_rate"] == 61.54

    def test_high_effectiveness_sessions_count(self):
        """Verify high effectiveness sessions are counted correctly."""
        records = [
            {
                "total_errors": 10,
                "errors_resolved": 9,
                "targeted_recovery_count": 7,
                "blind_retry_count": 1,
                "avg_turns_to_resolution": 2.5,
                "unacknowledged_errors": 0,
            },
            {
                "total_errors": 8,
                "errors_resolved": 7,
                "targeted_recovery_count": 6,
                "blind_retry_count": 1,
                "avg_turns_to_resolution": 3.0,
                "unacknowledged_errors": 0,
            },
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["high_effectiveness_sessions"] == 2

    def test_low_effectiveness_sessions_count(self):
        """Verify low effectiveness sessions are counted correctly."""
        records = [
            {
                "total_errors": 10,
                "errors_resolved": 3,
                "targeted_recovery_count": 1,
                "blind_retry_count": 6,
                "avg_turns_to_resolution": 12.0,
                "unacknowledged_errors": 4,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["low_effectiveness_sessions"] == 1

    def test_no_errors_session_perfect_score(self):
        """Verify session with no errors gets perfect score."""
        records = [
            {
                "total_errors": 0,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["sessions_with_errors"] == 0

    def test_optimal_verification_usage(self):
        """Verify optimal verification usage range (10-30%)."""
        records = [
            {
                "total_errors": 10,
                "verification_escalation_count": 2,  # 20%
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["verification_usage_rate"] == 20.0
        # Should contribute to good score

    def test_excessive_verification_usage_penalty(self):
        """Verify excessive verification usage is penalized."""
        records_optimal = [
            {
                "total_errors": 10,
                "errors_resolved": 9,
                "targeted_recovery_count": 7,
                "blind_retry_count": 1,
                "verification_escalation_count": 2,  # 20% optimal
                "avg_turns_to_resolution": 3.0,
                "unacknowledged_errors": 0,
            }
        ]
        records_excessive = [
            {
                "total_errors": 10,
                "errors_resolved": 9,
                "targeted_recovery_count": 7,
                "blind_retry_count": 1,
                "verification_escalation_count": 6,  # 60% excessive
                "avg_turns_to_resolution": 3.0,
                "unacknowledged_errors": 0,
            }
        ]
        result_optimal = analyze_pack_error_recovery_strategy(records_optimal)
        result_excessive = analyze_pack_error_recovery_strategy(records_excessive)
        # Optimal should score higher due to appropriate verification usage
        assert result_optimal["recovery_effectiveness_score"] > result_excessive["recovery_effectiveness_score"]

    def test_mixed_effectiveness_pack(self):
        """Verify pack with mixed effectiveness levels."""
        records = [
            {
                "total_errors": 5,
                "errors_resolved": 5,
                "targeted_recovery_count": 4,
                "blind_retry_count": 0,
                "avg_turns_to_resolution": 2.0,
                "unacknowledged_errors": 0,
            },
            {
                "total_errors": 10,
                "errors_resolved": 4,
                "targeted_recovery_count": 2,
                "blind_retry_count": 6,
                "avg_turns_to_resolution": 10.0,
                "unacknowledged_errors": 3,
            },
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["total_sessions"] == 2
        assert result["high_effectiveness_sessions"] == 1
        assert result["low_effectiveness_sessions"] == 1
        # Mixed effectiveness should yield moderate score
        assert 0.4 < result["recovery_effectiveness_score"] < 0.7

    def test_none_values_handled_gracefully(self):
        """Verify None values are handled without errors."""
        records = [
            {
                "total_errors": 5,
                "errors_resolved": None,
                "targeted_recovery_count": None,
            }
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["sessions_with_errors"] == 1
        assert result["error_resolution_rate"] == 0.0

    def test_non_mapping_records_skipped(self):
        """Verify non-mapping records are skipped gracefully."""
        records = [
            "invalid",
            {
                "total_errors": 5,
                "errors_resolved": 4,
            },
            123,
        ]
        result = analyze_pack_error_recovery_strategy(records)
        assert result["total_sessions"] == 1
        assert result["sessions_with_errors"] == 1
