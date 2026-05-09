"""Tests for pack error recovery pattern analyzer."""

import pytest

from synthesis.pack_error_recovery_patterns import analyze_pack_error_recovery


class TestAnalyzePackErrorRecovery:
    """Test main analyzer function."""

    def test_empty_errors_returns_perfect_resilience(self):
        """Verify empty error list returns perfect resilience score."""
        result = analyze_pack_error_recovery([])

        assert result["total_errors"] == 0
        assert result["errors_resolved"] == 0
        assert result["errors_unresolved"] == 0
        assert result["recovery_success_ratio"] == 0.0
        assert result["avg_detection_speed"] == 0.0
        assert result["avg_recovery_turns"] == 0.0
        assert result["total_recovery_attempts"] == 0
        assert result["avg_attempts_per_error"] == 0.0
        assert result["cascading_errors"] == 0
        assert result["graceful_degradation_score"] == 0.0
        assert result["resilience_score"] == 1.0  # Perfect resilience with no errors
        assert result["fast_detections"] == 0
        assert result["slow_detections"] == 0
        assert result["recovery_strategies_used"] == 0
        assert result["errors_by_type"] == {}

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_error_recovery(None)
        assert result["total_errors"] == 0
        assert result["resilience_score"] == 1.0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_error_recovery("not a list")

    def test_quick_successful_recovery(self):
        """Verify pack with quick successful error recovery."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "err1",
                "error_type": "build",
                "detected_at_turn": 5,
                "acknowledged_at_turn": 6,
                "resolved_at_turn": 8,
                "recovery_attempts": 2,
                "recovery_strategy": "targeted_read",
                "was_successful": True,
                "turns_in_error_state": 3,
            },
            {
                "error_id": "err2",
                "error_type": "test",
                "detected_at_turn": 10,
                "acknowledged_at_turn": 11,
                "resolved_at_turn": 13,
                "recovery_attempts": 1,
                "recovery_strategy": "verify",
                "was_successful": True,
                "turns_in_error_state": 3,
            },
        ])

        assert result["total_errors"] == 2
        assert result["errors_resolved"] == 2
        assert result["errors_unresolved"] == 0
        assert result["recovery_success_ratio"] == 100.0
        # (6-5 + 11-10) / 2 = 1.0
        assert result["avg_detection_speed"] == 1.0
        # (8-6 + 13-11) / 2 = 2.0
        assert result["avg_recovery_turns"] == 2.0
        assert result["total_recovery_attempts"] == 3
        assert result["avg_attempts_per_error"] == 1.5
        assert result["fast_detections"] == 2
        assert result["slow_detections"] == 0
        assert result["recovery_strategies_used"] == 2
        assert result["errors_by_type"]["build"] == 1
        assert result["errors_by_type"]["test"] == 1
        # High resilience due to 100% success, fast detection, quick recovery
        # (no degradation data provided, so score is 0.8)
        assert result["resilience_score"] == 0.8

    def test_multiple_failed_recovery_attempts(self):
        """Verify pack with multiple failed recovery attempts."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "err1",
                "error_type": "type_error",
                "detected_at_turn": 1,
                "acknowledged_at_turn": 2,
                "resolved_at_turn": None,  # Unresolved
                "recovery_attempts": 5,
                "recovery_strategy": "targeted_read",
                "was_successful": False,
                "turns_in_error_state": 10,
            },
            {
                "error_id": "err2",
                "error_type": "runtime",
                "detected_at_turn": 5,
                "acknowledged_at_turn": 8,
                "resolved_at_turn": None,
                "recovery_attempts": 3,
                "recovery_strategy": "ask",
                "was_successful": False,
                "turns_in_error_state": 8,
            },
        ])

        assert result["total_errors"] == 2
        assert result["errors_resolved"] == 0
        assert result["errors_unresolved"] == 2
        assert result["recovery_success_ratio"] == 0.0
        # (2-1 + 8-5) / 2 = 2.0
        assert result["avg_detection_speed"] == 2.0
        # Uses turns_in_error_state when resolved_at is None
        # (10 + 8) / 2 = 9.0
        assert result["avg_recovery_turns"] == 9.0
        assert result["total_recovery_attempts"] == 8
        assert result["avg_attempts_per_error"] == 4.0
        assert result["recovery_strategies_used"] == 2
        # Low resilience due to 0% success and slow recovery
        assert result["resilience_score"] <= 0.4

    def test_cascading_errors(self):
        """Verify pack with cascading errors."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "err1",
                "error_type": "build",
                "detected_at_turn": 1,
                "acknowledged_at_turn": 2,
                "resolved_at_turn": 5,
                "was_successful": True,
                "is_cascading_error": False,
            },
            {
                "error_id": "err2",
                "error_type": "test",
                "detected_at_turn": 6,
                "acknowledged_at_turn": 7,
                "resolved_at_turn": 9,
                "was_successful": True,
                "is_cascading_error": True,  # Caused by err1
            },
            {
                "error_id": "err3",
                "error_type": "type_error",
                "detected_at_turn": 10,
                "acknowledged_at_turn": 11,
                "resolved_at_turn": 13,
                "was_successful": True,
                "is_cascading_error": True,  # Caused by err1
            },
        ])

        assert result["total_errors"] == 3
        assert result["cascading_errors"] == 2
        assert result["errors_resolved"] == 3

    def test_error_free_pack(self):
        """Verify pack with no errors shows perfect resilience."""
        result = analyze_pack_error_recovery([])

        assert result["total_errors"] == 0
        assert result["resilience_score"] == 1.0

    def test_graceful_degradation_handling(self):
        """Verify graceful degradation score calculation."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "err1",
                "error_type": "test",
                "partial_failure_handled": True,
            },
            {
                "error_id": "err2",
                "error_type": "build",
                "partial_failure_handled": True,
            },
            {
                "error_id": "err3",
                "error_type": "runtime",
                "partial_failure_handled": False,
            },
            {
                "error_id": "err4",
                "error_type": "type_error",
                "partial_failure_handled": True,
            },
        ])

        assert result["total_errors"] == 4
        # 3 out of 4 handled gracefully = 75%
        assert result["graceful_degradation_score"] == 75.0

    def test_fast_vs_slow_detection(self):
        """Verify classification of fast and slow error detection."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "fast1",
                "detected_at_turn": 1,
                "acknowledged_at_turn": 2,  # 1 turn = fast
            },
            {
                "error_id": "fast2",
                "detected_at_turn": 5,
                "acknowledged_at_turn": 7,  # 2 turns = fast
            },
            {
                "error_id": "medium",
                "detected_at_turn": 10,
                "acknowledged_at_turn": 13,  # 3 turns = medium (not counted)
            },
            {
                "error_id": "slow1",
                "detected_at_turn": 15,
                "acknowledged_at_turn": 20,  # 5 turns = slow
            },
            {
                "error_id": "slow2",
                "detected_at_turn": 25,
                "acknowledged_at_turn": 31,  # 6 turns = slow
            },
        ])

        assert result["fast_detections"] == 2
        assert result["slow_detections"] == 2
        # (1 + 2 + 3 + 5 + 6) / 5 = 3.4
        assert result["avg_detection_speed"] == 3.4

    def test_recovery_strategy_diversity(self):
        """Verify tracking of different recovery strategies."""
        result = analyze_pack_error_recovery([
            {"error_id": "e1", "recovery_strategy": "targeted_read"},
            {"error_id": "e2", "recovery_strategy": "verify"},
            {"error_id": "e3", "recovery_strategy": "ask"},
            {"error_id": "e4", "recovery_strategy": "targeted_read"},  # Duplicate
            {"error_id": "e5", "recovery_strategy": "full_file_read"},
        ])

        assert result["recovery_strategies_used"] == 4  # Unique strategies

    def test_error_type_categorization(self):
        """Verify categorization of errors by type."""
        result = analyze_pack_error_recovery([
            {"error_id": "e1", "error_type": "build"},
            {"error_id": "e2", "error_type": "build"},
            {"error_id": "e3", "error_type": "test"},
            {"error_id": "e4", "error_type": "type_error"},
            {"error_id": "e5", "error_type": "runtime"},
            {"error_id": "e6", "error_type": "runtime"},
            {"error_id": "e7", "error_type": "runtime"},
        ])

        assert result["errors_by_type"]["build"] == 2
        assert result["errors_by_type"]["test"] == 1
        assert result["errors_by_type"]["type_error"] == 1
        assert result["errors_by_type"]["runtime"] == 3

    def test_mixed_success_and_failure(self):
        """Verify pack with both successful and failed recoveries."""
        result = analyze_pack_error_recovery([
            {"error_id": "e1", "was_successful": True},
            {"error_id": "e2", "was_successful": True},
            {"error_id": "e3", "was_successful": False},
            {"error_id": "e4", "was_successful": True},
            {"error_id": "e5", "was_successful": False},
        ])

        assert result["errors_resolved"] == 3
        assert result["errors_unresolved"] == 2
        # 3/5 = 60%
        assert result["recovery_success_ratio"] == 60.0

    def test_resilience_score_components(self):
        """Verify resilience score calculation with perfect metrics."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "e1",
                "detected_at_turn": 1,
                "acknowledged_at_turn": 2,  # Fast detection (1 turn)
                "resolved_at_turn": 4,  # Quick recovery (2 turns)
                "was_successful": True,
                "partial_failure_handled": True,
            },
            {
                "error_id": "e2",
                "detected_at_turn": 5,
                "acknowledged_at_turn": 6,  # Fast detection (1 turn)
                "resolved_at_turn": 8,  # Quick recovery (2 turns)
                "was_successful": True,
                "partial_failure_handled": True,
            },
        ])

        # All components perfect: 100% success, fast detection, quick recovery, graceful
        assert result["recovery_success_ratio"] == 100.0
        assert result["avg_detection_speed"] == 1.0
        assert result["avg_recovery_turns"] == 2.0
        assert result["graceful_degradation_score"] == 100.0
        # Should result in very high resilience
        assert result["resilience_score"] >= 0.95

    def test_resilience_score_poor_performance(self):
        """Verify resilience score with poor metrics."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "e1",
                "detected_at_turn": 1,
                "acknowledged_at_turn": 8,  # Slow detection (7 turns)
                "resolved_at_turn": 20,  # Slow recovery (12 turns)
                "was_successful": False,
                "partial_failure_handled": False,
            },
        ])

        # All components poor: 0% success, slow detection, slow recovery, no degradation
        assert result["recovery_success_ratio"] == 0.0
        assert result["avg_detection_speed"] == 7.0
        assert result["avg_recovery_turns"] == 12.0
        assert result["graceful_degradation_score"] == 0.0
        # Should result in low resilience
        assert result["resilience_score"] <= 0.3

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_error_recovery([
            "not a dict",
            {
                "error_id": "e1",
                "was_successful": True,
            },
            None,
        ])

        assert result["total_errors"] == 1

    def test_boolean_values_not_extracted_as_numbers(self):
        """Verify boolean values are not extracted as numbers."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "e1",
                "detected_at_turn": True,
                "acknowledged_at_turn": False,
            },
        ])

        assert result["avg_detection_speed"] == 0.0

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "e1",
                "detected_at_turn": 1.0,
                "acknowledged_at_turn": 3.0,
                "recovery_attempts": 2.0,
            },
        ])

        assert result["avg_detection_speed"] == 2.0
        assert result["total_recovery_attempts"] == 2

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "e1",
                # Most fields missing
            },
        ])

        assert result["total_errors"] == 1
        assert result["errors_resolved"] == 0
        assert result["avg_detection_speed"] == 0.0

    def test_recovery_attempts_tracking(self):
        """Verify recovery attempts tracking and averaging."""
        result = analyze_pack_error_recovery([
            {"error_id": "e1", "recovery_attempts": 1},
            {"error_id": "e2", "recovery_attempts": 3},
            {"error_id": "e3", "recovery_attempts": 5},
            {"error_id": "e4", "recovery_attempts": 2},
        ])

        assert result["total_recovery_attempts"] == 11
        # (1 + 3 + 5 + 2) / 4 = 2.75
        assert result["avg_attempts_per_error"] == 2.75

    def test_empty_string_strategy_not_counted(self):
        """Verify empty string strategies are not counted."""
        result = analyze_pack_error_recovery([
            {"error_id": "e1", "recovery_strategy": ""},
            {"error_id": "e2", "recovery_strategy": "verify"},
            {"error_id": "e3", "recovery_strategy": None},
        ])

        assert result["recovery_strategies_used"] == 1

    def test_partial_failure_no_data(self):
        """Verify graceful degradation when no partial failure data."""
        result = analyze_pack_error_recovery([
            {"error_id": "e1"},
            {"error_id": "e2"},
        ])

        assert result["graceful_degradation_score"] == 0.0

    def test_comprehensive_pack_all_fields(self):
        """Verify comprehensive pack with all fields populated."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "err1",
                "error_type": "build",
                "detected_at_turn": 1,
                "acknowledged_at_turn": 2,
                "resolved_at_turn": 5,
                "recovery_attempts": 3,
                "recovery_strategy": "targeted_read",
                "was_successful": True,
                "turns_in_error_state": 4,
                "is_cascading_error": False,
                "partial_failure_handled": True,
            },
            {
                "error_id": "err2",
                "error_type": "test",
                "detected_at_turn": 6,
                "acknowledged_at_turn": 7,
                "resolved_at_turn": 10,
                "recovery_attempts": 2,
                "recovery_strategy": "verify",
                "was_successful": True,
                "turns_in_error_state": 4,
                "is_cascading_error": False,
                "partial_failure_handled": True,
            },
        ])

        assert result["total_errors"] == 2
        assert result["errors_resolved"] == 2
        assert result["errors_unresolved"] == 0
        assert result["recovery_success_ratio"] == 100.0
        assert result["avg_detection_speed"] == 1.0
        assert result["avg_recovery_turns"] == 3.0
        assert result["total_recovery_attempts"] == 5
        assert result["avg_attempts_per_error"] == 2.5
        assert result["cascading_errors"] == 0
        assert result["graceful_degradation_score"] == 100.0
        assert result["recovery_strategies_used"] == 2
        assert result["errors_by_type"]["build"] == 1
        assert result["errors_by_type"]["test"] == 1

    def test_zero_detection_speed(self):
        """Verify handling of immediate detection (same turn)."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "e1",
                "detected_at_turn": 5,
                "acknowledged_at_turn": 5,  # Same turn = 0 speed
            },
        ])

        assert result["avg_detection_speed"] == 0.0
        assert result["fast_detections"] == 1

    def test_zero_recovery_turns(self):
        """Verify handling of immediate recovery."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "e1",
                "acknowledged_at_turn": 5,
                "resolved_at_turn": 5,  # Same turn = 0 recovery time
            },
        ])

        assert result["avg_recovery_turns"] == 0.0

    def test_resilience_with_moderate_performance(self):
        """Verify resilience score with moderate performance metrics."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "e1",
                "detected_at_turn": 1,
                "acknowledged_at_turn": 4,  # 3 turns (medium)
                "resolved_at_turn": 9,  # 5 turns recovery
                "was_successful": True,
                "partial_failure_handled": True,
            },
            {
                "error_id": "e2",
                "detected_at_turn": 10,
                "acknowledged_at_turn": 12,  # 2 turns (fast)
                "resolved_at_turn": 16,  # 4 turns recovery
                "was_successful": True,
                "partial_failure_handled": False,
            },
        ])

        # 100% success, moderate speed (2.5), moderate recovery (4.5), 50% degradation
        assert result["recovery_success_ratio"] == 100.0
        assert result["avg_detection_speed"] == 2.5
        assert result["avg_recovery_turns"] == 4.5
        assert result["graceful_degradation_score"] == 50.0
        # Should result in good but not excellent resilience
        assert 0.65 <= result["resilience_score"] <= 0.85

    def test_no_resolution_data_uses_error_state_turns(self):
        """Verify fallback to turns_in_error_state when resolution data missing."""
        result = analyze_pack_error_recovery([
            {
                "error_id": "e1",
                "acknowledged_at_turn": None,
                "resolved_at_turn": None,
                "turns_in_error_state": 7,
            },
            {
                "error_id": "e2",
                "acknowledged_at_turn": None,
                "resolved_at_turn": None,
                "turns_in_error_state": 5,
            },
        ])

        # Should use turns_in_error_state
        # (7 + 5) / 2 = 6.0
        assert result["avg_recovery_turns"] == 6.0

    def test_multiple_errors_same_type(self):
        """Verify counting multiple errors of the same type."""
        result = analyze_pack_error_recovery([
            {"error_id": "e1", "error_type": "test"},
            {"error_id": "e2", "error_type": "test"},
            {"error_id": "e3", "error_type": "test"},
            {"error_id": "e4", "error_type": "test"},
        ])

        assert result["errors_by_type"]["test"] == 4

    def test_non_string_error_type_not_tracked(self):
        """Verify non-string error types are not tracked."""
        result = analyze_pack_error_recovery([
            {"error_id": "e1", "error_type": 123},
            {"error_id": "e2", "error_type": None},
            {"error_id": "e3", "error_type": "build"},
        ])

        assert result["errors_by_type"] == {"build": 1}
