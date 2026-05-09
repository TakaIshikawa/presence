"""Tests for session AskUserQuestion pattern analyzer."""

import pytest

from synthesis.session_ask_user_question_pattern import (
    analyze_session_ask_user_question_pattern,
)


class TestAnalyzeSessionAskUserQuestionPattern:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_ask_user_question_pattern([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_questions"] == 0
        assert result["avg_questions_per_session"] == 0.0
        assert result["avg_clarification_ratio"] == 0.0
        assert result["avg_preference_ratio"] == 0.0
        assert result["avg_decision_ratio"] == 0.0
        assert result["avg_early_planning_ratio"] == 0.0
        assert result["avg_mid_implementation_ratio"] == 0.0
        assert result["avg_late_validation_ratio"] == 0.0
        assert result["avg_selected_option_rate"] == 0.0
        assert result["avg_custom_text_rate"] == 0.0
        assert result["avg_skipped_rate"] == 0.0
        assert result["avg_answer_utilization_rate"] == 0.0
        assert result["avg_redundant_question_rate"] == 0.0
        assert result["high_consultation_sessions"] == 0
        assert result["low_consultation_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_ask_user_question_pattern(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_ask_user_question_pattern("not a list")

    def test_session_with_no_questions(self):
        """Verify session with zero AskUserQuestion calls handled gracefully."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_questions"] == 0

    def test_high_consultation_high_utilization(self):
        """Verify high consultation with high answer utilization."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "clarification_questions": 4,
                "preference_questions": 3,
                "decision_questions": 3,
                "answers_utilized": 9,
            }
        ])

        assert result["sessions_with_questions"] == 1
        assert result["avg_questions_per_session"] == 10.0
        # 9 / 10 = 90%
        assert result["avg_answer_utilization_rate"] == 90.0
        assert result["high_consultation_sessions"] == 1
        assert result["low_consultation_sessions"] == 0

    def test_low_consultation_poor_utilization(self):
        """Verify low consultation with poor answer utilization."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "answers_utilized": 3,
            }
        ])

        # 3 / 10 = 30%
        assert result["avg_answer_utilization_rate"] == 30.0
        assert result["high_consultation_sessions"] == 0
        assert result["low_consultation_sessions"] == 1

    def test_question_type_distribution(self):
        """Verify question type distribution calculated correctly."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 20,
                "clarification_questions": 10,
                "preference_questions": 6,
                "decision_questions": 4,
            }
        ])

        # 10 / 20 = 50%
        assert result["avg_clarification_ratio"] == 50.0
        # 6 / 20 = 30%
        assert result["avg_preference_ratio"] == 30.0
        # 4 / 20 = 20%
        assert result["avg_decision_ratio"] == 20.0

    def test_question_timing_distribution(self):
        """Verify question timing distribution calculated correctly."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 20,
                "early_planning_questions": 10,
                "mid_implementation_questions": 8,
                "late_validation_questions": 2,
            }
        ])

        # 10 / 20 = 50%
        assert result["avg_early_planning_ratio"] == 50.0
        # 8 / 20 = 40%
        assert result["avg_mid_implementation_ratio"] == 40.0
        # 2 / 20 = 10%
        assert result["avg_late_validation_ratio"] == 10.0

    def test_response_type_rates(self):
        """Verify response type rates calculated correctly."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 20,
                "selected_option_responses": 12,
                "custom_text_responses": 6,
                "skipped_responses": 2,
            }
        ])

        # 12 / 20 = 60%
        assert result["avg_selected_option_rate"] == 60.0
        # 6 / 20 = 30%
        assert result["avg_custom_text_rate"] == 30.0
        # 2 / 20 = 10%
        assert result["avg_skipped_rate"] == 10.0

    def test_redundant_question_detection(self):
        """Verify redundant question rate calculation."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 20,
                "redundant_questions": 3,
            }
        ])

        # 3 / 20 = 15%
        assert result["avg_redundant_question_rate"] == 15.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "clarification_questions": 5,
                "answers_utilized": 9,
            },
            {
                "session_id": "session2",
                "total_ask_user_questions": 20,
                "clarification_questions": 10,
                "answers_utilized": 14,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_questions"] == 2
        # (10 + 20) / 2 = 15
        assert result["avg_questions_per_session"] == 15.0
        # Both 50% clarification
        assert result["avg_clarification_ratio"] == 50.0
        # (90% + 70%) / 2 = 80%
        assert result["avg_answer_utilization_rate"] == 80.0

    def test_balanced_question_pattern(self):
        """Verify balanced question pattern across types and timing."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 15,
                "clarification_questions": 5,
                "preference_questions": 5,
                "decision_questions": 5,
                "early_planning_questions": 6,
                "mid_implementation_questions": 6,
                "late_validation_questions": 3,
                "selected_option_responses": 10,
                "custom_text_responses": 4,
                "skipped_responses": 1,
                "answers_utilized": 12,
                "redundant_questions": 1,
            }
        ])

        assert result["avg_questions_per_session"] == 15.0
        # All types balanced at ~33%
        assert 33.0 <= result["avg_clarification_ratio"] <= 34.0
        assert 33.0 <= result["avg_preference_ratio"] <= 34.0
        assert 33.0 <= result["avg_decision_ratio"] <= 34.0
        # Early planning dominant (40%)
        assert result["avg_early_planning_ratio"] == 40.0
        # Mid implementation (40%)
        assert result["avg_mid_implementation_ratio"] == 40.0
        # Late validation (20%)
        assert result["avg_late_validation_ratio"] == 20.0
        # 12 / 15 = 80% utilization
        assert result["avg_answer_utilization_rate"] == 80.0
        # 1 / 15 = 6.67% redundant
        assert 6.0 <= result["avg_redundant_question_rate"] <= 7.0
        assert result["high_consultation_sessions"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_ask_user_question_pattern([
            "not a dict",
            {
                "session_id": "session1",
                "total_ask_user_questions": 5,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for integer fields."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": True,
                "clarification_questions": False,
            }
        ])

        assert result["sessions_with_questions"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                # Missing most fields
            }
        ])

        assert result["sessions_with_questions"] == 1
        assert result["avg_questions_per_session"] == 10.0
        # Missing fields result in 0.0 averages
        assert result["avg_clarification_ratio"] == 0.0

    def test_boundary_consultation_classification(self):
        """Verify boundary cases for consultation classification."""
        result = analyze_session_ask_user_question_pattern([
            # Exactly 70% (should not be high)
            {
                "session_id": "s1",
                "total_ask_user_questions": 10,
                "answers_utilized": 7,
            },
            # Just above 70% (should be high)
            {
                "session_id": "s2",
                "total_ask_user_questions": 10,
                "answers_utilized": 8,
            },
            # Exactly 40% (should not be low)
            {
                "session_id": "s3",
                "total_ask_user_questions": 10,
                "answers_utilized": 4,
            },
            # Just below 40% (should be low)
            {
                "session_id": "s4",
                "total_ask_user_questions": 10,
                "answers_utilized": 3,
            },
        ])

        # >70% means strictly greater
        assert result["high_consultation_sessions"] == 1
        # <40% means strictly less
        assert result["low_consultation_sessions"] == 1

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_ask_user_questions": 30,
                "clarification_questions": 12,
                "preference_questions": 10,
                "decision_questions": 8,
                "early_planning_questions": 15,
                "mid_implementation_questions": 10,
                "late_validation_questions": 5,
                "selected_option_responses": 20,
                "custom_text_responses": 8,
                "skipped_responses": 2,
                "answers_utilized": 25,
                "redundant_questions": 3,
            }
        ])

        assert result["sessions_with_questions"] == 1
        assert result["avg_questions_per_session"] == 30.0
        # 12 / 30 = 40%
        assert result["avg_clarification_ratio"] == 40.0
        # 10 / 30 = 33.33%
        assert 33.0 <= result["avg_preference_ratio"] <= 34.0
        # 8 / 30 = 26.67%
        assert 26.0 <= result["avg_decision_ratio"] <= 27.0
        # 15 / 30 = 50%
        assert result["avg_early_planning_ratio"] == 50.0
        # 10 / 30 = 33.33%
        assert 33.0 <= result["avg_mid_implementation_ratio"] <= 34.0
        # 5 / 30 = 16.67%
        assert 16.0 <= result["avg_late_validation_ratio"] <= 17.0
        # 20 / 30 = 66.67%
        assert 66.0 <= result["avg_selected_option_rate"] <= 67.0
        # 8 / 30 = 26.67%
        assert 26.0 <= result["avg_custom_text_rate"] <= 27.0
        # 2 / 30 = 6.67%
        assert 6.0 <= result["avg_skipped_rate"] <= 7.0
        # 25 / 30 = 83.33%
        assert 83.0 <= result["avg_answer_utilization_rate"] <= 84.0
        # 3 / 30 = 10%
        assert result["avg_redundant_question_rate"] == 10.0
        assert result["high_consultation_sessions"] == 1

    def test_zero_questions_no_division_error(self):
        """Verify zero questions doesn't cause division errors."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 0,
                "clarification_questions": 0,
                "answers_utilized": 0,
            }
        ])

        assert result["sessions_with_questions"] == 0
        assert result["avg_questions_per_session"] == 0.0

    def test_early_planning_emphasis(self):
        """Verify session with early planning emphasis."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "early_planning_questions": 8,
                "mid_implementation_questions": 2,
                "late_validation_questions": 0,
            }
        ])

        # 8 / 10 = 80%
        assert result["avg_early_planning_ratio"] == 80.0
        # 2 / 10 = 20%
        assert result["avg_mid_implementation_ratio"] == 20.0
        # 0 / 10 = 0%
        assert result["avg_late_validation_ratio"] == 0.0

    def test_high_redundancy_rate(self):
        """Verify detection of high redundancy rate."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "redundant_questions": 5,
            }
        ])

        # 5 / 10 = 50%
        assert result["avg_redundant_question_rate"] == 50.0

    def test_all_skipped_responses(self):
        """Verify handling of all skipped responses."""
        result = analyze_session_ask_user_question_pattern([
            {
                "session_id": "session1",
                "total_ask_user_questions": 5,
                "selected_option_responses": 0,
                "custom_text_responses": 0,
                "skipped_responses": 5,
            }
        ])

        assert result["avg_selected_option_rate"] == 0.0
        assert result["avg_custom_text_rate"] == 0.0
        # 5 / 5 = 100%
        assert result["avg_skipped_rate"] == 100.0

    def test_mixed_consultation_quality(self):
        """Verify mixed session quality classifications."""
        result = analyze_session_ask_user_question_pattern([
            # High consultation
            {
                "session_id": "s1",
                "total_ask_user_questions": 10,
                "answers_utilized": 9,
            },
            # Medium consultation (not classified)
            {
                "session_id": "s2",
                "total_ask_user_questions": 10,
                "answers_utilized": 6,
            },
            # Low consultation
            {
                "session_id": "s3",
                "total_ask_user_questions": 10,
                "answers_utilized": 2,
            },
        ])

        assert result["total_sessions"] == 3
        assert result["sessions_with_questions"] == 3
        # (90% + 60% + 20%) / 3 = 56.67%
        assert 56.0 <= result["avg_answer_utilization_rate"] <= 57.0
        assert result["high_consultation_sessions"] == 1
        assert result["low_consultation_sessions"] == 1
