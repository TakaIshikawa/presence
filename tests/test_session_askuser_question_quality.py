"""Tests for session AskUserQuestion frequency and question quality analyzer."""

import pytest

from synthesis.session_askuser_question_quality import (
    analyze_session_askuser_question_quality,
)


class TestAnalyzeSessionAskuserQuestionQuality:
    """Test main analyzer function."""

    def test_empty_records_returns_zero_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_session_askuser_question_quality([])
        assert result["total_sessions"] == 0
        assert result["total_questions"] == 0
        assert result["questions_per_session"] == 0.0
        assert result["askuser_question_quality_score"] == 0.0

    def test_none_records_returns_zero_metrics(self):
        """Verify None input returns zero metrics."""
        result = analyze_session_askuser_question_quality(None)
        assert result["total_sessions"] == 0
        assert result["total_questions"] == 0
        assert result["askuser_question_quality_score"] == 0.0

    def test_invalid_input_raises_value_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="must be a list"):
            analyze_session_askuser_question_quality("not a list")
        with pytest.raises(ValueError, match="must be a list"):
            analyze_session_askuser_question_quality(42)

    def test_single_session_high_quality(self):
        """Verify high-quality session: early questions, good options, actions taken."""
        records = [
            {
                "session_id": "s1",
                "total_askuser_calls": 5,
                "early_phase_questions": 4,
                "late_phase_questions": 1,
                "total_options_provided": 15,  # avg 3 options
                "multiselect_questions": 1,
                "questions_with_followup_action": 5,
                "redundant_questions": 0,
                "session_total_tool_calls": 30,
            }
        ]
        result = analyze_session_askuser_question_quality(records)
        assert result["total_sessions"] == 1
        assert result["total_questions"] == 5
        assert result["early_phase_question_rate"] == 80.0
        assert result["question_to_action_ratio"] == 100.0
        assert result["redundant_question_rate"] == 0.0
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 0
        assert result["askuser_question_quality_score"] >= 0.8

    def test_single_session_low_quality(self):
        """Verify low-quality session: late questions, no options, redundant."""
        records = [
            {
                "session_id": "s2",
                "total_askuser_calls": 6,
                "early_phase_questions": 0,
                "late_phase_questions": 6,
                "total_options_provided": 0,  # no options
                "multiselect_questions": 5,
                "questions_with_followup_action": 1,
                "redundant_questions": 4,
                "session_total_tool_calls": 50,
            }
        ]
        result = analyze_session_askuser_question_quality(records)
        assert result["total_sessions"] == 1
        assert result["early_phase_question_rate"] == 0.0
        assert result["late_phase_question_rate"] == 100.0
        assert result["redundant_question_rate"] > 50.0
        assert result["low_quality_sessions"] == 1
        assert result["high_quality_sessions"] == 0
        assert result["askuser_question_quality_score"] < 0.4

    def test_multiple_sessions_mixed_quality(self):
        """Verify mixed sessions produce blended metrics."""
        records = [
            {
                "session_id": "high",
                "total_askuser_calls": 4,
                "early_phase_questions": 3,
                "late_phase_questions": 1,
                "total_options_provided": 12,
                "multiselect_questions": 0,
                "questions_with_followup_action": 4,
                "redundant_questions": 0,
                "session_total_tool_calls": 20,
            },
            {
                "session_id": "low",
                "total_askuser_calls": 3,
                "early_phase_questions": 0,
                "late_phase_questions": 3,
                "total_options_provided": 0,
                "multiselect_questions": 3,
                "questions_with_followup_action": 0,
                "redundant_questions": 2,
                "session_total_tool_calls": 40,
            },
        ]
        result = analyze_session_askuser_question_quality(records)
        assert result["total_sessions"] == 2
        assert result["total_questions"] == 7
        assert result["high_quality_sessions"] >= 1
        assert result["low_quality_sessions"] >= 1
        # Blended score between high and low
        assert 0.2 < result["askuser_question_quality_score"] < 0.9

    def test_skips_non_mapping_records(self):
        """Verify non-dict entries are skipped."""
        records = [
            "not a dict",
            42,
            None,
            {
                "total_askuser_calls": 2,
                "early_phase_questions": 2,
                "late_phase_questions": 0,
                "total_options_provided": 6,
                "multiselect_questions": 0,
                "questions_with_followup_action": 2,
                "redundant_questions": 0,
                "session_total_tool_calls": 10,
            },
        ]
        result = analyze_session_askuser_question_quality(records)
        assert result["total_sessions"] == 1
        assert result["total_questions"] == 2

    def test_zero_questions_session(self):
        """Verify session with zero AskUserQuestion calls."""
        records = [
            {
                "session_id": "s_no_questions",
                "total_askuser_calls": 0,
                "early_phase_questions": 0,
                "late_phase_questions": 0,
                "total_options_provided": 0,
                "multiselect_questions": 0,
                "questions_with_followup_action": 0,
                "redundant_questions": 0,
                "session_total_tool_calls": 15,
            }
        ]
        result = analyze_session_askuser_question_quality(records)
        assert result["total_sessions"] == 1
        assert result["total_questions"] == 0
        assert result["questions_per_session"] == 0.0
        # Zero questions -> rates should be 0
        assert result["early_phase_question_rate"] == 0.0
        assert result["multiselect_usage_rate"] == 0.0

    def test_result_keys_complete(self):
        """Verify all expected keys are present in result."""
        result = analyze_session_askuser_question_quality([])
        expected_keys = {
            "total_sessions",
            "total_questions",
            "questions_per_session",
            "early_phase_question_rate",
            "late_phase_question_rate",
            "avg_options_per_question",
            "multiselect_usage_rate",
            "question_to_action_ratio",
            "redundant_question_rate",
            "high_quality_sessions",
            "low_quality_sessions",
            "askuser_question_quality_score",
        }
        assert set(result.keys()) == expected_keys

    def test_score_weights_early_and_action_positively(self):
        """Verify score calculation weights early-phase and action-linked questions."""
        # Session with all early, all actioned
        early_action = [
            {
                "total_askuser_calls": 4,
                "early_phase_questions": 4,
                "late_phase_questions": 0,
                "total_options_provided": 12,
                "multiselect_questions": 1,
                "questions_with_followup_action": 4,
                "redundant_questions": 0,
                "session_total_tool_calls": 20,
            }
        ]
        # Session with all late, no actions
        late_no_action = [
            {
                "total_askuser_calls": 4,
                "early_phase_questions": 0,
                "late_phase_questions": 4,
                "total_options_provided": 12,
                "multiselect_questions": 1,
                "questions_with_followup_action": 0,
                "redundant_questions": 0,
                "session_total_tool_calls": 20,
            }
        ]
        result_good = analyze_session_askuser_question_quality(early_action)
        result_bad = analyze_session_askuser_question_quality(late_no_action)
        assert result_good["askuser_question_quality_score"] > result_bad["askuser_question_quality_score"]
