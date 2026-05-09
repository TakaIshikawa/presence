"""Tests for final answer user facing summary analyzer."""

import pytest

from synthesis.final_answer_user_facing_summary import (
    analyze_final_answer_user_facing_summary,
)


class TestAnalyzeFinalAnswerUserFacingSummary:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_final_answer_user_facing_summary([])

        assert result["total_sessions"] == 0
        assert result["summary_presence_rate"] == 0.0
        assert result["avg_summary_length_words"] == 0.0
        assert result["avg_summary_length_chars"] == 0.0
        assert result["evidence_inclusion_rate"] == 0.0
        assert result["actionability_rate"] == 0.0
        assert result["task_alignment_rate"] == 0.0
        assert result["avg_clarity_score"] == 0.0
        assert result["avg_tone_score"] == 0.0
        assert result["appropriate_length_summaries"] == 0
        assert result["too_brief_summaries"] == 0
        assert result["too_verbose_summaries"] == 0
        assert result["high_quality_summaries"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_final_answer_user_facing_summary(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_final_answer_user_facing_summary("not a list")

    def test_session_with_final_summary(self):
        """Verify session with final summary present."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "has_final_summary": True,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["summary_presence_rate"] == 100.0

    def test_session_without_final_summary(self):
        """Verify session without final summary."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "has_final_summary": False,
            }
        ])

        assert result["summary_presence_rate"] == 0.0

    def test_summary_length_tracking(self):
        """Verify tracking of summary length in words and characters."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "summary_length_words": 250,
                "summary_length_chars": 1500,
            }
        ])

        assert result["avg_summary_length_words"] == 250.0
        assert result["avg_summary_length_chars"] == 1500.0
        assert result["appropriate_length_summaries"] == 1

    def test_too_brief_summary(self):
        """Verify detection of too brief summaries."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "summary_length_words": 50,
            }
        ])

        assert result["avg_summary_length_words"] == 50.0
        assert result["too_brief_summaries"] == 1
        assert result["appropriate_length_summaries"] == 0

    def test_too_verbose_summary(self):
        """Verify detection of too verbose summaries."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "summary_length_words": 600,
            }
        ])

        assert result["avg_summary_length_words"] == 600.0
        assert result["too_verbose_summaries"] == 1
        assert result["appropriate_length_summaries"] == 0

    def test_evidence_inclusion(self):
        """Verify tracking of evidence references in summaries."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "has_evidence_references": True,
            }
        ])

        assert result["evidence_inclusion_rate"] == 100.0

    def test_no_evidence_references(self):
        """Verify sessions without evidence references."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "has_evidence_references": False,
            }
        ])

        assert result["evidence_inclusion_rate"] == 0.0

    def test_actionable_next_steps(self):
        """Verify tracking of actionable next steps in summaries."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "has_actionable_next_steps": True,
            }
        ])

        assert result["actionability_rate"] == 100.0

    def test_no_actionable_next_steps(self):
        """Verify sessions without actionable next steps."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "has_actionable_next_steps": False,
            }
        ])

        assert result["actionability_rate"] == 0.0

    def test_task_alignment(self):
        """Verify tracking of summary alignment with task completion."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "summary_matches_completion": True,
            }
        ])

        assert result["task_alignment_rate"] == 100.0

    def test_task_misalignment(self):
        """Verify detection of summary misalignment."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "summary_matches_completion": False,
            }
        ])

        assert result["task_alignment_rate"] == 0.0

    def test_clarity_score_tracking(self):
        """Verify tracking of clarity scores."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "clarity_score": 85.5,
            }
        ])

        assert result["avg_clarity_score"] == 85.5

    def test_tone_score_tracking(self):
        """Verify tracking of tone appropriateness scores."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "tone_appropriateness_score": 90.0,
            }
        ])

        assert result["avg_tone_score"] == 90.0

    def test_high_quality_summary_detection(self):
        """Verify detection of high-quality summaries."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "clarity_score": 85.0,
                "tone_appropriateness_score": 90.0,
            }
        ])

        assert result["high_quality_summaries"] == 1

    def test_low_quality_summary(self):
        """Verify low-quality summaries not counted as high quality."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "clarity_score": 70.0,
                "tone_appropriateness_score": 75.0,
            }
        ])

        assert result["high_quality_summaries"] == 0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "has_final_summary": True,
                "summary_length_words": 200,
                "clarity_score": 80.0,
            },
            {
                "session_id": "session2",
                "has_final_summary": True,
                "summary_length_words": 300,
                "clarity_score": 90.0,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["summary_presence_rate"] == 100.0
        # (200 + 300) / 2 = 250
        assert result["avg_summary_length_words"] == 250.0
        # (80 + 90) / 2 = 85
        assert result["avg_clarity_score"] == 85.0

    def test_boundary_length_classification(self):
        """Verify boundary cases for length classification."""
        result = analyze_final_answer_user_facing_summary([
            # Exactly 100 (should be appropriate)
            {
                "session_id": "s1",
                "summary_length_words": 100,
            },
            # Exactly 500 (should be appropriate)
            {
                "session_id": "s2",
                "summary_length_words": 500,
            },
            # Just below 100 (should be brief)
            {
                "session_id": "s3",
                "summary_length_words": 99,
            },
            # Just above 500 (should be verbose)
            {
                "session_id": "s4",
                "summary_length_words": 501,
            },
        ])

        assert result["appropriate_length_summaries"] == 2
        assert result["too_brief_summaries"] == 1
        assert result["too_verbose_summaries"] == 1

    def test_boundary_quality_classification(self):
        """Verify boundary cases for quality classification."""
        result = analyze_final_answer_user_facing_summary([
            # Exactly 80 clarity, exactly 85 tone (should not be high quality)
            {
                "session_id": "s1",
                "clarity_score": 80.0,
                "tone_appropriateness_score": 85.0,
            },
            # Just above 80 clarity, just above 85 tone (should be high quality)
            {
                "session_id": "s2",
                "clarity_score": 81.0,
                "tone_appropriateness_score": 86.0,
            },
        ])

        # Both conditions must be STRICTLY greater
        assert result["high_quality_summaries"] == 1

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "comprehensive",
                "task_title": "Test Task",
                "has_final_summary": True,
                "summary_length_words": 250,
                "summary_length_chars": 1500,
                "has_evidence_references": True,
                "has_actionable_next_steps": True,
                "task_completed": True,
                "summary_matches_completion": True,
                "clarity_score": 88.0,
                "tone_appropriateness_score": 92.0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["summary_presence_rate"] == 100.0
        assert result["avg_summary_length_words"] == 250.0
        assert result["avg_summary_length_chars"] == 1500.0
        assert result["evidence_inclusion_rate"] == 100.0
        assert result["actionability_rate"] == 100.0
        assert result["task_alignment_rate"] == 100.0
        assert result["avg_clarity_score"] == 88.0
        assert result["avg_tone_score"] == 92.0
        assert result["appropriate_length_summaries"] == 1
        assert result["high_quality_summaries"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_final_answer_user_facing_summary([
            "not a dict",
            {
                "session_id": "session1",
                "has_final_summary": True,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_not_extracted_as_numbers(self):
        """Verify boolean values are not extracted as numbers."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "summary_length_words": True,
                "clarity_score": False,
            }
        ])

        assert result["avg_summary_length_words"] == 0.0
        assert result["avg_clarity_score"] == 0.0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                # Missing most fields
            }
        ])

        assert result["total_sessions"] == 1
        assert result["summary_presence_rate"] == 0.0

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_final_answer_user_facing_summary([
            {
                "session_id": "session1",
                "summary_length_words": 250.5,
                "clarity_score": 85.75,
            }
        ])

        assert result["avg_summary_length_words"] == 250.5
        assert result["avg_clarity_score"] == 85.75
