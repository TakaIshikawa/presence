"""Tests for session skill tool usage analyzer."""

import pytest

from synthesis.session_skill_tool_usage import (
    analyze_session_skill_tool_usage,
)


class TestAnalyzeSessionSkillToolUsage:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_skill_tool_usage([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_skills"] == 0
        assert result["avg_skill_invocations"] == 0.0
        assert result["avg_skills_per_task"] == 0.0
        assert result["avg_success_rate"] == 0.0
        assert result["avg_verify_usage"] == 0.0
        assert result["avg_commit_usage"] == 0.0
        assert result["avg_cache_usage"] == 0.0
        assert result["avg_other_usage"] == 0.0
        assert result["avg_skill_diversity"] == 0.0
        assert result["avg_redundant_call_rate"] == 0.0
        assert result["avg_appropriate_timing_rate"] == 0.0
        assert result["high_skill_usage_sessions"] == 0
        assert result["low_skill_usage_sessions"] == 0
        assert result["sessions_with_redundant_calls"] == 0
        assert result["high_diversity_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_skill_tool_usage(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_skill_tool_usage("not a list")

    def test_session_without_skills(self):
        """Verify session with zero skill invocations."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_skills"] == 0

    def test_session_with_skills(self):
        """Verify session with skill invocations."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 5,
                "successful_skills": 5,
                "failed_skills": 0,
            }
        ])

        assert result["sessions_with_skills"] == 1
        assert result["avg_skill_invocations"] == 5.0
        assert result["avg_success_rate"] == 100.0

    def test_verify_skill_usage(self):
        """Verify tracking of /verify skill usage."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 8,
                "skill_verify_count": 5,
                "skill_commit_count": 2,
                "skill_cache_count": 1,
            }
        ])

        assert result["avg_verify_usage"] == 5.0
        assert result["avg_commit_usage"] == 2.0
        assert result["avg_cache_usage"] == 1.0

    def test_skill_success_and_failure_rates(self):
        """Verify success vs failure rate calculation."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "successful_skills": 18,
                "failed_skills": 2,
            }
        ])

        # 18 / 20 = 90%
        assert result["avg_success_rate"] == 90.0

    def test_skill_diversity_tracking(self):
        """Verify tracking of unique skills used."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 15,
                "unique_skills_used": 5,
            }
        ])

        assert result["avg_skill_diversity"] == 5.0
        assert result["high_diversity_sessions"] == 1

    def test_low_skill_diversity(self):
        """Verify sessions with low skill diversity."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 10,
                "unique_skills_used": 2,
            }
        ])

        assert result["avg_skill_diversity"] == 2.0
        assert result["high_diversity_sessions"] == 0

    def test_redundant_skill_calls_detection(self):
        """Verify detection of redundant back-to-back skill calls."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 10,
                "redundant_skill_calls": 3,
            }
        ])

        # 3 / 10 = 30%
        assert result["avg_redundant_call_rate"] == 30.0
        assert result["sessions_with_redundant_calls"] == 1

    def test_no_redundant_calls(self):
        """Verify sessions with no redundant skill calls."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 10,
                "redundant_skill_calls": 0,
            }
        ])

        assert result["avg_redundant_call_rate"] == 0.0
        assert result["sessions_with_redundant_calls"] == 0

    def test_appropriate_timing_tracking(self):
        """Verify tracking of appropriate skill timing."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "appropriate_timing_count": 9,
                "inappropriate_timing_count": 1,
            }
        ])

        # 9 / 10 = 90%
        assert result["avg_appropriate_timing_rate"] == 90.0

    def test_high_skill_usage_classification(self):
        """Verify detection of high skill usage sessions."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 15,
            }
        ])

        assert result["avg_skill_invocations"] == 15.0
        assert result["high_skill_usage_sessions"] == 1
        assert result["low_skill_usage_sessions"] == 0

    def test_low_skill_usage_classification(self):
        """Verify detection of low skill usage sessions."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 2,
            }
        ])

        assert result["avg_skill_invocations"] == 2.0
        assert result["high_skill_usage_sessions"] == 0
        assert result["low_skill_usage_sessions"] == 1

    def test_skills_per_task_calculation(self):
        """Verify skills per task ratio calculation."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 12,
                "total_tasks": 4,
            }
        ])

        # 12 / 4 = 3
        assert result["avg_skills_per_task"] == 3.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 10,
                "skill_verify_count": 6,
                "successful_skills": 9,
                "failed_skills": 1,
            },
            {
                "session_id": "session2",
                "total_skill_invocations": 20,
                "skill_verify_count": 12,
                "successful_skills": 18,
                "failed_skills": 2,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_skills"] == 2
        # (10 + 20) / 2 = 15
        assert result["avg_skill_invocations"] == 15.0
        # (6 + 12) / 2 = 9
        assert result["avg_verify_usage"] == 9.0
        # (90% + 90%) / 2 = 90%
        assert result["avg_success_rate"] == 90.0

    def test_boundary_skill_usage_classification(self):
        """Verify boundary cases for skill usage classification."""
        result = analyze_session_skill_tool_usage([
            # Exactly 10 (should not be high)
            {
                "session_id": "s1",
                "total_skill_invocations": 10,
            },
            # Just above 10 (should be high)
            {
                "session_id": "s2",
                "total_skill_invocations": 11,
            },
            # Exactly 3 (should not be low)
            {
                "session_id": "s3",
                "total_skill_invocations": 3,
            },
            # Just below 3 (should be low)
            {
                "session_id": "s4",
                "total_skill_invocations": 2,
            },
        ])

        # >10 means strictly greater
        assert result["high_skill_usage_sessions"] == 1
        # <3 means strictly less
        assert result["low_skill_usage_sessions"] == 1

    def test_boundary_diversity_classification(self):
        """Verify boundary cases for diversity classification."""
        result = analyze_session_skill_tool_usage([
            # Exactly 4 (should not be high)
            {
                "session_id": "s1",
                "unique_skills_used": 4,
            },
            # Just above 4 (should be high)
            {
                "session_id": "s2",
                "unique_skills_used": 5,
            },
        ])

        # >4 means strictly greater
        assert result["high_diversity_sessions"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_skill_tool_usage([
            "not a dict",
            {
                "session_id": "session1",
                "total_skill_invocations": 5,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": True,
                "successful_skills": False,
            }
        ])

        assert result["sessions_with_skills"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 10,
                # Missing most fields
            }
        ])

        assert result["sessions_with_skills"] == 1
        assert result["avg_skill_invocations"] == 10.0
        # Missing fields result in 0.0 averages
        assert result["avg_success_rate"] == 0.0

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_skill_invocations": 25,
                "successful_skills": 23,
                "failed_skills": 2,
                "skill_verify_count": 10,
                "skill_commit_count": 8,
                "skill_cache_count": 5,
                "skill_other_count": 2,
                "unique_skills_used": 6,
                "redundant_skill_calls": 2,
                "appropriate_timing_count": 22,
                "inappropriate_timing_count": 3,
                "total_tasks": 5,
                "session_duration_seconds": 3600,
            }
        ])

        assert result["sessions_with_skills"] == 1
        assert result["avg_skill_invocations"] == 25.0
        # 25 / 5 = 5
        assert result["avg_skills_per_task"] == 5.0
        # 23 / 25 = 92%
        assert result["avg_success_rate"] == 92.0
        assert result["avg_verify_usage"] == 10.0
        assert result["avg_commit_usage"] == 8.0
        assert result["avg_cache_usage"] == 5.0
        assert result["avg_other_usage"] == 2.0
        assert result["avg_skill_diversity"] == 6.0
        # 2 / 25 = 8%
        assert result["avg_redundant_call_rate"] == 8.0
        # 22 / 25 = 88%
        assert result["avg_appropriate_timing_rate"] == 88.0
        assert result["high_skill_usage_sessions"] == 1
        assert result["sessions_with_redundant_calls"] == 1
        assert result["high_diversity_sessions"] == 1

    def test_mixed_session_quality(self):
        """Verify mixed session quality across multiple sessions."""
        result = analyze_session_skill_tool_usage([
            # High usage, high diversity
            {
                "session_id": "s1",
                "total_skill_invocations": 15,
                "unique_skills_used": 6,
            },
            # Medium usage
            {
                "session_id": "s2",
                "total_skill_invocations": 5,
                "unique_skills_used": 3,
            },
            # Low usage, low diversity
            {
                "session_id": "s3",
                "total_skill_invocations": 1,
                "unique_skills_used": 1,
            },
        ])

        assert result["total_sessions"] == 3
        assert result["sessions_with_skills"] == 3
        # (15 + 5 + 1) / 3 = 7
        assert result["avg_skill_invocations"] == 7.0
        # (6 + 3 + 1) / 3 = 3.33
        assert 3.0 <= result["avg_skill_diversity"] <= 4.0
        assert result["high_skill_usage_sessions"] == 1
        assert result["low_skill_usage_sessions"] == 1
        assert result["high_diversity_sessions"] == 1

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 10.5,
                "successful_skills": 9.5,
                "failed_skills": 1.0,
            }
        ])

        assert result["avg_skill_invocations"] == 10.5
        # 9.5 / 10.5 = 90.48%
        assert 90.0 <= result["avg_success_rate"] <= 91.0

    def test_zero_tasks_no_division_error(self):
        """Verify zero total tasks doesn't cause division errors."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 10,
                "total_tasks": 0,
            }
        ])

        # Should not crash, skills per task should not be calculated
        assert result["avg_skills_per_task"] == 0.0

    def test_all_skills_fail(self):
        """Verify sessions where all skills fail."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "successful_skills": 0,
                "failed_skills": 10,
            }
        ])

        assert result["avg_success_rate"] == 0.0

    def test_all_skills_succeed(self):
        """Verify sessions where all skills succeed."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "successful_skills": 10,
                "failed_skills": 0,
            }
        ])

        assert result["avg_success_rate"] == 100.0

    def test_only_verify_skills(self):
        """Verify session using only /verify skill."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "total_skill_invocations": 8,
                "skill_verify_count": 8,
                "skill_commit_count": 0,
                "skill_cache_count": 0,
                "skill_other_count": 0,
            }
        ])

        assert result["avg_verify_usage"] == 8.0
        assert result["avg_commit_usage"] == 0.0
        assert result["avg_cache_usage"] == 0.0
        assert result["avg_other_usage"] == 0.0

    def test_balanced_skill_distribution(self):
        """Verify session with balanced skill usage."""
        result = analyze_session_skill_tool_usage([
            {
                "session_id": "session1",
                "skill_verify_count": 5,
                "skill_commit_count": 5,
                "skill_cache_count": 5,
                "skill_other_count": 5,
            }
        ])

        assert result["avg_verify_usage"] == 5.0
        assert result["avg_commit_usage"] == 5.0
        assert result["avg_cache_usage"] == 5.0
        assert result["avg_other_usage"] == 5.0
