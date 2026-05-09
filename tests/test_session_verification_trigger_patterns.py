"""Tests for session verification trigger patterns analyzer."""

import pytest

from synthesis.session_verification_trigger_patterns import analyze_session_verification_trigger_patterns


class TestAnalyzeSessionVerificationTriggerPatterns:
    """Test main analyzer function."""

    def test_empty_verifications_returns_zeroed_metrics(self):
        """Verify empty verification list returns zero metrics."""
        result = analyze_session_verification_trigger_patterns([])

        assert result["total_verifications"] == 0
        assert result["trigger_contexts"] == []
        assert result["verification_tool_distribution"] == []
        assert result["avg_time_between_verifications"] == 0.0
        assert result["post_edit_verifications"] == 0
        assert result["post_error_verifications"] == 0
        assert result["explicit_request_verifications"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_verification_trigger_patterns(None)
        assert result["total_verifications"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_verification_trigger_patterns("not a list")

    def test_single_post_edit_verification(self):
        """Verify single post-edit verification is tracked."""
        result = analyze_session_verification_trigger_patterns([
            {
                "turn_index": 5,
                "trigger_type": "post_edit",
                "tool_used": "pytest",
                "success": True,
            }
        ])

        assert result["total_verifications"] == 1
        assert result["post_edit_verifications"] == 1
        assert result["trigger_contexts"][0]["trigger_type"] == "post_edit"

    def test_post_error_verification_tracked(self):
        """Verify post-error verification is tracked."""
        result = analyze_session_verification_trigger_patterns([
            {
                "turn_index": 10,
                "trigger_type": "post_error",
                "tool_used": "npm test",
                "success": False,
            }
        ])

        assert result["post_error_verifications"] == 1

    def test_explicit_request_verification_tracked(self):
        """Verify explicit request verification is tracked."""
        result = analyze_session_verification_trigger_patterns([
            {
                "turn_index": 3,
                "trigger_type": "explicit_request",
                "tool_used": "/verify",
                "success": True,
            }
        ])

        assert result["explicit_request_verifications"] == 1

    def test_multiple_trigger_types_distribution(self):
        """Verify distribution of multiple trigger types."""
        result = analyze_session_verification_trigger_patterns([
            {"trigger_type": "post_edit", "tool_used": "pytest"},
            {"trigger_type": "post_edit", "tool_used": "pytest"},
            {"trigger_type": "post_error", "tool_used": "npm test"},
            {"trigger_type": "explicit", "tool_used": "/verify"},
        ])

        assert result["total_verifications"] == 4
        assert result["post_edit_verifications"] == 2
        assert result["post_error_verifications"] == 1
        assert result["explicit_request_verifications"] == 1

    def test_trigger_context_percentages(self):
        """Verify trigger context percentages are calculated."""
        result = analyze_session_verification_trigger_patterns([
            {"trigger_type": "post_edit"},
            {"trigger_type": "post_edit"},
            {"trigger_type": "post_error"},
            {"trigger_type": "explicit"},
        ])

        # 2 out of 4 = 50%
        post_edit_trigger = next(t for t in result["trigger_contexts"] if t["trigger_type"] == "post_edit")
        assert post_edit_trigger["percentage"] == 50.0

    def test_verification_tool_distribution(self):
        """Verify verification tool distribution is tracked."""
        result = analyze_session_verification_trigger_patterns([
            {"tool_used": "pytest"},
            {"tool_used": "pytest"},
            {"tool_used": "npm test"},
            {"tool_used": "/verify"},
        ])

        tools = {t["tool"]: t["count"] for t in result["verification_tool_distribution"]}
        assert tools["pytest"] == 2
        assert tools["npm test"] == 1

    def test_tool_distribution_percentages(self):
        """Verify tool distribution percentages are calculated."""
        result = analyze_session_verification_trigger_patterns([
            {"tool_used": "pytest"},
            {"tool_used": "pytest"},
            {"tool_used": "npm test"},
            {"tool_used": "npm test"},
        ])

        # Both tools 50%
        pytest_tool = next(t for t in result["verification_tool_distribution"] if t["tool"] == "pytest")
        assert pytest_tool["percentage"] == 50.0

    def test_avg_time_between_verifications_from_turns(self):
        """Verify average time between verifications calculated from turn indices."""
        result = analyze_session_verification_trigger_patterns([
            {"turn_index": 5},
            {"turn_index": 15},
            {"turn_index": 20},
        ])

        # Intervals: [10, 5] -> average = 7.5
        assert result["avg_time_between_verifications"] == 7.5

    def test_avg_time_from_explicit_field(self):
        """Verify time tracking from time_since_last_verification field."""
        result = analyze_session_verification_trigger_patterns([
            {"time_since_last_verification": 10},
            {"time_since_last_verification": 20},
        ])

        # Average of [10, 20] = 15
        assert result["avg_time_between_verifications"] == 15.0

    def test_success_rate_by_trigger_type(self):
        """Verify success rates calculated per trigger type."""
        result = analyze_session_verification_trigger_patterns([
            {"trigger_type": "post_edit", "success": True},
            {"trigger_type": "post_edit", "success": True},
            {"trigger_type": "post_error", "success": False},
            {"trigger_type": "post_error", "success": True},
        ])

        success_by_trigger = {s["trigger_type"]: s["success_rate"] for s in result["verification_success_by_trigger_type"]}
        assert success_by_trigger["post_edit"] == 100.0
        assert success_by_trigger["post_error"] == 50.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_verification_trigger_patterns([
            "not a dict",
            {"trigger_type": "post_edit"},
        ])

        assert result["total_verifications"] == 1

    def test_missing_trigger_type_defaults_to_unknown(self):
        """Verify missing trigger type defaults to unknown."""
        result = analyze_session_verification_trigger_patterns([
            {"tool_used": "pytest"},
        ])

        assert result["trigger_contexts"][0]["trigger_type"] == "unknown"

    def test_empty_tool_used_handled(self):
        """Verify empty tool_used values are handled."""
        result = analyze_session_verification_trigger_patterns([
            {"trigger_type": "post_edit", "tool_used": ""},
        ])

        # Should not appear in distribution
        assert len(result["verification_tool_distribution"]) == 0

    def test_turn_index_negative_handled(self):
        """Verify negative turn indices don't cause negative intervals."""
        result = analyze_session_verification_trigger_patterns([
            {"turn_index": 5},
            {"turn_index": 3},
        ])

        # Negative interval should not be tracked
        assert result["avg_time_between_verifications"] == 0.0

    def test_sorted_by_count_descending(self):
        """Verify results are sorted by count descending."""
        result = analyze_session_verification_trigger_patterns([
            {"trigger_type": "A"},
            {"trigger_type": "B"},
            {"trigger_type": "B"},
            {"trigger_type": "C"},
            {"trigger_type": "C"},
            {"trigger_type": "C"},
        ])

        # Should be sorted C(3), B(2), A(1)
        triggers = [t["trigger_type"] for t in result["trigger_contexts"]]
        assert triggers == ["C", "B", "A"]

    def test_success_rate_all_successful(self):
        """Verify 100% success rate when all succeed."""
        result = analyze_session_verification_trigger_patterns([
            {"trigger_type": "post_edit", "success": True},
            {"trigger_type": "post_edit", "success": True},
            {"trigger_type": "post_edit", "success": True},
        ])

        success_rate = result["verification_success_by_trigger_type"][0]["success_rate"]
        assert success_rate == 100.0

    def test_success_rate_all_failed(self):
        """Verify 0% success rate when all fail."""
        result = analyze_session_verification_trigger_patterns([
            {"trigger_type": "post_error", "success": False},
            {"trigger_type": "post_error", "success": False},
        ])

        success_rate = result["verification_success_by_trigger_type"][0]["success_rate"]
        assert success_rate == 0.0

    def test_non_boolean_success_ignored(self):
        """Verify non-boolean success values are ignored."""
        result = analyze_session_verification_trigger_patterns([
            {"trigger_type": "post_edit", "success": "yes"},
            {"trigger_type": "post_edit", "success": True},
        ])

        # Only one boolean success value
        trigger_success = result["verification_success_by_trigger_type"][0]
        assert trigger_success["total"] == 1

    def test_explicit_trigger_alternative_name(self):
        """Verify 'explicit' is counted as explicit_request."""
        result = analyze_session_verification_trigger_patterns([
            {"trigger_type": "explicit"},
        ])

        assert result["explicit_request_verifications"] == 1
