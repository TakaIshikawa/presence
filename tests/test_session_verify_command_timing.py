"""Tests for session verify command timing analyzer."""

import pytest

from synthesis.session_verify_command_timing import analyze_session_verify_command_timing


class TestAnalyzeSessionVerifyCommandTiming:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_verify_command_timing([])

        assert result["total_tool_calls"] == 0
        assert result["verify_call_count"] == 0
        assert result["edit_call_count"] == 0
        assert result["avg_edits_between_verifies"] == 0.0
        assert result["verify_to_edit_ratio"] == 0.0
        assert result["single_edit_verifies"] == 0
        assert result["multi_edit_verifies"] == 0
        assert result["over_verification_rate"] == 0.0
        assert result["strategic_verification_rate"] == 0.0
        assert result["verify_frequency_per_10_edits"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_verify_command_timing(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_verify_command_timing("not a list")

    def test_single_edit_then_verify(self):
        """Verify single edit followed by verify."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Verify", "turn_index": 1},
        ])

        assert result["verify_call_count"] == 1
        assert result["edit_call_count"] == 1
        assert result["avg_edits_between_verifies"] == 1.0
        assert result["single_edit_verifies"] == 1
        assert result["multi_edit_verifies"] == 0
        # 100% over-verification (verify after single edit)
        assert result["over_verification_rate"] == 100.0

    def test_multiple_edits_then_verify(self):
        """Verify multiple edits followed by verify."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Verify", "turn_index": 3},
        ])

        assert result["verify_call_count"] == 1
        assert result["edit_call_count"] == 3
        assert result["avg_edits_between_verifies"] == 3.0
        assert result["single_edit_verifies"] == 0
        assert result["multi_edit_verifies"] == 1
        # 100% strategic verification (verify after multiple edits)
        assert result["strategic_verification_rate"] == 100.0

    def test_verify_to_edit_ratio_calculation(self):
        """Verify verify-to-edit ratio calculation."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Verify", "turn_index": 3},
        ])

        # 1 verify / (1 verify + 3 edit) = 25%
        assert result["verify_to_edit_ratio"] == 25.0

    def test_verify_frequency_per_10_edits(self):
        """Verify frequency per 10 edits calculation."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Edit", "turn_index": 3},
            {"tool_name": "Edit", "turn_index": 4},
            {"tool_name": "Verify", "turn_index": 5},
        ])

        # 1 verify / 5 edits * 10 = 2.0 verifies per 10 edits
        assert result["verify_frequency_per_10_edits"] == 2.0

    def test_mixed_verification_patterns(self):
        """Verify mixed single-edit and multi-edit verification."""
        result = analyze_session_verify_command_timing([
            # Single edit verify
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Verify", "turn_index": 1},
            # Multi edit verify
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Edit", "turn_index": 3},
            {"tool_name": "Edit", "turn_index": 4},
            {"tool_name": "Verify", "turn_index": 5},
            # Another single edit verify
            {"tool_name": "Edit", "turn_index": 6},
            {"tool_name": "Verify", "turn_index": 7},
        ])

        assert result["verify_call_count"] == 3
        assert result["edit_call_count"] == 5
        assert result["single_edit_verifies"] == 2
        assert result["multi_edit_verifies"] == 1
        # 2/3 = 66.67% over-verification
        assert result["over_verification_rate"] == 66.67
        # 1/3 = 33.33% strategic verification
        assert result["strategic_verification_rate"] == 33.33

    def test_average_edits_between_verifies(self):
        """Verify average edits between verifies calculation."""
        result = analyze_session_verify_command_timing([
            # 2 edits
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Verify", "turn_index": 2},
            # 4 edits
            {"tool_name": "Edit", "turn_index": 3},
            {"tool_name": "Edit", "turn_index": 4},
            {"tool_name": "Edit", "turn_index": 5},
            {"tool_name": "Edit", "turn_index": 6},
            {"tool_name": "Verify", "turn_index": 7},
            # 1 edit
            {"tool_name": "Edit", "turn_index": 8},
            {"tool_name": "Verify", "turn_index": 9},
        ])

        # (2 + 4 + 1) / 3 = 2.33
        assert result["avg_edits_between_verifies"] == 2.33

    def test_verify_without_prior_edit(self):
        """Verify verify without prior edit is handled."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Verify", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Verify", "turn_index": 2},
        ])

        assert result["verify_call_count"] == 2
        assert result["edit_call_count"] == 1
        # Only the second verify has an edit before it
        assert result["single_edit_verifies"] == 1

    def test_edits_without_verify(self):
        """Verify edits without subsequent verify."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
        ])

        assert result["verify_call_count"] == 0
        assert result["edit_call_count"] == 3
        # No verifies, so no edits between verifies
        assert result["avg_edits_between_verifies"] == 0.0

    def test_mixed_tool_calls(self):
        """Verify mixed tool calls with other tools."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Read", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Write", "turn_index": 2},
            {"tool_name": "Edit", "turn_index": 3},
            {"tool_name": "Bash", "command": "ls", "turn_index": 4},
            {"tool_name": "Verify", "turn_index": 5},
        ])

        assert result["total_tool_calls"] == 6
        assert result["verify_call_count"] == 1
        assert result["edit_call_count"] == 2

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "EDIT", "turn_index": 0},
            {"tool_name": "edit", "turn_index": 1},
            {"tool_name": "VERIFY", "turn_index": 2},
            {"tool_name": "verify", "turn_index": 3},
        ])

        assert result["verify_call_count"] == 2
        assert result["edit_call_count"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_verify_command_timing([
            "not a dict",
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Verify", "turn_index": 1},
        ])

        assert result["total_tool_calls"] == 2

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_verify_command_timing([
            {"turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Verify", "turn_index": 2},
        ])

        assert result["total_tool_calls"] == 2

    def test_zero_denominator_in_ratio(self):
        """Verify zero denominator in ratio calculation."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Read", "turn_index": 0},
        ])

        # No verify or edit calls
        assert result["verify_to_edit_ratio"] == 0.0

    def test_optimal_pattern_strategic_verification(self):
        """Verify optimal pattern of strategic verification."""
        result = analyze_session_verify_command_timing([
            # Complex multi-file change batch 1
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Edit", "turn_index": 3},
            {"tool_name": "Edit", "turn_index": 4},
            {"tool_name": "Verify", "turn_index": 5},
            # Complex multi-file change batch 2
            {"tool_name": "Edit", "turn_index": 6},
            {"tool_name": "Edit", "turn_index": 7},
            {"tool_name": "Edit", "turn_index": 8},
            {"tool_name": "Verify", "turn_index": 9},
        ])

        assert result["verify_call_count"] == 2
        assert result["edit_call_count"] == 8
        # All verifies after multiple edits = 100% strategic
        assert result["strategic_verification_rate"] == 100.0
        assert result["over_verification_rate"] == 0.0
        # Low verify frequency = efficient
        assert result["verify_frequency_per_10_edits"] == 2.5

    def test_anti_pattern_over_verification(self):
        """Verify anti-pattern of verifying after every edit."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Verify", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Verify", "turn_index": 3},
            {"tool_name": "Edit", "turn_index": 4},
            {"tool_name": "Verify", "turn_index": 5},
            {"tool_name": "Edit", "turn_index": 6},
            {"tool_name": "Verify", "turn_index": 7},
        ])

        assert result["verify_call_count"] == 4
        assert result["edit_call_count"] == 4
        # All verifies after single edits = 100% over-verification
        assert result["over_verification_rate"] == 100.0
        assert result["strategic_verification_rate"] == 0.0
        # High verify frequency = inefficient
        assert result["verify_frequency_per_10_edits"] == 10.0

    def test_anti_pattern_under_verification(self):
        """Verify anti-pattern of never verifying."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Edit", "turn_index": 3},
            {"tool_name": "Edit", "turn_index": 4},
            {"tool_name": "Edit", "turn_index": 5},
            {"tool_name": "Edit", "turn_index": 6},
            {"tool_name": "Edit", "turn_index": 7},
            {"tool_name": "Edit", "turn_index": 8},
            {"tool_name": "Edit", "turn_index": 9},
        ])

        # Many edits but no verification
        assert result["verify_call_count"] == 0
        assert result["edit_call_count"] == 10
        assert result["verify_frequency_per_10_edits"] == 0.0

    def test_whitespace_handling_in_tool_names(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "  Edit  ", "turn_index": 0},
            {"tool_name": "  Verify  ", "turn_index": 1},
        ])

        assert result["verify_call_count"] == 1
        assert result["edit_call_count"] == 1

    def test_consecutive_verifies(self):
        """Verify consecutive verifies without edits between."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Edit", "turn_index": 0},
            {"tool_name": "Verify", "turn_index": 1},
            {"tool_name": "Verify", "turn_index": 2},
            {"tool_name": "Verify", "turn_index": 3},
        ])

        assert result["verify_call_count"] == 3
        # Only first verify has edit before it
        assert result["single_edit_verifies"] == 1

    def test_high_edits_between_verifies(self):
        """Verify high edit count between verifies."""
        result = analyze_session_verify_command_timing([
            {"tool_name": "Edit", "turn_index": i} for i in range(20)
        ] + [
            {"tool_name": "Verify", "turn_index": 20},
        ])

        assert result["avg_edits_between_verifies"] == 20.0
        assert result["multi_edit_verifies"] == 1
