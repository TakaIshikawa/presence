"""Tests for session verification command reuse analyzer."""

import pytest

from synthesis.session_verification_command_reuse import (
    analyze_session_verification_command_reuse,
)


class TestAnalyzeSessionVerificationCommandReuse:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_verification_command_reuse([])

        assert result["total_commands"] == 0
        assert result["unique_commands"] == 0
        assert result["most_frequent_commands"] == []
        assert result["targeted_commands"] == 0
        assert result["broad_commands"] == 0
        assert result["targeted_to_broad_ratio"] == 0.0
        assert result["reuse_efficiency_score"] == 0.0
        assert result["avg_command_reuse"] == 0.0
        assert result["single_use_commands"] == 0
        assert result["highly_reused_commands"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_verification_command_reuse(None)
        assert result["total_commands"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_verification_command_reuse("not a list")

    def test_no_verification_commands(self):
        """Verify session with no verification commands."""
        result = analyze_session_verification_command_reuse([
            {
                "command_index": 1,
                "command": "",
            },
        ])

        # Empty command should not be counted
        assert result["total_commands"] == 1
        assert result["unique_commands"] == 0

    def test_single_command_reuse(self):
        """Verify session reusing single command multiple times."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "pytest tests/"},
            {"command_pattern": "pytest tests/"},
            {"command_pattern": "pytest tests/"},
            {"command_pattern": "pytest tests/"},
            {"command_pattern": "pytest tests/"},
        ])

        assert result["total_commands"] == 5
        assert result["unique_commands"] == 1
        # (5 - 1) / 5 * 100 = 80.0
        assert result["reuse_efficiency_score"] == 80.0
        assert result["avg_command_reuse"] == 5.0
        assert result["single_use_commands"] == 0
        assert result["highly_reused_commands"] == 1

    def test_diverse_commands_no_reuse(self):
        """Verify session with diverse commands and no reuse."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "pytest test1.py"},
            {"command_pattern": "pytest test2.py"},
            {"command_pattern": "mypy src/"},
            {"command_pattern": "ruff check ."},
        ])

        assert result["total_commands"] == 4
        assert result["unique_commands"] == 4
        # (4 - 4) / 4 * 100 = 0.0
        assert result["reuse_efficiency_score"] == 0.0
        assert result["avg_command_reuse"] == 1.0
        assert result["single_use_commands"] == 4
        assert result["highly_reused_commands"] == 0

    def test_targeted_vs_broad_commands(self):
        """Verify tracking of targeted vs broad commands."""
        result = analyze_session_verification_command_reuse([
            {"command": "pytest test_specific.py", "is_targeted": True},
            {"command": "pytest test_another.py", "is_targeted": True},
            {"command": "pytest tests/", "is_broad": True},
            {"command": "mypy .", "is_broad": True},
        ])

        assert result["targeted_commands"] == 2
        assert result["broad_commands"] == 2
        # 2/4 * 100 = 50.0
        assert result["targeted_to_broad_ratio"] == 50.0

    def test_all_targeted_commands(self):
        """Verify session with only targeted commands."""
        result = analyze_session_verification_command_reuse([
            {"command": "pytest test_file1.py", "is_targeted": True},
            {"command": "pytest test_file2.py", "is_targeted": True},
            {"command": "mypy src/module.py", "is_targeted": True},
        ])

        assert result["targeted_to_broad_ratio"] == 100.0

    def test_all_broad_commands(self):
        """Verify session with only broad commands."""
        result = analyze_session_verification_command_reuse([
            {"command": "pytest tests/", "is_broad": True},
            {"command": "mypy .", "is_broad": True},
        ])

        assert result["targeted_to_broad_ratio"] == 0.0

    def test_most_frequent_commands_tracking(self):
        """Verify tracking of most frequent command patterns."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "pytest tests/"},
            {"command_pattern": "pytest tests/"},
            {"command_pattern": "pytest tests/"},
            {"command_pattern": "mypy src/"},
            {"command_pattern": "mypy src/"},
            {"command_pattern": "ruff check ."},
        ])

        assert len(result["most_frequent_commands"]) == 3
        assert result["most_frequent_commands"][0]["command"] == "pytest tests/"
        assert result["most_frequent_commands"][0]["count"] == 3
        assert result["most_frequent_commands"][1]["command"] == "mypy src/"
        assert result["most_frequent_commands"][1]["count"] == 2

    def test_command_pattern_fallback_to_raw_command(self):
        """Verify fallback to raw command when pattern not provided."""
        result = analyze_session_verification_command_reuse([
            {"command": "pytest tests/test_file.py"},
            {"command": "pytest tests/test_file.py"},
        ])

        assert result["unique_commands"] == 1
        assert result["reuse_efficiency_score"] == 50.0

    def test_mixed_pattern_and_raw_commands(self):
        """Verify handling of mixed pattern and raw commands."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "pytest tests/"},
            {"command": "pytest tests/"},
            {"command_pattern": "mypy ."},
        ])

        assert result["unique_commands"] == 2

    def test_reuse_efficiency_calculation(self):
        """Verify reuse efficiency score calculation."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd2"},
            {"command_pattern": "cmd2"},
            {"command_pattern": "cmd3"},
        ])

        # 5 total, 3 unique
        # (5 - 3) / 5 * 100 = 40.0
        assert result["reuse_efficiency_score"] == 40.0

    def test_average_command_reuse_calculation(self):
        """Verify average command reuse calculation."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd2"},
            {"command_pattern": "cmd2"},
        ])

        # 5 total / 2 unique = 2.5
        assert result["avg_command_reuse"] == 2.5

    def test_single_use_commands_count(self):
        """Verify counting of single-use commands."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "cmd1"},  # Used once
            {"command_pattern": "cmd2"},
            {"command_pattern": "cmd2"},  # Used twice
            {"command_pattern": "cmd3"},  # Used once
        ])

        assert result["single_use_commands"] == 2

    def test_highly_reused_commands_count(self):
        """Verify counting of highly reused commands (5+ times)."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},  # Used 5 times
            {"command_pattern": "cmd2"},
            {"command_pattern": "cmd2"},
            {"command_pattern": "cmd2"},  # Used 3 times
        ])

        assert result["highly_reused_commands"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_verification_command_reuse([
            "not a dict",
            {"command_pattern": "pytest tests/"},
        ])

        assert result["total_commands"] == 1

    def test_whitespace_handling_in_patterns(self):
        """Verify whitespace is stripped from patterns."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "  pytest tests/  "},
            {"command_pattern": "pytest tests/"},
        ])

        # Should be treated as same command after stripping
        assert result["unique_commands"] == 1

    def test_empty_string_patterns_ignored(self):
        """Verify empty string patterns are ignored."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": ""},
            {"command_pattern": "   "},
            {"command_pattern": "valid_command"},
        ])

        assert result["unique_commands"] == 1

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_verification_command_reuse([
            {
                "command_index": 1,
                # Missing command fields
            },
        ])

        assert result["total_commands"] == 1
        assert result["unique_commands"] == 0

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_verification_command_reuse([
            {
                "command_index": 1,
                "command": "pytest test_file.py",
                "command_pattern": "pytest <file>",
                "is_targeted": True,
                "is_broad": False,
            },
            {
                "command_index": 2,
                "command": "pytest tests/",
                "command_pattern": "pytest tests/",
                "is_targeted": False,
                "is_broad": True,
            },
            {
                "command_index": 3,
                "command": "pytest test_another.py",
                "command_pattern": "pytest <file>",
                "is_targeted": True,
                "is_broad": False,
            },
        ])

        assert result["total_commands"] == 3
        assert result["unique_commands"] == 2
        assert result["targeted_commands"] == 2
        assert result["broad_commands"] == 1
        assert result["targeted_to_broad_ratio"] == 66.67
        # (3 - 2) / 3 * 100 = 33.33
        assert result["reuse_efficiency_score"] == 33.33

    def test_top_10_frequent_commands_limit(self):
        """Verify most frequent commands limited to top 10."""
        commands = [{"command_pattern": f"cmd{i}"} for i in range(15)]
        result = analyze_session_verification_command_reuse(commands)

        assert len(result["most_frequent_commands"]) == 10

    def test_edge_case_exactly_5_reuses(self):
        """Verify edge case of exactly 5 command reuses."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
        ])

        # Exactly 5 should count as highly reused
        assert result["highly_reused_commands"] == 1

    def test_edge_case_4_reuses_not_highly_reused(self):
        """Verify 4 reuses doesn't count as highly reused."""
        result = analyze_session_verification_command_reuse([
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
            {"command_pattern": "cmd1"},
        ])

        assert result["highly_reused_commands"] == 0
