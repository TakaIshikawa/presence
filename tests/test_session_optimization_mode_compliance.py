"""Tests for session CLAUDE_OPTIMIZATION_MODE compliance analyzer."""

import pytest

from synthesis.session_optimization_mode_compliance import (
    analyze_session_optimization_mode_compliance,
)


class TestAnalyzeSessionOptimizationModeCompliance:
    """Test main analyzer function."""

    def test_empty_records_returns_zeroed_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_session_optimization_mode_compliance([])

        assert result["total_turns"] == 0
        assert result["optimization_mode_detected"] == "unknown"
        assert result["total_read_calls"] == 0
        assert result["reads_with_offset_limit"] == 0
        assert result["offset_limit_usage_rate"] == 0.0
        assert result["total_lines_read"] == 0
        assert result["avg_lines_per_read"] == 0.0
        assert result["cache_command_count"] == 0
        assert result["verify_command_count"] == 0
        assert result["cache_command_rate"] == 0.0
        assert result["verify_command_rate"] == 0.0
        assert result["optimization_compliance_score"] == 0.0
        assert result["estimated_token_savings"] == 0.0
        assert result["mode_violations"] == 0
        assert result["example_good_targeted_read"] == {}
        assert result["example_missed_targeting"] == {}
        assert result["example_mode_violation"] == {}

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_optimization_mode_compliance(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_optimization_mode_compliance("not a list")

    def test_optimized_mode_detection_from_field(self):
        """Verify detection of optimized mode from optimization_mode field."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "optimized",
                "file_path": "a.py",
                "offset": 0,
                "limit": 30,
                "lines_read": 30,
            }
        ])

        assert result["optimization_mode_detected"] == "optimized"

    def test_baseline_mode_detection_from_env_vars(self):
        """Verify detection of baseline mode from environment_vars."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "environment_vars": {"CLAUDE_OPTIMIZATION_MODE": "baseline"},
                "file_path": "a.py",
                "lines_read": 100,
            }
        ])

        assert result["optimization_mode_detected"] == "baseline"

    def test_offset_limit_usage_rate_calculation(self):
        """Verify calculation of offset/limit usage rate."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "file_path": "a.py",
                "offset": 0,
                "limit": 30,
                "lines_read": 30,
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "file_path": "b.py",
                "offset": 50,
                "limit": 20,
                "lines_read": 20,
            },
            {
                "turn_index": 2,
                "tool_name": "Read",
                "file_path": "c.py",
                # No offset/limit
                "lines_read": 100,
            }
        ])

        assert result["total_read_calls"] == 3
        assert result["reads_with_offset_limit"] == 2
        assert result["offset_limit_usage_rate"] == 66.67  # 2/3 * 100

    def test_average_lines_per_read_calculation(self):
        """Verify calculation of average lines per read."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "file_path": "a.py",
                "lines_read": 30,
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "file_path": "b.py",
                "lines_read": 50,
            },
            {
                "turn_index": 2,
                "tool_name": "Read",
                "file_path": "c.py",
                "lines_read": 40,
            }
        ])

        assert result["total_lines_read"] == 120
        assert result["avg_lines_per_read"] == 40.0  # 120 / 3

    def test_cache_command_detection(self):
        """Verify detection of /cache commands."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Bash",
                "command": "/cache query src/main.py",
            },
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "command": "cache query tests/",
            },
            {
                "turn_index": 2,
                "tool_name": "Bash",
                "command": "git status",  # Not a cache command
            }
        ])

        assert result["cache_command_count"] == 2
        assert result["cache_command_rate"] == 66.67  # 2/3 * 100

    def test_verify_command_detection(self):
        """Verify detection of /verify commands."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Bash",
                "command": "/verify check",
            },
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "command": "verify build",
            },
            {
                "turn_index": 2,
                "tool_name": "Bash",
                "command": "pytest tests/",  # Not a verify command
            }
        ])

        assert result["verify_command_count"] == 2
        assert result["verify_command_rate"] == 66.67  # 2/3 * 100

    def test_baseline_mode_violation_with_cache(self):
        """Verify detection of /cache usage in baseline mode."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "baseline",
                "file_path": "a.py",
                "lines_read": 100,
            },
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "command": "/cache query a.py",  # Violation!
            }
        ])

        assert result["optimization_mode_detected"] == "baseline"
        assert result["mode_violations"] == 1
        assert result["example_mode_violation"]["type"] == "cache_in_baseline"

    def test_baseline_mode_violation_with_verify(self):
        """Verify detection of /verify usage in baseline mode."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "baseline",
                "file_path": "a.py",
                "lines_read": 100,
            },
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "command": "/verify build",  # Violation!
            }
        ])

        assert result["optimization_mode_detected"] == "baseline"
        assert result["mode_violations"] == 1
        assert result["example_mode_violation"]["type"] == "verify_in_baseline"

    def test_optimized_mode_high_compliance(self):
        """Verify high compliance score for good optimized behavior."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "optimized",
                "file_path": "a.py",
                "offset": 0,
                "limit": 30,
                "lines_read": 30,
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "file_path": "b.py",
                "offset": 50,
                "limit": 40,
                "lines_read": 40,
            },
            {
                "turn_index": 2,
                "tool_name": "Bash",
                "command": "/cache query a.py",
            },
            {
                "turn_index": 3,
                "tool_name": "Bash",
                "command": "/verify check",
            }
        ])

        # 100% offset usage, avg 35 lines, 25% cache rate, 25% verify rate
        assert result["offset_limit_usage_rate"] == 100.0
        assert result["avg_lines_per_read"] == 35.0
        assert result["cache_command_rate"] == 25.0
        assert result["verify_command_rate"] == 25.0
        assert result["optimization_compliance_score"] >= 0.9

    def test_optimized_mode_low_compliance(self):
        """Verify low compliance score for poor optimized behavior."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "optimized",
                "file_path": "a.py",
                # No offset/limit
                "lines_read": 200,
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "file_path": "b.py",
                # No offset/limit
                "lines_read": 150,
            },
            # No cache or verify commands
        ])

        # 0% offset usage, avg 175 lines, no cache/verify
        assert result["offset_limit_usage_rate"] == 0.0
        assert result["avg_lines_per_read"] == 175.0
        assert result["cache_command_rate"] == 0.0
        assert result["verify_command_rate"] == 0.0
        assert result["optimization_compliance_score"] < 0.3

    def test_baseline_mode_perfect_compliance(self):
        """Verify perfect compliance score for pure baseline."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "baseline",
                "file_path": "a.py",
                "lines_read": 200,
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "file_path": "b.py",
                "lines_read": 150,
            },
            # No optimization commands
        ])

        assert result["optimization_mode_detected"] == "baseline"
        assert result["mode_violations"] == 0
        assert result["optimization_compliance_score"] == 1.0

    def test_baseline_mode_failed_compliance(self):
        """Verify failed compliance for baseline with optimization commands."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "baseline",
                "file_path": "a.py",
                "lines_read": 200,
            },
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "command": "/cache query a.py",  # Violation
            }
        ])

        assert result["optimization_mode_detected"] == "baseline"
        assert result["mode_violations"] == 1
        assert result["optimization_compliance_score"] == 0.0

    def test_token_savings_estimation_optimized(self):
        """Verify token savings estimation for optimized mode."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "optimized",
                "file_path": "a.py",
                "offset": 0,
                "limit": 30,
                "lines_read": 30,
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "file_path": "b.py",
                "offset": 50,
                "limit": 40,
                "lines_read": 40,
            }
        ])

        # avg 35 lines vs baseline 237 lines, 100% offset usage
        # Expected: (237 - 35) / 237 * 100% = ~85% savings
        assert result["optimization_mode_detected"] == "optimized"
        assert result["avg_lines_per_read"] == 35.0
        assert result["offset_limit_usage_rate"] == 100.0
        assert result["estimated_token_savings"] > 80.0

    def test_token_savings_zero_for_baseline(self):
        """Verify token savings is zero for baseline mode."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "baseline",
                "file_path": "a.py",
                "lines_read": 200,
            }
        ])

        assert result["optimization_mode_detected"] == "baseline"
        assert result["estimated_token_savings"] == 0.0

    def test_example_good_targeted_read_captured(self):
        """Verify good targeted read example is captured."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "file_path": "src/main.py",
                "offset": -30,
                "limit": 30,
                "lines_read": 30,
            }
        ])

        assert result["example_good_targeted_read"]["file_path"] == "src/main.py"
        assert result["example_good_targeted_read"]["offset"] == -30
        assert result["example_good_targeted_read"]["limit"] == 30

    def test_example_missed_targeting_captured(self):
        """Verify missed targeting example is captured."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "file_path": "a.py",
                "lines_read": 200,
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "file_path": "a.py",  # Re-read without offset
                "lines_read": 200,
            }
        ])

        assert result["example_missed_targeting"]["file_path"] == "a.py"
        assert result["example_missed_targeting"]["read_count"] == 2

    def test_realistic_optimized_session(self):
        """Verify realistic optimized session pattern."""
        result = analyze_session_optimization_mode_compliance([
            # Exploration with targeted reads
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "optimized",
                "file_path": "src/main.py",
                "offset": 0,
                "limit": 50,
                "lines_read": 50,
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "file_path": "tests/test_main.py",
                "offset": 0,
                "limit": 60,
                "lines_read": 60,
            },
            # Cache check
            {
                "turn_index": 2,
                "tool_name": "Bash",
                "command": "/cache query src/main.py",
            },
            # Edit
            {
                "turn_index": 3,
                "tool_name": "Edit",
                "file_path": "src/main.py",
            },
            # Targeted verification read
            {
                "turn_index": 4,
                "tool_name": "Read",
                "file_path": "src/main.py",
                "offset": -30,
                "limit": 30,
                "lines_read": 30,
            },
            # Verify
            {
                "turn_index": 5,
                "tool_name": "Bash",
                "command": "/verify check",
            }
        ])

        assert result["optimization_mode_detected"] == "optimized"
        assert result["offset_limit_usage_rate"] == 100.0  # 3/3 reads
        assert result["avg_lines_per_read"] < 70.0  # (50+60+30)/3 = 46.67
        assert result["cache_command_count"] == 1
        assert result["verify_command_count"] == 1
        assert result["optimization_compliance_score"] >= 0.85

    def test_realistic_baseline_session(self):
        """Verify realistic baseline session pattern."""
        result = analyze_session_optimization_mode_compliance([
            # Natural full-file reads
            {
                "turn_index": 0,
                "tool_name": "Read",
                "optimization_mode": "baseline",
                "file_path": "src/main.py",
                "lines_read": 200,
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "file_path": "tests/test_main.py",
                "lines_read": 150,
            },
            # Edit
            {
                "turn_index": 2,
                "tool_name": "Edit",
                "file_path": "src/main.py",
            },
            # Re-read to verify
            {
                "turn_index": 3,
                "tool_name": "Read",
                "file_path": "src/main.py",
                "lines_read": 200,
            },
            # Regular Bash commands
            {
                "turn_index": 4,
                "tool_name": "Bash",
                "command": "pytest tests/",
            }
        ])

        assert result["optimization_mode_detected"] == "baseline"
        assert result["offset_limit_usage_rate"] == 0.0
        assert result["avg_lines_per_read"] > 100.0
        assert result["cache_command_count"] == 0
        assert result["verify_command_count"] == 0
        assert result["mode_violations"] == 0
        assert result["optimization_compliance_score"] == 1.0  # Perfect baseline

    def test_unknown_mode_neutral_score(self):
        """Verify neutral score when mode is unknown."""
        result = analyze_session_optimization_mode_compliance([
            {
                "turn_index": 0,
                "tool_name": "Read",
                # No mode indicator
                "file_path": "a.py",
                "lines_read": 100,
            }
        ])

        assert result["optimization_mode_detected"] == "unknown"
        assert result["optimization_compliance_score"] == 0.5

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_optimization_mode_compliance([
            "not a dict",
            {
                "turn_index": 0,
                "tool_name": "Read",
                "file_path": "a.py",
                "lines_read": 100,
            }
        ])

        assert result["total_turns"] == 1
        assert result["total_read_calls"] == 1
