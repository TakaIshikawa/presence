"""Tests for pack verification command pattern analyzer."""

import pytest

from synthesis.pack_verification_command_pattern import analyze_pack_verification_command_pattern


class TestAnalyzePackVerificationCommandPattern:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_verification_command_pattern([])

        assert result["total_packs"] == 0
        assert result["command_type_distribution"] == []
        assert result["test_scope_patterns"] == []
        assert result["common_flags"] == []
        assert result["avg_command_complexity_score"] == 0.0
        assert result["high_complexity_packs"] == 0
        assert result["low_complexity_packs"] == 0
        assert result["verification_strategy_consistency"] == 1.0
        assert result["strategy_patterns"] == []
        assert result["packs_without_verification"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_verification_command_pattern(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_verification_command_pattern("not a list")

    def test_single_pytest_command_classified(self):
        """Verify single pytest command is correctly classified."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/test_example.py",
            }
        ])

        assert result["total_packs"] == 1
        assert len(result["command_type_distribution"]) == 1
        assert result["command_type_distribution"][0]["type"] == "pytest"
        assert result["command_type_distribution"][0]["count"] == 1

    def test_npm_test_command_classified(self):
        """Verify npm test command is correctly classified."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "npm test -- --coverage",
            }
        ])

        assert result["command_type_distribution"][0]["type"] == "npm"

    def test_cargo_test_command_classified(self):
        """Verify cargo test command is correctly classified."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "cargo test --lib",
            }
        ])

        assert result["command_type_distribution"][0]["type"] == "cargo"

    def test_unknown_command_type_classified_as_other(self):
        """Verify unknown command types classified as 'other'."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "custom_test_runner --all",
            }
        ])

        assert result["command_type_distribution"][0]["type"] == "other"

    def test_single_file_scope_detected(self):
        """Verify single file test scope is detected."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/test_example.py",
            }
        ])

        assert len(result["test_scope_patterns"]) == 1
        assert result["test_scope_patterns"][0]["scope"] == "single_file"

    def test_package_scope_detected(self):
        """Verify package-level test scope is detected."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/unit/",
            }
        ])

        assert result["test_scope_patterns"][0]["scope"] == "package"

    def test_full_suite_scope_detected(self):
        """Verify full suite test scope is detected."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests",
            }
        ])

        assert result["test_scope_patterns"][0]["scope"] == "full_suite"

    def test_verbose_flag_extracted(self):
        """Verify verbose flag is extracted from command."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest -v tests/",
            }
        ])

        flags = [f["flag"] for f in result["common_flags"]]
        assert "verbose" in flags

    def test_coverage_flag_extracted(self):
        """Verify coverage flag is extracted from command."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest --cov=src tests/",
            }
        ])

        flags = [f["flag"] for f in result["common_flags"]]
        assert "coverage" in flags

    def test_multiple_flags_extracted(self):
        """Verify multiple flags are extracted from command."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest -v --cov=src -x tests/",
            }
        ])

        flags = [f["flag"] for f in result["common_flags"]]
        assert "verbose" in flags
        assert "coverage" in flags
        assert "failfast" in flags

    def test_command_complexity_score_simple(self):
        """Verify low complexity score for simple commands."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/",
            }
        ])

        assert result["avg_command_complexity_score"] < 3.0
        assert result["low_complexity_packs"] == 1

    def test_command_complexity_score_complex(self):
        """Verify high complexity score for complex commands."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest -v --cov=src --cov-report=html -x tests/ && npm run lint && cargo test",
            }
        ])

        assert result["avg_command_complexity_score"] > 5.0

    def test_chained_commands_increase_complexity(self):
        """Verify chained commands increase complexity score."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/ && npm test && cargo test",
            }
        ])

        # Three different command types should increase complexity
        assert result["avg_command_complexity_score"] > 3.0

    def test_multiple_command_types_in_chain(self):
        """Verify multiple command types detected in chained commands."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/ && npm test",
            }
        ])

        types = [t["type"] for t in result["command_type_distribution"]]
        assert "pytest" in types
        assert "npm" in types

    def test_pack_without_verification_counted(self):
        """Verify packs without verification commands are counted."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "",
            },
            {
                "pack_id": "pack2",
                # No verification_command key
            }
        ])

        assert result["packs_without_verification"] == 2
        assert result["total_packs"] == 2

    def test_task_level_commands_extracted(self):
        """Verify task-level verification commands are extracted."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"verification_command": "pytest tests/test_a.py"},
                    {"verification_command": "pytest tests/test_b.py"},
                ]
            }
        ])

        assert result["command_type_distribution"][0]["type"] == "pytest"
        assert result["command_type_distribution"][0]["count"] == 2

    def test_pack_and_task_commands_combined(self):
        """Verify pack and task commands are combined."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/",
                "tasks": [
                    {"verification_command": "npm test"},
                ]
            }
        ])

        types = [t["type"] for t in result["command_type_distribution"]]
        assert "pytest" in types
        assert "npm" in types

    def test_duplicate_commands_deduplicated(self):
        """Verify duplicate commands are deduplicated."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/",
                "tasks": [
                    {"verification_command": "pytest tests/"},
                    {"verification_command": "pytest tests/"},
                ]
            }
        ])

        # Should count as single command
        assert result["command_type_distribution"][0]["count"] == 1

    def test_strategy_consistency_single_pack(self):
        """Verify consistency is 1.0 for single pack."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/",
            }
        ])

        assert result["verification_strategy_consistency"] == 1.0

    def test_strategy_consistency_identical_commands(self):
        """Verify consistency is 1.0 for identical commands."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/",
            },
            {
                "pack_id": "pack2",
                "verification_command": "pytest tests/",
            },
            {
                "pack_id": "pack3",
                "verification_command": "pytest tests/",
            }
        ])

        assert result["verification_strategy_consistency"] == 1.0

    def test_strategy_consistency_different_commands(self):
        """Verify consistency is lower for different commands."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/",
            },
            {
                "pack_id": "pack2",
                "verification_command": "npm test",
            },
            {
                "pack_id": "pack3",
                "verification_command": "cargo test",
            }
        ])

        # All different strategies
        assert result["verification_strategy_consistency"] < 0.5

    def test_strategy_consistency_mixed_similarity(self):
        """Verify consistency reflects mixed similarity."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/",
            },
            {
                "pack_id": "pack2",
                "verification_command": "pytest tests/",
            },
            {
                "pack_id": "pack3",
                "verification_command": "npm test",
            }
        ])

        # 2 out of 3 are pytest: consistency ~0.67
        assert 0.6 <= result["verification_strategy_consistency"] <= 0.7

    def test_strategy_patterns_tracked(self):
        """Verify strategy patterns are tracked."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/test_a.py",
            },
            {
                "pack_id": "pack2",
                "verification_command": "pytest tests/test_b.py",
            }
        ])

        assert len(result["strategy_patterns"]) > 0
        # Should have pattern combining pytest + single_file
        pattern = result["strategy_patterns"][0]
        assert "pytest" in pattern["pattern"]
        assert "single_file" in pattern["pattern"]

    def test_percentage_distribution_calculations(self):
        """Verify percentage distributions are calculated correctly."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/",
            },
            {
                "pack_id": "pack2",
                "verification_command": "pytest tests/",
            },
            {
                "pack_id": "pack3",
                "verification_command": "npm test",
            }
        ])

        # 2 pytest out of 3 total = 66.67%
        pytest_dist = next(d for d in result["command_type_distribution"] if d["type"] == "pytest")
        assert pytest_dist["percentage"] > 65.0
        assert pytest_dist["percentage"] < 68.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_verification_command_pattern([
            "not a dict",
            {"pack_id": "pack1", "verification_command": "pytest tests/"},
        ])

        assert result["total_packs"] == 1

    def test_high_complexity_pack_classification(self):
        """Verify high complexity packs are classified correctly."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest -v -x --cov=src --cov-report=html tests/ && npm run lint && cargo test --all",
            }
        ])

        assert result["high_complexity_packs"] == 1
        assert result["avg_command_complexity_score"] > 7.0

    def test_low_complexity_pack_classification(self):
        """Verify low complexity packs are classified correctly."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest",
            }
        ])

        assert result["low_complexity_packs"] == 1
        assert result["avg_command_complexity_score"] < 3.0

    def test_parallel_flag_detected(self):
        """Verify parallel execution flag is detected."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest -n 4 tests/",
            }
        ])

        flags = [f["flag"] for f in result["common_flags"]]
        assert "parallel" in flags

    def test_markers_flag_detected(self):
        """Verify test markers flag is detected."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest -m unit tests/",
            }
        ])

        flags = [f["flag"] for f in result["common_flags"]]
        assert "markers" in flags

    def test_quiet_flag_detected(self):
        """Verify quiet flag is detected."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest -q tests/",
            }
        ])

        flags = [f["flag"] for f in result["common_flags"]]
        assert "quiet" in flags

    def test_command_with_pipes_increases_complexity(self):
        """Verify piped commands increase complexity."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/ | grep PASSED",
            }
        ])

        # Pipes should add complexity
        assert result["avg_command_complexity_score"] > 1.0

    def test_command_with_redirects_increases_complexity(self):
        """Verify redirected commands increase complexity."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/ > results.txt",
            }
        ])

        # Redirects should add complexity
        assert result["avg_command_complexity_score"] > 1.0

    def test_command_with_environment_vars_increases_complexity(self):
        """Verify environment variables increase complexity."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "ENV=test pytest tests/",
            }
        ])

        # Environment variables should add complexity
        assert result["avg_command_complexity_score"] > 1.0

    def test_jest_command_classified(self):
        """Verify jest command is correctly classified."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "jest --coverage",
            }
        ])

        assert result["command_type_distribution"][0]["type"] == "jest"

    def test_go_test_command_classified(self):
        """Verify go test command is correctly classified."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "go test ./...",
            }
        ])

        assert result["command_type_distribution"][0]["type"] == "go_test"

    def test_multiple_packs_average_complexity(self):
        """Verify average complexity calculated across multiple packs."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest",
            },
            {
                "pack_id": "pack2",
                "verification_command": "pytest -v --cov=src tests/",
            },
            {
                "pack_id": "pack3",
                "verification_command": "pytest -v -x --cov=src tests/ && npm test",
            }
        ])

        # Should have a mid-range average
        assert 2.0 < result["avg_command_complexity_score"] < 6.0

    def test_common_flags_sorted_by_frequency(self):
        """Verify common flags are sorted by frequency."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest -v tests/",
            },
            {
                "pack_id": "pack2",
                "verification_command": "pytest -v --cov=src tests/",
            },
            {
                "pack_id": "pack3",
                "verification_command": "pytest -v tests/",
            }
        ])

        # Verbose appears 3 times, coverage appears 1 time
        assert result["common_flags"][0]["flag"] == "verbose"
        assert result["common_flags"][0]["count"] == 3

    def test_test_command_key_supported(self):
        """Verify test_command key is supported for tasks."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"test_command": "pytest tests/test_a.py"},
                ]
            }
        ])

        assert result["command_type_distribution"][0]["type"] == "pytest"

    def test_task_verification_command_at_record_level(self):
        """Verify task_verification_command at record level is extracted."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "task_verification_command": "pytest tests/test_a.py",
            }
        ])

        assert result["command_type_distribution"][0]["type"] == "pytest"

    def test_semicolon_command_separator(self):
        """Verify semicolon is recognized as command separator."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "pytest tests/; npm test",
            }
        ])

        types = [t["type"] for t in result["command_type_distribution"]]
        assert "pytest" in types
        assert "npm" in types

    def test_whitespace_normalization(self):
        """Verify command whitespace is normalized."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "  pytest   tests/  ",
            }
        ])

        # Should still be classified correctly
        assert result["command_type_distribution"][0]["type"] == "pytest"

    def test_empty_command_string_handled(self):
        """Verify empty command strings are handled gracefully."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "   ",
            }
        ])

        assert result["packs_without_verification"] == 1

    def test_strategy_pattern_limits_to_top_10(self):
        """Verify strategy patterns are limited to top 10."""
        # Create 15 different patterns
        packs = [
            {"pack_id": f"pack{i}", "verification_command": f"pytest tests/test_{i}.py"}
            for i in range(15)
        ]

        result = analyze_pack_verification_command_pattern(packs)

        # Should limit to 10 patterns
        assert len(result["strategy_patterns"]) <= 10

    def test_complex_real_world_pytest_command(self):
        """Verify complex real-world pytest command is analyzed correctly."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "uv run --with pytest pytest tests/test_example.py -v",
            }
        ])

        # Should detect pytest even with uv run prefix
        types = [t["type"] for t in result["command_type_distribution"]]
        assert "pytest" in types

    def test_npm_run_test_variant(self):
        """Verify npm run test variant is classified correctly."""
        result = analyze_pack_verification_command_pattern([
            {
                "pack_id": "pack1",
                "verification_command": "npm run test",
            }
        ])

        assert result["command_type_distribution"][0]["type"] == "npm"
