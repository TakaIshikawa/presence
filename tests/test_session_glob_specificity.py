"""Tests for session Glob pattern specificity analyzer."""

import pytest

from synthesis.session_glob_specificity import analyze_session_glob_specificity


class TestAnalyzeSessionGlobSpecificity:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_glob_specificity([])

        assert result["total_turns"] == 0
        assert result["glob_invocations"] == 0
        assert result["exact_file_patterns"] == 0
        assert result["single_extension_patterns"] == 0
        assert result["recursive_extension_patterns"] == 0
        assert result["broad_wildcard_patterns"] == 0
        assert result["patterns_with_many_results"] == 0
        assert result["avg_result_count"] == 0.0
        assert result["targeted_vs_exploratory_ratio"] == 0.0
        assert result["specificity_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_glob_specificity(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_glob_specificity("not a list")

    def test_exact_file_pattern(self):
        """Verify exact filename pattern (no wildcards)."""
        result = analyze_session_glob_specificity([
            {
                "turn_index": 0,
                "tool_name": "Glob",
                "pattern": "src/main.py",
                "result_count": 1,
            }
        ])

        assert result["glob_invocations"] == 1
        assert result["exact_file_patterns"] == 1
        assert result["exact_file_ratio"] == 100.0
        assert result["targeted_patterns"] == 1

    def test_single_extension_pattern(self):
        """Verify *.ext pattern."""
        result = analyze_session_glob_specificity([
            {
                "turn_index": 0,
                "tool_name": "Glob",
                "pattern": "*.py",
                "result_count": 5,
            }
        ])

        assert result["single_extension_patterns"] == 1
        assert result["single_extension_ratio"] == 100.0
        assert result["targeted_patterns"] == 1

    def test_recursive_extension_pattern(self):
        """Verify **/*.ext pattern."""
        result = analyze_session_glob_specificity([
            {
                "turn_index": 0,
                "tool_name": "Glob",
                "pattern": "**/*.py",
                "result_count": 25,
            }
        ])

        assert result["recursive_extension_patterns"] == 1
        assert result["recursive_extension_ratio"] == 100.0
        assert result["exploratory_patterns"] == 1

    def test_broad_wildcard_pattern(self):
        """Verify broad pattern with multiple wildcards."""
        result = analyze_session_glob_specificity([
            {
                "turn_index": 0,
                "tool_name": "Glob",
                "pattern": "src/**/test_*.py",
                "result_count": 15,
            }
        ])

        assert result["broad_wildcard_patterns"] == 1
        assert result["broad_wildcard_ratio"] == 100.0
        assert result["exploratory_patterns"] == 1

    def test_mixed_pattern_types(self):
        """Verify mixed pattern categories."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Glob", "pattern": "file.txt"},
            {"turn_index": 1, "tool_name": "Glob", "pattern": "*.py"},
            {"turn_index": 2, "tool_name": "Glob", "pattern": "**/*.js"},
            {"turn_index": 3, "tool_name": "Glob", "pattern": "src/**/test_*.py"},
        ])

        assert result["glob_invocations"] == 4
        assert result["exact_file_patterns"] == 1
        assert result["single_extension_patterns"] == 1
        assert result["recursive_extension_patterns"] == 1
        assert result["broad_wildcard_patterns"] == 1
        assert result["exact_file_ratio"] == 25.0
        assert result["targeted_patterns"] == 2
        assert result["exploratory_patterns"] == 2
        assert result["targeted_vs_exploratory_ratio"] == 50.0

    def test_overly_broad_patterns(self):
        """Verify detection of patterns with many results."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Glob", "pattern": "*.py", "result_count": 60},
            {"turn_index": 1, "tool_name": "Glob", "pattern": "*.js", "result_count": 10},
            {"turn_index": 2, "tool_name": "Glob", "pattern": "**/*", "result_count": 200},
        ])

        assert result["patterns_with_many_results"] == 2
        # 2/3 = 66.67%
        assert result["overly_broad_ratio"] == 66.67

    def test_result_count_statistics(self):
        """Verify result count statistics."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Glob", "result_count": 5},
            {"turn_index": 1, "tool_name": "Glob", "result_count": 15},
            {"turn_index": 2, "tool_name": "Glob", "result_count": 10},
        ])

        assert result["result_counts"] == [5, 15, 10]
        # (5 + 15 + 10) / 3 = 10
        assert result["avg_result_count"] == 10.0
        assert result["median_result_count"] == 10.0

    def test_median_with_even_count(self):
        """Verify median calculation with even number of values."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Glob", "result_count": 5},
            {"turn_index": 1, "tool_name": "Glob", "result_count": 15},
        ])

        # (5 + 15) / 2 = 10
        assert result["median_result_count"] == 10.0

    def test_grep_instead_opportunities(self):
        """Verify detection of content searches using Glob."""
        result = analyze_session_glob_specificity([
            {
                "turn_index": 0,
                "tool_name": "Glob",
                "pattern": "*.py",
                "is_content_search": True,
            },
            {
                "turn_index": 1,
                "tool_name": "Glob",
                "pattern": "*.js",
                "is_content_search": False,
            },
        ])

        assert result["grep_instead_opportunities"] == 1
        assert result["grep_opportunity_ratio"] == 50.0

    def test_non_glob_tools_ignored(self):
        """Verify non-Glob tools are ignored."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Read"},
            {"turn_index": 1, "tool_name": "Glob", "pattern": "*.py"},
            {"turn_index": 2, "tool_name": "Grep"},
        ])

        assert result["total_turns"] == 3
        assert result["glob_invocations"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "GLOB", "pattern": "*.py"},
            {"turn_index": 1, "tool_name": "glob", "pattern": "*.js"},
            {"turn_index": 2, "tool_name": "Glob", "pattern": "*.ts"},
        ])

        assert result["glob_invocations"] == 3

    def test_missing_optional_fields(self):
        """Verify missing optional fields are handled gracefully."""
        result = analyze_session_glob_specificity([
            {
                "turn_index": 0,
                "tool_name": "Glob",
                # No pattern, no result_count
            }
        ])

        assert result["glob_invocations"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_glob_specificity([
            "not a dict",
            {"turn_index": 0, "tool_name": "Glob", "pattern": "*.py"},
            None,
        ])

        assert result["total_turns"] == 1
        assert result["glob_invocations"] == 1

    def test_whitespace_handling_in_tool_names(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "  Glob  ", "pattern": "*.py"}
        ])

        assert result["glob_invocations"] == 1

    def test_specificity_score_perfect_targeted(self):
        """Verify specificity score with perfect targeted usage."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Glob", "pattern": "file.py", "result_count": 1},
            {"turn_index": 1, "tool_name": "Glob", "pattern": "*.js", "result_count": 5},
            {"turn_index": 2, "tool_name": "Glob", "pattern": "test.txt", "result_count": 1},
        ])

        # All targeted, low result counts, no overly broad, no grep opportunities
        assert result["targeted_vs_exploratory_ratio"] == 100.0
        assert result["overly_broad_ratio"] == 0.0
        assert result["avg_result_count"] < 20.0
        assert result["grep_opportunity_ratio"] == 0.0
        assert result["specificity_score"] >= 0.9

    def test_specificity_score_poor_exploratory(self):
        """Verify specificity score with poor exploratory usage."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Glob", "pattern": "**/*", "result_count": 100},
            {"turn_index": 1, "tool_name": "Glob", "pattern": "src/**/*.py", "result_count": 80},
        ])

        # All exploratory, high result counts, overly broad
        assert result["targeted_vs_exploratory_ratio"] == 0.0
        assert result["overly_broad_ratio"] == 100.0
        assert result["specificity_score"] < 0.3

    def test_specificity_score_moderate_usage(self):
        """Verify specificity score with moderate usage."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Glob", "pattern": "*.py", "result_count": 10},
            {"turn_index": 1, "tool_name": "Glob", "pattern": "*.js", "result_count": 15},
            {"turn_index": 2, "tool_name": "Glob", "pattern": "**/*.ts", "result_count": 25},
        ])

        # 2 targeted, 1 exploratory, moderate results (good score)
        assert result["targeted_vs_exploratory_ratio"] == 66.67
        assert 0.8 <= result["specificity_score"] <= 1.0

    def test_negative_result_count_ignored(self):
        """Verify negative result counts are ignored."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Glob", "result_count": -1},
            {"turn_index": 1, "tool_name": "Glob", "result_count": 10},
        ])

        # Only non-negative counts included
        assert result["result_counts"] == [10]

    def test_zero_result_count_included(self):
        """Verify zero result counts are included."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Glob", "result_count": 0},
            {"turn_index": 1, "tool_name": "Glob", "result_count": 10},
        ])

        assert result["result_counts"] == [0, 10]
        assert result["avg_result_count"] == 5.0

    def test_comprehensive_session_analysis(self):
        """Verify comprehensive session with all features."""
        result = analyze_session_glob_specificity([
            {"turn_index": 0, "tool_name": "Read"},
            {"turn_index": 1, "tool_name": "Glob", "pattern": "src/main.py", "result_count": 1},
            {"turn_index": 2, "tool_name": "Glob", "pattern": "*.py", "result_count": 12},
            {"turn_index": 3, "tool_name": "Edit"},
            {"turn_index": 4, "tool_name": "Glob", "pattern": "**/*.js", "result_count": 30},
            {
                "turn_index": 5,
                "tool_name": "Glob",
                "pattern": "test_*.py",
                "result_count": 65,
                "is_content_search": True,
            },
            {"turn_index": 6, "tool_name": "Glob", "pattern": "*.txt", "result_count": 5},
        ])

        assert result["total_turns"] == 7
        assert result["glob_invocations"] == 5
        assert result["exact_file_patterns"] == 1
        assert result["single_extension_patterns"] == 2
        assert result["recursive_extension_patterns"] == 1
        assert result["broad_wildcard_patterns"] == 1
        assert result["patterns_with_many_results"] == 1
        # 1/5 = 20%
        assert result["overly_broad_ratio"] == 20.0
        # (1 + 12 + 30 + 65 + 5) / 5 = 22.6
        assert result["avg_result_count"] == 22.6
        # Sorted: [1, 5, 12, 30, 65] -> median = 12
        assert result["median_result_count"] == 12.0
        assert result["targeted_patterns"] == 3
        assert result["exploratory_patterns"] == 2
        # 3/5 = 60%
        assert result["targeted_vs_exploratory_ratio"] == 60.0
        assert result["grep_instead_opportunities"] == 1
        # 1/5 = 20%
        assert result["grep_opportunity_ratio"] == 20.0
