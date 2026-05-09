"""Tests for session tool result size distribution analyzer."""

import pytest

from synthesis.session_tool_result_size_distribution import analyze_session_tool_result_size_distribution


class TestAnalyzeSessionToolResultSizeDistribution:
    """Test main analyzer function."""

    def test_empty_results_returns_zeroed_metrics(self):
        """Verify empty result list returns zero metrics."""
        result = analyze_session_tool_result_size_distribution([])

        assert result["total_results"] == 0
        assert result["result_size_histogram"] == {
            "0_10kb": 0,
            "10_50kb": 0,
            "50_100kb": 0,
            "100kb_plus": 0,
        }
        assert result["oversized_results"] == []
        assert result["average_result_size"] == 0.0
        assert result["median_result_size"] == 0.0
        assert result["p95_result_size"] == 0.0
        assert result["tools_by_avg_result_size"] == {}

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_tool_result_size_distribution(None)
        assert result["total_results"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_tool_result_size_distribution("not a list")

    def test_single_small_result_categorized(self):
        """Verify single small result is correctly categorized."""
        result = analyze_session_tool_result_size_distribution([
            {
                "tool_name": "Read",
                "result_size_bytes": 5120,  # 5 KB
                "turn_index": 1,
            }
        ])

        assert result["total_results"] == 1
        assert result["result_size_histogram"]["0_10kb"] == 1
        assert result["average_result_size"] == 5.0
        assert result["median_result_size"] == 5.0

    def test_medium_result_10_to_50kb(self):
        """Verify medium result (10-50 KB) is correctly categorized."""
        result = analyze_session_tool_result_size_distribution([
            {
                "tool_name": "Grep",
                "result_size_bytes": 30720,  # 30 KB
                "turn_index": 1,
            }
        ])

        assert result["result_size_histogram"]["10_50kb"] == 1

    def test_large_result_50_to_100kb(self):
        """Verify large result (50-100 KB) is correctly categorized."""
        result = analyze_session_tool_result_size_distribution([
            {
                "tool_name": "Bash",
                "result_size_bytes": 76800,  # 75 KB
                "turn_index": 1,
            }
        ])

        assert result["result_size_histogram"]["50_100kb"] == 1

    def test_oversized_result_over_100kb(self):
        """Verify oversized result (>100 KB) is correctly categorized."""
        result = analyze_session_tool_result_size_distribution([
            {
                "tool_name": "Read",
                "result_size_bytes": 153600,  # 150 KB
                "turn_index": 1,
            }
        ])

        assert result["result_size_histogram"]["100kb_plus"] == 1
        assert len(result["oversized_results"]) == 1
        assert result["oversized_results"][0]["tool_name"] == "Read"
        assert result["oversized_results"][0]["result_size_kb"] == 150.0

    def test_multiple_results_histogram(self):
        """Verify multiple results are correctly distributed in histogram."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 5120},   # 5 KB
            {"tool_name": "Read", "result_size_bytes": 8192},   # 8 KB
            {"tool_name": "Grep", "result_size_bytes": 20480},  # 20 KB
            {"tool_name": "Bash", "result_size_bytes": 61440},  # 60 KB
            {"tool_name": "Edit", "result_size_bytes": 122880}, # 120 KB
        ])

        assert result["result_size_histogram"]["0_10kb"] == 2
        assert result["result_size_histogram"]["10_50kb"] == 1
        assert result["result_size_histogram"]["50_100kb"] == 1
        assert result["result_size_histogram"]["100kb_plus"] == 1

    def test_average_result_size_calculation(self):
        """Verify average result size is calculated correctly."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 10240},  # 10 KB
            {"tool_name": "Read", "result_size_bytes": 20480},  # 20 KB
            {"tool_name": "Read", "result_size_bytes": 30720},  # 30 KB
        ])

        # Average: (10 + 20 + 30) / 3 = 20 KB
        assert result["average_result_size"] == 20.0

    def test_median_result_size_odd_count(self):
        """Verify median result size with odd number of results."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 5120},   # 5 KB
            {"tool_name": "Read", "result_size_bytes": 15360},  # 15 KB
            {"tool_name": "Read", "result_size_bytes": 25600},  # 25 KB
        ])

        # Median of [5, 15, 25] = 15
        assert result["median_result_size"] == 15.0

    def test_median_result_size_even_count(self):
        """Verify median result size with even number of results."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 10240},  # 10 KB
            {"tool_name": "Read", "result_size_bytes": 20480},  # 20 KB
            {"tool_name": "Read", "result_size_bytes": 30720},  # 30 KB
            {"tool_name": "Read", "result_size_bytes": 40960},  # 40 KB
        ])

        # Median of [10, 20, 30, 40] = (20 + 30) / 2 = 25
        assert result["median_result_size"] == 25.0

    def test_p95_percentile_calculation(self):
        """Verify 95th percentile result size calculation."""
        # Create 20 results with known sizes
        sizes = list(range(1, 21))  # 1 KB to 20 KB
        records = [
            {"tool_name": "Read", "result_size_bytes": size * 1024}
            for size in sizes
        ]

        result = analyze_session_tool_result_size_distribution(records)

        # P95 of 20 values using linear interpolation: 0.95 * (20-1) = 18.05
        # Interpolates between index 18 (19 KB) and 19 (20 KB)
        # Result: 19 + 0.05 * (20 - 19) = 19.05 KB
        assert result["p95_result_size"] == 19.05

    def test_tools_ranked_by_average_size(self):
        """Verify tools are ranked by average result size."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 10240},  # 10 KB
            {"tool_name": "Read", "result_size_bytes": 20480},  # 20 KB
            {"tool_name": "Grep", "result_size_bytes": 5120},   # 5 KB
            {"tool_name": "Grep", "result_size_bytes": 5120},   # 5 KB
            {"tool_name": "Bash", "result_size_bytes": 30720},  # 30 KB
        ])

        tools = result["tools_by_avg_result_size"]

        # Bash: 30 KB average
        # Read: (10 + 20) / 2 = 15 KB average
        # Grep: (5 + 5) / 2 = 5 KB average
        assert list(tools.keys()) == ["Bash", "Read", "Grep"]
        assert tools["Bash"] == 30.0
        assert tools["Read"] == 15.0
        assert tools["Grep"] == 5.0

    def test_oversized_results_tracked(self):
        """Verify oversized results are tracked with details."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 153600, "turn_index": 5},
            {"tool_name": "Bash", "result_size_bytes": 204800, "turn_index": 10},
        ])

        assert len(result["oversized_results"]) == 2
        assert result["oversized_results"][0]["tool_name"] == "Read"
        assert result["oversized_results"][0]["result_size_kb"] == 150.0
        assert result["oversized_results"][0]["turn_index"] == 5
        assert result["oversized_results"][1]["tool_name"] == "Bash"
        assert result["oversized_results"][1]["result_size_kb"] == 200.0

    def test_oversized_results_limited_to_20(self):
        """Verify oversized results are limited to 20 examples."""
        # Create 25 oversized results (need >100 KB, not ==100 KB)
        records = [
            {"tool_name": "Read", "result_size_bytes": 102401, "turn_index": i}
            for i in range(25)
        ]

        result = analyze_session_tool_result_size_distribution(records)

        assert len(result["oversized_results"]) == 20

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_tool_result_size_distribution([
            "not a dict",
            {"tool_name": "Read", "result_size_bytes": 5120},
        ])

        assert result["total_results"] == 1

    def test_negative_size_skipped(self):
        """Verify negative sizes are skipped."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": -1024},
            {"tool_name": "Read", "result_size_bytes": 5120},
        ])

        assert result["total_results"] == 1

    def test_missing_size_skipped(self):
        """Verify records without size are skipped."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read"},
            {"tool_name": "Read", "result_size_bytes": 5120},
        ])

        assert result["total_results"] == 1

    def test_boolean_size_skipped(self):
        """Verify boolean values are not treated as sizes."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": True},
            {"tool_name": "Read", "result_size_bytes": 5120},
        ])

        assert result["total_results"] == 1

    def test_float_size_converted_to_int(self):
        """Verify float sizes are converted to integers."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 5120.7},
        ])

        assert result["total_results"] == 1
        assert result["average_result_size"] == 5.0

    def test_string_size_parsed(self):
        """Verify string sizes are parsed to integers."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": "5120"},
        ])

        assert result["total_results"] == 1
        assert result["average_result_size"] == 5.0

    def test_invalid_string_size_skipped(self):
        """Verify invalid string sizes are skipped."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": "invalid"},
            {"tool_name": "Read", "result_size_bytes": 5120},
        ])

        assert result["total_results"] == 1

    def test_zero_size_result_categorized(self):
        """Verify zero-size results are correctly categorized."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Edit", "result_size_bytes": 0},
        ])

        assert result["total_results"] == 1
        assert result["result_size_histogram"]["0_10kb"] == 1
        assert result["average_result_size"] == 0.0

    def test_boundary_10kb_categorized_correctly(self):
        """Verify 10 KB boundary is categorized correctly."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 10239},  # Just under 10 KB
            {"tool_name": "Read", "result_size_bytes": 10240},  # Exactly 10 KB
        ])

        # 10239 bytes = 9.999 KB -> 0_10kb
        # 10240 bytes = 10.0 KB -> 10_50kb
        assert result["result_size_histogram"]["0_10kb"] == 1
        assert result["result_size_histogram"]["10_50kb"] == 1

    def test_boundary_50kb_categorized_correctly(self):
        """Verify 50 KB boundary is categorized correctly."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 51199},  # Just under 50 KB
            {"tool_name": "Read", "result_size_bytes": 51200},  # Exactly 50 KB
        ])

        # 51199 bytes = 49.999 KB -> 10_50kb
        # 51200 bytes = 50.0 KB -> 50_100kb
        assert result["result_size_histogram"]["10_50kb"] == 1
        assert result["result_size_histogram"]["50_100kb"] == 1

    def test_boundary_100kb_categorized_correctly(self):
        """Verify 100 KB boundary is categorized correctly."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 102399},  # Just under 100 KB
            {"tool_name": "Read", "result_size_bytes": 102400},  # Exactly 100 KB
        ])

        # 102399 bytes = 99.999 KB -> 50_100kb
        # 102400 bytes = 100.0 KB -> 100kb_plus
        assert result["result_size_histogram"]["50_100kb"] == 1
        assert result["result_size_histogram"]["100kb_plus"] == 1

    def test_unknown_tool_name_handled(self):
        """Verify unknown tool names are handled gracefully."""
        result = analyze_session_tool_result_size_distribution([
            {"result_size_bytes": 5120},  # No tool_name
        ])

        assert "unknown" in result["tools_by_avg_result_size"]

    def test_turn_index_fallback(self):
        """Verify turn index fallback when not provided."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 102401},  # >100 KB to be oversized
        ])

        # Should use record index as fallback
        assert result["oversized_results"][0]["turn_index"] == 0

    def test_mixed_tool_types(self):
        """Verify multiple tool types are tracked correctly."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 10240},
            {"tool_name": "Grep", "result_size_bytes": 5120},
            {"tool_name": "Bash", "result_size_bytes": 15360},
            {"tool_name": "Edit", "result_size_bytes": 2048},
            {"tool_name": "Write", "result_size_bytes": 3072},
        ])

        assert len(result["tools_by_avg_result_size"]) == 5
        assert "Read" in result["tools_by_avg_result_size"]
        assert "Grep" in result["tools_by_avg_result_size"]
        assert "Bash" in result["tools_by_avg_result_size"]

    def test_large_dataset_performance(self):
        """Verify analyzer handles large datasets efficiently."""
        # Create 1000 results
        records = [
            {"tool_name": f"Tool{i % 10}", "result_size_bytes": (i % 50) * 1024}
            for i in range(1000)
        ]

        result = analyze_session_tool_result_size_distribution(records)

        assert result["total_results"] == 1000
        assert len(result["tools_by_avg_result_size"]) == 10

    def test_percentile_edge_case_single_result(self):
        """Verify percentile calculation with single result."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 10240},
        ])

        assert result["p95_result_size"] == 10.0
        assert result["median_result_size"] == 10.0

    def test_all_oversized_results(self):
        """Verify handling when all results are oversized."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 150000},
            {"tool_name": "Bash", "result_size_bytes": 200000},
            {"tool_name": "Grep", "result_size_bytes": 180000},
        ])

        assert result["result_size_histogram"]["100kb_plus"] == 3
        assert len(result["oversized_results"]) == 3

    def test_all_small_results(self):
        """Verify handling when all results are small."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Edit", "result_size_bytes": 1024},
            {"tool_name": "Write", "result_size_bytes": 2048},
            {"tool_name": "Read", "result_size_bytes": 5120},
        ])

        assert result["result_size_histogram"]["0_10kb"] == 3
        assert len(result["oversized_results"]) == 0

    def test_rounding_precision(self):
        """Verify result sizes are rounded to 2 decimal places."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "Read", "result_size_bytes": 1536},  # 1.5 KB
        ])

        assert result["average_result_size"] == 1.5
        assert result["median_result_size"] == 1.5

    def test_tool_ranking_stability(self):
        """Verify tool ranking is consistent and sorted."""
        result = analyze_session_tool_result_size_distribution([
            {"tool_name": "A", "result_size_bytes": 10240},
            {"tool_name": "B", "result_size_bytes": 30720},
            {"tool_name": "C", "result_size_bytes": 20480},
        ])

        tools = list(result["tools_by_avg_result_size"].keys())
        # Should be sorted by size descending: B (30), C (20), A (10)
        assert tools == ["B", "C", "A"]
