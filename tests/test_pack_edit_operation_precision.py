"""Tests for pack edit operation precision analyzer."""

import pytest

from synthesis.pack_edit_operation_precision import analyze_pack_edit_operation_precision


class TestAnalyzePackEditOperationPrecision:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_edit_operation_precision([])

        assert result["total_packs"] == 0
        assert result["avg_edit_success_rate"] == 0.0
        assert result["avg_lines_per_edit"] == 0.0
        assert result["avg_precision_ratio"] == 0.0
        assert result["small_edit_percentage"] == 0.0
        assert result["medium_edit_percentage"] == 0.0
        assert result["large_edit_percentage"] == 0.0
        assert result["high_precision_packs"] == 0
        assert result["low_precision_packs"] == 0
        assert result["size_failure_correlation"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_edit_operation_precision(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_edit_operation_precision("not a list")

    def test_single_pack_high_success_rate(self):
        """Verify pack with high edit success rate."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 10,
                "successful_edits": 9,
                "failed_edits": 1,
                "total_lines_changed": 100,
                "small_edits": 7,
                "medium_edits": 2,
                "large_edits": 1,
            }
        ])

        assert result["total_packs"] == 1
        # 9/10 = 90%
        assert result["avg_edit_success_rate"] == 90.0
        # 100 lines / 10 edits = 10 lines per edit
        assert result["avg_lines_per_edit"] == 10.0
        # 7/10 = 70% small edits
        assert result["avg_precision_ratio"] == 70.0

    def test_single_pack_low_success_rate(self):
        """Verify pack with low edit success rate."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 10,
                "successful_edits": 3,
                "failed_edits": 7,
            }
        ])

        # 3/10 = 30%
        assert result["avg_edit_success_rate"] == 30.0

    def test_edit_size_distribution(self):
        """Verify edit size distribution calculation."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 10,
                "small_edits": 6,
                "medium_edits": 3,
                "large_edits": 1,
            }
        ])

        # 6/10 = 60%
        assert result["small_edit_percentage"] == 60.0
        # 3/10 = 30%
        assert result["medium_edit_percentage"] == 30.0
        # 1/10 = 10%
        assert result["large_edit_percentage"] == 10.0

    def test_multiple_packs_averages(self):
        """Verify averages calculated across multiple packs."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 10,
                "successful_edits": 9,
                "total_lines_changed": 50,
                "small_edits": 8,
            },
            {
                "pack_id": "pack2",
                "total_edits": 10,
                "successful_edits": 7,
                "total_lines_changed": 100,
                "small_edits": 6,
            },
        ])

        # (90% + 70%) / 2 = 80%
        assert result["avg_edit_success_rate"] == 80.0
        # (5 + 10) / 2 = 7.5 lines per edit
        assert result["avg_lines_per_edit"] == 7.5
        # (80% + 60%) / 2 = 70%
        assert result["avg_precision_ratio"] == 70.0

    def test_high_precision_pack_classification(self):
        """Verify high precision pack classification (>80% small edits)."""
        result = analyze_pack_edit_operation_precision([
            # High precision
            {"pack_id": "p1", "total_edits": 10, "small_edits": 9},
            # Medium precision
            {"pack_id": "p2", "total_edits": 10, "small_edits": 5},
            # High precision
            {"pack_id": "p3", "total_edits": 10, "small_edits": 10},
        ])

        assert result["high_precision_packs"] == 2

    def test_low_precision_pack_classification(self):
        """Verify low precision pack classification (>50% large edits)."""
        result = analyze_pack_edit_operation_precision([
            # Low precision (large edits dominate)
            {"pack_id": "p1", "total_edits": 10, "large_edits": 6},
            # High precision
            {"pack_id": "p2", "total_edits": 10, "large_edits": 1},
            # Low precision
            {"pack_id": "p3", "total_edits": 10, "large_edits": 8},
        ])

        assert result["low_precision_packs"] == 2

    def test_lines_per_edit_calculation(self):
        """Verify lines per edit calculation."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 5,
                "total_lines_changed": 100,
            }
        ])

        # 100 lines / 5 edits = 20 lines per edit
        assert result["avg_lines_per_edit"] == 20.0

    def test_precision_ratio_calculation(self):
        """Verify precision ratio (small edits percentage)."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 20,
                "small_edits": 15,
            }
        ])

        # 15/20 = 75%
        assert result["avg_precision_ratio"] == 75.0

    def test_size_failure_correlation_positive(self):
        """Verify positive correlation (larger edits fail more)."""
        result = analyze_pack_edit_operation_precision([
            # Small edits, low failure rate
            {
                "pack_id": "p1",
                "total_edits": 10,
                "successful_edits": 9,
                "failed_edits": 1,
                "total_lines_changed": 50,
            },
            # Medium edits, medium failure rate
            {
                "pack_id": "p2",
                "total_edits": 10,
                "successful_edits": 7,
                "failed_edits": 3,
                "total_lines_changed": 100,
            },
            # Large edits, high failure rate
            {
                "pack_id": "p3",
                "total_edits": 10,
                "successful_edits": 5,
                "failed_edits": 5,
                "total_lines_changed": 200,
            },
        ])

        # Positive correlation: larger edits have higher failure rate
        assert result["size_failure_correlation"] > 0

    def test_size_failure_correlation_insufficient_data(self):
        """Verify correlation returns 0 with insufficient data."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "p1",
                "total_edits": 10,
                "successful_edits": 9,
                "failed_edits": 1,
                "total_lines_changed": 50,
            },
        ])

        # Only one data point, no correlation
        assert result["size_failure_correlation"] == 0.0

    def test_zero_denominator_in_success_rate(self):
        """Verify zero total edits handled in success rate."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 0,
                "successful_edits": 0,
            }
        ])

        assert result["avg_edit_success_rate"] == 0.0

    def test_zero_denominator_in_precision(self):
        """Verify zero total edits handled in precision ratio."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 0,
                "small_edits": 0,
            }
        ])

        assert result["avg_precision_ratio"] == 0.0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 10,
                # Missing other fields
            }
        ])

        assert result["total_packs"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_edit_operation_precision([
            "not a dict",
            {"pack_id": "pack1", "total_edits": 10},
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for integer fields."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": True,
                "successful_edits": False,
            }
        ])

        assert result["avg_edit_success_rate"] == 0.0

    def test_optimal_pattern_precise_edits_high_success(self):
        """Verify optimal pattern with precise edits and high success rate."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 20,
                "successful_edits": 19,
                "failed_edits": 1,
                "total_lines_changed": 100,
                "small_edits": 18,
                "medium_edits": 2,
                "large_edits": 0,
            }
        ])

        # High success rate
        assert result["avg_edit_success_rate"] == 95.0
        # Small lines per edit (5 lines average)
        assert result["avg_lines_per_edit"] == 5.0
        # High precision (90% small edits)
        assert result["avg_precision_ratio"] == 90.0
        assert result["high_precision_packs"] == 1

    def test_anti_pattern_large_replacements_low_success(self):
        """Verify anti-pattern with large replacements and low success rate."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 10,
                "successful_edits": 4,
                "failed_edits": 6,
                "total_lines_changed": 500,
                "small_edits": 1,
                "medium_edits": 2,
                "large_edits": 7,
            }
        ])

        # Low success rate
        assert result["avg_edit_success_rate"] == 40.0
        # Large lines per edit (50 lines average)
        assert result["avg_lines_per_edit"] == 50.0
        # Low precision (10% small edits)
        assert result["avg_precision_ratio"] == 10.0
        assert result["low_precision_packs"] == 1

    def test_edit_size_distribution_across_packs(self):
        """Verify edit size distribution aggregated across packs."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "p1",
                "small_edits": 10,
                "medium_edits": 5,
                "large_edits": 2,
            },
            {
                "pack_id": "p2",
                "small_edits": 8,
                "medium_edits": 3,
                "large_edits": 1,
            },
        ])

        # Total: 18 small, 8 medium, 3 large = 29 total
        # 18/29 = 62.07%
        assert result["small_edit_percentage"] == 62.07
        # 8/29 = 27.59%
        assert result["medium_edit_percentage"] == 27.59
        # 3/29 = 10.34%
        assert result["large_edit_percentage"] == 10.34

    def test_perfect_success_rate(self):
        """Verify 100% success rate calculation."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 10,
                "successful_edits": 10,
                "failed_edits": 0,
            }
        ])

        assert result["avg_edit_success_rate"] == 100.0

    def test_zero_success_rate(self):
        """Verify 0% success rate calculation."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "pack1",
                "total_edits": 10,
                "successful_edits": 0,
                "failed_edits": 10,
            }
        ])

        assert result["avg_edit_success_rate"] == 0.0

    def test_correlation_no_variance(self):
        """Verify correlation with no variance returns 0."""
        result = analyze_pack_edit_operation_precision([
            {
                "pack_id": "p1",
                "total_edits": 10,
                "failed_edits": 2,
                "total_lines_changed": 100,
            },
            {
                "pack_id": "p2",
                "total_edits": 10,
                "failed_edits": 2,
                "total_lines_changed": 100,
            },
        ])

        # No variance in either variable
        assert result["size_failure_correlation"] == 0.0

    def test_mixed_precision_patterns(self):
        """Verify mixed precision patterns across packs."""
        result = analyze_pack_edit_operation_precision([
            # High precision
            {"pack_id": "p1", "total_edits": 10, "small_edits": 9, "large_edits": 1},
            # Low precision
            {"pack_id": "p2", "total_edits": 10, "small_edits": 2, "large_edits": 8},
            # Medium precision
            {"pack_id": "p3", "total_edits": 10, "small_edits": 5, "large_edits": 3},
        ])

        assert result["high_precision_packs"] == 1
        assert result["low_precision_packs"] == 1
