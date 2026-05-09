"""Tests for pack Edit tool precision analyzer."""

import pytest

from synthesis.pack_edit_precision import (
    analyze_pack_edit_precision,
    _percentage,
    _average,
    _int,
)


class TestAnalyzePackEditPrecision:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_edit_precision([])

        assert result["total_packs"] == 0
        assert result["avg_total_edits"] == 0.0
        assert result["avg_edit_success_rate"] == 0.0
        assert result["avg_uniqueness_failure_rate"] == 0.0
        assert result["avg_edit_size"] == 0.0
        assert result["avg_replace_all_ratio"] == 0.0
        assert result["avg_edit_error_correlation"] == 0.0
        assert result["avg_verification_pass_rate"] == 0.0
        assert result["high_precision_packs"] == 0
        assert result["low_precision_packs"] == 0
        assert result["edit_size_distribution"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_edit_precision(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_edit_precision("not a list")

    def test_successful_edit(self):
        """Verify successful edit is tracked correctly."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {
                        "tool_name": "Edit",
                        "file_path": "/test.py",
                        "old_string_length": 50,
                        "new_string_length": 60,
                        "replace_all": False,
                        "outcome": "success",
                        "subsequent_error": False,
                        "verification_passed": True,
                    }
                ]
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_edits"] == 1.0
        assert result["avg_edit_success_rate"] == 100.0
        assert result["avg_uniqueness_failure_rate"] == 0.0
        assert result["avg_edit_size"] == 50.0
        assert result["avg_replace_all_ratio"] == 0.0
        assert result["avg_edit_error_correlation"] == 0.0
        assert result["avg_verification_pass_rate"] == 100.0
        assert result["high_precision_packs"] == 1

    def test_uniqueness_failure_detected(self):
        """Verify uniqueness failure is detected."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {
                        "tool_name": "Edit",
                        "old_string_length": 30,
                        "outcome": "uniqueness_failure",
                        "subsequent_error": False,
                    }
                ]
            }
        ])

        assert result["avg_edit_success_rate"] == 0.0
        assert result["avg_uniqueness_failure_rate"] == 100.0

    def test_replace_all_usage_tracked(self):
        """Verify replace_all usage is tracked."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {
                        "tool_name": "Edit",
                        "old_string_length": 40,
                        "replace_all": True,
                        "outcome": "success",
                    },
                    {
                        "tool_name": "Edit",
                        "old_string_length": 50,
                        "replace_all": False,
                        "outcome": "success",
                    }
                ]
            }
        ])

        # 1 out of 2 used replace_all
        assert result["avg_replace_all_ratio"] == 50.0

    def test_edit_induced_error_tracked(self):
        """Verify edit-induced errors are tracked."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {
                        "tool_name": "Edit",
                        "old_string_length": 60,
                        "outcome": "success",
                        "subsequent_error": True,
                    }
                ]
            }
        ])

        assert result["avg_edit_error_correlation"] == 100.0

    def test_verification_pass_rate_tracked(self):
        """Verify verification pass rate is tracked."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {
                        "tool_name": "Edit",
                        "old_string_length": 70,
                        "outcome": "success",
                        "verification_passed": True,
                    },
                    {
                        "tool_name": "Edit",
                        "old_string_length": 80,
                        "outcome": "success",
                        "verification_passed": False,
                    }
                ]
            }
        ])

        # 1 out of 2 passed verification
        assert result["avg_verification_pass_rate"] == 50.0

    def test_edit_size_distribution(self):
        """Verify edit size distribution is calculated."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {"tool_name": "Edit", "old_string_length": 30, "outcome": "success"},  # Small
                    {"tool_name": "Edit", "old_string_length": 100, "outcome": "success"},  # Medium
                    {"tool_name": "Edit", "old_string_length": 250, "outcome": "success"},  # Large
                    {"tool_name": "Edit", "old_string_length": 40, "outcome": "success"},  # Small
                ]
            }
        ])

        distribution = result["edit_size_distribution"]
        assert len(distribution) == 3

        small = next(d for d in distribution if "small" in d["category"])
        medium = next(d for d in distribution if "medium" in d["category"])
        large = next(d for d in distribution if "large" in d["category"])

        assert small["count"] == 2
        assert medium["count"] == 1
        assert large["count"] == 1
        assert small["percentage"] == 50.0
        assert medium["percentage"] == 25.0
        assert large["percentage"] == 25.0

    def test_high_precision_pack(self):
        """Verify high precision pack (>95% success) is detected."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {"tool_name": "Edit", "old_string_length": 50, "outcome": "success"}
                    for _ in range(20)
                ]
            }
        ])

        assert result["avg_edit_success_rate"] == 100.0
        assert result["high_precision_packs"] == 1
        assert result["low_precision_packs"] == 0

    def test_low_precision_pack(self):
        """Verify low precision pack (<80% success) is detected."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {"tool_name": "Edit", "old_string_length": 50, "outcome": "success"},
                    {"tool_name": "Edit", "old_string_length": 50, "outcome": "uniqueness_failure"},
                    {"tool_name": "Edit", "old_string_length": 50, "outcome": "uniqueness_failure"},
                ]
            }
        ])

        # 1 success out of 3 = 33.33%
        assert result["avg_edit_success_rate"] == pytest.approx(33.33, abs=0.01)
        assert result["high_precision_packs"] == 0
        assert result["low_precision_packs"] == 1

    def test_mixed_edit_outcomes(self):
        """Verify mixed edit outcomes are tracked correctly."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {
                        "tool_name": "Edit",
                        "old_string_length": 50,
                        "outcome": "success",
                        "verification_passed": True,
                    },
                    {
                        "tool_name": "Edit",
                        "old_string_length": 60,
                        "outcome": "uniqueness_failure",
                    },
                    {
                        "tool_name": "Edit",
                        "old_string_length": 70,
                        "outcome": "success",
                        "subsequent_error": True,
                    },
                ]
            }
        ])

        # 2 success, 1 failure = 66.67%
        assert result["avg_edit_success_rate"] == pytest.approx(66.67, abs=0.01)
        assert result["avg_uniqueness_failure_rate"] == pytest.approx(33.33, abs=0.01)
        assert result["avg_edit_error_correlation"] == pytest.approx(33.33, abs=0.01)
        assert result["avg_verification_pass_rate"] == pytest.approx(33.33, abs=0.01)

    def test_write_tool_included(self):
        """Verify Write tool events are included in analysis."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {"tool_name": "Write", "old_string_length": 100, "outcome": "success"},
                    {"tool_name": "Edit", "old_string_length": 50, "outcome": "success"},
                ]
            }
        ])

        assert result["avg_total_edits"] == 2.0
        assert result["avg_edit_size"] == 75.0  # (100 + 50) / 2

    def test_multiple_packs_aggregation(self):
        """Verify metrics are correctly aggregated across multiple packs."""
        result = analyze_pack_edit_precision([
            {
                "pack_id": "pack1",
                "edit_events": [
                    {"tool_name": "Edit", "old_string_length": 50, "outcome": "success"},
                    {"tool_name": "Edit", "old_string_length": 50, "outcome": "success"},
                ]
            },
            {
                "pack_id": "pack2",
                "edit_events": [
                    {"tool_name": "Edit", "old_string_length": 30, "outcome": "success"},
                    {"tool_name": "Edit", "old_string_length": 30, "outcome": "uniqueness_failure"},
                ]
            }
        ])

        assert result["total_packs"] == 2
        # Pack1: 100%, Pack2: 50% -> avg = 75%
        assert result["avg_edit_success_rate"] == 75.0
        # Average edit size: (50+50+30+30) / 4 = 40
        assert result["avg_edit_size"] == 40.0


class TestHelperFunctions:
    """Test helper functions."""

    def test_percentage_calculation(self):
        """Verify percentage calculation."""
        assert _percentage(50, 100) == 50.0
        assert _percentage(1, 3) == 33.33
        assert _percentage(0, 100) == 0.0

    def test_percentage_zero_denominator(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(50, 0) == 0.0

    def test_average_calculation(self):
        """Verify average calculation."""
        assert _average([1.0, 2.0, 3.0]) == 2.0
        assert _average([10.0, 20.0]) == 15.0
        assert _average([100.0]) == 100.0

    def test_average_empty_list(self):
        """Verify empty list returns 0.0."""
        assert _average([]) == 0.0

    def test_int_conversion(self):
        """Verify int conversion."""
        assert _int(42) == 42
        assert _int(42.7) == 42
        assert _int("42") == 42
        assert _int("not a number") is None
        assert _int(None) is None
        assert _int(True) is None
