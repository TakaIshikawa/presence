"""Tests for session offset/limit read accuracy analyzer."""

import pytest

from synthesis.session_offset_limit_accuracy import (
    analyze_session_offset_limit_accuracy,
    _percentage,
    _average,
    _int,
)


class TestAnalyzeSessionOffsetLimitAccuracy:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_offset_limit_accuracy([])

        assert result["total_reads"] == 0
        assert result["targeted_reads"] == 0
        assert result["coverage_accuracy"] == 0.0
        assert result["avg_read_precision"] == 0.0
        assert result["over_reading_ratio"] == 0.0
        assert result["under_reading_rate"] == 0.0
        assert result["avg_window_size"] == 0.0
        assert result["optimal_window_reads"] == 0
        assert result["excessive_window_reads"] == 0
        assert result["precision_by_purpose"] == []
        assert result["high_accuracy_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_offset_limit_accuracy(None)
        assert result["total_reads"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_offset_limit_accuracy("not a list")

    def test_accurate_targeted_read(self):
        """Verify accurate targeted read is detected."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": 1,
                "file_path": "/test.py",
                "offset": 100,
                "limit": 50,
                "lines_read": 50,
                "target_location": 120,
                "includes_target": True,
                "follow_up_read": False,
                "purpose": "error_context",
                "lines_needed": 50,
            }
        ])

        assert result["total_reads"] == 1
        assert result["targeted_reads"] == 1
        assert result["coverage_accuracy"] == 100.0
        assert result["avg_read_precision"] == 1.0  # 50/50
        assert result["over_reading_ratio"] == 0.0
        assert result["under_reading_rate"] == 0.0
        assert result["avg_window_size"] == 50.0
        assert result["optimal_window_reads"] == 1  # <70 lines
        assert result["high_accuracy_sessions"] == 1

    def test_over_reading_detected(self):
        """Verify over-reading (reading more than needed) is detected."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": 1,
                "offset": 0,
                "limit": 100,
                "lines_read": 100,
                "target_location": 10,
                "includes_target": True,
                "follow_up_read": False,
                "purpose": "edit_verification",
                "lines_needed": 20,
            }
        ])

        # Read 100 lines, needed 20 = 80 lines over-read
        assert result["avg_read_precision"] == 5.0  # 100/20
        assert result["over_reading_ratio"] == 80.0  # 80/100 * 100
        assert result["excessive_window_reads"] == 0  # 100 <= 200

    def test_under_reading_detected(self):
        """Verify under-reading (requiring follow-up) is detected."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": 1,
                "offset": 0,
                "limit": 10,
                "lines_read": 10,
                "target_location": 50,
                "includes_target": False,
                "follow_up_read": True,
                "purpose": "error_context",
                "lines_needed": 50,
            }
        ])

        assert result["coverage_accuracy"] == 0.0
        assert result["under_reading_rate"] == 100.0
        assert result["avg_read_precision"] == 0.2  # 10/50

    def test_optimal_window_size_tracking(self):
        """Verify optimal window size (<70 lines) is tracked."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": 1,
                "offset": 0,
                "limit": 30,
                "lines_read": 30,
                "includes_target": True,
                "purpose": "edit_verification",
                "lines_needed": 30,
            },
            {
                "read_index": 2,
                "offset": 0,
                "limit": 50,
                "lines_read": 50,
                "includes_target": True,
                "purpose": "error_context",
                "lines_needed": 50,
            }
        ])

        assert result["optimal_window_reads"] == 2  # Both <70 lines
        assert result["avg_window_size"] == 40.0  # (30 + 50) / 2

    def test_excessive_window_size_tracking(self):
        """Verify excessive window size (>200 lines) is tracked."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": 1,
                "offset": 0,
                "limit": 250,
                "lines_read": 250,
                "includes_target": True,
                "purpose": "exploration",
                "lines_needed": 50,
            }
        ])

        assert result["excessive_window_reads"] == 1
        assert result["avg_window_size"] == 250.0

    def test_mixed_accuracy_patterns(self):
        """Verify mixed accuracy patterns are tracked correctly."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": 1,
                "offset": 0,
                "limit": 50,
                "lines_read": 50,
                "includes_target": True,
                "purpose": "error_context",
                "lines_needed": 50,
            },
            {
                "read_index": 2,
                "offset": 100,
                "limit": 50,
                "lines_read": 50,
                "includes_target": False,
                "purpose": "error_context",
                "lines_needed": 50,
            },
            {
                "read_index": 3,
                "offset": 200,
                "limit": 50,
                "lines_read": 50,
                "includes_target": True,
                "purpose": "edit_verification",
                "lines_needed": 50,
            }
        ])

        # 2 out of 3 included target
        assert result["coverage_accuracy"] == pytest.approx(66.67, abs=0.01)
        assert result["avg_read_precision"] == 1.0  # All read exactly what was needed

    def test_precision_by_purpose_breakdown(self):
        """Verify precision is broken down by read purpose."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": 1,
                "offset": 0,
                "limit": 50,
                "lines_read": 50,
                "includes_target": True,
                "purpose": "error_context",
                "lines_needed": 50,
            },
            {
                "read_index": 2,
                "offset": 0,
                "limit": 100,
                "lines_read": 100,
                "includes_target": True,
                "purpose": "edit_verification",
                "lines_needed": 50,
            },
            {
                "read_index": 3,
                "offset": 0,
                "limit": 30,
                "lines_read": 30,
                "includes_target": False,
                "purpose": "error_context",
                "lines_needed": 30,
            }
        ])

        breakdown = result["precision_by_purpose"]
        assert len(breakdown) == 2  # error_context and edit_verification

        # Find error_context
        error_ctx = next((p for p in breakdown if p["purpose"] == "error_context"), None)
        assert error_ctx is not None
        assert error_ctx["total_reads"] == 2
        assert error_ctx["accuracy"] == 50.0  # 1 out of 2 included target
        assert error_ctx["avg_precision"] == 1.0  # (50/50 + 30/30) / 2

        # Find edit_verification
        edit_ver = next((p for p in breakdown if p["purpose"] == "edit_verification"), None)
        assert edit_ver is not None
        assert edit_ver["total_reads"] == 1
        assert edit_ver["accuracy"] == 100.0
        assert edit_ver["avg_precision"] == 2.0  # 100/50

    def test_negative_offset_read(self):
        """Verify negative offset (tail read) is counted as targeted."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": 1,
                "offset": -30,
                "limit": 30,
                "lines_read": 30,
                "includes_target": True,
                "purpose": "edit_verification",
                "lines_needed": 30,
            }
        ])

        assert result["targeted_reads"] == 1
        assert result["optimal_window_reads"] == 1

    def test_full_file_read_not_targeted(self):
        """Verify full file read (no offset/limit) is not counted as targeted."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": 1,
                "lines_read": 500,
                "includes_target": True,
                "purpose": "exploration",
                "lines_needed": 50,
            }
        ])

        assert result["total_reads"] == 1
        assert result["targeted_reads"] == 0
        assert result["avg_window_size"] == 0.0  # No limit specified

    def test_high_accuracy_threshold(self):
        """Verify high accuracy threshold (>90%) detection."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": i,
                "offset": 0,
                "limit": 50,
                "lines_read": 50,
                "includes_target": True,
                "purpose": "error_context",
                "lines_needed": 50,
            }
            for i in range(10)
        ])

        # 10 out of 10 = 100% accuracy
        assert result["coverage_accuracy"] == 100.0
        assert result["high_accuracy_sessions"] == 1

    def test_low_accuracy_below_threshold(self):
        """Verify low accuracy (<90%) is not marked as high accuracy."""
        result = analyze_session_offset_limit_accuracy([
            {
                "read_index": 1,
                "offset": 0,
                "limit": 50,
                "lines_read": 50,
                "includes_target": True,
                "purpose": "error_context",
                "lines_needed": 50,
            },
            {
                "read_index": 2,
                "offset": 100,
                "limit": 50,
                "lines_read": 50,
                "includes_target": False,
                "purpose": "error_context",
                "lines_needed": 50,
            }
        ])

        # 1 out of 2 = 50% accuracy
        assert result["coverage_accuracy"] == 50.0
        assert result["high_accuracy_sessions"] == 0


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
        assert _int(True) is None  # bool is not converted
