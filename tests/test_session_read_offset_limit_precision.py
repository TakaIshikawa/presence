"""Tests for session Read tool offset/limit precision analyzer."""

import pytest

from synthesis.session_read_offset_limit_precision import (
    analyze_session_read_offset_limit_precision,
)


class TestAnalyzeSessionReadOffsetLimitPrecision:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_read_offset_limit_precision([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_reads"] == 0
        assert result["avg_read_calls"] == 0.0
        assert result["avg_offset_limit_usage_rate"] == 0.0
        assert result["avg_limit_value"] == 0.0
        assert result["avg_negative_offset_rate"] == 0.0
        assert result["avg_lines_read_per_call"] == 0.0
        assert result["avg_targeted_read_rate"] == 0.0
        assert result["avg_full_file_read_rate"] == 0.0
        assert result["high_precision_sessions"] == 0
        assert result["low_precision_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_read_offset_limit_precision(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_read_offset_limit_precision("not a list")

    def test_session_with_no_read_calls(self):
        """Verify session with zero Read calls handled gracefully."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_reads"] == 0
        assert result["avg_read_calls"] == 0.0

    def test_high_precision_all_targeted_reads(self):
        """Verify high precision session with all targeted reads."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 20,
                "reads_with_limit": 20,
                "total_limit_value": 1000,
                "reads_with_negative_offset": 5,
                "total_lines_read": 1200,
                "targeted_reads": 20,
                "full_file_reads": 0,
            }
        ])

        assert result["sessions_with_reads"] == 1
        assert result["avg_read_calls"] == 20.0
        assert result["avg_offset_limit_usage_rate"] == 100.0
        assert result["avg_limit_value"] == 50.0
        assert result["avg_negative_offset_rate"] == 25.0
        assert result["avg_lines_read_per_call"] == 60.0
        assert result["avg_targeted_read_rate"] == 100.0
        assert result["avg_full_file_read_rate"] == 0.0
        assert result["high_precision_sessions"] == 1
        assert result["low_precision_sessions"] == 0

    def test_low_precision_all_full_file_reads(self):
        """Verify low precision session with all full-file reads."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 10,
                "reads_with_offset_or_limit": 0,
                "reads_with_limit": 0,
                "total_limit_value": 0,
                "reads_with_negative_offset": 0,
                "total_lines_read": 5000,
                "targeted_reads": 0,
                "full_file_reads": 10,
            }
        ])

        assert result["avg_offset_limit_usage_rate"] == 0.0
        assert result["avg_limit_value"] == 0.0
        assert result["avg_negative_offset_rate"] == 0.0
        assert result["avg_lines_read_per_call"] == 500.0
        assert result["avg_targeted_read_rate"] == 0.0
        assert result["avg_full_file_read_rate"] == 100.0
        assert result["high_precision_sessions"] == 0
        assert result["low_precision_sessions"] == 1

    def test_mixed_read_patterns(self):
        """Verify mixed targeted and full-file reads."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 15,
                "reads_with_limit": 12,
                "total_limit_value": 600,
                "reads_with_negative_offset": 3,
                "total_lines_read": 2000,
                "targeted_reads": 12,
                "full_file_reads": 5,
            }
        ])

        # 15 with offset/limit / 20 total = 75%
        assert result["avg_offset_limit_usage_rate"] == 75.0
        # 600 / 12 = 50 average limit
        assert result["avg_limit_value"] == 50.0
        # 3 / 20 = 15%
        assert result["avg_negative_offset_rate"] == 15.0
        # 2000 / 20 = 100 lines per call
        assert result["avg_lines_read_per_call"] == 100.0
        # 12 / 20 = 60%
        assert result["avg_targeted_read_rate"] == 60.0
        # 5 / 20 = 25%
        assert result["avg_full_file_read_rate"] == 25.0

    def test_high_negative_offset_usage(self):
        """Verify high negative offset usage (tail reads)."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 10,
                "reads_with_offset_or_limit": 10,
                "reads_with_negative_offset": 8,
                "total_lines_read": 300,
            }
        ])

        # 8 / 10 = 80% negative offset
        assert result["avg_negative_offset_rate"] == 80.0

    def test_large_limit_values(self):
        """Verify handling of large limit values."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 5,
                "reads_with_limit": 5,
                "total_limit_value": 5000,
            }
        ])

        # 5000 / 5 = 1000 average limit
        assert result["avg_limit_value"] == 1000.0

    def test_small_targeted_reads(self):
        """Verify small targeted reads with low limits."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 20,
                "reads_with_limit": 20,
                "total_limit_value": 600,
                "targeted_reads": 20,
                "total_lines_read": 600,
            }
        ])

        # 600 / 20 = 30 average limit
        assert result["avg_limit_value"] == 30.0
        # 600 / 20 = 30 lines per call
        assert result["avg_lines_read_per_call"] == 30.0
        assert result["avg_targeted_read_rate"] == 100.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 10,
                "reads_with_offset_or_limit": 10,
                "targeted_reads": 10,
            },
            {
                "session_id": "session2",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 10,
                "targeted_reads": 10,
            },
            {
                "session_id": "session3",
                "total_read_calls": 30,
                "reads_with_offset_or_limit": 20,
                "targeted_reads": 15,
            },
        ])

        assert result["total_sessions"] == 3
        assert result["sessions_with_reads"] == 3
        # (10 + 20 + 30) / 3 = 20.0
        assert result["avg_read_calls"] == 20.0
        # (100% + 50% + 66.67%) / 3 = 72.22%
        assert 72.0 <= result["avg_offset_limit_usage_rate"] <= 73.0
        # (100% + 50% + 50%) / 3 = 66.67%
        assert 66.0 <= result["avg_targeted_read_rate"] <= 67.0

    def test_high_precision_classification(self):
        """Verify high precision classification (>85% offset/limit usage)."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "s1",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 18,
            },
            {
                "session_id": "s2",
                "total_read_calls": 10,
                "reads_with_offset_or_limit": 9,
            },
        ])

        # s1: 18/20 = 90% (high)
        # s2: 9/10 = 90% (high)
        assert result["high_precision_sessions"] == 2
        assert result["low_precision_sessions"] == 0

    def test_low_precision_classification(self):
        """Verify low precision classification (<50% offset/limit usage)."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "s1",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 5,
            },
            {
                "session_id": "s2",
                "total_read_calls": 10,
                "reads_with_offset_or_limit": 2,
            },
        ])

        # s1: 5/20 = 25% (low)
        # s2: 2/10 = 20% (low)
        assert result["high_precision_sessions"] == 0
        assert result["low_precision_sessions"] == 2

    def test_medium_precision_not_classified(self):
        """Verify medium precision not classified as high or low."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 14,
            }
        ])

        # 14/20 = 70% (between 50% and 85%)
        assert result["avg_offset_limit_usage_rate"] == 70.0
        assert result["high_precision_sessions"] == 0
        assert result["low_precision_sessions"] == 0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_read_offset_limit_precision([
            "not a dict",
            {
                "session_id": "session1",
                "total_read_calls": 10,
            },
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_reads"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for integer fields."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": True,
                "reads_with_offset_or_limit": False,
            }
        ])

        assert result["sessions_with_reads"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 10,
                # Missing all other fields
            }
        ])

        assert result["sessions_with_reads"] == 1
        assert result["avg_read_calls"] == 10.0
        # Missing fields result in 0.0 averages
        assert result["avg_offset_limit_usage_rate"] == 0.0

    def test_zero_reads_with_limit(self):
        """Verify division by zero handled when no limit reads."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "session1",
                "total_read_calls": 10,
                "reads_with_limit": 0,
                "total_limit_value": 0,
            }
        ])

        # No limit reads, so no average calculated
        assert result["avg_limit_value"] == 0.0

    def test_boundary_precision_classification(self):
        """Verify boundary cases for precision classification."""
        result = analyze_session_read_offset_limit_precision([
            # Exactly 85% (should not be high)
            {
                "session_id": "s1",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 17,
            },
            # Just above 85% (should be high)
            {
                "session_id": "s2",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 18,
            },
            # Exactly 50% (should not be low)
            {
                "session_id": "s3",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 10,
            },
            # Just below 50% (should be low)
            {
                "session_id": "s4",
                "total_read_calls": 20,
                "reads_with_offset_or_limit": 9,
            },
        ])

        # >85% means strictly greater
        assert result["high_precision_sessions"] == 1
        # <50% means strictly less
        assert result["low_precision_sessions"] == 1

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_read_calls": 50,
                "reads_with_offset_or_limit": 45,
                "reads_with_limit": 40,
                "total_limit_value": 2000,
                "reads_with_negative_offset": 10,
                "total_lines_read": 3000,
                "targeted_reads": 38,
                "full_file_reads": 5,
            }
        ])

        assert result["sessions_with_reads"] == 1
        assert result["avg_read_calls"] == 50.0
        # 45 / 50 = 90%
        assert result["avg_offset_limit_usage_rate"] == 90.0
        # 2000 / 40 = 50
        assert result["avg_limit_value"] == 50.0
        # 10 / 50 = 20%
        assert result["avg_negative_offset_rate"] == 20.0
        # 3000 / 50 = 60
        assert result["avg_lines_read_per_call"] == 60.0
        # 38 / 50 = 76%
        assert result["avg_targeted_read_rate"] == 76.0
        # 5 / 50 = 10%
        assert result["avg_full_file_read_rate"] == 10.0
        assert result["high_precision_sessions"] == 1

    def test_optimal_pattern_high_targeted_low_full_file(self):
        """Verify optimal read pattern with high targeted, low full-file."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "optimal",
                "total_read_calls": 100,
                "reads_with_offset_or_limit": 95,
                "reads_with_limit": 90,
                "total_limit_value": 4500,
                "reads_with_negative_offset": 25,
                "total_lines_read": 6000,
                "targeted_reads": 90,
                "full_file_reads": 5,
            }
        ])

        # 95% offset/limit usage
        assert result["avg_offset_limit_usage_rate"] == 95.0
        # 4500 / 90 = 50 average limit
        assert result["avg_limit_value"] == 50.0
        # 25% negative offset
        assert result["avg_negative_offset_rate"] == 25.0
        # 60 lines average
        assert result["avg_lines_read_per_call"] == 60.0
        # 90% targeted
        assert result["avg_targeted_read_rate"] == 90.0
        # 5% full-file
        assert result["avg_full_file_read_rate"] == 5.0
        assert result["high_precision_sessions"] == 1

    def test_anti_pattern_no_precision(self):
        """Verify anti-pattern with no targeted reads."""
        result = analyze_session_read_offset_limit_precision([
            {
                "session_id": "anti-pattern",
                "total_read_calls": 50,
                "reads_with_offset_or_limit": 0,
                "reads_with_limit": 0,
                "total_limit_value": 0,
                "reads_with_negative_offset": 0,
                "total_lines_read": 25000,
                "targeted_reads": 0,
                "full_file_reads": 50,
            }
        ])

        assert result["avg_offset_limit_usage_rate"] == 0.0
        assert result["avg_limit_value"] == 0.0
        assert result["avg_negative_offset_rate"] == 0.0
        # 25000 / 50 = 500 lines average (very high)
        assert result["avg_lines_read_per_call"] == 500.0
        assert result["avg_targeted_read_rate"] == 0.0
        assert result["avg_full_file_read_rate"] == 100.0
        assert result["low_precision_sessions"] == 1
