"""Tests for session Read strategy analyzer."""

import pytest

from synthesis.session_read_strategy import analyze_session_read_strategy


class TestAnalyzeSessionReadStrategy:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_read_strategy([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_reads"] == 0
        assert result["avg_read_calls"] == 0.0
        assert result["avg_targeted_read_ratio"] == 0.0
        assert result["avg_lines_per_read"] == 0.0
        assert result["avg_reread_frequency"] == 0.0
        assert result["avg_read_after_edit_ratio"] == 0.0
        assert result["read_efficiency_score"] == 0.0
        assert result["high_efficiency_sessions"] == 0
        assert result["low_efficiency_sessions"] == 0
        assert result["baseline_mode_sessions"] == 0
        assert result["optimized_mode_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_read_strategy(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_read_strategy("not a list")

    def test_session_with_no_read_calls(self):
        """Verify session with zero Read calls handled gracefully."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_reads"] == 0

    def test_optimized_mode_high_efficiency(self):
        """Verify optimized mode with high targeted reads and low re-reads."""
        result = analyze_session_read_strategy([
            {
                "session_id": "optimized",
                "total_read_calls": 100,
                "targeted_reads": 87,
                "full_file_reads": 13,
                "total_lines_read": 6400,  # 64 lines avg
                "reread_calls": 20,  # 20% re-read
                "read_after_edit_calls": 15,
                "total_edit_calls": 20,  # 75% Read-after-Edit
            }
        ])

        assert result["sessions_with_reads"] == 1
        assert result["avg_read_calls"] == 100.0
        # 87 / 100 = 87%
        assert result["avg_targeted_read_ratio"] == 87.0
        # 6400 / 100 = 64 lines
        assert result["avg_lines_per_read"] == 64.0
        # 20 / 100 = 20%
        assert result["avg_reread_frequency"] == 20.0
        # 15 / 20 = 75%
        assert result["avg_read_after_edit_ratio"] == 75.0
        # Should have high efficiency score
        assert result["read_efficiency_score"] > 80.0
        assert result["high_efficiency_sessions"] == 1
        assert result["optimized_mode_sessions"] == 1
        assert result["baseline_mode_sessions"] == 0

    def test_baseline_mode_low_efficiency(self):
        """Verify baseline mode with low targeted reads and high re-reads."""
        result = analyze_session_read_strategy([
            {
                "session_id": "baseline",
                "total_read_calls": 50,
                "targeted_reads": 10,
                "full_file_reads": 40,
                "total_lines_read": 11850,  # 237 lines avg
                "reread_calls": 40,  # 80% re-read
                "read_after_edit_calls": 3,
                "total_edit_calls": 20,  # 15% Read-after-Edit
            }
        ])

        assert result["sessions_with_reads"] == 1
        # 10 / 50 = 20%
        assert result["avg_targeted_read_ratio"] == 20.0
        # 11850 / 50 = 237 lines
        assert result["avg_lines_per_read"] == 237.0
        # 40 / 50 = 80%
        assert result["avg_reread_frequency"] == 80.0
        # 3 / 20 = 15%
        assert result["avg_read_after_edit_ratio"] == 15.0
        # Should have low efficiency score
        assert result["read_efficiency_score"] < 50.0
        assert result["low_efficiency_sessions"] == 1
        assert result["baseline_mode_sessions"] == 1
        assert result["optimized_mode_sessions"] == 0

    def test_targeted_ratio_from_targeted_field(self):
        """Verify targeted ratio calculated from targeted_reads field."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": 100,
                "targeted_reads": 85,
            }
        ])

        # 85 / 100 = 85%
        assert result["avg_targeted_read_ratio"] == 85.0

    def test_targeted_ratio_inferred_from_full_file(self):
        """Verify targeted ratio inferred from full_file_reads when targeted not provided."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": 100,
                "full_file_reads": 25,
            }
        ])

        # (100 - 25) / 100 = 75%
        assert result["avg_targeted_read_ratio"] == 75.0

    def test_lines_per_read_from_field(self):
        """Verify lines per read used from field when available."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": 50,
                "avg_lines_per_read": 64.5,
            }
        ])

        assert result["avg_lines_per_read"] == 64.5

    def test_lines_per_read_calculated(self):
        """Verify lines per read calculated when not provided."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": 50,
                "total_lines_read": 3500,
            }
        ])

        # 3500 / 50 = 70.0
        assert result["avg_lines_per_read"] == 70.0

    def test_reread_frequency_calculation(self):
        """Verify re-read frequency calculated correctly."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": 100,
                "reread_calls": 25,
            }
        ])

        # 25 / 100 = 25%
        assert result["avg_reread_frequency"] == 25.0

    def test_read_after_edit_ratio_calculation(self):
        """Verify Read-after-Edit ratio calculated correctly."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": 50,
                "read_after_edit_calls": 12,
                "total_edit_calls": 20,
            }
        ])

        # 12 / 20 = 60%
        assert result["avg_read_after_edit_ratio"] == 60.0

    def test_read_after_edit_no_edits(self):
        """Verify Read-after-Edit not calculated when no edits."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": 50,
                "read_after_edit_calls": 0,
                "total_edit_calls": 0,
            }
        ])

        assert result["avg_read_after_edit_ratio"] == 0.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": 100,
                "targeted_reads": 90,
                "total_lines_read": 6000,
            },
            {
                "session_id": "session2",
                "total_read_calls": 50,
                "targeted_reads": 40,
                "total_lines_read": 4000,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_reads"] == 2
        # (100 + 50) / 2 = 75
        assert result["avg_read_calls"] == 75.0
        # (90% + 80%) / 2 = 85%
        assert result["avg_targeted_read_ratio"] == 85.0
        # (60 + 80) / 2 = 70
        assert result["avg_lines_per_read"] == 70.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_read_strategy([
            "not a dict",
            {
                "session_id": "session1",
                "total_read_calls": 50,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": True,
                "targeted_reads": False,
            }
        ])

        assert result["sessions_with_reads"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_read_strategy([
            {
                "session_id": "session1",
                "total_read_calls": 50,
                # Missing most fields
            }
        ])

        assert result["sessions_with_reads"] == 1
        assert result["avg_read_calls"] == 50.0
        # Missing fields result in 0.0 averages
        assert result["avg_targeted_read_ratio"] == 0.0

    def test_boundary_efficiency_classification(self):
        """Verify boundary cases for efficiency classification."""
        result = analyze_session_read_strategy([
            # Exactly 80: 30 (targeted) + 20 (lines) + 15 (reread) + 15 (verification) = 80
            {
                "session_id": "s1",
                "total_read_calls": 50,
                "targeted_reads": 36,  # 72% (30pts)
                "total_lines_read": 4500,  # 90 lines (20pts)
                "reread_calls": 24,  # 48% (15pts)
                "read_after_edit_calls": 13,
                "total_edit_calls": 20,  # 65% (15pts)
                # Total: 80pts
            },
            # Just above 80: 40 (targeted) + 25 (lines) + 20 (reread) + 0 (verification) = 85
            {
                "session_id": "s2",
                "total_read_calls": 50,
                "targeted_reads": 43,  # 86% (40pts)
                "total_lines_read": 3450,  # 69 lines (25pts)
                "reread_calls": 14,  # 28% (20pts)
                # Missing verification = 0pts
                # Total: 85pts
            },
            # Exactly 50: 20 (targeted) + 15 (lines) + 15 (reread) + 0 (verification) = 50
            {
                "session_id": "s3",
                "total_read_calls": 50,
                "targeted_reads": 26,  # 52% (20pts)
                "total_lines_read": 7000,  # 140 lines (15pts)
                "reread_calls": 24,  # 48% (15pts)
                # Missing verification = 0pts
                # Total: 50pts
            },
            # Below 50: 10 (targeted) + 10 (lines) + 0 (reread) + 5 (verification) = 25
            {
                "session_id": "s4",
                "total_read_calls": 50,
                "targeted_reads": 18,  # 36% (10pts)
                "total_lines_read": 9500,  # 190 lines (10pts)
                "reread_calls": 40,  # 80% (0pts)
                "read_after_edit_calls": 6,
                "total_edit_calls": 20,  # 30% (5pts)
                # Total: 25pts
            },
        ])

        # >80 means strictly greater
        assert result["high_efficiency_sessions"] == 1
        # <50 means strictly less
        assert result["low_efficiency_sessions"] == 1

    def test_baseline_mode_classification(self):
        """Verify baseline mode classification (<30% targeted)."""
        result = analyze_session_read_strategy([
            {
                "session_id": "baseline",
                "total_read_calls": 100,
                "targeted_reads": 25,  # 25% targeted
            }
        ])

        assert result["baseline_mode_sessions"] == 1
        assert result["optimized_mode_sessions"] == 0

    def test_optimized_mode_classification(self):
        """Verify optimized mode classification (>85% targeted)."""
        result = analyze_session_read_strategy([
            {
                "session_id": "optimized",
                "total_read_calls": 100,
                "targeted_reads": 90,  # 90% targeted
            }
        ])

        assert result["baseline_mode_sessions"] == 0
        assert result["optimized_mode_sessions"] == 1

    def test_mixed_mode_classification(self):
        """Verify sessions between 30-85% targeted are neither baseline nor optimized."""
        result = analyze_session_read_strategy([
            {
                "session_id": "mixed",
                "total_read_calls": 100,
                "targeted_reads": 60,  # 60% targeted
            }
        ])

        assert result["baseline_mode_sessions"] == 0
        assert result["optimized_mode_sessions"] == 0

    def test_efficiency_score_excellent_all_metrics(self):
        """Verify efficiency score with excellent metrics."""
        result = analyze_session_read_strategy([
            {
                "session_id": "excellent",
                "total_read_calls": 100,
                "targeted_reads": 90,  # 90% (40pts)
                "total_lines_read": 6000,  # 60 lines (25pts)
                "reread_calls": 20,  # 20% (20pts)
                "read_after_edit_calls": 15,
                "total_edit_calls": 20,  # 75% (15pts)
            }
        ])

        # Should score: 40 + 25 + 20 + 15 = 100
        assert result["read_efficiency_score"] == 100.0
        assert result["high_efficiency_sessions"] == 1

    def test_efficiency_score_poor_all_metrics(self):
        """Verify efficiency score with poor metrics."""
        result = analyze_session_read_strategy([
            {
                "session_id": "poor",
                "total_read_calls": 50,
                "targeted_reads": 10,  # 20% (0pts)
                "total_lines_read": 12500,  # 250 lines (0pts)
                "reread_calls": 40,  # 80% (0pts)
                "read_after_edit_calls": 2,
                "total_edit_calls": 20,  # 10% (0pts)
            }
        ])

        # Should score: 0 + 0 + 0 + 0 = 0
        assert result["read_efficiency_score"] == 0.0
        assert result["low_efficiency_sessions"] == 1

    def test_efficiency_score_mixed_metrics(self):
        """Verify efficiency score with mixed quality metrics."""
        result = analyze_session_read_strategy([
            {
                "session_id": "mixed",
                "total_read_calls": 100,
                "targeted_reads": 75,  # 75% (30pts)
                "total_lines_read": 9500,  # 95 lines (20pts)
                "reread_calls": 45,  # 45% (15pts)
                "read_after_edit_calls": 9,
                "total_edit_calls": 20,  # 45% (10pts)
            }
        ])

        # Should score: 30 + 20 + 15 + 10 = 75
        assert result["read_efficiency_score"] == 75.0

    def test_efficiency_score_only_targeted_data(self):
        """Verify efficiency score with only targeted read data."""
        result = analyze_session_read_strategy([
            {
                "session_id": "targeted_only",
                "total_read_calls": 100,
                "targeted_reads": 90,  # 90% (40pts)
            }
        ])

        # Should score: 40 + 0 + 0 + 0 = 40
        assert result["read_efficiency_score"] == 40.0

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_read_strategy([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_read_calls": 150,
                "targeted_reads": 135,
                "full_file_reads": 15,
                "total_lines_read": 9750,
                "reread_calls": 30,
                "unique_files_read": 25,
                "read_after_edit_calls": 20,
                "total_edit_calls": 30,
                "avg_lines_per_read": 65.0,
            }
        ])

        assert result["sessions_with_reads"] == 1
        assert result["avg_read_calls"] == 150.0
        # 135 / 150 = 90%
        assert result["avg_targeted_read_ratio"] == 90.0
        # avg_lines_per_read provided
        assert result["avg_lines_per_read"] == 65.0
        # 30 / 150 = 20%
        assert result["avg_reread_frequency"] == 20.0
        # 20 / 30 = 66.67%
        assert 66.0 <= result["avg_read_after_edit_ratio"] <= 67.0
        # Should have high efficiency
        assert result["read_efficiency_score"] > 80.0
        assert result["optimized_mode_sessions"] == 1
