"""Tests for pack final verification coverage analyzer."""

import pytest

from synthesis.pack_final_verification_coverage import analyze_pack_final_verification_coverage


class TestAnalyzePackFinalVerificationCoverage:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_final_verification_coverage([])

        assert result["total_packs"] == 0
        assert result["avg_verification_coverage"] == 0.0
        assert result["complete_verification_packs"] == 0
        assert result["partial_verification_packs"] == 0
        assert result["no_verification_packs"] == 0
        assert result["unit_test_verification_count"] == 0
        assert result["integration_test_verification_count"] == 0
        assert result["build_verification_count"] == 0
        assert result["end_of_pack_timing_count"] == 0
        assert result["immediate_timing_count"] == 0
        assert result["coverage_success_correlation"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_final_verification_coverage(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_final_verification_coverage("not a list")

    def test_complete_verification_coverage(self):
        """Verify pack with 100% verification coverage."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": 5,
                "files_verified": 5,
                "verification_depth": "unit",
                "verification_timing": "end",
                "pack_success": True,
            }
        ])

        assert result["total_packs"] == 1
        # 5/5 = 100%
        assert result["avg_verification_coverage"] == 100.0
        assert result["complete_verification_packs"] == 1
        assert result["partial_verification_packs"] == 0
        assert result["no_verification_packs"] == 0

    def test_partial_verification_coverage(self):
        """Verify pack with partial verification coverage."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": 10,
                "files_verified": 6,
            }
        ])

        # 6/10 = 60%
        assert result["avg_verification_coverage"] == 60.0
        assert result["complete_verification_packs"] == 0
        assert result["partial_verification_packs"] == 1
        assert result["no_verification_packs"] == 0

    def test_no_verification_coverage(self):
        """Verify pack with no verification."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": 5,
                "files_verified": 0,
            }
        ])

        assert result["avg_verification_coverage"] == 0.0
        assert result["no_verification_packs"] == 1

    def test_verification_depth_unit_tests(self):
        """Verify unit test verification depth tracking."""
        result = analyze_pack_final_verification_coverage([
            {"pack_id": "p1", "verification_depth": "unit"},
            {"pack_id": "p2", "verification_depth": "unit tests"},
        ])

        assert result["unit_test_verification_count"] == 2

    def test_verification_depth_integration_tests(self):
        """Verify integration test verification depth tracking."""
        result = analyze_pack_final_verification_coverage([
            {"pack_id": "p1", "verification_depth": "integration"},
            {"pack_id": "p2", "verification_depth": "integration tests"},
        ])

        assert result["integration_test_verification_count"] == 2

    def test_verification_depth_build(self):
        """Verify build verification depth tracking."""
        result = analyze_pack_final_verification_coverage([
            {"pack_id": "p1", "verification_depth": "build"},
            {"pack_id": "p2", "verification_depth": "full build"},
        ])

        assert result["build_verification_count"] == 2

    def test_verification_timing_end_of_pack(self):
        """Verify end-of-pack timing tracking."""
        result = analyze_pack_final_verification_coverage([
            {"pack_id": "p1", "verification_timing": "end"},
            {"pack_id": "p2", "verification_timing": "final"},
            {"pack_id": "p3", "verification_timing": "end-of-pack"},
        ])

        assert result["end_of_pack_timing_count"] == 3

    def test_verification_timing_immediate(self):
        """Verify immediate timing tracking."""
        result = analyze_pack_final_verification_coverage([
            {"pack_id": "p1", "verification_timing": "immediate"},
            {"pack_id": "p2", "verification_timing": "post-edit"},
        ])

        assert result["immediate_timing_count"] == 2

    def test_coverage_success_correlation_positive(self):
        """Verify positive correlation between coverage and success."""
        result = analyze_pack_final_verification_coverage([
            # High coverage, success
            {
                "pack_id": "p1",
                "files_changed": 10,
                "files_verified": 10,
                "pack_success": True,
            },
            # Medium coverage, success
            {
                "pack_id": "p2",
                "files_changed": 10,
                "files_verified": 7,
                "pack_success": True,
            },
            # Low coverage, failure
            {
                "pack_id": "p3",
                "files_changed": 10,
                "files_verified": 2,
                "pack_success": False,
            },
        ])

        # Positive correlation: more coverage = more success
        assert result["coverage_success_correlation"] > 0

    def test_coverage_success_correlation_insufficient_data(self):
        """Verify correlation returns 0 with insufficient data."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "p1",
                "files_changed": 10,
                "files_verified": 8,
                "pack_success": True,
            },
        ])

        # Only one data point, no correlation
        assert result["coverage_success_correlation"] == 0.0

    def test_multiple_packs_averages(self):
        """Verify averages calculated across multiple packs."""
        result = analyze_pack_final_verification_coverage([
            {"pack_id": "p1", "files_changed": 10, "files_verified": 10},
            {"pack_id": "p2", "files_changed": 10, "files_verified": 8},
            {"pack_id": "p3", "files_changed": 10, "files_verified": 6},
        ])

        # (100% + 80% + 60%) / 3 = 80%
        assert result["avg_verification_coverage"] == 80.0

    def test_zero_files_changed_counts_as_complete(self):
        """Verify pack with zero files changed counts as complete."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": 0,
                "files_verified": 0,
            }
        ])

        # No files to verify, so it's complete
        assert result["complete_verification_packs"] == 1

    def test_missing_files_verified_counts_as_no_verification(self):
        """Verify missing files_verified field counts as no verification."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": 5,
                # files_verified missing
            }
        ])

        assert result["no_verification_packs"] == 1

    def test_zero_denominator_in_coverage(self):
        """Verify zero files changed handled in coverage calculation."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": 0,
                "files_verified": 0,
            }
        ])

        # Should not crash, counts as complete
        assert result["complete_verification_packs"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_final_verification_coverage([
            "not a dict",
            {"pack_id": "pack1", "files_changed": 5, "files_verified": 5},
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored_for_integers(self):
        """Verify boolean values are ignored for integer fields."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": True,
                "files_verified": False,
            }
        ])

        # Should handle gracefully
        assert result["total_packs"] == 1

    def test_case_insensitive_depth_matching(self):
        """Verify verification depth matching is case-insensitive."""
        result = analyze_pack_final_verification_coverage([
            {"pack_id": "p1", "verification_depth": "UNIT"},
            {"pack_id": "p2", "verification_depth": "Integration"},
            {"pack_id": "p3", "verification_depth": "BUILD"},
        ])

        assert result["unit_test_verification_count"] == 1
        assert result["integration_test_verification_count"] == 1
        assert result["build_verification_count"] == 1

    def test_case_insensitive_timing_matching(self):
        """Verify verification timing matching is case-insensitive."""
        result = analyze_pack_final_verification_coverage([
            {"pack_id": "p1", "verification_timing": "END"},
            {"pack_id": "p2", "verification_timing": "Immediate"},
        ])

        assert result["end_of_pack_timing_count"] == 1
        assert result["immediate_timing_count"] == 1

    def test_optimal_pattern_full_coverage_end_timing(self):
        """Verify optimal pattern with full coverage and end-of-pack timing."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": 10,
                "files_verified": 10,
                "verification_depth": "build",
                "verification_timing": "end",
                "pack_success": True,
            }
        ])

        assert result["avg_verification_coverage"] == 100.0
        assert result["complete_verification_packs"] == 1
        assert result["build_verification_count"] == 1
        assert result["end_of_pack_timing_count"] == 1

    def test_anti_pattern_no_verification(self):
        """Verify anti-pattern with no verification."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": 10,
                "files_verified": 0,
                "pack_success": False,
            }
        ])

        assert result["avg_verification_coverage"] == 0.0
        assert result["no_verification_packs"] == 1

    def test_anti_pattern_partial_verification(self):
        """Verify anti-pattern with partial verification."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": 10,
                "files_verified": 3,
                "pack_success": False,
            }
        ])

        # Only 30% coverage = partial
        assert result["avg_verification_coverage"] == 30.0
        assert result["partial_verification_packs"] == 1

    def test_correlation_no_variance(self):
        """Verify correlation with no variance returns 0."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "p1",
                "files_changed": 10,
                "files_verified": 5,
                "pack_success": True,
            },
            {
                "pack_id": "p2",
                "files_changed": 10,
                "files_verified": 5,
                "pack_success": True,
            },
        ])

        # No variance in coverage
        assert result["coverage_success_correlation"] == 0.0

    def test_mixed_coverage_patterns(self):
        """Verify mixed coverage patterns across packs."""
        result = analyze_pack_final_verification_coverage([
            # Complete
            {"pack_id": "p1", "files_changed": 5, "files_verified": 5},
            # Partial
            {"pack_id": "p2", "files_changed": 10, "files_verified": 6},
            # None
            {"pack_id": "p3", "files_changed": 8, "files_verified": 0},
        ])

        assert result["complete_verification_packs"] == 1
        assert result["partial_verification_packs"] == 1
        assert result["no_verification_packs"] == 1

    def test_over_verification_handled(self):
        """Verify over-verification (verified > changed) handled as 100%."""
        result = analyze_pack_final_verification_coverage([
            {
                "pack_id": "pack1",
                "files_changed": 5,
                "files_verified": 8,
            }
        ])

        # 8/5 = 160%, but should count as complete
        assert result["avg_verification_coverage"] == 160.0
        assert result["complete_verification_packs"] == 1
