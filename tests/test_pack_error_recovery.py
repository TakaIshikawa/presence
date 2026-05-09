"""Tests for pack error recovery analyzer."""

import pytest

from synthesis.pack_error_recovery import analyze_pack_error_recovery


class TestAnalyzePackErrorRecovery:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_error_recovery([])

        assert result["total_packs"] == 0
        assert result["avg_total_errors"] == 0.0
        assert result["avg_build_error_ratio"] == 0.0
        assert result["avg_test_error_ratio"] == 0.0
        assert result["avg_runtime_error_ratio"] == 0.0
        assert result["avg_tool_error_ratio"] == 0.0
        assert result["avg_recovery_rate"] == 0.0
        assert result["avg_abandonment_rate"] == 0.0
        assert result["avg_retries_per_error"] == 0.0
        assert result["avg_error_clustering_rate"] == 0.0
        assert result["avg_resolution_time"] == 0.0
        assert result["recovery_efficiency_score"] == 0.0
        assert result["high_recovery_packs"] == 0
        assert result["low_recovery_packs"] == 0
        assert result["packs_with_abandoned_errors"] == 0
        assert result["efficient_recovery_packs"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_error_recovery(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_error_recovery("not a list")

    def test_pack_with_no_errors(self):
        """Verify pack with zero errors handled gracefully."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "pack1",
                "total_errors": 0,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_errors"] == 0.0

    def test_efficient_recovery_high_success(self):
        """Verify efficient recovery with high success rate and quick resolution."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "efficient",
                "total_errors": 20,
                "build_errors": 8,
                "test_errors": 7,
                "runtime_errors": 3,
                "tool_errors": 2,
                "recovered_errors": 19,
                "abandoned_errors": 1,
                "total_retry_attempts": 30,
                "unique_error_signatures": 18,
                "repeated_error_count": 2,
                "total_resolution_time_seconds": 1800,
            }
        ])

        assert result["avg_total_errors"] == 20.0
        # 8 / 20 = 40%
        assert result["avg_build_error_ratio"] == 40.0
        # 19 / 20 = 95%
        assert result["avg_recovery_rate"] == 95.0
        # 1 / 20 = 5%
        assert result["avg_abandonment_rate"] == 5.0
        # 30 / 20 = 1.5 retries per error
        assert result["avg_retries_per_error"] == 1.5
        # 2 / 20 = 10%
        assert result["avg_error_clustering_rate"] == 10.0
        # 1800 / 20 = 90s
        assert result["avg_resolution_time"] == 90.0
        assert result["recovery_efficiency_score"] > 80.0
        assert result["high_recovery_packs"] == 1
        assert result["efficient_recovery_packs"] == 1

    def test_poor_recovery_low_success(self):
        """Verify poor recovery with low success and repeated failures."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "poor",
                "total_errors": 25,
                "build_errors": 10,
                "test_errors": 10,
                "runtime_errors": 5,
                "recovered_errors": 10,
                "abandoned_errors": 15,
                "total_retry_attempts": 150,
                "unique_error_signatures": 8,
                "repeated_error_count": 17,
                "total_resolution_time_seconds": 12000,
            }
        ])

        assert result["avg_total_errors"] == 25.0
        # 10 / 25 = 40%
        assert result["avg_recovery_rate"] == 40.0
        # 15 / 25 = 60%
        assert result["avg_abandonment_rate"] == 60.0
        # 150 / 25 = 6.0 retries
        assert result["avg_retries_per_error"] == 6.0
        # 17 / 25 = 68%
        assert result["avg_error_clustering_rate"] == 68.0
        # 12000 / 25 = 480s
        assert result["avg_resolution_time"] == 480.0
        assert result["recovery_efficiency_score"] < 50.0
        assert result["low_recovery_packs"] == 1
        assert result["packs_with_abandoned_errors"] == 1

    def test_error_type_distribution(self):
        """Verify error type distribution calculated correctly."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "pack1",
                "total_errors": 100,
                "build_errors": 35,
                "test_errors": 30,
                "runtime_errors": 25,
                "tool_errors": 10,
            }
        ])

        # 35 / 100 = 35%
        assert result["avg_build_error_ratio"] == 35.0
        # 30 / 100 = 30%
        assert result["avg_test_error_ratio"] == 30.0
        # 25 / 100 = 25%
        assert result["avg_runtime_error_ratio"] == 25.0
        # 10 / 100 = 10%
        assert result["avg_tool_error_ratio"] == 10.0

    def test_retries_per_error_from_avg_field(self):
        """Verify retries per error used from field when available."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "pack1",
                "total_errors": 20,
                "avg_retries_per_error": 2.3,
            }
        ])

        assert result["avg_retries_per_error"] == 2.3

    def test_retries_per_error_calculated(self):
        """Verify retries per error calculated when not provided."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "pack1",
                "total_errors": 15,
                "total_retry_attempts": 45,
            }
        ])

        # 45 / 15 = 3.0
        assert result["avg_retries_per_error"] == 3.0

    def test_resolution_time_calculation(self):
        """Verify resolution time calculated per error."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "pack1",
                "total_errors": 10,
                "total_resolution_time_seconds": 2400,
            }
        ])

        # 2400 / 10 = 240s
        assert result["avg_resolution_time"] == 240.0

    def test_error_clustering_calculation(self):
        """Verify error clustering rate calculated correctly."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "pack1",
                "total_errors": 50,
                "unique_error_signatures": 20,
                "repeated_error_count": 15,
            }
        ])

        # 15 / 50 = 30%
        assert result["avg_error_clustering_rate"] == 30.0

    def test_multiple_packs_averaged(self):
        """Verify metrics averaged across multiple packs."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "pack1",
                "total_errors": 20,
                "recovered_errors": 18,
                "total_retry_attempts": 40,
            },
            {
                "pack_id": "pack2",
                "total_errors": 30,
                "recovered_errors": 24,
                "total_retry_attempts": 60,
            },
        ])

        assert result["total_packs"] == 2
        # (20 + 30) / 2 = 25
        assert result["avg_total_errors"] == 25.0
        # (90% + 80%) / 2 = 85%
        assert result["avg_recovery_rate"] == 85.0
        # (2.0 + 2.0) / 2 = 2.0
        assert result["avg_retries_per_error"] == 2.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_error_recovery([
            "not a dict",
            {
                "pack_id": "pack1",
                "total_errors": 10,
            },
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "pack1",
                "total_errors": True,
                "recovered_errors": False,
            }
        ])

        assert result["avg_total_errors"] == 0.0

    def test_abandoned_errors_detection(self):
        """Verify packs with abandoned errors detected."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "clean",
                "total_errors": 10,
                "abandoned_errors": 0,
            },
            {
                "pack_id": "abandoned",
                "total_errors": 10,
                "abandoned_errors": 3,
            },
        ])

        assert result["packs_with_abandoned_errors"] == 1

    def test_efficiency_score_excellent_all_metrics(self):
        """Verify efficiency score with excellent metrics."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "excellent",
                "total_errors": 20,
                "recovered_errors": 19,  # 95% recovery (35pts)
                "total_retry_attempts": 30,  # 1.5 retries (25pts)
                "total_resolution_time_seconds": 1500,  # 75s (20pts)
                "unique_error_signatures": 19,
                "repeated_error_count": 1,  # 5% clustering (10pts)
                "abandoned_errors": 1,  # 5% abandonment (10pts)
            }
        ])

        # Should score: 35 + 25 + 20 + 10 + 10 = 100
        assert result["recovery_efficiency_score"] == 100.0
        assert result["efficient_recovery_packs"] == 1

    def test_efficiency_score_poor_all_metrics(self):
        """Verify efficiency score with poor metrics."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "poor",
                "total_errors": 20,
                "recovered_errors": 8,  # 40% recovery (0pts)
                "total_retry_attempts": 120,  # 6.0 retries (0pts)
                "total_resolution_time_seconds": 8000,  # 400s (0pts)
                "unique_error_signatures": 5,
                "repeated_error_count": 15,  # 75% clustering (0pts)
                "abandoned_errors": 12,  # 60% abandonment (0pts)
            }
        ])

        # Should score: 0 + 0 + 0 + 0 + 0 = 0
        assert result["recovery_efficiency_score"] == 0.0

    def test_efficiency_score_mixed_metrics(self):
        """Verify efficiency score with mixed quality metrics."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "mixed",
                "total_errors": 20,
                "recovered_errors": 15,  # 75% recovery (25pts)
                "total_retry_attempts": 70,  # 3.5 retries (18pts)
                "total_resolution_time_seconds": 3600,  # 180s (15pts)
                "unique_error_signatures": 14,
                "repeated_error_count": 6,  # 30% clustering (7pts)
                "abandoned_errors": 4,  # 20% abandonment (7pts)
            }
        ])

        # Should score: 25 + 18 + 15 + 7 + 7 = 72
        assert result["recovery_efficiency_score"] == 72.0

    def test_boundary_recovery_classification(self):
        """Verify boundary cases for recovery classification."""
        result = analyze_pack_error_recovery([
            # Exactly 85% (should not be high)
            {
                "pack_id": "p1",
                "total_errors": 20,
                "recovered_errors": 17,  # 85%
            },
            # Just above 85% (should be high)
            {
                "pack_id": "p2",
                "total_errors": 20,
                "recovered_errors": 18,  # 90%
            },
            # Exactly 50% (should not be low)
            {
                "pack_id": "p3",
                "total_errors": 20,
                "recovered_errors": 10,  # 50%
            },
            # Below 50% (should be low)
            {
                "pack_id": "p4",
                "total_errors": 20,
                "recovered_errors": 9,  # 45%
            },
        ])

        # >85 means strictly greater
        assert result["high_recovery_packs"] == 1
        # <50 means strictly less
        assert result["low_recovery_packs"] == 1

    def test_comprehensive_pack_all_fields(self):
        """Verify comprehensive pack with all fields populated."""
        result = analyze_pack_error_recovery([
            {
                "pack_id": "comprehensive",
                "pack_title": "Test Pack",
                "total_errors": 50,
                "build_errors": 20,
                "test_errors": 15,
                "runtime_errors": 10,
                "tool_errors": 5,
                "other_errors": 0,
                "recovered_errors": 45,
                "abandoned_errors": 5,
                "total_retry_attempts": 100,
                "unique_error_signatures": 45,
                "repeated_error_count": 5,
                "total_resolution_time_seconds": 4500,
                "avg_retries_per_error": 2.0,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_errors"] == 50.0
        # 20 / 50 = 40%
        assert result["avg_build_error_ratio"] == 40.0
        # 45 / 50 = 90%
        assert result["avg_recovery_rate"] == 90.0
        # 5 / 50 = 10%
        assert result["avg_abandonment_rate"] == 10.0
        # avg_retries_per_error provided
        assert result["avg_retries_per_error"] == 2.0
        # 5 / 50 = 10%
        assert result["avg_error_clustering_rate"] == 10.0
        # 4500 / 50 = 90s
        assert result["avg_resolution_time"] == 90.0
        # Should have high efficiency
        assert result["recovery_efficiency_score"] > 80.0
