"""Tests for pack cache hit rate analyzer."""

import pytest

from synthesis.pack_cache_hit_rate import analyze_pack_cache_hit_rate


class TestAnalyzePackCacheHitRate:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_cache_hit_rate([])

        assert result["total_packs"] == 0
        assert result["avg_cache_query_frequency"] == 0.0
        assert result["avg_cache_hit_rate"] == 0.0
        assert result["avg_cache_coverage"] == 0.0
        assert result["avg_cache_to_read_ratio"] == 0.0
        assert result["high_hit_rate_packs"] == 0
        assert result["low_hit_rate_packs"] == 0
        assert result["no_cache_packs"] == 0
        assert result["token_efficiency_correlation"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_cache_hit_rate(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_cache_hit_rate("not a list")

    def test_single_pack_with_high_hit_rate(self):
        """Verify pack with high cache hit rate."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 10,
                "cache_hit_count": 9,
                "cache_snapshot_count": 5,
                "total_files_read": 10,
                "read_tool_count": 20,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_cache_query_frequency"] == 10.0
        # 9 hits / 10 queries = 90%
        assert result["avg_cache_hit_rate"] == 90.0
        # 5 cached / 10 read = 50%
        assert result["avg_cache_coverage"] == 50.0
        # 10 queries / 20 reads = 50%
        assert result["avg_cache_to_read_ratio"] == 50.0
        assert result["high_hit_rate_packs"] == 1
        assert result["low_hit_rate_packs"] == 0

    def test_single_pack_with_low_hit_rate(self):
        """Verify pack with low cache hit rate."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 10,
                "cache_hit_count": 1,
                "cache_snapshot_count": 2,
                "total_files_read": 20,
                "read_tool_count": 30,
            }
        ])

        assert result["total_packs"] == 1
        # 1 hit / 10 queries = 10%
        assert result["avg_cache_hit_rate"] == 10.0
        assert result["high_hit_rate_packs"] == 0
        assert result["low_hit_rate_packs"] == 1

    def test_pack_with_no_cache_usage(self):
        """Verify pack with no cache usage."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 0,
                "cache_hit_count": 0,
                "total_files_read": 10,
                "read_tool_count": 15,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_cache_query_frequency"] == 0.0
        assert result["no_cache_packs"] == 1

    def test_multiple_packs_average_calculations(self):
        """Verify averages calculated across multiple packs."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 10,
                "cache_hit_count": 8,
                "cache_snapshot_count": 5,
                "total_files_read": 10,
                "read_tool_count": 20,
            },
            {
                "pack_id": "pack2",
                "cache_query_count": 20,
                "cache_hit_count": 16,
                "cache_snapshot_count": 8,
                "total_files_read": 10,
                "read_tool_count": 30,
            },
            {
                "pack_id": "pack3",
                "cache_query_count": 15,
                "cache_hit_count": 12,
                "cache_snapshot_count": 6,
                "total_files_read": 10,
                "read_tool_count": 25,
            },
        ])

        assert result["total_packs"] == 3
        # (10 + 20 + 15) / 3 = 15
        assert result["avg_cache_query_frequency"] == 15.0
        # (80% + 80% + 80%) / 3 = 80%
        assert result["avg_cache_hit_rate"] == 80.0
        # (50% + 80% + 60%) / 3 = 63.33%
        assert result["avg_cache_coverage"] == 63.33

    def test_hit_rate_classification(self):
        """Verify hit rate classification into high/low categories."""
        result = analyze_pack_cache_hit_rate([
            # High hit rate (> 80%)
            {"pack_id": "p1", "cache_query_count": 10, "cache_hit_count": 9},
            # Medium hit rate (not classified)
            {"pack_id": "p2", "cache_query_count": 10, "cache_hit_count": 5},
            # Low hit rate (< 20%)
            {"pack_id": "p3", "cache_query_count": 10, "cache_hit_count": 1},
            # High hit rate (> 80%)
            {"pack_id": "p4", "cache_query_count": 10, "cache_hit_count": 10},
        ])

        assert result["high_hit_rate_packs"] == 2
        assert result["low_hit_rate_packs"] == 1

    def test_cache_coverage_calculation(self):
        """Verify cache coverage percentage calculation."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_snapshot_count": 8,
                "total_files_read": 10,
                "cache_query_count": 5,
                "cache_hit_count": 4,
            }
        ])

        # 8 cached / 10 read = 80%
        assert result["avg_cache_coverage"] == 80.0

    def test_cache_to_read_ratio_calculation(self):
        """Verify cache-to-read ratio calculation."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 15,
                "read_tool_count": 30,
                "cache_hit_count": 10,
            }
        ])

        # 15 queries / 30 reads = 50%
        assert result["avg_cache_to_read_ratio"] == 50.0

    def test_token_efficiency_correlation_positive(self):
        """Verify positive correlation between cache usage and token efficiency."""
        result = analyze_pack_cache_hit_rate([
            # High cache usage, low tokens (high efficiency)
            {"pack_id": "p1", "cache_query_count": 20, "total_tokens": 5000},
            # Medium cache usage, medium tokens
            {"pack_id": "p2", "cache_query_count": 10, "total_tokens": 10000},
            # Low cache usage, high tokens (low efficiency)
            {"pack_id": "p3", "cache_query_count": 5, "total_tokens": 20000},
        ])

        # Should show positive correlation (more cache = fewer tokens)
        assert result["token_efficiency_correlation"] > 0

    def test_token_efficiency_correlation_insufficient_data(self):
        """Verify correlation returns 0 with insufficient data."""
        result = analyze_pack_cache_hit_rate([
            {"pack_id": "p1", "cache_query_count": 10, "total_tokens": 5000},
        ])

        # Only one data point, no correlation
        assert result["token_efficiency_correlation"] == 0.0

    def test_zero_denominator_in_hit_rate(self):
        """Verify zero cache queries handled in hit rate calculation."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 0,
                "cache_hit_count": 0,
            }
        ])

        assert result["avg_cache_hit_rate"] == 0.0

    def test_zero_denominator_in_coverage(self):
        """Verify zero files read handled in coverage calculation."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_snapshot_count": 5,
                "total_files_read": 0,
            }
        ])

        assert result["avg_cache_coverage"] == 0.0

    def test_zero_denominator_in_cache_to_read_ratio(self):
        """Verify zero reads handled in cache-to-read ratio."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 5,
                "read_tool_count": 0,
            }
        ])

        assert result["avg_cache_to_read_ratio"] == 0.0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 10,
                # Missing other fields
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_cache_query_frequency"] == 10.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_cache_hit_rate([
            "not a dict",
            {"pack_id": "pack1", "cache_query_count": 10},
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for integer fields."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": True,
                "cache_hit_count": False,
            }
        ])

        assert result["avg_cache_query_frequency"] == 0.0

    def test_optimal_pattern_high_hit_rate_good_coverage(self):
        """Verify optimal pattern with high hit rate and good coverage."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 15,
                "cache_hit_count": 14,
                "cache_snapshot_count": 18,
                "total_files_read": 20,
                "read_tool_count": 25,
            }
        ])

        # High hit rate (93.33%)
        assert result["avg_cache_hit_rate"] > 90.0
        # Good coverage (90%)
        assert result["avg_cache_coverage"] == 90.0
        assert result["high_hit_rate_packs"] == 1

    def test_anti_pattern_no_cache_usage(self):
        """Verify anti-pattern of no cache usage."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 0,
                "total_files_read": 20,
                "read_tool_count": 50,
            },
            {
                "pack_id": "pack2",
                "cache_query_count": 0,
                "total_files_read": 15,
                "read_tool_count": 40,
            },
        ])

        assert result["no_cache_packs"] == 2
        assert result["avg_cache_query_frequency"] == 0.0

    def test_anti_pattern_low_hit_rate_poor_coverage(self):
        """Verify anti-pattern with low hit rate and poor coverage."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 10,
                "cache_hit_count": 1,
                "cache_snapshot_count": 2,
                "total_files_read": 20,
            }
        ])

        # Low hit rate (10%)
        assert result["avg_cache_hit_rate"] == 10.0
        # Poor coverage (10%)
        assert result["avg_cache_coverage"] == 10.0
        assert result["low_hit_rate_packs"] == 1

    def test_correlation_no_variance(self):
        """Verify correlation with no variance returns 0."""
        result = analyze_pack_cache_hit_rate([
            {"pack_id": "p1", "cache_query_count": 10, "total_tokens": 5000},
            {"pack_id": "p2", "cache_query_count": 10, "total_tokens": 5000},
        ])

        # No variance in either variable
        assert result["token_efficiency_correlation"] == 0.0

    def test_perfect_hit_rate(self):
        """Verify 100% hit rate calculation."""
        result = analyze_pack_cache_hit_rate([
            {
                "pack_id": "pack1",
                "cache_query_count": 10,
                "cache_hit_count": 10,
            }
        ])

        assert result["avg_cache_hit_rate"] == 100.0
        assert result["high_hit_rate_packs"] == 1

    def test_mixed_cache_usage_patterns(self):
        """Verify mixed patterns of cache usage."""
        result = analyze_pack_cache_hit_rate([
            # No cache
            {"pack_id": "p1", "cache_query_count": 0, "cache_hit_count": 0},
            # Low hit rate
            {"pack_id": "p2", "cache_query_count": 10, "cache_hit_count": 1},
            # Medium hit rate
            {"pack_id": "p3", "cache_query_count": 10, "cache_hit_count": 5},
            # High hit rate
            {"pack_id": "p4", "cache_query_count": 10, "cache_hit_count": 9},
        ])

        assert result["total_packs"] == 4
        assert result["no_cache_packs"] == 1
        assert result["low_hit_rate_packs"] == 1
        assert result["high_hit_rate_packs"] == 1
        # Average hit rate: (10% + 50% + 90%) / 3 = 50%
        assert result["avg_cache_hit_rate"] == 50.0
