"""Tests for pack Read-cache integration analyzer."""

import pytest

from synthesis.pack_read_cache_integration import analyze_pack_read_cache_integration


class TestAnalyzePackReadCacheIntegration:
    """Test main analyzer function."""

    def test_empty_records_returns_zero_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_pack_read_cache_integration([])

        assert result["total_sessions"] == 0
        assert result["total_cache_queries"] == 0
        assert result["total_cache_snapshots"] == 0
        assert result["reads_with_offset_limit"] == 0
        assert result["cache_query_before_read_rate"] == 0.0
        assert result["cache_snapshot_after_read_rate"] == 0.0
        assert result["offset_limit_adoption_rate"] == 0.0
        assert result["cache_hit_rate"] == 0.0
        assert result["anti_pattern_reads_without_query"] == 0
        assert result["anti_pattern_full_reads_with_cache"] == 0
        assert result["anti_pattern_no_snapshot"] == 0
        assert result["anti_pattern_rate"] == 0.0
        assert result["effectiveness_score"] == 0.0

    def test_none_input_treated_as_empty(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_read_cache_integration(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_read_cache_integration("not a list")

    def test_single_session_efficient_cache_usage(self):
        """Verify efficient cache usage metrics."""
        records = [
            {
                "cache_queries_before_read": 10,
                "cache_snapshots_after_read": 8,
                "reads_with_offset_limit_after_cache": 9,
                "cache_hit_count": 10,
                "cache_miss_count": 0,
                "reads_without_prior_query": 0,
                "full_reads_with_cache_available": 0,
                "full_reads_without_snapshot": 0,
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        assert result["total_sessions"] == 1
        assert result["total_cache_queries"] == 10
        assert result["total_cache_snapshots"] == 8
        assert result["cache_hit_rate"] == 100.0
        assert result["cache_query_before_read_rate"] == 100.0
        assert result["offset_limit_adoption_rate"] == 90.0
        assert result["effectiveness_score"] > 0.85

    def test_cache_hit_rate_calculation(self):
        """Verify cache hit rate is calculated correctly."""
        records = [
            {
                "cache_hit_count": 7,
                "cache_miss_count": 3,
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        # 7 hits out of 10 attempts = 70%
        assert result["cache_hit_rate"] == 70.0

    def test_cache_query_before_read_rate(self):
        """Verify cache query before read rate calculation."""
        records = [
            {
                "cache_queries_before_read": 8,
                "reads_without_prior_query": 2,
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        # 8 out of 10 reads had cache query = 80%
        assert result["cache_query_before_read_rate"] == 80.0

    def test_cache_snapshot_after_read_rate(self):
        """Verify snapshot rate after full reads."""
        records = [
            {
                "cache_snapshots_after_read": 6,
                "full_reads_without_snapshot": 2,
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        # 6 out of 8 full reads had snapshot = 75%
        assert result["cache_snapshot_after_read_rate"] == 75.0

    def test_offset_limit_adoption_rate(self):
        """Verify offset/limit adoption after cache hits."""
        records = [
            {
                "reads_with_offset_limit_after_cache": 17,
                "cache_hit_count": 20,
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        # 17 out of 20 cache hits used offset/limit = 85%
        assert result["offset_limit_adoption_rate"] == 85.0

    def test_anti_pattern_detection(self):
        """Verify anti-pattern metrics are tracked."""
        records = [
            {
                "reads_without_prior_query": 5,
                "full_reads_with_cache_available": 3,
                "full_reads_without_snapshot": 2,
                "cache_queries_before_read": 10,
                "cache_snapshots_after_read": 8,
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        assert result["anti_pattern_reads_without_query"] == 5
        assert result["anti_pattern_full_reads_with_cache"] == 3
        assert result["anti_pattern_no_snapshot"] == 2
        # Total anti-patterns = 10, total operations = 15 + 10 = 25
        assert result["anti_pattern_rate"] == 40.0

    def test_multiple_sessions_aggregation(self):
        """Verify metrics are aggregated across sessions."""
        records = [
            {
                "cache_queries_before_read": 5,
                "cache_snapshots_after_read": 4,
                "cache_hit_count": 5,
                "cache_miss_count": 0,
            },
            {
                "cache_queries_before_read": 3,
                "cache_snapshots_after_read": 2,
                "cache_hit_count": 2,
                "cache_miss_count": 1,
            },
        ]

        result = analyze_pack_read_cache_integration(records)

        assert result["total_sessions"] == 2
        assert result["total_cache_queries"] == 8
        assert result["total_cache_snapshots"] == 6
        # 7 hits out of 8 attempts = 87.5%
        assert result["cache_hit_rate"] == 87.5

    def test_effectiveness_score_high(self):
        """Verify high effectiveness score for optimal usage."""
        records = [
            {
                "cache_queries_before_read": 20,
                "cache_snapshots_after_read": 18,
                "reads_with_offset_limit_after_cache": 19,
                "cache_hit_count": 20,
                "cache_miss_count": 2,
                "reads_without_prior_query": 0,
                "full_reads_with_cache_available": 0,
                "full_reads_without_snapshot": 0,
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        # Should have very high effectiveness
        assert result["effectiveness_score"] >= 0.90

    def test_effectiveness_score_low(self):
        """Verify low effectiveness score for poor usage."""
        records = [
            {
                "cache_queries_before_read": 2,
                "cache_snapshots_after_read": 1,
                "reads_with_offset_limit_after_cache": 1,
                "cache_hit_count": 2,
                "cache_miss_count": 8,
                "reads_without_prior_query": 15,
                "full_reads_with_cache_available": 10,
                "full_reads_without_snapshot": 12,
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        # Should have low effectiveness
        assert result["effectiveness_score"] <= 0.30

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        records = [
            "not a dict",
            {
                "cache_queries_before_read": 5,
                "cache_hit_count": 5,
            },
        ]

        result = analyze_pack_read_cache_integration(records)

        assert result["total_sessions"] == 1
        assert result["total_cache_queries"] == 5

    def test_missing_fields_handled(self):
        """Verify missing fields are treated as zero."""
        records = [
            {
                "cache_queries_before_read": 10,
                # Other fields missing
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        assert result["total_cache_queries"] == 10
        assert result["total_cache_snapshots"] == 0
        assert result["cache_hit_rate"] == 0.0

    def test_zero_denominator_in_percentages(self):
        """Verify zero denominator handling in percentage calculations."""
        records = [
            {
                "cache_queries_before_read": 0,
                "cache_snapshots_after_read": 0,
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        # Should handle gracefully without division by zero
        assert result["cache_query_before_read_rate"] == 0.0
        assert result["cache_snapshot_after_read_rate"] == 0.0

    def test_invalid_field_types_handled(self):
        """Verify invalid field types are handled."""
        records = [
            {
                "cache_queries_before_read": "not a number",
                "cache_snapshots_after_read": None,
                "cache_hit_count": True,  # Boolean should be rejected
                "cache_miss_count": 3.5,  # Float converted to int
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        # Invalid types should be treated as 0
        assert result["total_cache_queries"] == 0
        assert result["total_cache_snapshots"] == 0
        # Float should be converted
        assert result["cache_hit_rate"] == 0.0

    def test_effectiveness_score_components(self):
        """Verify effectiveness score calculation components."""
        # Perfect scores on all components
        records = [
            {
                "cache_queries_before_read": 100,
                "cache_snapshots_after_read": 100,
                "reads_with_offset_limit_after_cache": 100,
                "cache_hit_count": 100,
                "cache_miss_count": 0,
                "reads_without_prior_query": 0,
                "full_reads_with_cache_available": 0,
                "full_reads_without_snapshot": 0,
            }
        ]

        result = analyze_pack_read_cache_integration(records)

        # Perfect usage should yield 1.0
        assert result["effectiveness_score"] == 1.0

    def test_anti_pattern_penalty_threshold(self):
        """Verify anti-pattern penalty applies above 10% threshold."""
        # Just below threshold
        records_low = [
            {
                "cache_queries_before_read": 90,
                "cache_snapshots_after_read": 90,
                "reads_without_prior_query": 5,
                "full_reads_with_cache_available": 3,
                "full_reads_without_snapshot": 2,
            }
        ]

        # Well above threshold
        records_high = [
            {
                "cache_queries_before_read": 50,
                "cache_snapshots_after_read": 50,
                "reads_without_prior_query": 30,
                "full_reads_with_cache_available": 20,
                "full_reads_without_snapshot": 10,
            }
        ]

        result_low = analyze_pack_read_cache_integration(records_low)
        result_high = analyze_pack_read_cache_integration(records_high)

        # High anti-pattern rate should have lower score
        assert result_low["effectiveness_score"] > result_high["effectiveness_score"]

    def test_realistic_pack_scenario(self):
        """Verify realistic pack execution scenario."""
        records = [
            {
                "cache_queries_before_read": 15,
                "cache_snapshots_after_read": 12,
                "reads_with_offset_limit_after_cache": 13,
                "cache_hit_count": 14,
                "cache_miss_count": 1,
                "reads_without_prior_query": 2,
                "full_reads_with_cache_available": 1,
                "full_reads_without_snapshot": 3,
            },
            {
                "cache_queries_before_read": 8,
                "cache_snapshots_after_read": 7,
                "reads_with_offset_limit_after_cache": 7,
                "cache_hit_count": 8,
                "cache_miss_count": 0,
                "reads_without_prior_query": 1,
                "full_reads_with_cache_available": 0,
                "full_reads_without_snapshot": 1,
            },
        ]

        result = analyze_pack_read_cache_integration(records)

        # Should have reasonable metrics
        assert result["total_sessions"] == 2
        assert result["cache_hit_rate"] > 90.0
        assert result["cache_query_before_read_rate"] > 85.0
        assert result["effectiveness_score"] > 0.75
