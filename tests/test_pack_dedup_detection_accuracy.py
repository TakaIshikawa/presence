"""Tests for pack deduplication detection accuracy analyzer."""

import pytest

from synthesis.pack_dedup_detection_accuracy import analyze_pack_dedup_detection_accuracy


class TestAnalyzePackDedupDetectionAccuracy:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_dedup_detection_accuracy([])

        assert result["total_packs"] == 0
        assert result["avg_detection_rate"] == 0.0
        assert result["avg_false_positive_rate"] == 0.0
        assert result["avg_semantic_vs_regex_ratio"] == 0.0
        assert result["avg_multi_layer_detection_rate"] == 0.0
        assert result["high_accuracy_packs"] == 0
        assert result["low_accuracy_packs"] == 0
        assert result["opening_clause_total"] == 0
        assert result["stale_pattern_total"] == 0
        assert result["semantic_embedding_total"] == 0
        assert result["quality_correlation"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_dedup_detection_accuracy(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_dedup_detection_accuracy("not a list")

    def test_high_detection_rate_low_false_positives(self):
        """Verify pack with high detection rate and low false positives."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "total_items": 100,
                "actual_duplicates": 20,
                "detected_duplicates": 19,
                "false_positives": 2,
            }
        ])

        assert result["total_packs"] == 1
        # 19/20 = 95%
        assert result["avg_detection_rate"] == 95.0
        # 2/100 = 2%
        assert result["avg_false_positive_rate"] == 2.0
        assert result["high_accuracy_packs"] == 1

    def test_low_detection_rate_high_false_positives(self):
        """Verify pack with low detection rate and high false positives."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "total_items": 100,
                "actual_duplicates": 20,
                "detected_duplicates": 8,
                "false_positives": 25,
            }
        ])

        # 8/20 = 40%
        assert result["avg_detection_rate"] == 40.0
        # 25/100 = 25%
        assert result["avg_false_positive_rate"] == 25.0
        assert result["low_accuracy_packs"] == 1

    def test_semantic_vs_regex_ratio_calculation(self):
        """Verify semantic embedding vs regex pattern ratio."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "opening_clause_hits": 10,
                "stale_pattern_hits": 5,
                "semantic_embedding_hits": 15,
            }
        ])

        # Regex: 10 + 5 = 15
        # Semantic: 15
        # Total: 30
        # 15/30 = 50%
        assert result["avg_semantic_vs_regex_ratio"] == 50.0
        assert result["opening_clause_total"] == 10
        assert result["stale_pattern_total"] == 5
        assert result["semantic_embedding_total"] == 15

    def test_multi_layer_detection_rate(self):
        """Verify multi-layer detection rate calculation."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "detected_duplicates": 20,
                "multi_layer_hits": 8,
            }
        ])

        # 8/20 = 40%
        assert result["avg_multi_layer_detection_rate"] == 40.0

    def test_quality_correlation_positive(self):
        """Verify positive correlation between dedup and quality."""
        result = analyze_pack_dedup_detection_accuracy([
            # High detection, high quality
            {
                "pack_id": "p1",
                "actual_duplicates": 10,
                "detected_duplicates": 9,
                "content_quality_score": 90.0,
            },
            # Medium detection, medium quality
            {
                "pack_id": "p2",
                "actual_duplicates": 10,
                "detected_duplicates": 6,
                "content_quality_score": 70.0,
            },
            # Low detection, low quality
            {
                "pack_id": "p3",
                "actual_duplicates": 10,
                "detected_duplicates": 3,
                "content_quality_score": 50.0,
            },
        ])

        # Positive correlation: better dedup = higher quality
        assert result["quality_correlation"] > 0

    def test_quality_correlation_insufficient_data(self):
        """Verify correlation returns 0 with insufficient data."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "p1",
                "actual_duplicates": 10,
                "detected_duplicates": 8,
                "content_quality_score": 80.0,
            },
        ])

        # Only one data point, no correlation
        assert result["quality_correlation"] == 0.0

    def test_multiple_packs_averages(self):
        """Verify averages calculated across multiple packs."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "p1",
                "actual_duplicates": 10,
                "detected_duplicates": 9,
                "total_items": 100,
                "false_positives": 5,
            },
            {
                "pack_id": "p2",
                "actual_duplicates": 10,
                "detected_duplicates": 7,
                "total_items": 100,
                "false_positives": 10,
            },
        ])

        # Detection: (90% + 70%) / 2 = 80%
        assert result["avg_detection_rate"] == 80.0
        # FP: (5% + 10%) / 2 = 7.5%
        assert result["avg_false_positive_rate"] == 7.5

    def test_layer_totals_aggregation(self):
        """Verify layer detection totals aggregated across packs."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "p1",
                "opening_clause_hits": 10,
                "stale_pattern_hits": 5,
                "semantic_embedding_hits": 15,
            },
            {
                "pack_id": "p2",
                "opening_clause_hits": 8,
                "stale_pattern_hits": 3,
                "semantic_embedding_hits": 12,
            },
        ])

        assert result["opening_clause_total"] == 18
        assert result["stale_pattern_total"] == 8
        assert result["semantic_embedding_total"] == 27

    def test_perfect_detection_rate(self):
        """Verify 100% detection rate calculation."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "actual_duplicates": 10,
                "detected_duplicates": 10,
            }
        ])

        assert result["avg_detection_rate"] == 100.0

    def test_zero_false_positives(self):
        """Verify 0% false positive rate calculation."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "total_items": 100,
                "false_positives": 0,
            }
        ])

        assert result["avg_false_positive_rate"] == 0.0

    def test_zero_denominator_in_detection_rate(self):
        """Verify zero actual duplicates handled."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "actual_duplicates": 0,
                "detected_duplicates": 0,
            }
        ])

        assert result["avg_detection_rate"] == 0.0

    def test_zero_denominator_in_fp_rate(self):
        """Verify zero total items handled."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "total_items": 0,
                "false_positives": 0,
            }
        ])

        assert result["avg_false_positive_rate"] == 0.0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "total_items": 100,
                # Missing other fields
            }
        ])

        assert result["total_packs"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_dedup_detection_accuracy([
            "not a dict",
            {"pack_id": "pack1", "total_items": 100},
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "total_items": True,
                "actual_duplicates": False,
            }
        ])

        assert result["avg_detection_rate"] == 0.0

    def test_optimal_pattern_high_accuracy_all_layers(self):
        """Verify optimal pattern with high accuracy and all layers used."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "total_items": 100,
                "actual_duplicates": 20,
                "detected_duplicates": 19,
                "false_positives": 2,
                "opening_clause_hits": 6,
                "stale_pattern_hits": 5,
                "semantic_embedding_hits": 8,
                "multi_layer_hits": 5,
                "content_quality_score": 95.0,
            }
        ])

        # High detection rate
        assert result["avg_detection_rate"] == 95.0
        # Low false positive rate
        assert result["avg_false_positive_rate"] == 2.0
        # All layers contributing
        assert result["opening_clause_total"] > 0
        assert result["stale_pattern_total"] > 0
        assert result["semantic_embedding_total"] > 0
        # Multi-layer detection present
        assert result["avg_multi_layer_detection_rate"] > 0
        assert result["high_accuracy_packs"] == 1

    def test_anti_pattern_low_detection_high_fp(self):
        """Verify anti-pattern with low detection and high false positives."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "total_items": 100,
                "actual_duplicates": 30,
                "detected_duplicates": 10,
                "false_positives": 25,
                "content_quality_score": 40.0,
            }
        ])

        # Low detection rate (33.33%)
        assert result["avg_detection_rate"] == 33.33
        # High false positive rate (25%)
        assert result["avg_false_positive_rate"] == 25.0
        assert result["low_accuracy_packs"] == 1

    def test_semantic_dominance_over_regex(self):
        """Verify semantic embeddings dominating over regex patterns."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "opening_clause_hits": 3,
                "stale_pattern_hits": 2,
                "semantic_embedding_hits": 20,
            }
        ])

        # Regex: 5, Semantic: 20, Total: 25
        # 20/25 = 80%
        assert result["avg_semantic_vs_regex_ratio"] == 80.0

    def test_regex_dominance_over_semantic(self):
        """Verify regex patterns dominating over semantic embeddings."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "opening_clause_hits": 15,
                "stale_pattern_hits": 10,
                "semantic_embedding_hits": 5,
            }
        ])

        # Regex: 25, Semantic: 5, Total: 30
        # 5/30 = 16.67%
        assert result["avg_semantic_vs_regex_ratio"] == 16.67

    def test_correlation_no_variance(self):
        """Verify correlation with no variance returns 0."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "p1",
                "actual_duplicates": 10,
                "detected_duplicates": 5,
                "content_quality_score": 70.0,
            },
            {
                "pack_id": "p2",
                "actual_duplicates": 10,
                "detected_duplicates": 5,
                "content_quality_score": 70.0,
            },
        ])

        # No variance in either variable
        assert result["quality_correlation"] == 0.0

    def test_mixed_accuracy_patterns(self):
        """Verify mixed accuracy patterns across packs."""
        result = analyze_pack_dedup_detection_accuracy([
            # High accuracy
            {
                "pack_id": "p1",
                "total_items": 100,
                "actual_duplicates": 10,
                "detected_duplicates": 10,
                "false_positives": 2,
            },
            # Low accuracy
            {
                "pack_id": "p2",
                "total_items": 100,
                "actual_duplicates": 10,
                "detected_duplicates": 3,
                "false_positives": 25,
            },
            # Medium accuracy
            {
                "pack_id": "p3",
                "total_items": 100,
                "actual_duplicates": 10,
                "detected_duplicates": 7,
                "false_positives": 10,
            },
        ])

        assert result["high_accuracy_packs"] == 1
        assert result["low_accuracy_packs"] == 1

    def test_no_regex_hits_only_semantic(self):
        """Verify handling when only semantic embeddings used."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "opening_clause_hits": 0,
                "stale_pattern_hits": 0,
                "semantic_embedding_hits": 20,
            }
        ])

        # 100% semantic
        assert result["avg_semantic_vs_regex_ratio"] == 100.0

    def test_no_semantic_hits_only_regex(self):
        """Verify handling when only regex patterns used."""
        result = analyze_pack_dedup_detection_accuracy([
            {
                "pack_id": "pack1",
                "opening_clause_hits": 10,
                "stale_pattern_hits": 5,
                "semantic_embedding_hits": 0,
            }
        ])

        # 0% semantic
        assert result["avg_semantic_vs_regex_ratio"] == 0.0
