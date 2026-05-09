"""Tests for pack optimization mode strategy compliance scorer."""

import pytest

from synthesis.pack_optimization_mode_strategy import analyze_pack_optimization_mode_strategy


class TestAnalyzePackOptimizationModeStrategy:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_optimization_mode_strategy([])

        assert result["total_sessions"] == 0
        assert result["baseline_sessions"] == 0
        assert result["optimized_sessions"] == 0
        assert result["mixed_sessions"] == 0
        assert result["pack_level_read_offset_limit_ratio"] == 0.0
        assert result["pack_average_lines_per_read"] == 0.0
        assert result["sessions_using_cache"] == 0
        assert result["cache_adoption_rate"] == 0.0
        assert result["sessions_using_verify"] == 0
        assert result["verify_adoption_rate"] == 0.0
        assert result["avg_verify_to_read_ratio"] == 0.0
        assert result["pack_token_reduction_estimate"] == 0.0
        assert result["optimization_mode_classification"] == "unknown"
        assert result["run1_compliance_score"] == 0.0
        assert result["pack_strategy_effectiveness_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_optimization_mode_strategy(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_optimization_mode_strategy("not a list")

    def test_single_session_baseline_mode(self):
        """Verify single baseline session."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "session1",
                "optimization_mode": "baseline",
                "read_offset_limit_ratio": 20.0,
                "avg_lines_per_read": 200.0,
                "cache_commands_used": False,
                "verify_commands_used": False,
                "total_read_calls": 100,
            }
        ])

        assert result["baseline_sessions"] == 1
        assert result["optimized_sessions"] == 0
        assert result["optimization_mode_classification"] == "baseline"

    def test_single_session_optimized_mode(self):
        """Verify single optimized session."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "session1",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 90.0,
                "avg_lines_per_read": 60.0,
                "cache_commands_used": True,
                "verify_commands_used": True,
                "verify_to_read_ratio": 5.0,
                "total_read_calls": 100,
                "estimated_token_savings": 55.0,
            }
        ])

        assert result["baseline_sessions"] == 0
        assert result["optimized_sessions"] == 1
        assert result["optimization_mode_classification"] == "optimized"
        assert result["cache_adoption_rate"] == 100.0
        assert result["verify_adoption_rate"] == 100.0

    def test_multi_session_aggregation(self):
        """Verify aggregation across multiple sessions."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "session1",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 90.0,
                "avg_lines_per_read": 60.0,
                "total_read_calls": 100,
                "cache_commands_used": True,
            },
            {
                "session_id": "session2",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 85.0,
                "avg_lines_per_read": 70.0,
                "total_read_calls": 100,
                "cache_commands_used": True,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["optimized_sessions"] == 2
        # Aggregate: (90*100 + 85*100) / 200 reads = 87.5%
        assert result["pack_level_read_offset_limit_ratio"] == 87.5
        # Aggregate: (60*100 + 70*100) / 200 = 65.0
        assert result["pack_average_lines_per_read"] == 65.0
        assert result["cache_adoption_rate"] == 100.0

    def test_pack_read_offset_limit_ratio_calculation(self):
        """Verify pack-level offset/limit ratio calculation."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "session1",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 100.0,  # 100 reads with offset
                "avg_lines_per_read": 50.0,
                "total_read_calls": 100,
            },
            {
                "session_id": "session2",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 50.0,  # 50 reads with offset
                "avg_lines_per_read": 50.0,
                "total_read_calls": 100,
            },
        ])

        # (100 + 50) / 200 = 75%
        assert result["pack_level_read_offset_limit_ratio"] == 75.0

    def test_pack_average_lines_per_read_calculation(self):
        """Verify pack-level average lines per read."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "session1",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 80.0,
                "avg_lines_per_read": 40.0,  # 4000 total lines
                "total_read_calls": 100,
            },
            {
                "session_id": "session2",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 80.0,
                "avg_lines_per_read": 80.0,  # 8000 total lines
                "total_read_calls": 100,
            },
        ])

        # (4000 + 8000) / 200 = 60.0
        assert result["pack_average_lines_per_read"] == 60.0

    def test_cache_adoption_rate(self):
        """Verify cache adoption rate calculation."""
        result = analyze_pack_optimization_mode_strategy([
            {"session_id": "s1", "optimization_mode": "optimized", "cache_commands_used": True, "total_read_calls": 10},
            {"session_id": "s2", "optimization_mode": "optimized", "cache_commands_used": True, "total_read_calls": 10},
            {"session_id": "s3", "optimization_mode": "optimized", "cache_commands_used": False, "total_read_calls": 10},
            {"session_id": "s4", "optimization_mode": "optimized", "cache_commands_used": True, "total_read_calls": 10},
        ])

        # 3/4 = 75%
        assert result["cache_adoption_rate"] == 75.0

    def test_verify_adoption_rate(self):
        """Verify verify command adoption rate."""
        result = analyze_pack_optimization_mode_strategy([
            {"session_id": "s1", "optimization_mode": "optimized", "verify_commands_used": True, "total_read_calls": 10},
            {"session_id": "s2", "optimization_mode": "optimized", "verify_commands_used": False, "total_read_calls": 10},
            {"session_id": "s3", "optimization_mode": "optimized", "verify_commands_used": True, "total_read_calls": 10},
        ])

        # 2/3 = 66.67%
        assert result["verify_adoption_rate"] == 66.67

    def test_avg_verify_to_read_ratio(self):
        """Verify average verify-to-read ratio calculation."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "s1",
                "optimization_mode": "optimized",
                "verify_to_read_ratio": 5.0,
                "total_read_calls": 100,
            },
            {
                "session_id": "s2",
                "optimization_mode": "optimized",
                "verify_to_read_ratio": 10.0,
                "total_read_calls": 100,
            },
            {
                "session_id": "s3",
                "optimization_mode": "optimized",
                "verify_to_read_ratio": 15.0,
                "total_read_calls": 100,
            },
        ])

        # (5 + 10 + 15) / 3 = 10.0
        assert result["avg_verify_to_read_ratio"] == 10.0

    def test_pack_token_reduction_estimate(self):
        """Verify pack token reduction estimate."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "s1",
                "optimization_mode": "optimized",
                "estimated_token_savings": 60.0,
                "total_read_calls": 100,
            },
            {
                "session_id": "s2",
                "optimization_mode": "optimized",
                "estimated_token_savings": 50.0,
                "total_read_calls": 100,
            },
        ])

        # (60 + 50) / 2 = 55.0
        assert result["pack_token_reduction_estimate"] == 55.0

    def test_classification_all_baseline(self):
        """Verify classification with all baseline sessions."""
        result = analyze_pack_optimization_mode_strategy([
            {"session_id": "s1", "optimization_mode": "baseline", "total_read_calls": 10},
            {"session_id": "s2", "optimization_mode": "baseline", "total_read_calls": 10},
            {"session_id": "s3", "optimization_mode": "baseline", "total_read_calls": 10},
            {"session_id": "s4", "optimization_mode": "baseline", "total_read_calls": 10},
            {"session_id": "s5", "optimization_mode": "baseline", "total_read_calls": 10},
        ])

        assert result["optimization_mode_classification"] == "baseline"

    def test_classification_all_optimized(self):
        """Verify classification with all optimized sessions."""
        result = analyze_pack_optimization_mode_strategy([
            {"session_id": "s1", "optimization_mode": "optimized", "total_read_calls": 10},
            {"session_id": "s2", "optimization_mode": "optimized", "total_read_calls": 10},
            {"session_id": "s3", "optimization_mode": "optimized", "total_read_calls": 10},
            {"session_id": "s4", "optimization_mode": "optimized", "total_read_calls": 10},
            {"session_id": "s5", "optimization_mode": "optimized", "total_read_calls": 10},
        ])

        assert result["optimization_mode_classification"] == "optimized"

    def test_classification_mixed_sessions(self):
        """Verify classification with mixed mode sessions."""
        result = analyze_pack_optimization_mode_strategy([
            {"session_id": "s1", "optimization_mode": "baseline", "read_offset_limit_ratio": 20.0, "avg_lines_per_read": 200.0, "total_read_calls": 50},
            {"session_id": "s2", "optimization_mode": "optimized", "read_offset_limit_ratio": 90.0, "avg_lines_per_read": 60.0, "total_read_calls": 50},
            {"session_id": "s3", "optimization_mode": "unknown", "read_offset_limit_ratio": 50.0, "avg_lines_per_read": 100.0, "total_read_calls": 50},
        ])

        # No single mode >80%, check metrics
        # (20*50 + 90*50 + 50*50) / 150 = 53.33% (mixed)
        assert result["optimization_mode_classification"] == "mixed"

    def test_run1_compliance_perfect(self):
        """Verify perfect Run #1 compliance (87%, 64 lines)."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "s1",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 87.0,
                "avg_lines_per_read": 64.0,
                "total_read_calls": 100,
            }
        ])

        # Perfect compliance: 87% offset, 64 lines
        assert result["run1_compliance_score"] == 1.0

    def test_run1_compliance_below_target(self):
        """Verify Run #1 compliance below target."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "s1",
                "optimization_mode": "baseline",
                "read_offset_limit_ratio": 40.0,
                "avg_lines_per_read": 150.0,
                "total_read_calls": 100,
            }
        ])

        # Below targets: lower score
        assert result["run1_compliance_score"] < 0.5

    def test_effectiveness_score_optimal_pattern(self):
        """Verify effectiveness score for optimal optimization pattern."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "s1",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 90.0,
                "avg_lines_per_read": 60.0,
                "cache_commands_used": True,
                "verify_commands_used": True,
                "verify_to_read_ratio": 5.0,
                "total_read_calls": 100,
                "estimated_token_savings": 55.0,
            }
        ])

        # Excellent metrics: high effectiveness
        assert result["pack_strategy_effectiveness_score"] > 0.9

    def test_effectiveness_score_poor_pattern(self):
        """Verify effectiveness score for poor optimization pattern."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "s1",
                "optimization_mode": "baseline",
                "read_offset_limit_ratio": 10.0,
                "avg_lines_per_read": 250.0,
                "cache_commands_used": False,
                "verify_commands_used": False,
                "total_read_calls": 100,
            }
        ])

        # Poor metrics: low effectiveness
        assert result["pack_strategy_effectiveness_score"] < 0.3

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_optimization_mode_strategy([
            "not a dict",
            {
                "session_id": "s1",
                "optimization_mode": "optimized",
                "total_read_calls": 100,
            },
        ])

        assert result["total_sessions"] == 1

    def test_missing_fields_handled_gracefully(self):
        """Verify missing fields are handled with defaults."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "s1",
                # All other fields missing
            }
        ])

        assert result["total_sessions"] == 1
        assert result["pack_level_read_offset_limit_ratio"] == 0.0

    def test_zero_read_calls_handled(self):
        """Verify sessions with zero read calls are handled."""
        result = analyze_pack_optimization_mode_strategy([
            {
                "session_id": "s1",
                "optimization_mode": "optimized",
                "total_read_calls": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["pack_average_lines_per_read"] == 0.0

    def test_comprehensive_pack_scenario(self):
        """Verify comprehensive pack with mixed optimization patterns."""
        result = analyze_pack_optimization_mode_strategy([
            # Session 1: Excellent optimized pattern
            {
                "session_id": "s1",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 90.0,
                "avg_lines_per_read": 60.0,
                "cache_commands_used": True,
                "verify_commands_used": True,
                "verify_to_read_ratio": 5.0,
                "total_read_calls": 100,
                "estimated_token_savings": 60.0,
            },
            # Session 2: Good optimized pattern
            {
                "session_id": "s2",
                "optimization_mode": "optimized",
                "read_offset_limit_ratio": 85.0,
                "avg_lines_per_read": 70.0,
                "cache_commands_used": True,
                "verify_commands_used": True,
                "verify_to_read_ratio": 8.0,
                "total_read_calls": 100,
                "estimated_token_savings": 55.0,
            },
            # Session 3: Baseline pattern
            {
                "session_id": "s3",
                "optimization_mode": "baseline",
                "read_offset_limit_ratio": 15.0,
                "avg_lines_per_read": 200.0,
                "cache_commands_used": False,
                "verify_commands_used": False,
                "total_read_calls": 100,
            },
        ])

        assert result["total_sessions"] == 3
        assert result["baseline_sessions"] == 1
        assert result["optimized_sessions"] == 2
        # (90*100 + 85*100 + 15*100) / 300 = 63.33%
        assert result["pack_level_read_offset_limit_ratio"] == 63.33
        # (60*100 + 70*100 + 200*100) / 300 = 110.0
        assert result["pack_average_lines_per_read"] == 110.0
        # 2/3 = 66.67%
        assert result["cache_adoption_rate"] == 66.67
        # (5 + 8) / 2 = 6.5
        assert result["avg_verify_to_read_ratio"] == 6.5
        # (60 + 55) / 2 = 57.5
        assert result["pack_token_reduction_estimate"] == 57.5
        # Mixed classification
        assert result["optimization_mode_classification"] == "mixed"
        # Moderate scores
        assert 0.4 < result["run1_compliance_score"] < 0.8
        assert 0.4 < result["pack_strategy_effectiveness_score"] < 0.8
