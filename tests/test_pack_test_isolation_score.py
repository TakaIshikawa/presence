"""Tests for pack test isolation score analyzer."""

import pytest

from synthesis.pack_test_isolation_score import analyze_pack_test_isolation_score


class TestAnalyzePackTestIsolationScore:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_test_isolation_score([])

        assert result["total_packs"] == 0
        assert result["avg_total_tests"] == 0.0
        assert result["avg_test_isolation_score"] == 0.0
        assert result["high_isolation_packs"] == 0
        assert result["low_isolation_packs"] == 0
        assert result["avg_shared_fixtures_ratio"] == 0.0
        assert result["avg_order_dependency_ratio"] == 0.0
        assert result["avg_cleanup_issue_ratio"] == 0.0
        assert result["total_pollution_cases"] == 0
        assert result["packs_with_pollution"] == 0
        assert result["avg_rerun_ratio"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_test_isolation_score(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_test_isolation_score("not a list")

    def test_perfect_isolation_all_independent_tests(self):
        """Verify perfect isolation score with fully independent tests."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 0,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_tests"] == 10.0
        assert result["avg_test_isolation_score"] == 1.0
        assert result["high_isolation_packs"] == 1
        assert result["low_isolation_packs"] == 0
        assert result["avg_shared_fixtures_ratio"] == 0.0
        assert result["avg_order_dependency_ratio"] == 0.0
        assert result["total_pollution_cases"] == 0

    def test_poor_isolation_with_shared_fixtures(self):
        """Verify poor isolation due to shared mutable fixtures."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 8,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            }
        ])

        # 8 shared / 10 total = 80% -> penalty ~0.16
        assert result["avg_shared_fixtures_ratio"] == 80.0
        assert result["avg_test_isolation_score"] < 1.0
        assert result["low_isolation_packs"] == 1

    def test_order_dependent_tests_detected(self):
        """Verify detection of order-dependent tests."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 0,
                "order_dependent_tests": 6,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            }
        ])

        # 6 order-dependent / 10 total = 60%
        assert result["avg_order_dependency_ratio"] == 60.0
        # Order dependency has high penalty (0.25 per 20%)
        assert result["avg_test_isolation_score"] < 0.6
        assert result["low_isolation_packs"] == 1

    def test_global_state_modification_without_cleanup(self):
        """Verify detection of global state modifications without cleanup."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 0,
                "order_dependent_tests": 0,
                "global_state_modifications": 4,
                "tests_without_cleanup": 4,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            }
        ])

        # Global state and cleanup issues both present
        assert result["avg_cleanup_issue_ratio"] == 40.0
        assert result["avg_test_isolation_score"] < 0.9

    def test_cross_test_pollution_detected(self):
        """Verify detection of cross-test data pollution."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 0,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 3,
                "test_reruns_needed": 0,
            }
        ])

        assert result["total_pollution_cases"] == 3
        assert result["packs_with_pollution"] == 1
        # 3 pollution cases = -0.3 penalty
        assert result["avg_test_isolation_score"] == 0.7

    def test_tests_requiring_reruns(self):
        """Verify detection of tests that need reruns to pass."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 0,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 4,
            }
        ])

        # 4 reruns / 10 total = 40%
        assert result["avg_rerun_ratio"] == 40.0
        # Rerun penalty should be applied
        assert result["avg_test_isolation_score"] < 1.0

    def test_multiple_isolation_issues_combined(self):
        """Verify multiple isolation issues compound score penalty."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 5,
                "order_dependent_tests": 3,
                "global_state_modifications": 2,
                "tests_without_cleanup": 4,
                "cross_test_pollution_cases": 2,
                "test_reruns_needed": 1,
            }
        ])

        # Multiple issues should result in low score
        assert result["avg_test_isolation_score"] < 0.5
        assert result["low_isolation_packs"] == 1
        assert result["total_pollution_cases"] == 2

    def test_multiple_packs_average_calculations(self):
        """Verify averages calculated across multiple packs."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 0,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            },
            {
                "pack_id": "pack2",
                "total_tests": 20,
                "shared_fixtures": 4,
                "order_dependent_tests": 2,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 1,
                "test_reruns_needed": 0,
            },
            {
                "pack_id": "pack3",
                "total_tests": 15,
                "shared_fixtures": 0,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            },
        ])

        assert result["total_packs"] == 3
        # (10 + 20 + 15) / 3 = 15
        assert result["avg_total_tests"] == 15.0
        # pack2 has some issues, others perfect
        assert 0.8 < result["avg_test_isolation_score"] < 1.0
        assert result["high_isolation_packs"] == 2
        assert result["total_pollution_cases"] == 1

    def test_isolation_score_classification(self):
        """Verify isolation score classification into high/low categories."""
        result = analyze_pack_test_isolation_score([
            # Perfect isolation
            {
                "pack_id": "p1",
                "total_tests": 10,
                "shared_fixtures": 0,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            },
            # Medium isolation (not classified)
            {
                "pack_id": "p2",
                "total_tests": 10,
                "shared_fixtures": 3,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            },
            # Poor isolation
            {
                "pack_id": "p3",
                "total_tests": 10,
                "shared_fixtures": 8,
                "order_dependent_tests": 6,
                "global_state_modifications": 4,
                "tests_without_cleanup": 5,
                "cross_test_pollution_cases": 3,
                "test_reruns_needed": 2,
            },
        ])

        assert result["high_isolation_packs"] == 1
        assert result["low_isolation_packs"] == 1

    def test_zero_tests_handled_gracefully(self):
        """Verify pack with zero tests is handled gracefully."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 0,
                "shared_fixtures": 0,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_tests"] == 0.0
        # Zero tests should not contribute to average
        assert result["avg_test_isolation_score"] == 0.0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                # Only required field provided
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_tests"] == 10.0
        # Missing fields should be treated as no issues
        assert result["avg_test_isolation_score"] == 1.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_test_isolation_score([
            "not a dict",
            {"pack_id": "pack1", "total_tests": 10},
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for integer fields."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": True,
                "shared_fixtures": False,
            }
        ])

        assert result["avg_total_tests"] == 0.0

    def test_optimal_pattern_no_shared_state(self):
        """Verify optimal pattern with no shared state."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 20,
                "shared_fixtures": 0,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            }
        ])

        assert result["avg_test_isolation_score"] == 1.0
        assert result["high_isolation_packs"] == 1
        assert result["avg_shared_fixtures_ratio"] == 0.0
        assert result["total_pollution_cases"] == 0

    def test_anti_pattern_all_tests_order_dependent(self):
        """Verify anti-pattern with all tests order-dependent."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 10,
                "order_dependent_tests": 10,
                "global_state_modifications": 10,
                "tests_without_cleanup": 10,
                "cross_test_pollution_cases": 5,
                "test_reruns_needed": 5,
            }
        ])

        # Severe isolation problems
        assert result["avg_test_isolation_score"] == 0.0
        assert result["low_isolation_packs"] == 1
        assert result["avg_shared_fixtures_ratio"] == 100.0
        assert result["avg_order_dependency_ratio"] == 100.0

    def test_anti_pattern_high_pollution_cases(self):
        """Verify anti-pattern with high pollution cases."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 0,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 15,
                "test_reruns_needed": 0,
            }
        ])

        assert result["total_pollution_cases"] == 15
        assert result["packs_with_pollution"] == 1
        # High pollution should severely impact score (capped at -1.0)
        assert result["avg_test_isolation_score"] == 0.0

    def test_shared_fixtures_ratio_calculation(self):
        """Verify shared fixtures ratio percentage calculation."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 20,
                "shared_fixtures": 5,
            }
        ])

        # 5 shared / 20 total = 25%
        assert result["avg_shared_fixtures_ratio"] == 25.0

    def test_cleanup_issue_ratio_calculation(self):
        """Verify cleanup issue ratio calculation."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 50,
                "tests_without_cleanup": 10,
            }
        ])

        # 10 without cleanup / 50 total = 20%
        assert result["avg_cleanup_issue_ratio"] == 20.0

    def test_multiple_packs_pollution_aggregation(self):
        """Verify pollution cases aggregated across packs."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "p1",
                "total_tests": 10,
                "cross_test_pollution_cases": 2,
            },
            {
                "pack_id": "p2",
                "total_tests": 10,
                "cross_test_pollution_cases": 0,
            },
            {
                "pack_id": "p3",
                "total_tests": 10,
                "cross_test_pollution_cases": 3,
            },
        ])

        # 2 + 0 + 3 = 5 total
        assert result["total_pollution_cases"] == 5
        # 2 packs have pollution
        assert result["packs_with_pollution"] == 2

    def test_partial_isolation_issues(self):
        """Verify partial isolation issues affect score proportionally."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 2,
                "order_dependent_tests": 1,
                "global_state_modifications": 0,
                "tests_without_cleanup": 1,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            }
        ])

        # Some issues but not severe
        assert 0.5 < result["avg_test_isolation_score"] < 1.0
        assert result["high_isolation_packs"] == 0
        assert result["low_isolation_packs"] == 0

    def test_medium_isolation_not_classified(self):
        """Verify medium isolation scores are not classified as high or low."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 10,
                "shared_fixtures": 2,
                "order_dependent_tests": 1,
                "global_state_modifications": 1,
                "tests_without_cleanup": 1,
                "cross_test_pollution_cases": 1,
                "test_reruns_needed": 0,
            }
        ])

        # Score should be medium range (0.5-0.8)
        assert 0.5 <= result["avg_test_isolation_score"] <= 0.8
        assert result["high_isolation_packs"] == 0
        assert result["low_isolation_packs"] == 0

    def test_score_clamped_to_zero(self):
        """Verify isolation score cannot go below zero."""
        result = analyze_pack_test_isolation_score([
            {
                "pack_id": "pack1",
                "total_tests": 5,
                "shared_fixtures": 5,
                "order_dependent_tests": 5,
                "global_state_modifications": 5,
                "tests_without_cleanup": 5,
                "cross_test_pollution_cases": 20,
                "test_reruns_needed": 5,
            }
        ])

        # Extreme issues should clamp to 0.0
        assert result["avg_test_isolation_score"] == 0.0

    def test_mixed_isolation_quality_across_packs(self):
        """Verify mixed isolation quality across multiple packs."""
        result = analyze_pack_test_isolation_score([
            # Perfect
            {
                "pack_id": "p1",
                "total_tests": 10,
                "shared_fixtures": 0,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            },
            # Good
            {
                "pack_id": "p2",
                "total_tests": 10,
                "shared_fixtures": 1,
                "order_dependent_tests": 0,
                "global_state_modifications": 0,
                "tests_without_cleanup": 0,
                "cross_test_pollution_cases": 0,
                "test_reruns_needed": 0,
            },
            # Medium
            {
                "pack_id": "p3",
                "total_tests": 10,
                "shared_fixtures": 3,
                "order_dependent_tests": 2,
                "global_state_modifications": 1,
                "tests_without_cleanup": 1,
                "cross_test_pollution_cases": 1,
                "test_reruns_needed": 1,
            },
            # Poor
            {
                "pack_id": "p4",
                "total_tests": 10,
                "shared_fixtures": 7,
                "order_dependent_tests": 5,
                "global_state_modifications": 3,
                "tests_without_cleanup": 4,
                "cross_test_pollution_cases": 2,
                "test_reruns_needed": 2,
            },
        ])

        assert result["total_packs"] == 4
        assert result["high_isolation_packs"] >= 1
        assert result["low_isolation_packs"] >= 1
        # Average should be somewhere in middle
        assert 0.3 < result["avg_test_isolation_score"] < 0.9
