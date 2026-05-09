"""Tests for pack branch merge readiness analyzer."""

import pytest

from synthesis.pack_branch_merge_readiness import analyze_pack_branch_merge_readiness


class TestAnalyzePackBranchMergeReadiness:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_branch_merge_readiness([])

        assert result["total_packs"] == 0
        assert result["ready_packs"] == 0
        assert result["needs_work_packs"] == 0
        assert result["high_risk_packs"] == 0
        assert result["blocked_packs"] == 0
        assert result["avg_verification_pass_rate"] == 0.0
        assert result["avg_test_coverage_delta"] == 0.0
        assert result["total_type_safety_issues"] == 0
        assert result["avg_merge_conflict_risk"] == 0.0
        assert result["total_unresolved_todos"] == 0
        assert result["stale_branches"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_branch_merge_readiness(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_branch_merge_readiness("not a list")

    def test_ready_pack_all_checks_pass(self):
        """Verify pack is ready when all checks pass."""
        result = analyze_pack_branch_merge_readiness([
            {
                "pack_id": "pack1",
                "verification_pass_rate": 100.0,
                "test_coverage_delta": 5.0,
                "type_safety_issues": 0,
                "merge_conflict_risk_score": 0.0,
                "unresolved_todos_count": 0,
                "branch_staleness_days": 2,
            }
        ])

        assert result["ready_packs"] == 1
        assert result["needs_work_packs"] == 0

    def test_needs_work_pack_with_todos(self):
        """Verify pack needs work when TODOs exist."""
        result = analyze_pack_branch_merge_readiness([
            {
                "verification_pass_rate": 100.0,
                "type_safety_issues": 0,
                "merge_conflict_risk_score": 0.0,
                "unresolved_todos_count": 3,
            }
        ])

        assert result["needs_work_packs"] == 1
        assert result["total_unresolved_todos"] == 3

    def test_needs_work_pack_with_type_error(self):
        """Verify pack needs work with single type error."""
        result = analyze_pack_branch_merge_readiness([
            {
                "verification_pass_rate": 100.0,
                "type_safety_issues": 1,
            }
        ])

        assert result["needs_work_packs"] == 1
        assert result["total_type_safety_issues"] == 1

    def test_high_risk_pack_low_pass_rate(self):
        """Verify pack is high risk with <80% pass rate."""
        result = analyze_pack_branch_merge_readiness([
            {
                "verification_pass_rate": 75.0,
            }
        ])

        assert result["high_risk_packs"] == 1

    def test_high_risk_pack_multiple_type_errors(self):
        """Verify pack is high risk with >2 type errors."""
        result = analyze_pack_branch_merge_readiness([
            {
                "verification_pass_rate": 100.0,
                "type_safety_issues": 3,
            }
        ])

        assert result["high_risk_packs"] == 1

    def test_high_risk_pack_stale_branch(self):
        """Verify pack is high risk when >14 days old."""
        result = analyze_pack_branch_merge_readiness([
            {
                "verification_pass_rate": 100.0,
                "type_safety_issues": 0,
                "branch_staleness_days": 20,
            }
        ])

        assert result["high_risk_packs"] == 1

    def test_blocked_pack_very_low_pass_rate(self):
        """Verify pack is blocked with <50% pass rate."""
        result = analyze_pack_branch_merge_readiness([
            {
                "verification_pass_rate": 40.0,
            }
        ])

        assert result["blocked_packs"] == 1

    def test_blocked_pack_many_type_errors(self):
        """Verify pack is blocked with >5 type errors."""
        result = analyze_pack_branch_merge_readiness([
            {
                "type_safety_issues": 8,
            }
        ])

        assert result["blocked_packs"] == 1

    def test_blocked_pack_high_conflict_risk(self):
        """Verify pack is blocked with >0.7 conflict risk."""
        result = analyze_pack_branch_merge_readiness([
            {
                "merge_conflict_risk_score": 0.8,
            }
        ])

        assert result["blocked_packs"] == 1

    def test_stale_branches_count(self):
        """Verify stale branches (>7 days) are counted."""
        result = analyze_pack_branch_merge_readiness([
            {"branch_staleness_days": 5},
            {"branch_staleness_days": 10},
            {"branch_staleness_days": 15},
        ])

        assert result["stale_branches"] == 2

    def test_avg_verification_pass_rate(self):
        """Verify average verification pass rate calculation."""
        result = analyze_pack_branch_merge_readiness([
            {"verification_pass_rate": 100.0},
            {"verification_pass_rate": 80.0},
            {"verification_pass_rate": 90.0},
        ])

        # (100 + 80 + 90) / 3 = 90
        assert result["avg_verification_pass_rate"] == 90.0

    def test_avg_test_coverage_delta(self):
        """Verify average test coverage delta calculation."""
        result = analyze_pack_branch_merge_readiness([
            {"test_coverage_delta": 5.0},
            {"test_coverage_delta": -2.0},
            {"test_coverage_delta": 10.0},
        ])

        # (5 - 2 + 10) / 3 = 4.33
        assert result["avg_test_coverage_delta"] == 4.33

    def test_avg_merge_conflict_risk(self):
        """Verify average merge conflict risk calculation."""
        result = analyze_pack_branch_merge_readiness([
            {"merge_conflict_risk_score": 0.1},
            {"merge_conflict_risk_score": 0.3},
            {"merge_conflict_risk_score": 0.2},
        ])

        # (0.1 + 0.3 + 0.2) / 3 = 0.2
        assert result["avg_merge_conflict_risk"] == 0.2

    def test_total_type_safety_issues(self):
        """Verify total type safety issues across packs."""
        result = analyze_pack_branch_merge_readiness([
            {"type_safety_issues": 2},
            {"type_safety_issues": 3},
            {"type_safety_issues": 1},
        ])

        assert result["total_type_safety_issues"] == 6

    def test_total_unresolved_todos(self):
        """Verify total unresolved TODOs across packs."""
        result = analyze_pack_branch_merge_readiness([
            {"unresolved_todos_count": 5},
            {"unresolved_todos_count": 2},
            {"unresolved_todos_count": 3},
        ])

        assert result["total_unresolved_todos"] == 10

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_branch_merge_readiness([
            "not a dict",
            {"verification_pass_rate": 100.0},
        ])

        assert result["total_packs"] == 1

    def test_missing_metrics_handled(self):
        """Verify missing metrics don't cause errors."""
        result = analyze_pack_branch_merge_readiness([
            {"pack_id": "pack1"},
        ])

        assert result["total_packs"] == 1
        # Should be categorized as ready (no failures)
        assert result["ready_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are not treated as numeric."""
        result = analyze_pack_branch_merge_readiness([
            {"verification_pass_rate": True, "type_safety_issues": False},
        ])

        assert result["avg_verification_pass_rate"] == 0.0
        assert result["total_type_safety_issues"] == 0

    def test_float_type_issues_converted_to_int(self):
        """Verify float type issue counts are converted to int."""
        result = analyze_pack_branch_merge_readiness([
            {"type_safety_issues": 3.7},
        ])

        assert result["total_type_safety_issues"] == 3

    def test_multiple_packs_mixed_readiness(self):
        """Verify mixed readiness across multiple packs."""
        result = analyze_pack_branch_merge_readiness([
            {"verification_pass_rate": 100.0, "type_safety_issues": 0},
            {"verification_pass_rate": 95.0},
            {"verification_pass_rate": 70.0},
            {"verification_pass_rate": 40.0},
        ])

        assert result["total_packs"] == 4
        assert result["ready_packs"] == 1
        assert result["needs_work_packs"] == 1
        assert result["high_risk_packs"] == 1
        assert result["blocked_packs"] == 1

    def test_conflict_risk_boundary_cases(self):
        """Verify conflict risk boundary classifications."""
        result = analyze_pack_branch_merge_readiness([
            {"merge_conflict_risk_score": 0.2, "verification_pass_rate": 100.0},
            {"merge_conflict_risk_score": 0.21, "verification_pass_rate": 100.0},
            {"merge_conflict_risk_score": 0.5, "verification_pass_rate": 100.0},
            {"merge_conflict_risk_score": 0.51, "verification_pass_rate": 100.0},
        ])

        # 0.2 = ready, 0.21 = needs_work, 0.5 = needs_work, 0.51 = high_risk
        assert result["ready_packs"] == 1
        assert result["needs_work_packs"] == 2
        assert result["high_risk_packs"] == 1

    def test_negative_coverage_delta_handled(self):
        """Verify negative coverage deltas are handled correctly."""
        result = analyze_pack_branch_merge_readiness([
            {"test_coverage_delta": -5.0},
            {"test_coverage_delta": 3.0},
        ])

        # Average: (-5 + 3) / 2 = -1
        assert result["avg_test_coverage_delta"] == -1.0
