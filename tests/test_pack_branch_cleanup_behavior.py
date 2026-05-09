"""Tests for pack branch cleanup behavior analyzer."""

import pytest

from synthesis.pack_branch_cleanup_behavior import analyze_pack_branch_cleanup_behavior


class TestAnalyzePackBranchCleanupBehavior:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_branch_cleanup_behavior([])

        assert result["total_packs"] == 0
        assert result["packs_with_branches"] == 0
        assert result["branches_created"] == 0
        assert result["branches_deleted"] == 0
        assert result["orphaned_branches"] == 0
        assert result["cleanup_success_rate"] == 0.0
        assert result["avg_cleanup_timing_seconds"] == 0.0
        assert result["timely_cleanup_count"] == 0
        assert result["naming_violations"] == 0
        assert result["naming_consistency_rate"] == 0.0
        assert result["completed_packs_cleaned"] == 0
        assert result["failed_packs_cleaned"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_branch_cleanup_behavior(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_branch_cleanup_behavior("not a list")

    def test_pack_without_branch(self):
        """Verify pack without branch creation is tracked."""
        result = analyze_pack_branch_cleanup_behavior([
            {"pack_id": "pack1", "pack_status": "completed"}
        ])

        assert result["total_packs"] == 1
        assert result["packs_with_branches"] == 0
        assert result["branches_created"] == 0

    def test_pack_with_branch_creation_only(self):
        """Verify pack with branch creation but no deletion."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test",
                "branch_deleted": False,
                "pack_status": "completed",
                "branch_name_valid": True,
            }
        ])

        assert result["total_packs"] == 1
        assert result["packs_with_branches"] == 1
        assert result["branches_created"] == 1
        assert result["branches_deleted"] == 0
        assert result["orphaned_branches"] == 1
        assert result["cleanup_success_rate"] == 0.0

    def test_pack_with_successful_cleanup(self):
        """Verify pack with successful branch cleanup."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test",
                "branch_deleted": True,
                "pack_status": "completed",
                "cleanup_timing_seconds": 120.0,
                "branch_name_valid": True,
            }
        ])

        assert result["branches_created"] == 1
        assert result["branches_deleted"] == 1
        assert result["orphaned_branches"] == 0
        assert result["cleanup_success_rate"] == 100.0
        assert result["avg_cleanup_timing_seconds"] == 120.0
        assert result["timely_cleanup_count"] == 1
        assert result["completed_packs_cleaned"] == 1

    def test_multiple_packs_with_cleanup(self):
        """Verify multiple packs with varying cleanup behavior."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test1",
                "branch_deleted": True,
                "pack_status": "completed",
                "cleanup_timing_seconds": 100.0,
                "branch_name_valid": True,
            },
            {
                "pack_id": "pack2",
                "branch_created": "feature/test2",
                "branch_deleted": True,
                "pack_status": "completed",
                "cleanup_timing_seconds": 200.0,
                "branch_name_valid": True,
            },
            {
                "pack_id": "pack3",
                "branch_created": "feature/test3",
                "branch_deleted": False,
                "pack_status": "failed",
                "branch_name_valid": True,
            },
        ])

        assert result["total_packs"] == 3
        assert result["packs_with_branches"] == 3
        assert result["branches_created"] == 3
        assert result["branches_deleted"] == 2
        assert result["orphaned_branches"] == 1
        assert result["cleanup_success_rate"] == 66.67
        assert result["avg_cleanup_timing_seconds"] == 150.0
        assert result["completed_packs_cleaned"] == 2
        assert result["failed_packs_cleaned"] == 0

    def test_timely_cleanup_detection(self):
        """Verify timely cleanup detection within 300 seconds."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test1",
                "branch_deleted": True,
                "cleanup_timing_seconds": 150.0,
                "pack_status": "completed",
                "branch_name_valid": True,
            },
            {
                "pack_id": "pack2",
                "branch_created": "feature/test2",
                "branch_deleted": True,
                "cleanup_timing_seconds": 500.0,
                "pack_status": "completed",
                "branch_name_valid": True,
            },
        ])

        assert result["timely_cleanup_count"] == 1
        assert result["avg_cleanup_timing_seconds"] == 325.0

    def test_naming_violations_tracked(self):
        """Verify naming violations are tracked."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "invalid-name",
                "branch_deleted": True,
                "pack_status": "completed",
                "branch_name_valid": False,
            },
            {
                "pack_id": "pack2",
                "branch_created": "feature/valid",
                "branch_deleted": True,
                "pack_status": "completed",
                "branch_name_valid": True,
            },
            {
                "pack_id": "pack3",
                "branch_created": "another-invalid",
                "branch_deleted": True,
                "pack_status": "completed",
                "branch_name_valid": False,
            },
        ])

        assert result["naming_violations"] == 2
        assert result["naming_consistency_rate"] == 33.33

    def test_failed_pack_with_cleanup(self):
        """Verify failed pack with branch cleanup."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test",
                "branch_deleted": True,
                "pack_status": "failed",
                "cleanup_timing_seconds": 50.0,
                "branch_name_valid": True,
            }
        ])

        assert result["branches_deleted"] == 1
        assert result["failed_packs_cleaned"] == 1
        assert result["completed_packs_cleaned"] == 0

    def test_cleanup_without_timing_data(self):
        """Verify cleanup without timing data is handled."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test",
                "branch_deleted": True,
                "pack_status": "completed",
                "branch_name_valid": True,
            }
        ])

        assert result["branches_deleted"] == 1
        assert result["avg_cleanup_timing_seconds"] == 0.0
        assert result["timely_cleanup_count"] == 0

    def test_zero_branches_cleanup_rate(self):
        """Verify cleanup rate with zero branches created."""
        result = analyze_pack_branch_cleanup_behavior([
            {"pack_id": "pack1", "pack_status": "completed"},
            {"pack_id": "pack2", "pack_status": "completed"},
        ])

        assert result["cleanup_success_rate"] == 0.0
        assert result["naming_consistency_rate"] == 0.0

    def test_all_orphaned_branches(self):
        """Verify all branches orphaned scenario."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test1",
                "branch_deleted": False,
                "pack_status": "completed",
                "branch_name_valid": True,
            },
            {
                "pack_id": "pack2",
                "branch_created": "feature/test2",
                "branch_deleted": False,
                "pack_status": "failed",
                "branch_name_valid": True,
            },
        ])

        assert result["branches_created"] == 2
        assert result["branches_deleted"] == 0
        assert result["orphaned_branches"] == 2
        assert result["cleanup_success_rate"] == 0.0

    def test_perfect_cleanup_scenario(self):
        """Verify perfect cleanup with all branches cleaned."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test1",
                "branch_deleted": True,
                "pack_status": "completed",
                "cleanup_timing_seconds": 100.0,
                "branch_name_valid": True,
            },
            {
                "pack_id": "pack2",
                "branch_created": "feature/test2",
                "branch_deleted": True,
                "pack_status": "completed",
                "cleanup_timing_seconds": 200.0,
                "branch_name_valid": True,
            },
        ])

        assert result["cleanup_success_rate"] == 100.0
        assert result["orphaned_branches"] == 0
        assert result["naming_consistency_rate"] == 100.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_branch_cleanup_behavior([
            "not a dict",
            {
                "pack_id": "pack1",
                "branch_created": "feature/test",
                "branch_deleted": True,
                "pack_status": "completed",
                "branch_name_valid": True,
            },
        ])

        assert result["total_packs"] == 1
        assert result["branches_created"] == 1

    def test_whitespace_handling_in_branch_name(self):
        """Verify whitespace in branch name is stripped."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "  feature/test  ",
                "branch_deleted": True,
                "pack_status": "completed",
                "branch_name_valid": True,
            }
        ])

        assert result["branches_created"] == 1

    def test_empty_branch_name_not_counted(self):
        """Verify empty branch name is not counted as created."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "",
                "branch_deleted": True,
                "pack_status": "completed",
                "branch_name_valid": True,
            }
        ])

        assert result["packs_with_branches"] == 0
        assert result["branches_created"] == 0

    def test_none_branch_name_not_counted(self):
        """Verify None branch name is not counted as created."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": None,
                "branch_deleted": True,
                "pack_status": "completed",
                "branch_name_valid": True,
            }
        ])

        assert result["packs_with_branches"] == 0
        assert result["branches_created"] == 0

    def test_missing_fields_handled_gracefully(self):
        """Verify missing fields are handled gracefully."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test",
            }
        ])

        assert result["branches_created"] == 1
        assert result["branches_deleted"] == 0
        assert result["orphaned_branches"] == 1

    def test_boolean_values_extracted_correctly(self):
        """Verify boolean extraction handles edge cases."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test1",
                "branch_deleted": True,
                "pack_status": "completed",
                "branch_name_valid": True,
            },
            {
                "pack_id": "pack2",
                "branch_created": "feature/test2",
                "branch_deleted": False,
                "pack_status": "completed",
                "branch_name_valid": False,
            },
        ])

        assert result["branches_deleted"] == 1
        assert result["orphaned_branches"] == 1
        assert result["naming_violations"] == 1

    def test_numeric_values_converted_to_float(self):
        """Verify numeric values are converted to float."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test",
                "branch_deleted": True,
                "pack_status": "completed",
                "cleanup_timing_seconds": 100,  # int
                "branch_name_valid": True,
            }
        ])

        assert result["avg_cleanup_timing_seconds"] == 100.0

    def test_skipped_pack_status(self):
        """Verify skipped pack status is handled."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test",
                "branch_deleted": True,
                "pack_status": "skipped",
                "branch_name_valid": True,
            }
        ])

        # Should not count in completed or failed
        assert result["completed_packs_cleaned"] == 0
        assert result["failed_packs_cleaned"] == 0
        assert result["branches_deleted"] == 1

    def test_mixed_pack_statuses(self):
        """Verify mixed pack statuses are tracked correctly."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test1",
                "branch_deleted": True,
                "pack_status": "completed",
                "branch_name_valid": True,
            },
            {
                "pack_id": "pack2",
                "branch_created": "feature/test2",
                "branch_deleted": True,
                "pack_status": "failed",
                "branch_name_valid": True,
            },
            {
                "pack_id": "pack3",
                "branch_created": "feature/test3",
                "branch_deleted": True,
                "pack_status": "skipped",
                "branch_name_valid": True,
            },
        ])

        assert result["completed_packs_cleaned"] == 1
        assert result["failed_packs_cleaned"] == 1
        assert result["branches_deleted"] == 3

    def test_edge_case_timing_exactly_300_seconds(self):
        """Verify cleanup timing exactly at 300 seconds threshold."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test",
                "branch_deleted": True,
                "pack_status": "completed",
                "cleanup_timing_seconds": 300.0,
                "branch_name_valid": True,
            }
        ])

        assert result["timely_cleanup_count"] == 1

    def test_edge_case_timing_just_over_300_seconds(self):
        """Verify cleanup timing just over 300 seconds is not timely."""
        result = analyze_pack_branch_cleanup_behavior([
            {
                "pack_id": "pack1",
                "branch_created": "feature/test",
                "branch_deleted": True,
                "pack_status": "completed",
                "cleanup_timing_seconds": 300.1,
                "branch_name_valid": True,
            }
        ])

        assert result["timely_cleanup_count"] == 0

    def test_real_world_scenario(self):
        """Verify real-world scenario with mixed cleanup behavior."""
        result = analyze_pack_branch_cleanup_behavior([
            # Successful pack with good cleanup
            {
                "pack_id": "pack1",
                "branch_created": "feature/user-auth",
                "branch_deleted": True,
                "pack_status": "completed",
                "cleanup_timing_seconds": 45.0,
                "branch_name_valid": True,
            },
            # Failed pack with cleanup
            {
                "pack_id": "pack2",
                "branch_created": "feature/api-endpoint",
                "branch_deleted": True,
                "pack_status": "failed",
                "cleanup_timing_seconds": 120.0,
                "branch_name_valid": True,
            },
            # Orphaned branch with invalid name
            {
                "pack_id": "pack3",
                "branch_created": "bad_branch_name",
                "branch_deleted": False,
                "pack_status": "completed",
                "branch_name_valid": False,
            },
            # Pack without branch
            {
                "pack_id": "pack4",
                "pack_status": "completed",
            },
            # Slow cleanup but successful
            {
                "pack_id": "pack5",
                "branch_created": "feature/refactor",
                "branch_deleted": True,
                "pack_status": "completed",
                "cleanup_timing_seconds": 450.0,
                "branch_name_valid": True,
            },
        ])

        assert result["total_packs"] == 5
        assert result["packs_with_branches"] == 4
        assert result["branches_created"] == 4
        assert result["branches_deleted"] == 3
        assert result["orphaned_branches"] == 1
        assert result["cleanup_success_rate"] == 75.0
        assert result["avg_cleanup_timing_seconds"] == 205.0
        assert result["timely_cleanup_count"] == 2
        assert result["naming_violations"] == 1
        assert result["naming_consistency_rate"] == 75.0
        assert result["completed_packs_cleaned"] == 2
        assert result["failed_packs_cleaned"] == 1
