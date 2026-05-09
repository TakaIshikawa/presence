"""Tests for pack file scope consistency analyzer."""

import pytest

from synthesis.pack_file_scope_consistency import analyze_pack_file_scope_consistency


class TestAnalyzePackFileScopeConsistency:
    """Test main analyzer function."""

    def test_empty_tasks_returns_zeroed_metrics(self):
        """Verify empty task list returns zero metrics."""
        result = analyze_pack_file_scope_consistency([])

        assert result["total_tasks"] == 0
        assert result["tasks_with_overlapping_files"] == 0
        assert result["total_expected_files"] == 0
        assert result["total_shared_files"] == 0
        assert result["total_actual_edits"] == 0
        assert result["total_scope_divergence"] == 0
        assert result["consistency_score"] == 0.0
        assert result["hotspot_collision_rate"] == 0.0
        assert result["avg_overlap_per_task"] == 0.0
        assert result["scope_divergence_rate"] == 0.0
        assert result["tasks_with_no_overlap"] == 0
        assert result["tasks_with_complete_overlap"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_file_scope_consistency(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_file_scope_consistency("not a list")

    def test_non_overlapping_files(self):
        """Verify tasks with no overlapping files."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": ["file1.py", "file2.py"],
                "actual_files_edited": ["file1.py", "file2.py"],
                "shared_files_count": 0,
                "scope_divergence_count": 0,
            },
            {
                "task_id": "task2",
                "expected_files": ["file3.py", "file4.py"],
                "actual_files_edited": ["file3.py", "file4.py"],
                "shared_files_count": 0,
                "scope_divergence_count": 0,
            },
        ])

        assert result["total_tasks"] == 2
        assert result["tasks_with_overlapping_files"] == 0
        assert result["tasks_with_no_overlap"] == 2
        assert result["total_expected_files"] == 4
        assert result["total_shared_files"] == 0
        assert result["total_actual_edits"] == 4
        assert result["consistency_score"] == 100.0
        assert result["avg_overlap_per_task"] == 0.0

    def test_partial_overlap(self):
        """Verify tasks with partial file overlap."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": ["file1.py", "file2.py", "shared.py"],
                "actual_files_edited": ["file1.py", "file2.py", "shared.py"],
                "shared_files_count": 1,
            },
            {
                "task_id": "task2",
                "expected_files": ["file3.py", "shared.py"],
                "actual_files_edited": ["file3.py", "shared.py"],
                "shared_files_count": 1,
            },
        ])

        assert result["tasks_with_overlapping_files"] == 2
        assert result["total_shared_files"] == 2
        assert result["avg_overlap_per_task"] == 1.0

    def test_complete_hotspot_collision(self):
        """Verify tasks with complete hotspot collision."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": ["hotspot.py"],
                "actual_files_edited": ["hotspot.py"],
                "shared_files_count": 1,
                "is_hotspot_file": True,
            },
            {
                "task_id": "task2",
                "expected_files": ["hotspot.py"],
                "actual_files_edited": ["hotspot.py"],
                "shared_files_count": 1,
                "is_hotspot_file": True,
            },
            {
                "task_id": "task3",
                "expected_files": ["hotspot.py"],
                "actual_files_edited": ["hotspot.py"],
                "shared_files_count": 1,
                "is_hotspot_file": True,
            },
        ])

        assert result["total_tasks"] == 3
        assert result["hotspot_collision_rate"] == 100.0
        assert result["tasks_with_complete_overlap"] == 3

    def test_scope_divergence_tracking(self):
        """Verify tracking of scope divergence (unexpected edits)."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": ["file1.py"],
                "actual_files_edited": ["file1.py", "unexpected1.py", "unexpected2.py"],
                "scope_divergence_count": 2,
            },
        ])

        assert result["total_scope_divergence"] == 2
        assert result["total_actual_edits"] == 3
        # 2/3 * 100 = 66.67
        assert result["scope_divergence_rate"] == 66.67

    def test_perfect_consistency(self):
        """Verify perfect consistency (all edits within scope)."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": ["file1.py", "file2.py"],
                "actual_files_edited": ["file1.py", "file2.py"],
                "scope_divergence_count": 0,
            },
        ])

        assert result["consistency_score"] == 100.0
        assert result["scope_divergence_rate"] == 0.0

    def test_partial_consistency(self):
        """Verify partial consistency (some edits outside scope)."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": ["file1.py", "file2.py"],
                "actual_files_edited": ["file1.py", "file2.py", "unexpected.py"],
            },
        ])

        # 2 within scope, 1 outside scope
        # consistency: 2/3 * 100 = 66.67
        # divergence: 1/3 * 100 = 33.33
        assert result["consistency_score"] == 66.67
        assert result["scope_divergence_rate"] == 33.33

    def test_no_hotspot_collisions(self):
        """Verify tasks with no hotspot collisions."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "is_hotspot_file": False,
            },
            {
                "task_id": "task2",
                "is_hotspot_file": False,
            },
        ])

        assert result["hotspot_collision_rate"] == 0.0

    def test_mixed_hotspot_usage(self):
        """Verify mixed hotspot usage across tasks."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "is_hotspot_file": True,
            },
            {
                "task_id": "task2",
                "is_hotspot_file": False,
            },
            {
                "task_id": "task3",
                "is_hotspot_file": True,
            },
            {
                "task_id": "task4",
                "is_hotspot_file": False,
            },
        ])

        # 2/4 * 100 = 50.0
        assert result["hotspot_collision_rate"] == 50.0

    def test_well_distributed_edits(self):
        """Verify well-distributed edits (low overlap)."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": ["file1.py", "file2.py", "file3.py"],
                "actual_files_edited": ["file1.py", "file2.py", "file3.py"],
                "shared_files_count": 0,
            },
            {
                "task_id": "task2",
                "expected_files": ["file4.py", "file5.py"],
                "actual_files_edited": ["file4.py", "file5.py"],
                "shared_files_count": 0,
            },
            {
                "task_id": "task3",
                "expected_files": ["file6.py"],
                "actual_files_edited": ["file6.py"],
                "shared_files_count": 0,
            },
        ])

        assert result["tasks_with_overlapping_files"] == 0
        assert result["tasks_with_no_overlap"] == 3
        assert result["avg_overlap_per_task"] == 0.0

    def test_concentrated_edits(self):
        """Verify concentrated edits (high overlap)."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": ["shared1.py", "shared2.py"],
                "shared_files_count": 2,
            },
            {
                "task_id": "task2",
                "expected_files": ["shared1.py", "shared2.py"],
                "shared_files_count": 2,
            },
            {
                "task_id": "task3",
                "expected_files": ["shared1.py", "shared2.py"],
                "shared_files_count": 2,
            },
        ])

        assert result["tasks_with_overlapping_files"] == 3
        assert result["tasks_with_no_overlap"] == 0
        assert result["total_shared_files"] == 6
        assert result["avg_overlap_per_task"] == 2.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_file_scope_consistency([
            "not a dict",
            {
                "task_id": "task1",
                "expected_files": ["file1.py"],
            },
        ])

        assert result["total_tasks"] == 1

    def test_boolean_values_not_extracted_as_numbers(self):
        """Verify boolean values are not extracted as numbers."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "shared_files_count": True,
                "scope_divergence_count": False,
            },
        ])

        assert result["total_shared_files"] == 0
        assert result["total_scope_divergence"] == 0

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "shared_files_count": 2.0,
                "scope_divergence_count": 1.0,
            },
        ])

        assert result["total_shared_files"] == 2
        assert result["total_scope_divergence"] == 1

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                # Missing most fields
            },
        ])

        assert result["total_tasks"] == 1
        assert result["total_expected_files"] == 0
        assert result["total_actual_edits"] == 0

    def test_comprehensive_task_all_fields(self):
        """Verify comprehensive task with all fields populated."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "comprehensive",
                "expected_files": ["file1.py", "file2.py", "shared.py"],
                "actual_files_edited": ["file1.py", "file2.py", "shared.py"],
                "shared_files_count": 1,
                "is_hotspot_file": True,
                "scope_divergence_count": 0,
            },
        ])

        assert result["total_tasks"] == 1
        assert result["tasks_with_overlapping_files"] == 1
        assert result["total_expected_files"] == 3
        assert result["total_shared_files"] == 1
        assert result["total_actual_edits"] == 3
        assert result["total_scope_divergence"] == 0
        assert result["consistency_score"] == 100.0
        assert result["hotspot_collision_rate"] == 100.0
        assert result["avg_overlap_per_task"] == 1.0
        assert result["scope_divergence_rate"] == 0.0

    def test_empty_file_lists(self):
        """Verify handling of empty file lists."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": [],
                "actual_files_edited": [],
            },
        ])

        assert result["total_expected_files"] == 0
        assert result["total_actual_edits"] == 0
        assert result["consistency_score"] == 0.0

    def test_expected_files_no_actual_edits(self):
        """Verify task with expected files but no actual edits."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": ["file1.py", "file2.py"],
                "actual_files_edited": [],
            },
        ])

        assert result["total_expected_files"] == 2
        assert result["total_actual_edits"] == 0
        assert result["consistency_score"] == 0.0

    def test_actual_edits_no_expected_files(self):
        """Verify task with actual edits but no expected files."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": [],
                "actual_files_edited": ["file1.py", "file2.py"],
            },
        ])

        assert result["total_expected_files"] == 0
        assert result["total_actual_edits"] == 2
        # All edits are outside scope (0/2)
        assert result["consistency_score"] == 0.0
        assert result["scope_divergence_rate"] == 100.0

    def test_complete_overlap_detection(self):
        """Verify detection of tasks with complete overlap."""
        result = analyze_pack_file_scope_consistency([
            {
                "task_id": "task1",
                "expected_files": ["file1.py", "file2.py"],
                "shared_files_count": 2,
            },
            {
                "task_id": "task2",
                "expected_files": ["file1.py"],
                "shared_files_count": 0,
            },
        ])

        assert result["tasks_with_complete_overlap"] == 1

    def test_average_overlap_calculation(self):
        """Verify average overlap per task calculation."""
        result = analyze_pack_file_scope_consistency([
            {"task_id": "task1", "shared_files_count": 1},
            {"task_id": "task2", "shared_files_count": 3},
            {"task_id": "task3", "shared_files_count": 2},
        ])

        # (1 + 3 + 2) / 3 = 2.0
        assert result["avg_overlap_per_task"] == 2.0

    def test_multiple_tasks_mixed_scenarios(self):
        """Verify multiple tasks with mixed scenarios."""
        result = analyze_pack_file_scope_consistency([
            # Task with perfect alignment
            {
                "task_id": "task1",
                "expected_files": ["file1.py"],
                "actual_files_edited": ["file1.py"],
                "shared_files_count": 0,
                "is_hotspot_file": False,
            },
            # Task with scope divergence
            {
                "task_id": "task2",
                "expected_files": ["file2.py"],
                "actual_files_edited": ["file2.py", "extra.py"],
                "shared_files_count": 1,
                "is_hotspot_file": True,
                "scope_divergence_count": 1,
            },
            # Task with partial overlap
            {
                "task_id": "task3",
                "expected_files": ["file3.py", "file4.py"],
                "actual_files_edited": ["file3.py"],
                "shared_files_count": 1,
                "is_hotspot_file": False,
            },
        ])

        assert result["total_tasks"] == 3
        assert result["tasks_with_overlapping_files"] == 2
        assert result["tasks_with_no_overlap"] == 1
        assert result["total_scope_divergence"] == 1
        assert result["hotspot_collision_rate"] == 33.33
