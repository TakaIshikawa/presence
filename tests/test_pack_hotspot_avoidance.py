"""Tests for pack hotspot avoidance analyzer."""

import pytest

from synthesis.pack_hotspot_avoidance import analyze_pack_hotspot_avoidance


class TestAnalyzePackHotspotAvoidance:
    """Test main analyzer function."""

    def test_empty_tasks_returns_zeroed_metrics(self):
        """Verify empty task list returns zero metrics."""
        result = analyze_pack_hotspot_avoidance([])

        assert result["total_tasks"] == 0
        assert result["tasks_touching_hotspots"] == 0
        assert result["hotspot_collision_rate"] == 0.0
        assert result["total_expected_files"] == 0
        assert result["total_hotspot_files"] == 0
        assert result["total_non_hotspot_files"] == 0
        assert result["avoidance_score"] == 0.0
        assert result["avg_hotspots_per_task"] == 0.0
        assert result["tasks_with_no_hotspots"] == 0
        assert result["tasks_with_all_hotspots"] == 0
        assert result["well_distributed_edits"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_hotspot_avoidance(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_hotspot_avoidance("not a list")

    def test_no_hotspot_tasks(self):
        """Verify pack with no hotspot file usage."""
        result = analyze_pack_hotspot_avoidance([
            {
                "task_id": "task1",
                "expected_files": ["file1.py", "file2.py"],
                "hotspot_files_count": 0,
                "is_touching_hotspot": False,
            },
            {
                "task_id": "task2",
                "expected_files": ["file3.py", "file4.py"],
                "hotspot_files_count": 0,
                "is_touching_hotspot": False,
            },
        ])

        assert result["total_tasks"] == 2
        assert result["tasks_touching_hotspots"] == 0
        assert result["hotspot_collision_rate"] == 0.0
        assert result["total_expected_files"] == 4
        assert result["total_hotspot_files"] == 0
        assert result["total_non_hotspot_files"] == 4
        assert result["avoidance_score"] == 100.0
        assert result["tasks_with_no_hotspots"] == 2

    def test_partial_hotspot_usage(self):
        """Verify pack with partial hotspot usage."""
        result = analyze_pack_hotspot_avoidance([
            {
                "task_id": "task1",
                "expected_files": ["file1.py", "hotspot.py"],
                "hotspot_files_count": 1,
                "total_files_count": 2,
                "is_touching_hotspot": True,
            },
            {
                "task_id": "task2",
                "expected_files": ["file2.py", "file3.py"],
                "hotspot_files_count": 0,
                "total_files_count": 2,
                "is_touching_hotspot": False,
            },
        ])

        assert result["tasks_touching_hotspots"] == 1
        assert result["hotspot_collision_rate"] == 50.0
        assert result["total_expected_files"] == 4
        assert result["total_hotspot_files"] == 1
        assert result["total_non_hotspot_files"] == 3
        # 3/4 * 100 = 75.0
        assert result["avoidance_score"] == 75.0

    def test_all_hotspot_pack(self):
        """Verify pack where all tasks use hotspot files."""
        result = analyze_pack_hotspot_avoidance([
            {
                "task_id": "task1",
                "expected_files": ["hotspot1.py"],
                "hotspot_files_count": 1,
                "total_files_count": 1,
                "is_touching_hotspot": True,
            },
            {
                "task_id": "task2",
                "expected_files": ["hotspot2.py"],
                "hotspot_files_count": 1,
                "total_files_count": 1,
                "is_touching_hotspot": True,
            },
        ])

        assert result["hotspot_collision_rate"] == 100.0
        assert result["avoidance_score"] == 0.0
        assert result["tasks_with_all_hotspots"] == 2

    def test_well_distributed_edits(self):
        """Verify detection of well-distributed edits."""
        result = analyze_pack_hotspot_avoidance([
            {
                "task_id": "task1",
                "unique_files_edited": ["file1.py", "file2.py", "file3.py"],
            },
            {
                "task_id": "task2",
                "unique_files_edited": ["file4.py", "file5.py"],
            },
        ])

        # Task1 has 3+ unique files, task2 has <3
        assert result["well_distributed_edits"] == 1

    def test_concentrated_edits(self):
        """Verify detection of concentrated edits (few files)."""
        result = analyze_pack_hotspot_avoidance([
            {
                "task_id": "task1",
                "unique_files_edited": ["hotspot.py"],
            },
            {
                "task_id": "task2",
                "unique_files_edited": ["hotspot.py"],
            },
        ])

        assert result["well_distributed_edits"] == 0

    def test_avoidance_score_calculation(self):
        """Verify avoidance score calculation."""
        result = analyze_pack_hotspot_avoidance([
            {
                "expected_files": ["f1.py", "f2.py", "f3.py", "hotspot.py"],
                "hotspot_files_count": 1,
            },
        ])

        # 3 non-hotspot / 4 total = 75%
        assert result["avoidance_score"] == 75.0

    def test_avg_hotspots_per_task(self):
        """Verify average hotspots per task calculation."""
        result = analyze_pack_hotspot_avoidance([
            {"hotspot_files_count": 1},
            {"hotspot_files_count": 3},
            {"hotspot_files_count": 2},
        ])

        # (1 + 3 + 2) / 3 = 2.0
        assert result["avg_hotspots_per_task"] == 2.0

    def test_tasks_with_no_hotspots_count(self):
        """Verify counting of tasks with no hotspots."""
        result = analyze_pack_hotspot_avoidance([
            {"hotspot_files_count": 0},
            {"hotspot_files_count": 2},
            {"hotspot_files_count": 0},
            {"hotspot_files_count": 1},
        ])

        assert result["tasks_with_no_hotspots"] == 2

    def test_tasks_with_all_hotspots_count(self):
        """Verify counting of tasks with all hotspot files."""
        result = analyze_pack_hotspot_avoidance([
            {
                "hotspot_files_count": 2,
                "total_files_count": 2,
            },
            {
                "hotspot_files_count": 1,
                "total_files_count": 3,
            },
        ])

        assert result["tasks_with_all_hotspots"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_hotspot_avoidance([
            "not a dict",
            {
                "task_id": "task1",
                "expected_files": ["file1.py"],
            },
        ])

        assert result["total_tasks"] == 1

    def test_boolean_values_not_extracted_as_numbers(self):
        """Verify boolean values are not extracted as numbers."""
        result = analyze_pack_hotspot_avoidance([
            {
                "hotspot_files_count": True,
                "total_files_count": False,
            },
        ])

        assert result["total_hotspot_files"] == 0

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_pack_hotspot_avoidance([
            {
                "hotspot_files_count": 2.0,
                "total_files_count": 5.0,
            },
        ])

        assert result["total_hotspot_files"] == 2

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_hotspot_avoidance([
            {
                "task_id": "task1",
                # Missing most fields
            },
        ])

        assert result["total_tasks"] == 1
        assert result["total_expected_files"] == 0
        assert result["total_hotspot_files"] == 0

    def test_comprehensive_pack_all_fields(self):
        """Verify comprehensive pack with all fields populated."""
        result = analyze_pack_hotspot_avoidance([
            {
                "task_id": "task1",
                "expected_files": ["file1.py", "file2.py", "hotspot.py"],
                "hotspot_files_count": 1,
                "is_touching_hotspot": True,
                "total_files_count": 3,
                "unique_files_edited": ["file1.py", "file2.py", "hotspot.py"],
            },
            {
                "task_id": "task2",
                "expected_files": ["file3.py", "file4.py"],
                "hotspot_files_count": 0,
                "is_touching_hotspot": False,
                "total_files_count": 2,
                "unique_files_edited": ["file3.py", "file4.py"],
            },
        ])

        assert result["total_tasks"] == 2
        assert result["tasks_touching_hotspots"] == 1
        assert result["hotspot_collision_rate"] == 50.0
        assert result["total_expected_files"] == 5
        assert result["total_hotspot_files"] == 1
        assert result["total_non_hotspot_files"] == 4
        assert result["avoidance_score"] == 80.0
        assert result["avg_hotspots_per_task"] == 0.5
        assert result["tasks_with_no_hotspots"] == 1
        # Only task1 has 3 unique files, task2 has 2
        assert result["well_distributed_edits"] == 1

    def test_total_files_count_fallback(self):
        """Verify total_files_count falls back to expected_files length."""
        result = analyze_pack_hotspot_avoidance([
            {
                "expected_files": ["f1.py", "f2.py"],
                "hotspot_files_count": 2,
                # No total_files_count provided
            },
        ])

        # Should use len(expected_files) = 2
        assert result["tasks_with_all_hotspots"] == 1

    def test_high_avoidance_score(self):
        """Verify high avoidance score (>70%)."""
        result = analyze_pack_hotspot_avoidance([
            {
                "expected_files": ["f1", "f2", "f3", "f4", "f5"],
                "hotspot_files_count": 1,
            },
        ])

        # 4/5 = 80%
        assert result["avoidance_score"] == 80.0

    def test_low_avoidance_score(self):
        """Verify low avoidance score (<30%)."""
        result = analyze_pack_hotspot_avoidance([
            {
                "expected_files": ["f1", "f2", "f3", "f4"],
                "hotspot_files_count": 3,
            },
        ])

        # 1/4 = 25%
        assert result["avoidance_score"] == 25.0

    def test_edge_case_zero_expected_files(self):
        """Verify handling of zero expected files."""
        result = analyze_pack_hotspot_avoidance([
            {
                "expected_files": [],
            },
        ])

        assert result["total_expected_files"] == 0
        assert result["avoidance_score"] == 0.0

    def test_edge_case_exactly_3_unique_files(self):
        """Verify exactly 3 unique files counts as well-distributed."""
        result = analyze_pack_hotspot_avoidance([
            {
                "unique_files_edited": ["f1", "f2", "f3"],
            },
        ])

        assert result["well_distributed_edits"] == 1

    def test_edge_case_2_unique_files_not_distributed(self):
        """Verify 2 unique files doesn't count as well-distributed."""
        result = analyze_pack_hotspot_avoidance([
            {
                "unique_files_edited": ["f1", "f2"],
            },
        ])

        assert result["well_distributed_edits"] == 0

    def test_multiple_tasks_mixed_scenarios(self):
        """Verify multiple tasks with varied hotspot patterns."""
        result = analyze_pack_hotspot_avoidance([
            # No hotspots, well-distributed
            {
                "expected_files": ["f1", "f2", "f3"],
                "hotspot_files_count": 0,
                "is_touching_hotspot": False,
                "unique_files_edited": ["f1", "f2", "f3"],
            },
            # All hotspots
            {
                "expected_files": ["h1", "h2"],
                "hotspot_files_count": 2,
                "total_files_count": 2,
                "is_touching_hotspot": True,
                "unique_files_edited": ["h1"],
            },
            # Mixed
            {
                "expected_files": ["f4", "h3"],
                "hotspot_files_count": 1,
                "is_touching_hotspot": True,
                "unique_files_edited": ["f4", "h3", "f5", "f6"],
            },
        ])

        assert result["total_tasks"] == 3
        assert result["tasks_touching_hotspots"] == 2
        assert result["hotspot_collision_rate"] == 66.67
        assert result["tasks_with_no_hotspots"] == 1
        assert result["tasks_with_all_hotspots"] == 1
        assert result["well_distributed_edits"] == 2
