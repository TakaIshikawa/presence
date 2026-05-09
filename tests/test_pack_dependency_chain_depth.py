"""Tests for pack dependency chain depth analyzer."""

import pytest

from synthesis.pack_dependency_chain_depth import (
    analyze_pack_dependency_chain_depth,
)


class TestAnalyzePackDependencyChainDepth:
    """Test main analyzer function."""

    def test_empty_tasks_returns_zeroed_metrics(self):
        """Verify empty task list returns zero metrics."""
        result = analyze_pack_dependency_chain_depth([])

        assert result["total_tasks"] == 0
        assert result["root_tasks"] == 0
        assert result["root_task_percentage"] == 0.0
        assert result["max_dependency_chain_depth"] == 0
        assert result["avg_chain_depth"] == 0.0
        assert result["tasks_with_dependencies"] == 0
        assert result["circular_dependencies_detected"] == 0
        assert result["parallelization_potential_score"] == 0.0
        assert result["tasks_at_depth_0"] == 0
        assert result["tasks_at_depth_1"] == 0
        assert result["tasks_at_depth_2_plus"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_dependency_chain_depth(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_dependency_chain_depth("not a list")

    def test_all_independent_tasks(self):
        """Verify pack with all independent (root) tasks."""
        result = analyze_pack_dependency_chain_depth([
            {
                "task_id": "task1",
                "dependencies": [],
                "chain_depth": 0,
                "is_root_task": True,
            },
            {
                "task_id": "task2",
                "dependencies": [],
                "chain_depth": 0,
                "is_root_task": True,
            },
            {
                "task_id": "task3",
                "dependencies": [],
                "chain_depth": 0,
                "is_root_task": True,
            },
        ])

        assert result["total_tasks"] == 3
        assert result["root_tasks"] == 3
        assert result["root_task_percentage"] == 100.0
        assert result["max_dependency_chain_depth"] == 0
        assert result["avg_chain_depth"] == 0.0
        assert result["tasks_with_dependencies"] == 0
        assert result["parallelization_potential_score"] == 100.0
        assert result["tasks_at_depth_0"] == 3

    def test_linear_dependency_chain(self):
        """Verify pack with linear dependency chain."""
        result = analyze_pack_dependency_chain_depth([
            {
                "task_id": "task1",
                "dependencies": [],
                "chain_depth": 0,
                "is_root_task": True,
            },
            {
                "task_id": "task2",
                "dependencies": ["task1"],
                "chain_depth": 1,
                "is_root_task": False,
            },
            {
                "task_id": "task3",
                "dependencies": ["task2"],
                "chain_depth": 2,
                "is_root_task": False,
            },
            {
                "task_id": "task4",
                "dependencies": ["task3"],
                "chain_depth": 3,
                "is_root_task": False,
            },
        ])

        assert result["total_tasks"] == 4
        assert result["root_tasks"] == 1
        assert result["root_task_percentage"] == 25.0
        assert result["max_dependency_chain_depth"] == 3
        # (0 + 1 + 2 + 3) / 4 = 1.5
        assert result["avg_chain_depth"] == 1.5
        assert result["tasks_with_dependencies"] == 3
        assert result["parallelization_potential_score"] == 25.0
        assert result["tasks_at_depth_0"] == 1
        assert result["tasks_at_depth_1"] == 1
        assert result["tasks_at_depth_2_plus"] == 2

    def test_tree_structure_dependencies(self):
        """Verify pack with tree-structured dependencies."""
        result = analyze_pack_dependency_chain_depth([
            # Root task
            {
                "task_id": "root",
                "dependencies": [],
                "chain_depth": 0,
                "is_root_task": True,
            },
            # Two tasks depending on root
            {
                "task_id": "child1",
                "dependencies": ["root"],
                "chain_depth": 1,
            },
            {
                "task_id": "child2",
                "dependencies": ["root"],
                "chain_depth": 1,
            },
            # Tasks depending on children
            {
                "task_id": "grandchild1",
                "dependencies": ["child1"],
                "chain_depth": 2,
            },
            {
                "task_id": "grandchild2",
                "dependencies": ["child2"],
                "chain_depth": 2,
            },
        ])

        assert result["total_tasks"] == 5
        assert result["root_tasks"] == 1
        assert result["max_dependency_chain_depth"] == 2
        assert result["tasks_at_depth_0"] == 1
        assert result["tasks_at_depth_1"] == 2
        assert result["tasks_at_depth_2_plus"] == 2

    def test_circular_dependency_detection(self):
        """Verify detection of circular dependencies."""
        result = analyze_pack_dependency_chain_depth([
            {
                "task_id": "task1",
                "dependencies": ["task2"],
                "has_circular_dependency": True,
            },
            {
                "task_id": "task2",
                "dependencies": ["task1"],
                "has_circular_dependency": True,
            },
        ])

        assert result["circular_dependencies_detected"] == 2

    def test_no_circular_dependencies(self):
        """Verify packs with no circular dependencies."""
        result = analyze_pack_dependency_chain_depth([
            {
                "task_id": "task1",
                "dependencies": [],
                "has_circular_dependency": False,
            },
            {
                "task_id": "task2",
                "dependencies": ["task1"],
                "has_circular_dependency": False,
            },
        ])

        assert result["circular_dependencies_detected"] == 0

    def test_high_parallelization_potential(self):
        """Verify high parallelization potential (>70% root tasks)."""
        result = analyze_pack_dependency_chain_depth([
            {"task_id": "t1", "is_root_task": True},
            {"task_id": "t2", "is_root_task": True},
            {"task_id": "t3", "is_root_task": True},
            {"task_id": "t4", "is_root_task": False},
        ])

        # 3/4 = 75%
        assert result["parallelization_potential_score"] == 75.0

    def test_low_parallelization_potential(self):
        """Verify low parallelization potential (<30% root tasks)."""
        result = analyze_pack_dependency_chain_depth([
            {"task_id": "t1", "is_root_task": True},
            {"task_id": "t2", "is_root_task": False},
            {"task_id": "t3", "is_root_task": False},
            {"task_id": "t4", "is_root_task": False},
        ])

        # 1/4 = 25%
        assert result["parallelization_potential_score"] == 25.0

    def test_root_task_via_empty_dependencies(self):
        """Verify task with empty dependencies list is treated as root."""
        result = analyze_pack_dependency_chain_depth([
            {
                "task_id": "task1",
                "dependencies": [],
            },
        ])

        assert result["root_tasks"] == 1

    def test_depth_distribution(self):
        """Verify correct distribution of tasks by depth."""
        result = analyze_pack_dependency_chain_depth([
            {"chain_depth": 0, "is_root_task": True},
            {"chain_depth": 0, "is_root_task": True},
            {"chain_depth": 1},
            {"chain_depth": 1},
            {"chain_depth": 1},
            {"chain_depth": 2},
            {"chain_depth": 3},
            {"chain_depth": 4},
        ])

        assert result["tasks_at_depth_0"] == 2
        assert result["tasks_at_depth_1"] == 3
        assert result["tasks_at_depth_2_plus"] == 3

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_dependency_chain_depth([
            "not a dict",
            {
                "task_id": "task1",
                "is_root_task": True,
            },
        ])

        assert result["total_tasks"] == 1

    def test_boolean_values_not_extracted_as_numbers(self):
        """Verify boolean values are not extracted as numbers."""
        result = analyze_pack_dependency_chain_depth([
            {
                "task_id": "task1",
                "chain_depth": True,
            },
        ])

        assert result["avg_chain_depth"] == 0.0

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_pack_dependency_chain_depth([
            {
                "task_id": "task1",
                "chain_depth": 2.0,
            },
        ])

        assert result["max_dependency_chain_depth"] == 2

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_dependency_chain_depth([
            {
                "task_id": "task1",
                # Missing most fields
            },
        ])

        assert result["total_tasks"] == 1
        assert result["root_tasks"] == 0

    def test_comprehensive_pack_all_fields(self):
        """Verify comprehensive pack with all fields populated."""
        result = analyze_pack_dependency_chain_depth([
            {
                "task_id": "root1",
                "dependencies": [],
                "chain_depth": 0,
                "is_root_task": True,
                "has_circular_dependency": False,
            },
            {
                "task_id": "root2",
                "dependencies": [],
                "chain_depth": 0,
                "is_root_task": True,
                "has_circular_dependency": False,
            },
            {
                "task_id": "dependent1",
                "dependencies": ["root1"],
                "chain_depth": 1,
                "is_root_task": False,
                "has_circular_dependency": False,
            },
        ])

        assert result["total_tasks"] == 3
        assert result["root_tasks"] == 2
        assert result["root_task_percentage"] == 66.67
        assert result["max_dependency_chain_depth"] == 1
        assert result["avg_chain_depth"] == 0.33
        assert result["tasks_with_dependencies"] == 1
        assert result["circular_dependencies_detected"] == 0
        assert result["parallelization_potential_score"] == 66.67

    def test_max_depth_tracking(self):
        """Verify maximum depth tracking."""
        result = analyze_pack_dependency_chain_depth([
            {"chain_depth": 1},
            {"chain_depth": 5},
            {"chain_depth": 3},
        ])

        assert result["max_dependency_chain_depth"] == 5

    def test_average_depth_calculation(self):
        """Verify average depth calculation."""
        result = analyze_pack_dependency_chain_depth([
            {"chain_depth": 0},
            {"chain_depth": 1},
            {"chain_depth": 2},
            {"chain_depth": 3},
        ])

        # (0 + 1 + 2 + 3) / 4 = 1.5
        assert result["avg_chain_depth"] == 1.5

    def test_mixed_root_detection_methods(self):
        """Verify both is_root_task flag and empty dependencies work."""
        result = analyze_pack_dependency_chain_depth([
            # Explicit flag
            {"task_id": "t1", "is_root_task": True},
            # Empty dependencies
            {"task_id": "t2", "dependencies": []},
            # Non-root
            {"task_id": "t3", "dependencies": ["t1"]},
        ])

        assert result["root_tasks"] == 2

    def test_multiple_circular_dependencies(self):
        """Verify detection of multiple circular dependencies."""
        result = analyze_pack_dependency_chain_depth([
            {"task_id": "t1", "has_circular_dependency": True},
            {"task_id": "t2", "has_circular_dependency": True},
            {"task_id": "t3", "has_circular_dependency": True},
            {"task_id": "t4", "has_circular_dependency": False},
        ])

        assert result["circular_dependencies_detected"] == 3

    def test_edge_case_depth_boundary(self):
        """Verify depth categorization boundaries."""
        result = analyze_pack_dependency_chain_depth([
            {"chain_depth": 0, "is_root_task": True},
            {"chain_depth": 1},
            {"chain_depth": 2},
        ])

        assert result["tasks_at_depth_0"] == 1
        assert result["tasks_at_depth_1"] == 1
        assert result["tasks_at_depth_2_plus"] == 1

    def test_deep_dependency_chain(self):
        """Verify handling of deep dependency chains."""
        result = analyze_pack_dependency_chain_depth([
            {"chain_depth": 0, "is_root_task": True},
            {"chain_depth": 1},
            {"chain_depth": 2},
            {"chain_depth": 3},
            {"chain_depth": 4},
            {"chain_depth": 5},
            {"chain_depth": 6},
        ])

        assert result["max_dependency_chain_depth"] == 6
        assert result["tasks_at_depth_2_plus"] == 5
