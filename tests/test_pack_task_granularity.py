"""Tests for pack task granularity analyzer."""

import pytest

from synthesis.pack_task_granularity import (
    analyze_pack_task_granularity,
    _calculate_max_dependency_depth,
    _detect_file_conflicts,
    _classify_pack_granularity,
    _determine_overall_pattern,
)


class TestAnalyzePackTaskGranularity:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_task_granularity([])

        assert result["total_packs"] == 0
        assert result["scope_distribution"] == {"small": 0, "medium": 0, "large": 0}
        assert result["avg_tasks_per_pack"] == 0.0
        assert result["max_dependency_depth"] == 0
        assert result["avg_dependency_depth"] == 0.0
        assert result["file_conflicts"] == []
        assert result["independence_ratio"] == 0.0
        assert result["granularity_pattern"] == "empty"

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_task_granularity(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_task_granularity("not a list")

    def test_single_task_pack(self):
        """Verify single task pack is analyzed."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "estimated_scope": "small",
                        "expected_files": ["foo.py"],
                        "dependencies": [],
                    }
                ]
            }
        ])

        assert result["total_packs"] == 1
        assert result["scope_distribution"]["small"] == 1
        assert result["avg_tasks_per_pack"] == 1.0
        assert result["independence_ratio"] == 100.0
        assert result["granularity_pattern"] == "mono_task"

    def test_scope_distribution_tracked(self):
        """Verify scope distribution is tracked correctly."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "task1", "estimated_scope": "small", "dependencies": []},
                    {"task_id": "task2", "estimated_scope": "medium", "dependencies": []},
                    {"task_id": "task3", "estimated_scope": "large", "dependencies": []},
                    {"task_id": "task4", "estimated_scope": "small", "dependencies": []},
                ]
            }
        ])

        assert result["scope_distribution"]["small"] == 2
        assert result["scope_distribution"]["medium"] == 1
        assert result["scope_distribution"]["large"] == 1

    def test_avg_tasks_per_pack_calculated(self):
        """Verify average tasks per pack is calculated."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "task1", "estimated_scope": "small", "dependencies": []},
                    {"task_id": "task2", "estimated_scope": "small", "dependencies": []},
                ]
            },
            {
                "pack_id": "pack2",
                "tasks": [
                    {"task_id": "task3", "estimated_scope": "small", "dependencies": []},
                    {"task_id": "task4", "estimated_scope": "small", "dependencies": []},
                    {"task_id": "task5", "estimated_scope": "small", "dependencies": []},
                    {"task_id": "task6", "estimated_scope": "small", "dependencies": []},
                ]
            },
        ])

        # (2 + 4) / 2 = 3.0
        assert result["avg_tasks_per_pack"] == 3.0

    def test_dependency_depth_calculated(self):
        """Verify dependency depth is calculated."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "task1", "dependencies": []},
                    {"task_id": "task2", "dependencies": ["task1"]},
                    {"task_id": "task3", "dependencies": ["task2"]},
                ]
            }
        ])

        # task3 depends on task2, which depends on task1 = depth 2
        assert result["max_dependency_depth"] == 2

    def test_independence_ratio_calculated(self):
        """Verify independence ratio is calculated."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "task1", "dependencies": []},
                    {"task_id": "task2", "dependencies": []},
                    {"task_id": "task3", "dependencies": ["task1"]},
                    {"task_id": "task4", "dependencies": []},
                ]
            }
        ])

        # 3 independent out of 4 = 75%
        assert result["independence_ratio"] == 75.0

    def test_file_conflicts_detected(self):
        """Verify file conflicts are detected."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "task1", "expected_files": ["foo.py"], "dependencies": []},
                    {"task_id": "task2", "expected_files": ["foo.py"], "dependencies": []},
                ]
            }
        ])

        conflicts = result["file_conflicts"]
        assert len(conflicts) == 1
        assert conflicts[0]["file"] == "foo.py"
        assert conflicts[0]["task_count"] == 2

    def test_well_balanced_pattern(self):
        """Verify well balanced pattern classification."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "task1", "estimated_scope": "small", "dependencies": []},
                    {"task_id": "task2", "estimated_scope": "medium", "dependencies": []},
                    {"task_id": "task3", "estimated_scope": "large", "dependencies": []},
                ]
            }
        ])

        assert result["granularity_pattern"] == "well_balanced"

    def test_over_granular_pattern(self):
        """Verify over granular pattern classification."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": f"task{i}", "estimated_scope": "small", "dependencies": []}
                    for i in range(10)
                ]
            }
        ])

        assert result["granularity_pattern"] == "over_granular"

    def test_under_granular_pattern(self):
        """Verify under granular pattern classification."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "task1", "estimated_scope": "large", "dependencies": []},
                    {"task_id": "task2", "estimated_scope": "large", "dependencies": []},
                    {"task_id": "task3", "estimated_scope": "large", "dependencies": []},
                ]
            }
        ])

        assert result["granularity_pattern"] == "under_granular"

    def test_file_conflicts_limited_to_ten(self):
        """Verify file conflicts are limited to 10."""
        tasks = [
            {"task_id": f"task{i}", "expected_files": ["common.py"], "dependencies": []}
            for i in range(20)
        ]
        result = analyze_pack_task_granularity([
            {"pack_id": "pack1", "tasks": tasks}
        ])

        assert len(result["file_conflicts"]) <= 10

    def test_malformed_pack_skipped(self):
        """Verify non-dict packs are skipped."""
        result = analyze_pack_task_granularity([
            "not a dict",
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "task1", "estimated_scope": "small", "dependencies": []}
                ]
            },
        ])

        assert result["total_packs"] == 1

    def test_missing_tasks_skipped(self):
        """Verify packs without tasks are handled."""
        result = analyze_pack_task_granularity([
            {"pack_id": "pack1"},
        ])

        assert result["total_packs"] == 1

    def test_non_list_tasks_skipped(self):
        """Verify non-list tasks are skipped."""
        result = analyze_pack_task_granularity([
            {"pack_id": "pack1", "tasks": "not a list"},
        ])

        assert result["total_packs"] == 0  # Skipped


class TestCalculateMaxDependencyDepth:
    """Test dependency depth calculation helper."""

    def test_empty_tasks_returns_zero(self):
        """Verify empty tasks returns zero depth."""
        assert _calculate_max_dependency_depth([]) == 0

    def test_no_dependencies_returns_zero(self):
        """Verify tasks without dependencies return zero."""
        tasks = [
            {"task_id": "task1", "dependencies": []},
            {"task_id": "task2", "dependencies": []},
        ]
        assert _calculate_max_dependency_depth(tasks) == 0

    def test_single_dependency_depth_one(self):
        """Verify single dependency returns depth 1."""
        tasks = [
            {"task_id": "task1", "dependencies": []},
            {"task_id": "task2", "dependencies": ["task1"]},
        ]
        assert _calculate_max_dependency_depth(tasks) == 1

    def test_chain_dependency_depth_calculated(self):
        """Verify chain dependency depth is calculated."""
        tasks = [
            {"task_id": "task1", "dependencies": []},
            {"task_id": "task2", "dependencies": ["task1"]},
            {"task_id": "task3", "dependencies": ["task2"]},
        ]
        assert _calculate_max_dependency_depth(tasks) == 2

    def test_multiple_dependency_chains(self):
        """Verify maximum depth from multiple chains."""
        tasks = [
            {"task_id": "task1", "dependencies": []},
            {"task_id": "task2", "dependencies": ["task1"]},
            {"task_id": "task3", "dependencies": []},
            {"task_id": "task4", "dependencies": ["task3"]},
            {"task_id": "task5", "dependencies": ["task4"]},
            {"task_id": "task6", "dependencies": ["task5"]},
        ]
        # Longest chain: task6 -> task5 -> task4 -> task3 = depth 3
        assert _calculate_max_dependency_depth(tasks) == 3


class TestDetectFileConflicts:
    """Test file conflict detection helper."""

    def test_empty_tasks_returns_empty(self):
        """Verify empty tasks returns no conflicts."""
        conflicts = _detect_file_conflicts("pack1", [])
        assert conflicts == []

    def test_no_overlap_returns_empty(self):
        """Verify tasks without file overlap return no conflicts."""
        tasks = [
            {"task_id": "task1", "expected_files": ["foo.py"]},
            {"task_id": "task2", "expected_files": ["bar.py"]},
        ]
        conflicts = _detect_file_conflicts("pack1", tasks)
        assert len(conflicts) == 0

    def test_overlap_detected(self):
        """Verify file overlap is detected."""
        tasks = [
            {"task_id": "task1", "expected_files": ["foo.py"]},
            {"task_id": "task2", "expected_files": ["foo.py"]},
        ]
        conflicts = _detect_file_conflicts("pack1", tasks)

        assert len(conflicts) == 1
        assert conflicts[0]["file"] == "foo.py"
        assert conflicts[0]["task_count"] == 2

    def test_multiple_files_overlap(self):
        """Verify multiple file overlaps are detected."""
        tasks = [
            {"task_id": "task1", "expected_files": ["foo.py", "bar.py"]},
            {"task_id": "task2", "expected_files": ["foo.py", "baz.py"]},
            {"task_id": "task3", "expected_files": ["bar.py"]},
        ]
        conflicts = _detect_file_conflicts("pack1", tasks)

        # Both foo.py and bar.py have conflicts
        assert len(conflicts) == 2

    def test_three_tasks_same_file(self):
        """Verify three tasks with same file."""
        tasks = [
            {"task_id": "task1", "expected_files": ["foo.py"]},
            {"task_id": "task2", "expected_files": ["foo.py"]},
            {"task_id": "task3", "expected_files": ["foo.py"]},
        ]
        conflicts = _detect_file_conflicts("pack1", tasks)

        assert len(conflicts) == 1
        assert conflicts[0]["task_count"] == 3


class TestClassifyPackGranularity:
    """Test pack granularity classification helper."""

    def test_empty_tasks_returns_empty(self):
        """Verify empty tasks returns empty."""
        assert _classify_pack_granularity([]) == "empty"

    def test_single_task_returns_mono_task(self):
        """Verify single task returns mono_task."""
        tasks = [{"estimated_scope": "small"}]
        assert _classify_pack_granularity(tasks) == "mono_task"

    def test_well_balanced_classification(self):
        """Verify well balanced classification."""
        tasks = [
            {"estimated_scope": "small"},
            {"estimated_scope": "medium"},
            {"estimated_scope": "large"},
        ]
        assert _classify_pack_granularity(tasks) == "well_balanced"

    def test_over_granular_classification(self):
        """Verify over granular classification (>80% small)."""
        tasks = [{"estimated_scope": "small"} for _ in range(9)] + [{"estimated_scope": "medium"}]
        assert _classify_pack_granularity(tasks) == "over_granular"

    def test_under_granular_classification(self):
        """Verify under granular classification (>60% large)."""
        tasks = [{"estimated_scope": "large"} for _ in range(7)] + [{"estimated_scope": "small"}] * 3
        assert _classify_pack_granularity(tasks) == "under_granular"


class TestDetermineOverallPattern:
    """Test overall pattern determination helper."""

    def test_empty_patterns_returns_empty(self):
        """Verify empty patterns returns empty."""
        assert _determine_overall_pattern([]) == "empty"

    def test_single_pattern_returns_that_pattern(self):
        """Verify single pattern is returned."""
        assert _determine_overall_pattern(["well_balanced"]) == "well_balanced"

    def test_most_common_pattern_returned(self):
        """Verify most common pattern is returned."""
        patterns = ["well_balanced", "over_granular", "well_balanced", "well_balanced"]
        assert _determine_overall_pattern(patterns) == "well_balanced"


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_well_balanced_pack(self):
        """Simulate well balanced pack."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "estimated_scope": "small",
                        "expected_files": ["foo.py"],
                        "dependencies": [],
                    },
                    {
                        "task_id": "task2",
                        "estimated_scope": "medium",
                        "expected_files": ["bar.py"],
                        "dependencies": ["task1"],
                    },
                    {
                        "task_id": "task3",
                        "estimated_scope": "large",
                        "expected_files": ["baz.py"],
                        "dependencies": [],
                    },
                ]
            }
        ])

        assert result["granularity_pattern"] == "well_balanced"
        assert result["independence_ratio"] == 66.67  # 2 out of 3
        assert len(result["file_conflicts"]) == 0

    def test_over_granular_pack(self):
        """Simulate over granular pack with many small tasks."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": f"task{i}",
                        "estimated_scope": "small",
                        "expected_files": [f"file{i}.py"],
                        "dependencies": [],
                    }
                    for i in range(10)
                ]
            }
        ])

        assert result["granularity_pattern"] == "over_granular"
        assert result["independence_ratio"] == 100.0

    def test_complex_dependency_chain(self):
        """Simulate complex dependency chain."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "task1", "dependencies": []},
                    {"task_id": "task2", "dependencies": ["task1"]},
                    {"task_id": "task3", "dependencies": ["task2"]},
                    {"task_id": "task4", "dependencies": ["task3"]},
                ]
            }
        ])

        assert result["max_dependency_depth"] == 3
        assert result["independence_ratio"] == 25.0  # Only task1 is independent

    def test_pack_with_file_conflicts(self):
        """Simulate pack with file ownership conflicts."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "task1", "expected_files": ["shared.py", "foo.py"], "dependencies": []},
                    {"task_id": "task2", "expected_files": ["shared.py", "bar.py"], "dependencies": []},
                    {"task_id": "task3", "expected_files": ["shared.py"], "dependencies": []},
                ]
            }
        ])

        conflicts = result["file_conflicts"]
        assert len(conflicts) == 1
        assert conflicts[0]["file"] == "shared.py"
        assert conflicts[0]["task_count"] == 3

    def test_empty_pack(self):
        """Simulate empty pack."""
        result = analyze_pack_task_granularity([])

        assert result["total_packs"] == 0
        assert result["granularity_pattern"] == "empty"
