"""Tests for pack task granularity analyzer."""

import pytest

from synthesis.pack_task_granularity import (
    analyze_pack_task_granularity,
    _average_float,
    _average_int,
    _calculate_max_dependency_depth,
    _calculate_scope_distribution,
    _count_overlapping_files,
    _median,
)


class TestAnalyzePackTaskGranularity:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_task_granularity([])

        assert result["total_tasks"] == 0
        assert result["scope_distribution"] == {}
        assert result["average_scope"] == 0
        assert result["median_scope"] == 0
        assert result["average_files_per_task"] == 0.0
        assert result["max_dependency_depth"] == 0
        assert result["independent_task_count"] == 0
        assert result["dependent_task_count"] == 0
        assert result["independence_ratio"] == 0.0
        assert result["overlapping_file_pairs"] == 0
        assert result["granularity_pattern"] == "empty"

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_task_granularity(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_task_granularity("not a list")

    def test_single_task(self):
        """Verify single task is handled."""
        result = analyze_pack_task_granularity([
            {
                "task_id": "task1",
                "estimated_scope": 100,
                "expected_files": ["file1.py"],
                "is_independent": True,
            },
        ])

        assert result["total_tasks"] == 1
        assert result["average_scope"] == 100
        assert result["independence_ratio"] == 100.0
        assert result["granularity_pattern"] == "simple"

    def test_scope_distribution_calculation(self):
        """Verify scope distribution is calculated correctly."""
        result = analyze_pack_task_granularity([
            {"task_id": "t1", "estimated_scope": 30, "is_independent": True},  # tiny
            {"task_id": "t2", "estimated_scope": 100, "is_independent": True},  # small
            {"task_id": "t3", "estimated_scope": 300, "is_independent": True},  # medium
            {"task_id": "t4", "estimated_scope": 700, "is_independent": True},  # large
            {"task_id": "t5", "estimated_scope": 1500, "is_independent": True},  # xlarge
        ])

        dist = result["scope_distribution"]
        assert dist["tiny"] == 1
        assert dist["small"] == 1
        assert dist["medium"] == 1
        assert dist["large"] == 1
        assert dist["xlarge"] == 1

    def test_average_and_median_scope(self):
        """Verify average and median calculations."""
        result = analyze_pack_task_granularity([
            {"task_id": "t1", "estimated_scope": 100, "is_independent": True},
            {"task_id": "t2", "estimated_scope": 200, "is_independent": True},
            {"task_id": "t3", "estimated_scope": 300, "is_independent": True},
        ])

        assert result["average_scope"] == 200
        assert result["median_scope"] == 200

    def test_dependency_depth_calculation(self):
        """Verify dependency depth is calculated."""
        result = analyze_pack_task_granularity([
            {"task_id": "t1", "dependencies": [], "is_independent": True},
            {"task_id": "t2", "dependencies": ["t1"], "is_independent": False},
            {"task_id": "t3", "dependencies": ["t2"], "is_independent": False},
            {"task_id": "t4", "dependencies": ["t3"], "is_independent": False},
        ])

        assert result["max_dependency_depth"] == 3

    def test_independence_ratio(self):
        """Verify independence ratio calculation."""
        result = analyze_pack_task_granularity([
            {"task_id": "t1", "is_independent": True},
            {"task_id": "t2", "is_independent": True},
            {"task_id": "t3", "is_independent": False},
            {"task_id": "t4", "is_independent": False},
        ])

        assert result["independent_task_count"] == 2
        assert result["dependent_task_count"] == 2
        assert result["independence_ratio"] == 50.0

    def test_overlapping_files_detection(self):
        """Verify overlapping files are detected."""
        result = analyze_pack_task_granularity([
            {"task_id": "t1", "expected_files": ["a.py", "b.py"]},
            {"task_id": "t2", "expected_files": ["b.py", "c.py"]},  # Overlaps with t1
            {"task_id": "t3", "expected_files": ["d.py", "e.py"]},  # No overlap
        ])

        assert result["overlapping_file_pairs"] == 1

    def test_average_files_per_task(self):
        """Verify average files per task calculation."""
        result = analyze_pack_task_granularity([
            {"task_id": "t1", "expected_files": ["a.py", "b.py"]},
            {"task_id": "t2", "expected_files": ["c.py"]},
            {"task_id": "t3", "expected_files": ["d.py", "e.py", "f.py"]},
        ])

        # (2 + 1 + 3) / 3 = 2.0
        assert result["average_files_per_task"] == 2.0

    def test_optimal_granularity_pattern(self):
        """Verify optimal pattern classification."""
        tasks = []
        for i in range(5):
            tasks.append({
                "task_id": f"t{i}",
                "estimated_scope": 250,  # Medium
                "dependencies": [],
                "is_independent": True,
            })

        result = analyze_pack_task_granularity(tasks)

        assert result["granularity_pattern"] == "optimal"

    def test_over_granular_pattern(self):
        """Verify over-granular pattern classification."""
        tasks = []
        # Many tiny tasks with deep dependencies
        for i in range(10):
            tasks.append({
                "task_id": f"t{i}",
                "estimated_scope": 30,  # Tiny
                "dependencies": [f"t{i-1}"] if i > 0 else [],
                "is_independent": i == 0,
            })

        result = analyze_pack_task_granularity(tasks)

        assert result["granularity_pattern"] == "over_granular"

    def test_under_granular_pattern(self):
        """Verify under-granular pattern classification."""
        tasks = []
        for i in range(5):
            tasks.append({
                "task_id": f"t{i}",
                "estimated_scope": 800,  # Large
                "is_independent": True,
            })

        result = analyze_pack_task_granularity(tasks)

        assert result["granularity_pattern"] == "under_granular"

    def test_unbalanced_pattern(self):
        """Verify unbalanced pattern classification."""
        result = analyze_pack_task_granularity([
            {"task_id": "t1", "estimated_scope": 20, "is_independent": True},  # tiny
            {"task_id": "t2", "estimated_scope": 1500, "is_independent": True},  # xlarge
            {"task_id": "t3", "estimated_scope": 25, "is_independent": True},  # tiny
            {"task_id": "t4", "estimated_scope": 2000, "is_independent": True},  # xlarge
        ])

        assert result["granularity_pattern"] == "unbalanced"

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_task_granularity([
            "not a dict",
            {"task_id": "t1", "estimated_scope": 100, "is_independent": True},
        ])

        assert result["total_tasks"] == 1


class TestHelperFunctions:
    """Test helper functions."""

    def test_average_int_normal(self):
        """Verify integer average calculation."""
        assert _average_int([10, 20, 30]) == 20

    def test_average_int_empty(self):
        """Verify integer average with empty list."""
        assert _average_int([]) == 0

    def test_average_int_rounding(self):
        """Verify integer average rounds."""
        assert _average_int([10, 15, 20]) == 15

    def test_average_float_normal(self):
        """Verify float average calculation."""
        assert _average_float([2, 4, 6]) == 4.0

    def test_average_float_empty(self):
        """Verify float average with empty list."""
        assert _average_float([]) == 0.0

    def test_average_float_rounding(self):
        """Verify float average rounds to 2 decimals."""
        assert _average_float([1, 2, 3]) == 2.0

    def test_median_odd_count(self):
        """Verify median with odd number of values."""
        assert _median([1, 2, 3, 4, 5]) == 3

    def test_median_even_count(self):
        """Verify median with even number of values."""
        assert _median([1, 2, 3, 4]) == 2  # (2+3)/2

    def test_median_empty(self):
        """Verify median with empty list."""
        assert _median([]) == 0

    def test_median_unsorted(self):
        """Verify median works with unsorted list."""
        assert _median([5, 1, 3, 2, 4]) == 3

    def test_calculate_scope_distribution(self):
        """Verify scope distribution calculation."""
        scopes = [30, 100, 300, 700, 1500]
        dist = _calculate_scope_distribution(scopes)

        assert dist["tiny"] == 1
        assert dist["small"] == 1
        assert dist["medium"] == 1
        assert dist["large"] == 1
        assert dist["xlarge"] == 1

    def test_calculate_max_dependency_depth_linear(self):
        """Verify dependency depth with linear chain."""
        records = [
            {"task_id": "t1", "dependencies": []},
            {"task_id": "t2", "dependencies": ["t1"]},
            {"task_id": "t3", "dependencies": ["t2"]},
        ]
        assert _calculate_max_dependency_depth(records) == 2

    def test_calculate_max_dependency_depth_parallel(self):
        """Verify dependency depth with parallel tasks."""
        records = [
            {"task_id": "t1", "dependencies": []},
            {"task_id": "t2", "dependencies": []},
            {"task_id": "t3", "dependencies": []},
        ]
        assert _calculate_max_dependency_depth(records) == 0

    def test_calculate_max_dependency_depth_tree(self):
        """Verify dependency depth with tree structure."""
        records = [
            {"task_id": "t1", "dependencies": []},
            {"task_id": "t2", "dependencies": ["t1"]},
            {"task_id": "t3", "dependencies": ["t1"]},
            {"task_id": "t4", "dependencies": ["t2", "t3"]},
        ]
        assert _calculate_max_dependency_depth(records) == 2

    def test_count_overlapping_files_no_overlap(self):
        """Verify no overlapping files detected."""
        task_files = {
            "t1": {"a.py", "b.py"},
            "t2": {"c.py", "d.py"},
        }
        assert _count_overlapping_files(task_files) == 0

    def test_count_overlapping_files_one_pair(self):
        """Verify one overlapping pair detected."""
        task_files = {
            "t1": {"a.py", "b.py"},
            "t2": {"b.py", "c.py"},
        }
        assert _count_overlapping_files(task_files) == 1

    def test_count_overlapping_files_multiple_pairs(self):
        """Verify multiple overlapping pairs detected."""
        task_files = {
            "t1": {"a.py", "b.py"},
            "t2": {"b.py", "c.py"},
            "t3": {"a.py", "d.py"},
        }
        # t1-t2 (b.py), t1-t3 (a.py) = 2 pairs
        assert _count_overlapping_files(task_files) == 2

    def test_count_overlapping_files_empty(self):
        """Verify empty task files."""
        assert _count_overlapping_files({}) == 0


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_well_balanced_pack(self):
        """Simulate well-balanced pack with good granularity."""
        result = analyze_pack_task_granularity([
            {
                "task_id": "t1",
                "estimated_scope": 250,
                "expected_files": ["a.py", "b.py"],
                "dependencies": [],
                "is_independent": True,
            },
            {
                "task_id": "t2",
                "estimated_scope": 300,
                "expected_files": ["c.py"],
                "dependencies": [],
                "is_independent": True,
            },
            {
                "task_id": "t3",
                "estimated_scope": 280,
                "expected_files": ["d.py", "e.py"],
                "dependencies": ["t1"],
                "is_independent": False,
            },
        ])

        assert result["granularity_pattern"] in ("optimal", "balanced")
        assert result["max_dependency_depth"] == 1

    def test_over_granular_pack(self):
        """Simulate over-granular pack with many small tasks."""
        tasks = []
        for i in range(8):
            tasks.append({
                "task_id": f"task{i}",
                "estimated_scope": 40,  # Tiny
                "expected_files": [f"file{i}.py"],
                "dependencies": [f"task{i-1}"] if i > 0 else [],
                "is_independent": i == 0,
            })

        result = analyze_pack_task_granularity(tasks)

        assert result["granularity_pattern"] == "over_granular"
        assert result["max_dependency_depth"] > 3

    def test_under_granular_pack(self):
        """Simulate under-granular pack with few large tasks."""
        result = analyze_pack_task_granularity([
            {
                "task_id": "t1",
                "estimated_scope": 900,
                "expected_files": [f"file{i}.py" for i in range(10)],
                "is_independent": True,
            },
            {
                "task_id": "t2",
                "estimated_scope": 850,
                "expected_files": [f"other{i}.py" for i in range(8)],
                "is_independent": True,
            },
            {
                "task_id": "t3",
                "estimated_scope": 920,
                "expected_files": [f"more{i}.py" for i in range(9)],
                "is_independent": True,
            },
        ])

        assert result["granularity_pattern"] == "under_granular"
        assert result["average_files_per_task"] > 8.0

    def test_mixed_granularity_pack(self):
        """Simulate pack with mixed task sizes."""
        result = analyze_pack_task_granularity([
            {"task_id": "t1", "estimated_scope": 100, "is_independent": True},
            {"task_id": "t2", "estimated_scope": 250, "is_independent": True},
            {"task_id": "t3", "estimated_scope": 400, "is_independent": True},
            {"task_id": "t4", "estimated_scope": 150, "is_independent": True},
            {"task_id": "t5", "estimated_scope": 300, "is_independent": True},
        ])

        assert result["total_tasks"] == 5
        assert result["granularity_pattern"] in ("balanced", "optimal")

    def test_overlapping_files_conflict(self):
        """Simulate tasks with overlapping file expectations."""
        result = analyze_pack_task_granularity([
            {
                "task_id": "t1",
                "estimated_scope": 200,
                "expected_files": ["shared.py", "a.py"],
                "is_independent": True,
            },
            {
                "task_id": "t2",
                "estimated_scope": 200,
                "expected_files": ["shared.py", "b.py"],
                "is_independent": True,
            },
            {
                "task_id": "t3",
                "estimated_scope": 200,
                "expected_files": ["c.py"],
                "is_independent": True,
            },
        ])

        assert result["overlapping_file_pairs"] == 1  # t1-t2 overlap on shared.py
