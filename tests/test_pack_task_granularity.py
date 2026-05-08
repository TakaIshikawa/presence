"""Tests for pack task granularity analyzer."""

import pytest

from synthesis.pack_task_granularity import analyze_pack_task_granularity


class TestAnalyzePackTaskGranularity:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_task_granularity([])

        assert result["total_packs"] == 0
        assert result["total_tasks"] == 0
        assert result["scope_distribution"] == {}
        assert result["average_tasks_per_pack"] == 0.0
        assert result["max_dependency_depth"] == 0
        assert result["average_dependency_depth"] == 0.0
        assert result["file_overlap_count"] == 0
        assert result["independent_task_count"] == 0
        assert result["dependent_task_count"] == 0
        assert result["independence_ratio"] == 0.0
        assert result["granularity_rating"] == "well_balanced"

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_task_granularity(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_task_granularity("not a list")

    def test_single_task_independent(self):
        """Verify single independent task is counted."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "estimated_scope": "small",
                "expected_files": ["src/foo.py"],
                "depends_on": [],
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_tasks"] == 1
        assert result["independent_task_count"] == 1
        assert result["dependent_task_count"] == 0
        assert result["independence_ratio"] == 100.0
        assert result["max_dependency_depth"] == 0

    def test_single_task_with_dependency(self):
        """Verify single task with dependency is counted."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "estimated_scope": "small",
                "expected_files": ["src/foo.py"],
                "depends_on": ["task-0"],
            }
        ])

        assert result["independent_task_count"] == 0
        assert result["dependent_task_count"] == 1
        assert result["independence_ratio"] == 0.0

    def test_scope_distribution_tracked(self):
        """Verify scope distribution is tracked."""
        result = analyze_pack_task_granularity([
            {"pack_id": "pack-1", "task_id": "task-1", "estimated_scope": "small"},
            {"pack_id": "pack-1", "task_id": "task-2", "estimated_scope": "small"},
            {"pack_id": "pack-1", "task_id": "task-3", "estimated_scope": "medium"},
        ])

        assert result["scope_distribution"]["small"] == 2
        assert result["scope_distribution"]["medium"] == 1

    def test_multiple_packs_counted(self):
        """Verify multiple packs are counted."""
        result = analyze_pack_task_granularity([
            {"pack_id": "pack-1", "task_id": "task-1", "estimated_scope": "small"},
            {"pack_id": "pack-2", "task_id": "task-2", "estimated_scope": "small"},
        ])

        assert result["total_packs"] == 2
        assert result["total_tasks"] == 2
        assert result["average_tasks_per_pack"] == 1.0

    def test_average_tasks_per_pack(self):
        """Verify average tasks per pack is calculated."""
        result = analyze_pack_task_granularity([
            {"pack_id": "pack-1", "task_id": "task-1", "estimated_scope": "small"},
            {"pack_id": "pack-1", "task_id": "task-2", "estimated_scope": "small"},
            {"pack_id": "pack-2", "task_id": "task-3", "estimated_scope": "small"},
        ])

        # 3 tasks / 2 packs = 1.5
        assert result["average_tasks_per_pack"] == 1.5

    def test_file_overlap_detected(self):
        """Verify file overlap within pack is detected."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "expected_files": ["src/foo.py"],
            },
            {
                "pack_id": "pack-1",
                "task_id": "task-2",
                "expected_files": ["src/foo.py"],
            }
        ])

        assert result["file_overlap_count"] == 1

    def test_no_file_overlap_across_packs(self):
        """Verify file overlap is not counted across different packs."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "expected_files": ["src/foo.py"],
            },
            {
                "pack_id": "pack-2",
                "task_id": "task-2",
                "expected_files": ["src/foo.py"],
            }
        ])

        assert result["file_overlap_count"] == 0

    def test_multiple_file_overlaps(self):
        """Verify multiple file overlaps are counted."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "expected_files": ["src/foo.py", "src/bar.py"],
            },
            {
                "pack_id": "pack-1",
                "task_id": "task-2",
                "expected_files": ["src/foo.py", "src/bar.py"],
            }
        ])

        assert result["file_overlap_count"] == 2

    def test_dependency_depth_simple(self):
        """Verify dependency depth for simple chain."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "depends_on": [],
            },
            {
                "pack_id": "pack-1",
                "task_id": "task-2",
                "depends_on": ["task-1"],
            }
        ])

        # task-2 depends on task-1, so depth = 1
        assert result["max_dependency_depth"] == 1

    def test_dependency_depth_chain(self):
        """Verify dependency depth for longer chain."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "depends_on": [],
            },
            {
                "pack_id": "pack-1",
                "task_id": "task-2",
                "depends_on": ["task-1"],
            },
            {
                "pack_id": "pack-1",
                "task_id": "task-3",
                "depends_on": ["task-2"],
            }
        ])

        # task-1 -> task-2 -> task-3, depth = 2
        assert result["max_dependency_depth"] == 2

    def test_average_dependency_depth(self):
        """Verify average dependency depth is calculated."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "depends_on": [],
            },
            {
                "pack_id": "pack-1",
                "task_id": "task-2",
                "depends_on": ["task-1"],
            },
            {
                "pack_id": "pack-1",
                "task_id": "task-3",
                "depends_on": [],
            }
        ])

        # Depths: 0, 1, 0 -> average = 0.33
        assert result["average_dependency_depth"] == 0.33

    def test_granularity_well_balanced(self):
        """Verify well-balanced granularity rating."""
        result = analyze_pack_task_granularity([
            {"pack_id": "pack-1", "task_id": "task-1", "depends_on": []},
            {"pack_id": "pack-1", "task_id": "task-2", "depends_on": []},
            {"pack_id": "pack-1", "task_id": "task-3", "depends_on": ["task-1"]},
        ])

        # 3 tasks per pack, 66% independence, depth 1
        assert result["granularity_rating"] == "well_balanced"

    def test_granularity_over_granular(self):
        """Verify over-granular rating for many dependent tasks."""
        records = []
        # Create 10 tasks where task 0, 1 are independent, rest depend on task-0
        for i in range(10):
            depends = ["task-0"] if i > 1 else []
            records.append({
                "pack_id": "pack-1",
                "task_id": f"task-{i}",
                "depends_on": depends,
            })

        result = analyze_pack_task_granularity(records)

        # 10 tasks, 20% independence (2/10), depth 1, should be over_granular
        assert result["granularity_rating"] == "over_granular"

    def test_granularity_under_granular(self):
        """Verify under-granular rating for few independent tasks."""
        result = analyze_pack_task_granularity([
            {"pack_id": "pack-1", "task_id": "task-1", "depends_on": []},
            {"pack_id": "pack-1", "task_id": "task-2", "depends_on": []},
        ])

        # 2 tasks, 100% independence
        assert result["granularity_rating"] == "under_granular"

    def test_granularity_deep_dependencies(self):
        """Verify deep_dependencies rating for long chains."""
        records = []
        for i in range(7):
            records.append({
                "pack_id": "pack-1",
                "task_id": f"task-{i}",
                "depends_on": [f"task-{i-1}"] if i > 0 else [],
            })

        result = analyze_pack_task_granularity(records)

        # Depth 6, should be deep_dependencies
        assert result["granularity_rating"] == "deep_dependencies"

    def test_circular_dependency_handled(self):
        """Verify circular dependencies don't cause infinite loop."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "depends_on": ["task-2"],
            },
            {
                "pack_id": "pack-1",
                "task_id": "task-2",
                "depends_on": ["task-1"],
            }
        ])

        # Should not crash, depth should be limited
        assert result["max_dependency_depth"] >= 0

    def test_missing_dependency_handled(self):
        """Verify missing dependency references are handled."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "depends_on": ["task-nonexistent"],
            }
        ])

        # Should not crash
        assert result["total_tasks"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_task_granularity([
            "not a dict",
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
            }
        ])

        assert result["total_tasks"] == 1

    def test_missing_pack_id_uses_index(self):
        """Verify missing pack_id uses index as fallback."""
        result = analyze_pack_task_granularity([
            {
                "task_id": "task-1",
            }
        ])

        assert result["total_packs"] == 1

    def test_missing_task_id_uses_index(self):
        """Verify missing task_id uses index as fallback."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
            }
        ])

        assert result["total_tasks"] == 1

    def test_missing_scope_uses_unknown(self):
        """Verify missing scope uses 'unknown'."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
            }
        ])

        assert result["scope_distribution"]["unknown"] == 1

    def test_scope_case_insensitive(self):
        """Verify scope values are normalized to lowercase."""
        result = analyze_pack_task_granularity([
            {"pack_id": "pack-1", "task_id": "task-1", "estimated_scope": "SMALL"},
            {"pack_id": "pack-1", "task_id": "task-2", "estimated_scope": "Small"},
        ])

        assert result["scope_distribution"]["small"] == 2

    def test_file_path_normalization(self):
        """Verify file paths are normalized."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "expected_files": ["./src/foo.py"],
            },
            {
                "pack_id": "pack-1",
                "task_id": "task-2",
                "expected_files": ["src/foo.py"],
            }
        ])

        # Should detect overlap after normalization
        assert result["file_overlap_count"] == 1

    def test_windows_path_normalization(self):
        """Verify Windows paths are normalized."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "expected_files": ["src\\foo.py"],
            },
            {
                "pack_id": "pack-1",
                "task_id": "task-2",
                "expected_files": ["src/foo.py"],
            }
        ])

        assert result["file_overlap_count"] == 1

    def test_single_file_as_string(self):
        """Verify single file as string is handled."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "expected_files": "src/foo.py",
            }
        ])

        assert result["total_tasks"] == 1

    def test_single_dependency_as_string(self):
        """Verify single dependency as string is handled."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "depends_on": "task-0",
            }
        ])

        assert result["dependent_task_count"] == 1

    def test_empty_dependency_list_independent(self):
        """Verify empty depends_on list means independent."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "depends_on": [],
            }
        ])

        assert result["independent_task_count"] == 1

    def test_none_dependency_independent(self):
        """Verify None depends_on means independent."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack-1",
                "task_id": "task-1",
                "depends_on": None,
            }
        ])

        assert result["independent_task_count"] == 1

    def test_complex_dependency_graph(self):
        """Verify complex dependency graph is handled."""
        result = analyze_pack_task_granularity([
            {"pack_id": "pack-1", "task_id": "task-1", "depends_on": []},
            {"pack_id": "pack-1", "task_id": "task-2", "depends_on": ["task-1"]},
            {"pack_id": "pack-1", "task_id": "task-3", "depends_on": ["task-1"]},
            {"pack_id": "pack-1", "task_id": "task-4", "depends_on": ["task-2", "task-3"]},
        ])

        # task-1 (0) -> task-2 (1) -> task-4 (2)
        # task-1 (0) -> task-3 (1) -> task-4 (2)
        assert result["max_dependency_depth"] == 2

    def test_large_pack_many_tasks(self):
        """Verify large pack with many tasks."""
        records = []
        for i in range(15):
            records.append({
                "pack_id": "pack-1",
                "task_id": f"task-{i}",
                "estimated_scope": "small",
                "depends_on": [],
            })

        result = analyze_pack_task_granularity(records)

        assert result["total_tasks"] == 15
        assert result["average_tasks_per_pack"] == 15.0
        assert result["independence_ratio"] == 100.0
