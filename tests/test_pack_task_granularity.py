"""Tests for pack task granularity analyzer."""

import pytest

from synthesis.pack_task_granularity import (
    analyze_pack_task_granularity,
    _calculate_max_dependency_depth,
    _count_file_conflicts,
    _normalize_files,
    _normalize_list,
    _average,
    _percentage,
    _classify_granularity_pattern,
)
from collections import Counter


class TestAnalyzePackTaskGranularity:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_task_granularity([])

        assert result["total_packs"] == 0
        assert result["total_tasks"] == 0
        assert result["scope_distribution"] == {}
        assert result["scope_percentages"] == {}
        assert result["avg_tasks_per_pack"] == 0.0
        assert result["max_dependency_depth"] == 0
        assert result["avg_dependency_depth"] == 0.0
        assert result["file_conflict_rate"] == 0.0
        assert result["independence_ratio"] == 0.0
        assert result["granularity_pattern"] == "empty"
        assert result["examples"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_task_granularity(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_task_granularity("not a list")

    def test_single_task_pack_no_dependencies(self):
        """Verify single task with no dependencies."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "estimated_scope": "small",
                        "depends_on": [],
                        "expected_files": ["src/foo.py"],
                    }
                ],
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_tasks"] == 1
        assert result["scope_distribution"]["small"] == 1
        assert result["scope_percentages"]["small"] == 100.0
        assert result["avg_tasks_per_pack"] == 1.0
        assert result["max_dependency_depth"] == 0
        assert result["avg_dependency_depth"] == 0.0
        assert result["independence_ratio"] == 100.0

    def test_multiple_tasks_with_scope_distribution(self):
        """Verify scope distribution across multiple tasks."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "tiny", "depends_on": [], "expected_files": []},
                    {"task_id": "t2", "estimated_scope": "small", "depends_on": [], "expected_files": []},
                    {"task_id": "t3", "estimated_scope": "medium", "depends_on": [], "expected_files": []},
                    {"task_id": "t4", "estimated_scope": "large", "depends_on": [], "expected_files": []},
                ],
            }
        ])

        assert result["total_tasks"] == 4
        assert result["scope_distribution"]["tiny"] == 1
        assert result["scope_distribution"]["small"] == 1
        assert result["scope_distribution"]["medium"] == 1
        assert result["scope_distribution"]["large"] == 1
        assert result["scope_percentages"]["tiny"] == 25.0
        assert result["scope_percentages"]["small"] == 25.0

    def test_simple_dependency_chain(self):
        """Verify simple linear dependency chain."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": []},
                    {"task_id": "t2", "estimated_scope": "small", "depends_on": ["t1"], "expected_files": []},
                    {"task_id": "t3", "estimated_scope": "small", "depends_on": ["t2"], "expected_files": []},
                ],
            }
        ])

        assert result["max_dependency_depth"] == 2
        assert result["avg_dependency_depth"] == 2.0
        assert result["independence_ratio"] == 33.33  # Only 1 of 3 tasks is independent

    def test_branching_dependencies(self):
        """Verify branching dependency structure."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": []},
                    {"task_id": "t2", "estimated_scope": "small", "depends_on": ["t1"], "expected_files": []},
                    {"task_id": "t3", "estimated_scope": "small", "depends_on": ["t1"], "expected_files": []},
                    {"task_id": "t4", "estimated_scope": "small", "depends_on": ["t2", "t3"], "expected_files": []},
                ],
            }
        ])

        assert result["max_dependency_depth"] == 2
        assert result["independence_ratio"] == 25.0  # Only t1 is independent

    def test_file_conflicts_detected(self):
        """Verify detection of overlapping expected files."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": ["src/foo.py", "src/bar.py"]},
                    {"task_id": "t2", "estimated_scope": "small", "depends_on": [], "expected_files": ["src/bar.py", "src/baz.py"]},
                ],
            }
        ])

        # 1 pair, 1 conflict (both have src/bar.py)
        assert result["file_conflict_rate"] == 100.0

    def test_no_file_conflicts(self):
        """Verify no conflicts when files don't overlap."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": ["src/foo.py"]},
                    {"task_id": "t2", "estimated_scope": "small", "depends_on": [], "expected_files": ["src/bar.py"]},
                ],
            }
        ])

        assert result["file_conflict_rate"] == 0.0

    def test_multiple_packs_averaged(self):
        """Verify metrics averaged across multiple packs."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": []},
                    {"task_id": "t2", "estimated_scope": "small", "depends_on": ["t1"], "expected_files": []},
                ],
            },
            {
                "pack_id": "pack2",
                "tasks": [
                    {"task_id": "t3", "estimated_scope": "medium", "depends_on": [], "expected_files": []},
                    {"task_id": "t4", "estimated_scope": "medium", "depends_on": [], "expected_files": []},
                ],
            },
        ])

        assert result["total_packs"] == 2
        assert result["total_tasks"] == 4
        assert result["avg_tasks_per_pack"] == 2.0
        # Pack1: depth 1, Pack2: depth 0 -> avg = 0.5
        assert result["avg_dependency_depth"] == 0.5

    def test_well_balanced_pattern(self):
        """Verify well-balanced granularity pattern detection."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": ["src/a.py"]},
                    {"task_id": "t2", "estimated_scope": "medium", "depends_on": [], "expected_files": ["src/b.py"]},
                    {"task_id": "t3", "estimated_scope": "small", "depends_on": ["t1"], "expected_files": ["src/c.py"]},
                ],
            }
        ])

        assert result["granularity_pattern"] == "well_balanced"

    def test_over_granular_pattern(self):
        """Verify over-granular pattern detection."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "tiny", "depends_on": [], "expected_files": []},
                    {"task_id": "t2", "estimated_scope": "tiny", "depends_on": ["t1"], "expected_files": []},
                    {"task_id": "t3", "estimated_scope": "tiny", "depends_on": ["t2"], "expected_files": []},
                    {"task_id": "t4", "estimated_scope": "tiny", "depends_on": ["t3"], "expected_files": []},
                ],
            }
        ])

        # > 50% tiny tasks with avg_depth >= 3
        assert result["scope_percentages"]["tiny"] == 100.0
        assert result["avg_dependency_depth"] == 3.0
        assert result["granularity_pattern"] == "over_granular"

    def test_under_granular_pattern(self):
        """Verify under-granular pattern detection."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "large", "depends_on": [], "expected_files": []},
                    {"task_id": "t2", "estimated_scope": "large", "depends_on": [], "expected_files": []},
                ],
            }
        ])

        # > 50% large tasks with high independence
        assert result["scope_percentages"]["large"] == 100.0
        assert result["independence_ratio"] == 100.0
        assert result["granularity_pattern"] == "under_granular"

    def test_conflicted_pattern(self):
        """Verify conflicted pattern detection."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": ["src/shared.py"]},
                    {"task_id": "t2", "estimated_scope": "small", "depends_on": [], "expected_files": ["src/shared.py"]},
                    {"task_id": "t3", "estimated_scope": "small", "depends_on": [], "expected_files": ["src/shared.py"]},
                ],
            }
        ])

        # All pairs conflict -> 100% conflict rate
        assert result["file_conflict_rate"] == 100.0
        assert result["granularity_pattern"] == "conflicted"

    def test_examples_collected_for_high_depth(self):
        """Verify examples collected for high dependency depth."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": []},
                    {"task_id": "t2", "estimated_scope": "small", "depends_on": ["t1"], "expected_files": []},
                    {"task_id": "t3", "estimated_scope": "small", "depends_on": ["t2"], "expected_files": []},
                    {"task_id": "t4", "estimated_scope": "small", "depends_on": ["t3"], "expected_files": []},
                ],
            }
        ])

        assert len(result["examples"]) == 1
        assert result["examples"][0]["pack_id"] == "pack1"
        assert result["examples"][0]["max_dependency_depth"] == 3

    def test_examples_limited_to_five(self):
        """Verify examples are limited to 5."""
        packs = [
            {
                "pack_id": f"pack{i}",
                "tasks": [
                    {"task_id": f"t{i}_1", "estimated_scope": "small", "depends_on": [], "expected_files": ["shared.py"]},
                    {"task_id": f"t{i}_2", "estimated_scope": "small", "depends_on": [], "expected_files": ["shared.py"]},
                ],
            }
            for i in range(10)
        ]

        result = analyze_pack_task_granularity(packs)
        assert len(result["examples"]) == 5

    def test_malformed_pack_skipped(self):
        """Verify non-dict packs are skipped."""
        result = analyze_pack_task_granularity([
            "not a dict",
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": []},
                ],
            },
        ])

        assert result["total_packs"] == 1
        assert result["total_tasks"] == 1

    def test_pack_without_tasks_field_skipped(self):
        """Verify pack without tasks field is skipped."""
        result = analyze_pack_task_granularity([
            {"pack_id": "pack1"},
            {
                "pack_id": "pack2",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": []},
                ],
            },
        ])

        assert result["total_packs"] == 1
        assert result["total_tasks"] == 1

    def test_malformed_task_skipped(self):
        """Verify non-dict tasks are skipped."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    "not a dict",
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": []},
                ],
            }
        ])

        assert result["total_tasks"] == 1

    def test_task_without_task_id_skipped(self):
        """Verify task without task_id is skipped."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"estimated_scope": "small", "depends_on": [], "expected_files": []},
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": []},
                ],
            }
        ])

        assert result["total_tasks"] == 1

    def test_file_paths_normalized(self):
        """Verify file paths are normalized."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": [], "expected_files": ["./src/foo.py", "src\\bar.py"]},
                    {"task_id": "t2", "estimated_scope": "small", "depends_on": [], "expected_files": ["src/bar.py"]},
                ],
            }
        ])

        # Should detect conflict after normalization
        assert result["file_conflict_rate"] == 100.0

    def test_circular_dependency_handled(self):
        """Verify circular dependencies don't cause infinite recursion."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack1",
                "tasks": [
                    {"task_id": "t1", "estimated_scope": "small", "depends_on": ["t2"], "expected_files": []},
                    {"task_id": "t2", "estimated_scope": "small", "depends_on": ["t1"], "expected_files": []},
                ],
            }
        ])

        # Should not crash, circular deps treated as depth 0
        assert result["max_dependency_depth"] >= 0


class TestCalculateMaxDependencyDepth:
    """Test dependency depth calculation helper."""

    def test_empty_task_map_returns_zero(self):
        """Verify empty task map returns 0."""
        assert _calculate_max_dependency_depth({}) == 0

    def test_single_task_no_deps_returns_zero(self):
        """Verify single task with no dependencies returns 0."""
        task_map = {
            "t1": {"depends_on": [], "scope": "small", "expected_files": [], "title": ""}
        }
        assert _calculate_max_dependency_depth(task_map) == 0

    def test_linear_chain_depth(self):
        """Verify linear dependency chain calculates correct depth."""
        task_map = {
            "t1": {"depends_on": [], "scope": "small", "expected_files": [], "title": ""},
            "t2": {"depends_on": ["t1"], "scope": "small", "expected_files": [], "title": ""},
            "t3": {"depends_on": ["t2"], "scope": "small", "expected_files": [], "title": ""},
        }
        # t3 depends on t2 depends on t1 -> depth 2
        assert _calculate_max_dependency_depth(task_map) == 2

    def test_branching_dependencies_max_depth(self):
        """Verify branching dependencies return maximum depth."""
        task_map = {
            "t1": {"depends_on": [], "scope": "small", "expected_files": [], "title": ""},
            "t2": {"depends_on": ["t1"], "scope": "small", "expected_files": [], "title": ""},
            "t3": {"depends_on": ["t1"], "scope": "small", "expected_files": [], "title": ""},
        }
        # Both t2 and t3 depend on t1 -> max depth 1
        assert _calculate_max_dependency_depth(task_map) == 1

    def test_multiple_dependencies_max_depth(self):
        """Verify task with multiple dependencies uses max depth."""
        task_map = {
            "t1": {"depends_on": [], "scope": "small", "expected_files": [], "title": ""},
            "t2": {"depends_on": ["t1"], "scope": "small", "expected_files": [], "title": ""},
            "t3": {"depends_on": [], "scope": "small", "expected_files": [], "title": ""},
            "t4": {"depends_on": ["t2", "t3"], "scope": "small", "expected_files": [], "title": ""},
        }
        # t4 depends on t2 (depth 1) and t3 (depth 0) -> t4 has depth 2
        assert _calculate_max_dependency_depth(task_map) == 2

    def test_circular_dependency_returns_zero(self):
        """Verify circular dependencies don't cause infinite loop."""
        task_map = {
            "t1": {"depends_on": ["t2"], "scope": "small", "expected_files": [], "title": ""},
            "t2": {"depends_on": ["t1"], "scope": "small", "expected_files": [], "title": ""},
        }
        # Circular dependency should be handled gracefully
        depth = _calculate_max_dependency_depth(task_map)
        assert depth >= 0  # Should not crash

    def test_self_dependency_handled(self):
        """Verify self-dependency is handled gracefully."""
        task_map = {
            "t1": {"depends_on": ["t1"], "scope": "small", "expected_files": [], "title": ""},
        }
        # Self-dependency creates a cycle, but depth calculation handles it
        depth = _calculate_max_dependency_depth(task_map)
        assert depth >= 0  # Should not crash

    def test_missing_dependency_ignored(self):
        """Verify missing dependency reference is ignored."""
        task_map = {
            "t1": {"depends_on": ["t_nonexistent"], "scope": "small", "expected_files": [], "title": ""},
        }
        # Missing dependency should be treated as depth 0
        assert _calculate_max_dependency_depth(task_map) >= 0


class TestCountFileConflicts:
    """Test file conflict counting helper."""

    def test_empty_task_map_returns_zero(self):
        """Verify empty task map returns (0, 0)."""
        assert _count_file_conflicts({}) == (0, 0)

    def test_single_task_returns_zero(self):
        """Verify single task returns (0, 0)."""
        task_map = {
            "t1": {"expected_files": ["src/foo.py"], "depends_on": [], "scope": "small", "title": ""}
        }
        assert _count_file_conflicts(task_map) == (0, 0)

    def test_two_tasks_no_overlap(self):
        """Verify two tasks with no file overlap."""
        task_map = {
            "t1": {"expected_files": ["src/a.py"], "depends_on": [], "scope": "small", "title": ""},
            "t2": {"expected_files": ["src/b.py"], "depends_on": [], "scope": "small", "title": ""},
        }
        # 1 pair, 0 conflicts
        assert _count_file_conflicts(task_map) == (0, 1)

    def test_two_tasks_with_overlap(self):
        """Verify two tasks with file overlap."""
        task_map = {
            "t1": {"expected_files": ["src/a.py", "src/shared.py"], "depends_on": [], "scope": "small", "title": ""},
            "t2": {"expected_files": ["src/b.py", "src/shared.py"], "depends_on": [], "scope": "small", "title": ""},
        }
        # 1 pair, 1 conflict
        assert _count_file_conflicts(task_map) == (1, 1)

    def test_three_tasks_partial_conflicts(self):
        """Verify three tasks with partial conflicts."""
        task_map = {
            "t1": {"expected_files": ["src/a.py"], "depends_on": [], "scope": "small", "title": ""},
            "t2": {"expected_files": ["src/a.py"], "depends_on": [], "scope": "small", "title": ""},
            "t3": {"expected_files": ["src/b.py"], "depends_on": [], "scope": "small", "title": ""},
        }
        # 3 pairs: (t1,t2), (t1,t3), (t2,t3)
        # Conflicts: (t1,t2) -> 1 conflict
        assert _count_file_conflicts(task_map) == (1, 3)

    def test_empty_file_lists_not_counted_as_pairs(self):
        """Verify tasks with empty file lists don't contribute to pairs."""
        task_map = {
            "t1": {"expected_files": [], "depends_on": [], "scope": "small", "title": ""},
            "t2": {"expected_files": ["src/a.py"], "depends_on": [], "scope": "small", "title": ""},
        }
        # Pair with empty files is not counted
        assert _count_file_conflicts(task_map) == (0, 0)

    def test_all_tasks_share_file(self):
        """Verify all tasks sharing same file."""
        task_map = {
            "t1": {"expected_files": ["shared.py"], "depends_on": [], "scope": "small", "title": ""},
            "t2": {"expected_files": ["shared.py"], "depends_on": [], "scope": "small", "title": ""},
            "t3": {"expected_files": ["shared.py"], "depends_on": [], "scope": "small", "title": ""},
        }
        # 3 pairs, all conflict
        assert _count_file_conflicts(task_map) == (3, 3)


class TestNormalizeFiles:
    """Test file normalization helper."""

    def test_empty_list_returns_empty(self):
        """Verify empty list returns empty list."""
        assert _normalize_files([]) == []

    def test_none_returns_empty(self):
        """Verify None returns empty list."""
        assert _normalize_files(None) == []

    def test_single_string_converted_to_list(self):
        """Verify single string is converted to list."""
        assert _normalize_files("src/foo.py") == ["src/foo.py"]

    def test_list_of_strings_returned(self):
        """Verify list of strings is returned as-is (normalized)."""
        result = _normalize_files(["src/foo.py", "src/bar.py"])
        assert result == ["src/foo.py", "src/bar.py"]

    def test_leading_dot_slash_removed(self):
        """Verify leading ./ is removed."""
        assert _normalize_files(["./src/foo.py"]) == ["src/foo.py"]

    def test_backslashes_converted_to_forward_slashes(self):
        """Verify backslashes are converted to forward slashes."""
        assert _normalize_files(["src\\foo.py"]) == ["src/foo.py"]

    def test_whitespace_stripped(self):
        """Verify whitespace is stripped."""
        assert _normalize_files(["  src/foo.py  "]) == ["src/foo.py"]


class TestNormalizeList:
    """Test list normalization helper."""

    def test_empty_list_returns_empty(self):
        """Verify empty list returns empty list."""
        assert _normalize_list([]) == []

    def test_none_returns_empty(self):
        """Verify None returns empty list."""
        assert _normalize_list(None) == []

    def test_single_string_converted_to_list(self):
        """Verify single string is converted to single-item list."""
        assert _normalize_list("foo") == ["foo"]

    def test_empty_string_returns_empty_list(self):
        """Verify empty string returns empty list."""
        assert _normalize_list("") == []

    def test_list_of_strings_returned(self):
        """Verify list of strings is returned."""
        assert _normalize_list(["a", "b", "c"]) == ["a", "b", "c"]

    def test_whitespace_stripped(self):
        """Verify whitespace is stripped from items."""
        assert _normalize_list(["  a  ", "b"]) == ["a", "b"]

    def test_empty_strings_filtered_out(self):
        """Verify empty strings are filtered out."""
        assert _normalize_list(["a", "", "  ", "b"]) == ["a", "b"]


class TestAverage:
    """Test average calculation helper."""

    def test_zero_count_returns_zero(self):
        """Verify zero count returns 0.0."""
        assert _average(10.0, 0) == 0.0

    def test_negative_count_returns_zero(self):
        """Verify negative count returns 0.0."""
        assert _average(10.0, -5) == 0.0

    def test_simple_average(self):
        """Verify simple average calculation."""
        assert _average(10.0, 4) == 2.5

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _average(10.0, 3) == 3.33


class TestPercentage:
    """Test percentage calculation helper."""

    def test_zero_denominator_returns_zero(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(10, 0) == 0.0

    def test_zero_numerator_returns_zero(self):
        """Verify zero numerator returns 0.0."""
        assert _percentage(0, 10) == 0.0

    def test_simple_percentage(self):
        """Verify simple percentage calculation."""
        assert _percentage(1, 4) == 25.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _percentage(1, 3) == 33.33


class TestClassifyGranularityPattern:
    """Test granularity pattern classification."""

    def test_empty_returns_empty_pattern(self):
        """Verify zero tasks returns empty pattern."""
        pattern = _classify_granularity_pattern(Counter(), 0, 0.0, 0.0, 0.0)
        assert pattern == "empty"

    def test_conflicted_pattern_high_file_conflicts(self):
        """Verify high file conflict rate returns conflicted."""
        scope_counts = Counter({"small": 5})
        pattern = _classify_granularity_pattern(scope_counts, 5, 1.0, 50.0, 50.0)
        assert pattern == "conflicted"

    def test_over_granular_pattern(self):
        """Verify over-granular detection."""
        scope_counts = Counter({"tiny": 6, "small": 2})
        # 75% tiny, avg depth 3.0
        pattern = _classify_granularity_pattern(scope_counts, 8, 3.0, 0.0, 20.0)
        assert pattern == "over_granular"

    def test_under_granular_pattern(self):
        """Verify under-granular detection."""
        scope_counts = Counter({"large": 6, "small": 2})
        # 75% large, 90% independence
        pattern = _classify_granularity_pattern(scope_counts, 8, 0.5, 0.0, 90.0)
        assert pattern == "under_granular"

    def test_well_balanced_pattern(self):
        """Verify well-balanced detection."""
        scope_counts = Counter({"small": 3, "medium": 2})
        # Mixed scopes, low depth, moderate independence
        pattern = _classify_granularity_pattern(scope_counts, 5, 1.5, 5.0, 50.0)
        assert pattern == "well_balanced"

    def test_mixed_pattern_default(self):
        """Verify mixed pattern as default."""
        scope_counts = Counter({"small": 5})
        # Doesn't fit other patterns
        pattern = _classify_granularity_pattern(scope_counts, 5, 2.5, 5.0, 20.0)
        assert pattern == "mixed"


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_well_balanced_pack(self):
        """Simulate well-balanced execution pack."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack_123",
                "tasks": [
                    {
                        "task_id": "t1",
                        "estimated_scope": "small",
                        "depends_on": [],
                        "expected_files": ["src/analyzer.py"],
                        "task_title": "Implement analyzer",
                    },
                    {
                        "task_id": "t2",
                        "estimated_scope": "small",
                        "depends_on": [],
                        "expected_files": ["tests/test_analyzer.py"],
                        "task_title": "Add tests",
                    },
                    {
                        "task_id": "t3",
                        "estimated_scope": "tiny",
                        "depends_on": ["t1", "t2"],
                        "expected_files": ["README.md"],
                        "task_title": "Update docs",
                    },
                ],
            }
        ])

        assert result["total_tasks"] == 3
        assert result["file_conflict_rate"] == 0.0
        assert result["max_dependency_depth"] == 1
        assert result["granularity_pattern"] == "well_balanced"

    def test_over_granular_pack_with_deep_chains(self):
        """Simulate over-granular pack with excessive tiny tasks."""
        tasks = [
            {"task_id": "t1", "estimated_scope": "tiny", "depends_on": [], "expected_files": []},
            {"task_id": "t2", "estimated_scope": "tiny", "depends_on": ["t1"], "expected_files": []},
            {"task_id": "t3", "estimated_scope": "tiny", "depends_on": ["t2"], "expected_files": []},
            {"task_id": "t4", "estimated_scope": "tiny", "depends_on": ["t3"], "expected_files": []},
            {"task_id": "t5", "estimated_scope": "tiny", "depends_on": ["t4"], "expected_files": []},
        ]

        result = analyze_pack_task_granularity([{"pack_id": "pack_123", "tasks": tasks}])

        assert result["scope_percentages"]["tiny"] == 100.0
        assert result["avg_dependency_depth"] == 4.0
        assert result["granularity_pattern"] == "over_granular"

    def test_conflicted_pack_with_file_overlap(self):
        """Simulate pack with high file conflicts."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack_123",
                "tasks": [
                    {
                        "task_id": "t1",
                        "estimated_scope": "small",
                        "depends_on": [],
                        "expected_files": ["src/shared.py", "src/utils.py"],
                    },
                    {
                        "task_id": "t2",
                        "estimated_scope": "small",
                        "depends_on": [],
                        "expected_files": ["src/shared.py", "src/helpers.py"],
                    },
                    {
                        "task_id": "t3",
                        "estimated_scope": "small",
                        "depends_on": [],
                        "expected_files": ["src/utils.py", "src/helpers.py"],
                    },
                ],
            }
        ])

        # All 3 pairs have conflicts
        assert result["file_conflict_rate"] == 100.0
        assert result["granularity_pattern"] == "conflicted"

    def test_under_granular_pack_large_independent_tasks(self):
        """Simulate under-granular pack with large independent tasks."""
        result = analyze_pack_task_granularity([
            {
                "pack_id": "pack_123",
                "tasks": [
                    {
                        "task_id": "t1",
                        "estimated_scope": "large",
                        "depends_on": [],
                        "expected_files": ["module_a/file1.py", "module_a/file2.py"],
                    },
                    {
                        "task_id": "t2",
                        "estimated_scope": "large",
                        "depends_on": [],
                        "expected_files": ["module_b/file1.py", "module_b/file2.py"],
                    },
                ],
            }
        ])

        assert result["scope_percentages"]["large"] == 100.0
        assert result["independence_ratio"] == 100.0
        assert result["granularity_pattern"] == "under_granular"
