"""Tests for pack cross-task tool reuse efficiency analyzer."""

import pytest

from synthesis.pack_cross_task_tool_reuse import (
    analyze_pack_cross_task_tool_reuse,
    _count_unique_tool_calls,
    _calculate_cache_opportunity_score,
    _extract_primary_param,
    _percentage,
    _average,
)


class TestAnalyzePackCrossTaskToolReuse:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_cross_task_tool_reuse([])

        assert result["total_packs"] == 0
        assert result["avg_total_tool_calls"] == 0.0
        assert result["avg_unique_tool_calls"] == 0.0
        assert result["avg_tool_reuse_percentage"] == 0.0
        assert result["avg_file_reuse_percentage"] == 0.0
        assert result["avg_redundant_read_ratio"] == 0.0
        assert result["avg_grep_pattern_reuse"] == 0.0
        assert result["avg_cache_opportunity_score"] == 0.0
        assert result["high_reuse_packs"] == 0
        assert result["low_reuse_packs"] == 0
        assert result["common_reuse_patterns"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_cross_task_tool_reuse(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_cross_task_tool_reuse("not a list")

    def test_pack_with_no_tool_reuse(self):
        """Verify pack with unique tool calls has 0% reuse."""
        result = analyze_pack_cross_task_tool_reuse([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                        ]
                    },
                    {
                        "task_id": "task2",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_tool_calls"] == 2.0
        assert result["avg_unique_tool_calls"] == 2.0
        assert result["avg_tool_reuse_percentage"] == 0.0
        assert result["low_reuse_packs"] == 1

    def test_pack_with_high_file_reuse(self):
        """Verify pack with file reuse is detected."""
        result = analyze_pack_cross_task_tool_reuse([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                        ]
                    },
                    {
                        "task_id": "task2",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                        ]
                    },
                    {
                        "task_id": "task3",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_tool_calls"] == 3.0
        assert result["avg_unique_tool_calls"] == 1.0
        assert result["avg_tool_reuse_percentage"] == pytest.approx(66.67, abs=0.01)
        assert result["avg_file_reuse_percentage"] == 100.0  # 1 file, accessed 3 times
        assert result["avg_redundant_read_ratio"] == pytest.approx(66.67, abs=0.01)
        # 66.67% reuse is not >70%, so not high_reuse_packs
        assert result["high_reuse_packs"] == 0
        assert result["low_reuse_packs"] == 0  # Also not <30%

    def test_grep_pattern_reuse_tracked(self):
        """Verify Grep pattern reuse is tracked."""
        result = analyze_pack_cross_task_tool_reuse([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "tool_calls": [
                            {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                        ]
                    },
                    {
                        "task_id": "task2",
                        "tool_calls": [
                            {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                        ]
                    },
                    {
                        "task_id": "task3",
                        "tool_calls": [
                            {"tool_name": "Grep", "parameters": {"pattern": "foo"}},
                        ]
                    }
                ]
            }
        ])

        # 3 total Grep calls, 2 unique patterns
        # Reuse = (3 - 2) / 3 = 33.33%
        assert result["avg_grep_pattern_reuse"] == pytest.approx(33.33, abs=0.01)

    def test_mixed_tool_reuse(self):
        """Verify mixed tool types with varying reuse."""
        result = analyze_pack_cross_task_tool_reuse([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                            {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                        ]
                    },
                    {
                        "task_id": "task2",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                            {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                            {"tool_name": "Edit", "parameters": {"file_path": "/b.py"}},
                        ]
                    }
                ]
            }
        ])

        # Total: 5 calls
        # Unique: Read /a.py, Grep test, Edit /b.py = 3
        # Reuse: (5 - 3) / 5 = 40%
        assert result["avg_total_tool_calls"] == 5.0
        assert result["avg_unique_tool_calls"] == 3.0
        assert result["avg_tool_reuse_percentage"] == 40.0

    def test_cache_opportunity_score_high(self):
        """Verify high cache opportunity score for files with multiple accesses."""
        result = analyze_pack_cross_task_tool_reuse([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": f"task{i}",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/config.py"}},
                        ]
                    }
                    for i in range(5)
                ]
            }
        ])

        # Same file read 5 times = high cache opportunity
        assert result["avg_cache_opportunity_score"] > 70.0

    def test_cache_opportunity_score_low(self):
        """Verify low cache opportunity score for unique file accesses."""
        result = analyze_pack_cross_task_tool_reuse([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": f"task{i}",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": f"/file{i}.py"}},
                        ]
                    }
                    for i in range(5)
                ]
            }
        ])

        # 5 unique files = low cache opportunity
        assert result["avg_cache_opportunity_score"] == 0.0

    def test_common_reuse_patterns_tracked(self):
        """Verify common reuse patterns are identified."""
        result = analyze_pack_cross_task_tool_reuse([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/config.py"}},
                            {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                        ]
                    },
                    {
                        "task_id": "task2",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/config.py"}},
                            {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                        ]
                    },
                    {
                        "task_id": "task3",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/config.py"}},
                        ]
                    }
                ]
            }
        ])

        patterns = result["common_reuse_patterns"]
        assert len(patterns) > 0
        # Most reused should be Read /config.py (3 times)
        assert patterns[0]["tool"] == "Read"
        assert patterns[0]["parameter"] == "/config.py"
        assert patterns[0]["reuse_count"] == 3

    def test_read_with_offset_limit_uniqueness(self):
        """Verify Read calls with different offset/limit are unique."""
        result = analyze_pack_cross_task_tool_reuse([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {
                                "file_path": "/a.py",
                                "offset": 0,
                                "limit": 50
                            }},
                        ]
                    },
                    {
                        "task_id": "task2",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {
                                "file_path": "/a.py",
                                "offset": 100,
                                "limit": 50
                            }},
                        ]
                    }
                ]
            }
        ])

        # Same file, different offset/limit = 2 unique calls
        assert result["avg_unique_tool_calls"] == 2.0
        assert result["avg_tool_reuse_percentage"] == 0.0

    def test_multiple_packs_aggregation(self):
        """Verify metrics are correctly aggregated across multiple packs."""
        result = analyze_pack_cross_task_tool_reuse([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                        ]
                    }
                ]
            },
            {
                "pack_id": "pack2",
                "tasks": [
                    {
                        "task_id": "task1",
                        "tool_calls": [
                            {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
                            {"tool_name": "Read", "parameters": {"file_path": "/c.py"}},
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 2
        # Pack1: 2 calls, 1 unique = 50% reuse
        # Pack2: 2 calls, 2 unique = 0% reuse
        # Average: 25%
        assert result["avg_tool_reuse_percentage"] == 25.0


class TestCountUniqueToolCalls:
    """Test unique tool call counting."""

    def test_identical_read_calls(self):
        """Verify identical Read calls count as one unique."""
        tool_calls = [
            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
        ]
        assert _count_unique_tool_calls(tool_calls) == 1

    def test_different_file_paths(self):
        """Verify different file paths count as unique."""
        tool_calls = [
            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
            {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
        ]
        assert _count_unique_tool_calls(tool_calls) == 2

    def test_read_with_different_offset_limit(self):
        """Verify Read with different offset/limit are unique."""
        tool_calls = [
            {"tool_name": "Read", "parameters": {"file_path": "/a.py", "offset": 0, "limit": 50}},
            {"tool_name": "Read", "parameters": {"file_path": "/a.py", "offset": 100, "limit": 50}},
        ]
        assert _count_unique_tool_calls(tool_calls) == 2

    def test_grep_with_same_pattern(self):
        """Verify Grep with same pattern counts as one."""
        tool_calls = [
            {"tool_name": "Grep", "parameters": {"pattern": "test"}},
            {"tool_name": "Grep", "parameters": {"pattern": "test"}},
        ]
        assert _count_unique_tool_calls(tool_calls) == 1

    def test_mixed_tool_types(self):
        """Verify different tool types are unique."""
        tool_calls = [
            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
            {"tool_name": "Grep", "parameters": {"pattern": "test"}},
            {"tool_name": "Edit", "parameters": {"file_path": "/a.py"}},
        ]
        assert _count_unique_tool_calls(tool_calls) == 3


class TestCalculateCacheOpportunityScore:
    """Test cache opportunity score calculation."""

    def test_no_files_returns_zero(self):
        """Verify empty file access returns 0."""
        assert _calculate_cache_opportunity_score({}, {}) == 0.0

    def test_single_file_single_access_low_score(self):
        """Verify single access has low score."""
        file_access = {"/a.py": 1}
        read_sigs = {"/a.py||": 1}
        score = _calculate_cache_opportunity_score(file_access, read_sigs)
        assert score == 0.0

    def test_single_file_multiple_access_high_score(self):
        """Verify multiple accesses to same file has high score."""
        file_access = {"/a.py": 5}
        read_sigs = {"/a.py||": 5}
        score = _calculate_cache_opportunity_score(file_access, read_sigs)
        assert score > 70.0

    def test_multiple_files_mixed_access(self):
        """Verify mixed access pattern has moderate score."""
        file_access = {
            "/a.py": 3,
            "/b.py": 1,
            "/c.py": 2
        }
        read_sigs = {
            "/a.py||": 3,
            "/b.py||": 1,
            "/c.py||": 2
        }
        score = _calculate_cache_opportunity_score(file_access, read_sigs)
        assert 30.0 < score < 70.0


class TestExtractPrimaryParam:
    """Test primary parameter extraction."""

    def test_read_extracts_file_path(self):
        """Verify Read tool extracts file_path."""
        params = {"file_path": "/test.py"}
        assert _extract_primary_param("Read", params) == "/test.py"

    def test_grep_extracts_pattern(self):
        """Verify Grep tool extracts pattern."""
        params = {"pattern": "test.*"}
        assert _extract_primary_param("Grep", params) == "test.*"

    def test_glob_extracts_pattern(self):
        """Verify Glob tool extracts pattern."""
        params = {"pattern": "*.py"}
        assert _extract_primary_param("Glob", params) == "*.py"

    def test_edit_extracts_file_path(self):
        """Verify Edit tool extracts file_path."""
        params = {"file_path": "/test.py"}
        assert _extract_primary_param("Edit", params) == "/test.py"

    def test_bash_extracts_truncated_command(self):
        """Verify Bash tool extracts truncated command."""
        params = {"command": "a" * 100}
        result = _extract_primary_param("Bash", params)
        assert len(result) == 50

    def test_unknown_tool_returns_empty(self):
        """Verify unknown tool returns empty string."""
        params = {"some": "param"}
        assert _extract_primary_param("UnknownTool", params) == ""


class TestHelperFunctions:
    """Test helper functions."""

    def test_percentage_calculation(self):
        """Verify percentage calculation."""
        assert _percentage(50, 100) == 50.0
        assert _percentage(1, 3) == 33.33
        assert _percentage(0, 100) == 0.0

    def test_percentage_zero_denominator(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(50, 0) == 0.0

    def test_average_calculation(self):
        """Verify average calculation."""
        assert _average([1.0, 2.0, 3.0]) == 2.0
        assert _average([10.0, 20.0]) == 15.0
        assert _average([100.0]) == 100.0

    def test_average_empty_list(self):
        """Verify empty list returns 0.0."""
        assert _average([]) == 0.0
