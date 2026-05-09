"""Tests for pack tool call density analyzer."""

import pytest

from synthesis.pack_tool_call_density import analyze_pack_tool_call_density


class TestAnalyzePackToolCallDensity:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_tool_call_density([])

        assert result["total_packs"] == 0
        assert result["avg_total_tool_calls"] == 0.0
        assert result["avg_tool_calls_per_task"] == 0.0
        assert result["avg_read_call_ratio"] == 0.0
        assert result["avg_edit_call_ratio"] == 0.0
        assert result["avg_write_call_ratio"] == 0.0
        assert result["avg_grep_call_ratio"] == 0.0
        assert result["avg_glob_call_ratio"] == 0.0
        assert result["avg_bash_call_ratio"] == 0.0
        assert result["avg_task_call_ratio"] == 0.0
        assert result["avg_parallel_block_ratio"] == 0.0
        assert result["avg_clustering_ratio"] == 0.0
        assert result["high_density_packs"] == 0
        assert result["low_density_packs"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_tool_call_density(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_tool_call_density("not a list")

    def test_single_pack_basic_metrics(self):
        """Verify basic metrics for single pack."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "task_count": 5,
                "read_calls": 50,
                "edit_calls": 20,
                "write_calls": 10,
                "grep_calls": 5,
                "glob_calls": 5,
                "bash_calls": 8,
                "task_calls": 2,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_tool_calls"] == 100.0
        # 100 / 5 = 20 calls per task
        assert result["avg_tool_calls_per_task"] == 20.0
        # 50 / 100 = 50%
        assert result["avg_read_call_ratio"] == 50.0
        # 20 / 100 = 20%
        assert result["avg_edit_call_ratio"] == 20.0
        # 10 / 100 = 10%
        assert result["avg_write_call_ratio"] == 10.0

    def test_high_density_pack(self):
        """Verify high density pack classification (>50 calls/task)."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 300,
                "task_count": 5,
            }
        ])

        # 300 / 5 = 60 calls per task
        assert result["avg_tool_calls_per_task"] == 60.0
        assert result["high_density_packs"] == 1
        assert result["low_density_packs"] == 0

    def test_low_density_pack(self):
        """Verify low density pack classification (<10 calls/task)."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 20,
                "task_count": 5,
            }
        ])

        # 20 / 5 = 4 calls per task
        assert result["avg_tool_calls_per_task"] == 4.0
        assert result["high_density_packs"] == 0
        assert result["low_density_packs"] == 1

    def test_medium_density_not_classified(self):
        """Verify medium density not classified as high or low."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 150,
                "task_count": 5,
            }
        ])

        # 150 / 5 = 30 calls per task (10-50 range)
        assert result["avg_tool_calls_per_task"] == 30.0
        assert result["high_density_packs"] == 0
        assert result["low_density_packs"] == 0

    def test_tool_type_distribution(self):
        """Verify tool type distribution calculated correctly."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "task_count": 1,
                "read_calls": 40,
                "edit_calls": 25,
                "write_calls": 15,
                "grep_calls": 10,
                "glob_calls": 5,
                "bash_calls": 3,
                "task_calls": 2,
            }
        ])

        assert result["avg_read_call_ratio"] == 40.0
        assert result["avg_edit_call_ratio"] == 25.0
        assert result["avg_write_call_ratio"] == 15.0
        assert result["avg_grep_call_ratio"] == 10.0
        assert result["avg_glob_call_ratio"] == 5.0
        assert result["avg_bash_call_ratio"] == 3.0
        assert result["avg_task_call_ratio"] == 2.0

    def test_parallel_tool_blocks(self):
        """Verify parallel tool block ratio calculation."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "parallel_tool_blocks": 40,
            }
        ])

        # 40 / 100 = 40%
        assert result["avg_parallel_block_ratio"] == 40.0

    def test_tool_call_clustering(self):
        """Verify tool call clustering ratio calculation."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "clustered_tool_calls": 50,
            }
        ])

        # 50 / 100 = 50%
        assert result["avg_clustering_ratio"] == 50.0

    def test_multiple_packs_averaged(self):
        """Verify metrics averaged across multiple packs."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "task_count": 5,
                "read_calls": 50,
            },
            {
                "pack_id": "pack2",
                "total_tool_calls": 200,
                "task_count": 10,
                "read_calls": 100,
            },
            {
                "pack_id": "pack3",
                "total_tool_calls": 150,
                "task_count": 5,
                "read_calls": 75,
            },
        ])

        assert result["total_packs"] == 3
        # (100 + 200 + 150) / 3 = 150
        assert result["avg_total_tool_calls"] == 150.0
        # (100/5 + 200/10 + 150/5) / 3 = (20 + 20 + 30) / 3 = 23.33
        assert 23.0 <= result["avg_tool_calls_per_task"] <= 24.0
        # All 50% Read
        assert result["avg_read_call_ratio"] == 50.0

    def test_zero_tool_calls(self):
        """Verify pack with zero tool calls handled gracefully."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 0,
                "task_count": 5,
            }
        ])

        assert result["avg_total_tool_calls"] == 0.0
        # 0 / 5 = 0 calls per task
        assert result["avg_tool_calls_per_task"] == 0.0

    def test_missing_task_count(self):
        """Verify missing task count handled gracefully."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                # Missing task_count
            }
        ])

        assert result["avg_total_tool_calls"] == 100.0
        # No task count means no per-task calculation
        assert result["avg_tool_calls_per_task"] == 0.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_tool_call_density([
            "not a dict",
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
            },
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for integer fields."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": True,
                "task_count": False,
            }
        ])

        assert result["avg_total_tool_calls"] == 0.0

    def test_missing_optional_tool_fields(self):
        """Verify missing optional tool type fields handled gracefully."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "task_count": 5,
                # Missing all tool type fields
            }
        ])

        assert result["avg_total_tool_calls"] == 100.0
        # Missing fields result in 0.0 ratios
        assert result["avg_read_call_ratio"] == 0.0
        assert result["avg_edit_call_ratio"] == 0.0

    def test_read_heavy_pack(self):
        """Verify pack dominated by Read calls."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "read_calls": 90,
                "edit_calls": 5,
                "write_calls": 5,
            }
        ])

        assert result["avg_read_call_ratio"] == 90.0
        assert result["avg_edit_call_ratio"] == 5.0
        assert result["avg_write_call_ratio"] == 5.0

    def test_edit_heavy_pack(self):
        """Verify pack dominated by Edit calls."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "read_calls": 10,
                "edit_calls": 80,
                "bash_calls": 10,
            }
        ])

        assert result["avg_read_call_ratio"] == 10.0
        assert result["avg_edit_call_ratio"] == 80.0
        assert result["avg_bash_call_ratio"] == 10.0

    def test_balanced_tool_distribution(self):
        """Verify pack with balanced tool distribution."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "read_calls": 20,
                "edit_calls": 20,
                "write_calls": 20,
                "grep_calls": 20,
                "bash_calls": 20,
            }
        ])

        assert result["avg_read_call_ratio"] == 20.0
        assert result["avg_edit_call_ratio"] == 20.0
        assert result["avg_write_call_ratio"] == 20.0
        assert result["avg_grep_call_ratio"] == 20.0
        assert result["avg_bash_call_ratio"] == 20.0

    def test_high_parallel_block_usage(self):
        """Verify high parallel block usage pattern."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "parallel_tool_blocks": 60,
            }
        ])

        assert result["avg_parallel_block_ratio"] == 60.0

    def test_low_parallel_block_usage(self):
        """Verify low parallel block usage pattern."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "parallel_tool_blocks": 5,
            }
        ])

        assert result["avg_parallel_block_ratio"] == 5.0

    def test_high_clustering(self):
        """Verify high tool call clustering pattern."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "clustered_tool_calls": 80,
            }
        ])

        assert result["avg_clustering_ratio"] == 80.0

    def test_low_clustering(self):
        """Verify low tool call clustering pattern."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "pack1",
                "total_tool_calls": 100,
                "clustered_tool_calls": 10,
            }
        ])

        assert result["avg_clustering_ratio"] == 10.0

    def test_boundary_density_classification(self):
        """Verify boundary cases for density classification."""
        result = analyze_pack_tool_call_density([
            # Exactly 50 (should not be high)
            {
                "pack_id": "p1",
                "total_tool_calls": 50,
                "task_count": 1,
            },
            # Just above 50 (should be high)
            {
                "pack_id": "p2",
                "total_tool_calls": 51,
                "task_count": 1,
            },
            # Exactly 10 (should not be low)
            {
                "pack_id": "p3",
                "total_tool_calls": 10,
                "task_count": 1,
            },
            # Just below 10 (should be low)
            {
                "pack_id": "p4",
                "total_tool_calls": 9,
                "task_count": 1,
            },
        ])

        # >50 means strictly greater
        assert result["high_density_packs"] == 1
        # <10 means strictly less
        assert result["low_density_packs"] == 1

    def test_comprehensive_pack_all_fields(self):
        """Verify comprehensive pack with all fields populated."""
        result = analyze_pack_tool_call_density([
            {
                "pack_id": "comprehensive",
                "pack_title": "Test Pack",
                "total_tool_calls": 200,
                "task_count": 8,
                "read_calls": 60,
                "edit_calls": 50,
                "write_calls": 30,
                "grep_calls": 20,
                "glob_calls": 15,
                "bash_calls": 20,
                "task_calls": 5,
                "parallel_tool_blocks": 80,
                "clustered_tool_calls": 100,
            }
        ])

        assert result["avg_total_tool_calls"] == 200.0
        # 200 / 8 = 25
        assert result["avg_tool_calls_per_task"] == 25.0
        assert result["avg_read_call_ratio"] == 30.0
        assert result["avg_edit_call_ratio"] == 25.0
        assert result["avg_write_call_ratio"] == 15.0
        assert result["avg_grep_call_ratio"] == 10.0
        assert result["avg_glob_call_ratio"] == 7.5
        assert result["avg_bash_call_ratio"] == 10.0
        assert result["avg_task_call_ratio"] == 2.5
        assert result["avg_parallel_block_ratio"] == 40.0
        assert result["avg_clustering_ratio"] == 50.0
