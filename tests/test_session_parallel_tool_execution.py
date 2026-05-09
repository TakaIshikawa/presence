"""Tests for session parallel tool execution pattern analyzer."""

import pytest

from synthesis.session_parallel_tool_execution import (
    analyze_session_parallel_tool_execution,
    _is_parallel_execution,
    _extract_tool_names,
    _check_parameter_independence,
    _could_be_parallel,
    _percentage,
    _average,
    _calculate_correlation,
)


class TestAnalyzeSessionParallelToolExecution:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_parallel_tool_execution([])

        assert result["total_messages"] == 0
        assert result["multi_tool_messages"] == 0
        assert result["parallel_messages"] == 0
        assert result["sequential_messages"] == 0
        assert result["parallel_batch_percentage"] == 0.0
        assert result["avg_tools_per_batch"] == 0.0
        assert result["max_tools_per_batch"] == 0
        assert result["total_tool_calls"] == 0
        assert result["parallel_tool_calls"] == 0
        assert result["sequential_tool_calls"] == 0
        assert result["wasted_sequencing_count"] == 0
        assert result["wasted_sequencing_ratio"] == 0.0
        assert result["independence_detection_accuracy"] == 0.0
        assert result["common_parallel_patterns"] == []
        assert result["common_wasted_patterns"] == []
        assert result["token_efficiency_correlation"] == 0.0
        assert result["avg_tokens_parallel"] == 0.0
        assert result["avg_tokens_sequential"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_parallel_tool_execution(None)
        assert result["total_messages"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_parallel_tool_execution("not a list")

    def test_single_tool_call_message_skipped(self):
        """Verify messages with single tool call are counted but skipped."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/test.py"}}
                ]
            }
        ])

        assert result["total_messages"] == 1
        assert result["multi_tool_messages"] == 0
        assert result["total_tool_calls"] == 1
        assert result["sequential_tool_calls"] == 1

    def test_parallel_batch_detected(self):
        """Verify parallel tool batch is correctly detected."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "is_parallel_block": True,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
                    {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                ]
            }
        ])

        assert result["multi_tool_messages"] == 1
        assert result["parallel_messages"] == 1
        assert result["sequential_messages"] == 0
        assert result["parallel_batch_percentage"] == 100.0
        assert result["avg_tools_per_batch"] == 3.0
        assert result["max_tools_per_batch"] == 3
        assert result["parallel_tool_calls"] == 3
        assert result["sequential_tool_calls"] == 0

    def test_sequential_execution_detected(self):
        """Verify sequential execution is detected when flag is set."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "is_parallel_block": False,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
                ]
            }
        ])

        assert result["multi_tool_messages"] == 1
        assert result["parallel_messages"] == 0
        assert result["sequential_messages"] == 1
        assert result["parallel_batch_percentage"] == 0.0
        assert result["sequential_tool_calls"] == 2

    def test_wasted_sequencing_detected(self):
        """Verify wasted sequencing (independent sequential calls) is detected."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "is_parallel_block": False,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
                    {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                ]
            }
        ])

        assert result["wasted_sequencing_count"] == 3
        assert result["wasted_sequencing_ratio"] == 100.0
        assert len(result["common_wasted_patterns"]) > 0

    def test_mixed_parallel_and_sequential_messages(self):
        """Verify mixed parallelization patterns are tracked correctly."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "is_parallel_block": True,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
                ]
            },
            {
                "message_index": 2,
                "is_parallel_block": False,
                "tool_calls": [
                    {"tool_name": "Edit", "parameters": {"file_path": "/a.py"}},
                    {"tool_name": "Edit", "parameters": {"file_path": "/b.py"}},
                ]
            },
            {
                "message_index": 3,
                "is_parallel_block": True,
                "tool_calls": [
                    {"tool_name": "Grep", "parameters": {"pattern": "foo"}},
                    {"tool_name": "Grep", "parameters": {"pattern": "bar"}},
                    {"tool_name": "Glob", "parameters": {"pattern": "*.py"}},
                ]
            }
        ])

        assert result["total_messages"] == 3
        assert result["multi_tool_messages"] == 3
        assert result["parallel_messages"] == 2
        assert result["sequential_messages"] == 1
        assert result["parallel_batch_percentage"] == pytest.approx(66.67, abs=0.01)
        assert result["avg_tools_per_batch"] == 2.5  # (2 + 3) / 2
        assert result["max_tools_per_batch"] == 3

    def test_token_efficiency_correlation_negative(self):
        """Verify negative correlation when parallel uses fewer tokens."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "is_parallel_block": True,
                "tokens_used": 100,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
                ]
            },
            {
                "message_index": 2,
                "is_parallel_block": False,
                "tokens_used": 200,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/c.py"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/d.py"}},
                ]
            }
        ])

        assert result["avg_tokens_parallel"] == 100.0
        assert result["avg_tokens_sequential"] == 200.0
        assert result["token_efficiency_correlation"] < 0  # Negative correlation

    def test_token_efficiency_correlation_positive(self):
        """Verify positive correlation when parallel uses more tokens."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "is_parallel_block": True,
                "tokens_used": 300,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
                ]
            },
            {
                "message_index": 2,
                "is_parallel_block": False,
                "tokens_used": 150,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/c.py"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/d.py"}},
                ]
            }
        ])

        assert result["avg_tokens_parallel"] == 300.0
        assert result["avg_tokens_sequential"] == 150.0
        assert result["token_efficiency_correlation"] > 0  # Positive correlation

    def test_common_parallel_patterns_tracked(self):
        """Verify common parallel patterns are tracked."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "is_parallel_block": True,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                    {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                ]
            },
            {
                "message_index": 2,
                "is_parallel_block": True,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
                    {"tool_name": "Grep", "parameters": {"pattern": "foo"}},
                ]
            },
            {
                "message_index": 3,
                "is_parallel_block": True,
                "tool_calls": [
                    {"tool_name": "Glob", "parameters": {"pattern": "*.py"}},
                    {"tool_name": "Glob", "parameters": {"pattern": "*.js"}},
                ]
            }
        ])

        patterns = result["common_parallel_patterns"]
        assert len(patterns) > 0
        # Most common should be ["Grep", "Read"] appearing twice
        assert patterns[0]["count"] == 2
        assert set(patterns[0]["tools"]) == {"Grep", "Read"}

    def test_large_parallel_batch(self):
        """Verify large parallel batches are tracked correctly."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "is_parallel_block": True,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": f"/file{i}.py"}}
                    for i in range(10)
                ]
            }
        ])

        assert result["avg_tools_per_batch"] == 10.0
        assert result["max_tools_per_batch"] == 10
        assert result["parallel_tool_calls"] == 10

    def test_independence_detection_with_different_files(self):
        """Verify independence detection for different file paths."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "is_parallel_block": True,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/c.py"}},
                ]
            }
        ])

        # Should detect as independent (different files)
        assert result["independence_detection_accuracy"] == 100.0

    def test_independence_detection_with_same_file_edits(self):
        """Verify dependent detection for edits on same file."""
        result = analyze_session_parallel_tool_execution([
            {
                "message_index": 1,
                "is_parallel_block": True,
                "tool_calls": [
                    {"tool_name": "Edit", "parameters": {"file_path": "/a.py"}},
                    {"tool_name": "Edit", "parameters": {"file_path": "/a.py"}},
                ]
            }
        ])

        # Should detect as dependent (same file edits)
        assert result["independence_detection_accuracy"] == 0.0


class TestIsParallelExecution:
    """Test parallel execution detection."""

    def test_explicit_parallel_flag_true(self):
        """Verify explicit is_parallel_block flag is honored."""
        record = {"is_parallel_block": True}
        tool_calls = [{"tool_name": "Read"}]
        assert _is_parallel_execution(record, tool_calls) is True

    def test_explicit_parallel_flag_false(self):
        """Verify explicit is_parallel_block=False is honored."""
        record = {"is_parallel_block": False}
        tool_calls = [{"tool_name": "Read"}]
        assert _is_parallel_execution(record, tool_calls) is False

    def test_execution_mode_parallel(self):
        """Verify execution_mode=parallel is detected."""
        record = {}
        tool_calls = [{"tool_name": "Read", "execution_mode": "parallel"}]
        assert _is_parallel_execution(record, tool_calls) is True

    def test_default_heuristic_multiple_tools(self):
        """Verify default heuristic treats multiple tools as parallel."""
        record = {}
        tool_calls = [{"tool_name": "Read"}, {"tool_name": "Grep"}]
        assert _is_parallel_execution(record, tool_calls) is True


class TestExtractToolNames:
    """Test tool name extraction."""

    def test_extract_single_tool_name(self):
        """Verify single tool name extraction."""
        tool_calls = [{"tool_name": "Read"}]
        assert _extract_tool_names(tool_calls) == ["Read"]

    def test_extract_multiple_tool_names(self):
        """Verify multiple tool names extraction."""
        tool_calls = [
            {"tool_name": "Read"},
            {"tool_name": "Grep"},
            {"tool_name": "Edit"}
        ]
        assert _extract_tool_names(tool_calls) == ["Read", "Grep", "Edit"]

    def test_extract_with_whitespace_stripping(self):
        """Verify whitespace is stripped from tool names."""
        tool_calls = [{"tool_name": "  Read  "}, {"tool_name": "Grep\n"}]
        assert _extract_tool_names(tool_calls) == ["Read", "Grep"]

    def test_skip_invalid_entries(self):
        """Verify invalid entries are skipped."""
        tool_calls = [
            {"tool_name": "Read"},
            "invalid",
            {"tool_name": ""},
            {"other": "field"},
            {"tool_name": "Grep"}
        ]
        assert _extract_tool_names(tool_calls) == ["Read", "Grep"]


class TestCheckParameterIndependence:
    """Test parameter independence checking."""

    def test_different_file_paths_are_independent(self):
        """Verify different file paths are detected as independent."""
        tool_calls = [
            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
            {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
        ]
        assert _check_parameter_independence(tool_calls) is True

    def test_same_file_reads_are_independent(self):
        """Verify reading same file multiple times is independent."""
        tool_calls = [
            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
        ]
        assert _check_parameter_independence(tool_calls) is True

    def test_same_file_edits_are_dependent(self):
        """Verify editing same file multiple times is dependent."""
        tool_calls = [
            {"tool_name": "Edit", "parameters": {"file_path": "/a.py"}},
            {"tool_name": "Edit", "parameters": {"file_path": "/a.py"}},
        ]
        assert _check_parameter_independence(tool_calls) is False

    def test_same_file_writes_are_dependent(self):
        """Verify writing to same file multiple times is dependent."""
        tool_calls = [
            {"tool_name": "Write", "parameters": {"file_path": "/a.py"}},
            {"tool_name": "Write", "parameters": {"file_path": "/a.py"}},
        ]
        assert _check_parameter_independence(tool_calls) is False

    def test_different_patterns_are_independent(self):
        """Verify different grep patterns are independent."""
        tool_calls = [
            {"tool_name": "Grep", "parameters": {"pattern": "foo"}},
            {"tool_name": "Grep", "parameters": {"pattern": "bar"}},
        ]
        assert _check_parameter_independence(tool_calls) is True

    def test_mixed_tools_are_independent(self):
        """Verify mixed tool types with different params are independent."""
        tool_calls = [
            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
            {"tool_name": "Grep", "parameters": {"pattern": "test"}},
            {"tool_name": "Glob", "parameters": {"pattern": "*.js"}},
        ]
        assert _check_parameter_independence(tool_calls) is True

    def test_single_call_is_independent(self):
        """Verify single tool call is independent."""
        tool_calls = [{"tool_name": "Read", "parameters": {"file_path": "/a.py"}}]
        assert _check_parameter_independence(tool_calls) is True

    def test_empty_list_is_independent(self):
        """Verify empty list is independent."""
        assert _check_parameter_independence([]) is True


class TestCouldBeParallel:
    """Test wasted sequencing detection."""

    def test_independent_calls_could_be_parallel(self):
        """Verify independent calls are flagged as parallelizable."""
        tool_calls = [
            {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
            {"tool_name": "Read", "parameters": {"file_path": "/b.py"}},
        ]
        assert _could_be_parallel(tool_calls) is True

    def test_dependent_calls_cannot_be_parallel(self):
        """Verify dependent calls are not flagged as parallelizable."""
        tool_calls = [
            {"tool_name": "Edit", "parameters": {"file_path": "/a.py"}},
            {"tool_name": "Edit", "parameters": {"file_path": "/a.py"}},
        ]
        assert _could_be_parallel(tool_calls) is False

    def test_single_call_cannot_be_parallel(self):
        """Verify single call is not flagged as parallelizable."""
        tool_calls = [{"tool_name": "Read", "parameters": {"file_path": "/a.py"}}]
        assert _could_be_parallel(tool_calls) is False


class TestHelperFunctions:
    """Test helper functions."""

    def test_percentage_calculation(self):
        """Verify percentage calculation."""
        assert _percentage(50, 100) == 50.0
        assert _percentage(1, 3) == 33.33
        assert _percentage(0, 100) == 0.0
        assert _percentage(100, 100) == 100.0

    def test_percentage_zero_denominator(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(50, 0) == 0.0
        assert _percentage(0, 0) == 0.0

    def test_average_calculation(self):
        """Verify average calculation."""
        assert _average([1, 2, 3, 4, 5]) == 3.0
        assert _average([10, 20]) == 15.0
        assert _average([100]) == 100.0

    def test_average_empty_list(self):
        """Verify empty list returns 0.0."""
        assert _average([]) == 0.0

    def test_calculate_correlation_negative(self):
        """Verify negative correlation calculation."""
        parallel = [100, 100]
        sequential = [200, 200]
        corr = _calculate_correlation(parallel, sequential)
        assert corr < 0  # Parallel uses fewer tokens

    def test_calculate_correlation_positive(self):
        """Verify positive correlation calculation."""
        parallel = [300, 300]
        sequential = [150, 150]
        corr = _calculate_correlation(parallel, sequential)
        assert corr > 0  # Parallel uses more tokens

    def test_calculate_correlation_zero(self):
        """Verify zero correlation when equal."""
        parallel = [100, 100]
        sequential = [100, 100]
        corr = _calculate_correlation(parallel, sequential)
        assert corr == 0.0

    def test_calculate_correlation_empty_lists(self):
        """Verify empty lists return 0.0."""
        assert _calculate_correlation([], []) == 0.0
        assert _calculate_correlation([100], []) == 0.0
        assert _calculate_correlation([], [100]) == 0.0
