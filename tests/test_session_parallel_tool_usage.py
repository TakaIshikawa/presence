"""Tests for session parallel tool usage analyzer."""

import pytest

from synthesis.session_parallel_tool_usage import (
    _average,
    _percentage,
    analyze_session_parallel_tool_usage,
)


def test_empty_input_returns_zeroed_metrics():
    result = analyze_session_parallel_tool_usage([])

    assert result["total_turns"] == 0
    assert result["turns_with_tools"] == 0
    assert result["total_tool_calls"] == 0
    assert result["parallel_turns"] == 0
    assert result["parallel_usage_rate"] == 0.0
    assert result["total_parallel_batches"] == 0
    assert result["tool_parallelization"] == {}


def test_none_input_treated_as_empty_list():
    result = analyze_session_parallel_tool_usage(None)

    assert result["total_turns"] == 0


def test_invalid_input_type_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_parallel_tool_usage("not a list")


def test_single_tool_call_registers_no_parallelization():
    result = analyze_session_parallel_tool_usage([
        {"turn_index": 1, "tool_calls": [{"tool_name": "Read"}]},
    ])

    assert result["total_turns"] == 1
    assert result["turns_with_tools"] == 1
    assert result["total_tool_calls"] == 1
    assert result["parallel_turns"] == 0
    assert result["parallel_usage_rate"] == 0.0


def test_parallel_tool_calls_are_counted():
    result = analyze_session_parallel_tool_usage([
        {
            "turn_index": 1,
            "optimization_mode": "optimized",
            "tool_calls": [
                {"tool_name": "Read"},
                {"tool_name": "Read"},
                {"tool_name": "Grep"},
            ],
        }
    ])

    assert result["parallel_turns"] == 1
    assert result["parallel_usage_rate"] == 100.0
    assert result["total_parallel_batches"] == 1
    assert result["avg_parallel_batch_size"] == 3.0
    assert result["max_parallel_batch_size"] == 3
    assert result["tool_parallelization"] == {"Read": 1, "Grep": 1}
    assert result["mode_comparison"]["optimized"]["parallel_usage_rate"] == 100.0


def test_consecutive_different_single_tool_turns_count_as_missed_opportunity():
    result = analyze_session_parallel_tool_usage([
        {"turn_index": 1, "tool_calls": [{"tool_name": "Read"}]},
        {"turn_index": 2, "tool_calls": [{"tool_name": "Grep"}]},
    ])

    assert result["missed_opportunities"] == 1


def test_helper_functions():
    assert _percentage(2, 4) == 50.0
    assert _percentage(1, 0) == 0.0
    assert _average(6, 3) == 2.0
