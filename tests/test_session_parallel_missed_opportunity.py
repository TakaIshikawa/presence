"""Tests for session parallel tool call missed opportunity analyzer."""

import pytest

from src.synthesis.session_parallel_missed_opportunity import (
    analyze_session_parallel_missed_opportunity,
)


def _turn(tool_name, tool_params=None, tool_result="", turn_index=0, **kwargs):
    """Helper to build a turn dict."""
    return {
        "turn_index": turn_index,
        "tool_name": tool_name,
        "tool_params": tool_params or {},
        "tool_result": tool_result,
        "assistant_response": kwargs.get("assistant_response", ""),
        "is_error": kwargs.get("is_error", False),
        "is_last_turn": kwargs.get("is_last_turn", False),
    }


class TestInputValidation:
    def test_none_input_returns_empty_result(self):
        result = analyze_session_parallel_missed_opportunity(None)
        assert result["total_turns"] == 0
        assert result["total_missed_opportunities"] == 0
        assert result["parallel_efficiency_score"] == 1.0

    def test_empty_list_returns_empty_result(self):
        result = analyze_session_parallel_missed_opportunity([])
        assert result["total_turns"] == 0
        assert result["total_missed_opportunities"] == 0
        assert result["parallel_efficiency_score"] == 1.0

    def test_non_list_raises_value_error(self):
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_parallel_missed_opportunity("not a list")

    def test_non_dict_records_skipped(self):
        result = analyze_session_parallel_missed_opportunity([42, "bad", None])
        assert result["total_turns"] == 0


class TestSequentialIndependentDetection:
    def test_two_reads_different_files_detected(self):
        turns = [
            _turn("Read", {"file_path": "/a.py"}, turn_index=0),
            _turn("Read", {"file_path": "/b.py"}, turn_index=1),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["sequential_independent_pairs"] == 1
        assert result["total_missed_opportunities"] >= 1

    def test_read_then_edit_same_file_not_detected(self):
        turns = [
            _turn("Read", {"file_path": "/a.py"}, turn_index=0),
            _turn("Edit", {"file_path": "/a.py"}, turn_index=1),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["sequential_independent_pairs"] == 0

    def test_grep_then_read_result_file_not_detected(self):
        """Grep returns a file path, then Read uses that path — dependent."""
        turns = [
            _turn("Grep", {"pattern": "def foo", "path": "/src"}, tool_result="/src/bar.py:10: def foo():", turn_index=0),
            _turn("Read", {"file_path": "/src/bar.py"}, turn_index=1),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["sequential_independent_pairs"] == 0

    def test_grep_then_grep_different_patterns_detected(self):
        turns = [
            _turn("Grep", {"pattern": "import os", "path": "/src"}, turn_index=0),
            _turn("Grep", {"pattern": "import sys", "path": "/lib"}, turn_index=1),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["sequential_independent_pairs"] == 1

    def test_glob_then_grep_detected(self):
        turns = [
            _turn("Glob", {"pattern": "*.py", "path": "/src"}, turn_index=0),
            _turn("Grep", {"pattern": "TODO", "path": "/tests"}, turn_index=1),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["sequential_independent_pairs"] == 1

    def test_bash_state_modifiers_not_detected(self):
        """Bash calls with state-modifying commands are not independent."""
        turns = [
            _turn("Bash", {"command": "git add -A"}, turn_index=0),
            _turn("Bash", {"command": "git commit -m 'fix'"}, turn_index=1),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["sequential_independent_pairs"] == 0


class TestRepeatedSingleToolSequences:
    def test_three_consecutive_reads_detected(self):
        turns = [
            _turn("Read", {"file_path": "/a.py"}, turn_index=0),
            _turn("Read", {"file_path": "/b.py"}, turn_index=1),
            _turn("Read", {"file_path": "/c.py"}, turn_index=2),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["repeated_single_tool_sequences"] == 1

    def test_two_consecutive_reads_not_detected(self):
        """Need 3+ for repeated sequence detection."""
        turns = [
            _turn("Read", {"file_path": "/a.py"}, turn_index=0),
            _turn("Read", {"file_path": "/b.py"}, turn_index=1),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["repeated_single_tool_sequences"] == 0

    def test_four_consecutive_greps_detected(self):
        turns = [
            _turn("Grep", {"pattern": f"pattern_{i}", "path": f"/dir{i}"}, turn_index=i)
            for i in range(4)
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["repeated_single_tool_sequences"] == 1

    def test_same_file_reads_not_detected_as_repeated(self):
        """Repeated reads to same file are dependent, not a parallelization miss."""
        turns = [
            _turn("Read", {"file_path": "/a.py"}, turn_index=0),
            _turn("Read", {"file_path": "/a.py"}, turn_index=1),
            _turn("Read", {"file_path": "/a.py"}, turn_index=2),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["repeated_single_tool_sequences"] == 0


class TestIndependentTaskLaunches:
    def test_two_unrelated_task_launches_detected(self):
        turns = [
            _turn("Task", {"prompt": "Search for auth code", "description": "find auth"}, turn_index=0),
            _turn("Task", {"prompt": "Run the test suite", "description": "run tests"}, turn_index=1),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["independent_task_launches"] == 1

    def test_task_depending_on_previous_result_not_detected(self):
        turns = [
            _turn(
                "Task",
                {"prompt": "Find the config file", "description": "find config"},
                tool_result="/src/config.py found",
                turn_index=0,
            ),
            _turn(
                "Task",
                {"prompt": "Read /src/config.py and summarize", "description": "read config", "file_path": "/src/config.py"},
                turn_index=1,
            ),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["independent_task_launches"] == 0


class TestScoring:
    def test_perfect_parallelization_score_1(self):
        """Single turn or no missed opportunities → score 1.0."""
        turns = [_turn("Read", {"file_path": "/a.py"}, turn_index=0)]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["parallel_efficiency_score"] == 1.0
        assert result["parallelizable_turn_ratio"] == 0.0

    def test_all_sequential_low_score(self):
        """Many missed opportunities → low score."""
        turns = [
            _turn("Read", {"file_path": f"/{chr(97+i)}.py"}, turn_index=i)
            for i in range(6)
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert result["parallel_efficiency_score"] < 0.5
        assert result["parallelizable_turn_ratio"] > 0.5
        assert result["total_missed_opportunities"] > 0

    def test_score_clamped_to_0_1(self):
        """Scores must be in [0.0, 1.0]."""
        turns = [
            _turn("Read", {"file_path": f"/{i}.py"}, turn_index=i)
            for i in range(20)
        ]
        result = analyze_session_parallel_missed_opportunity(turns)
        assert 0.0 <= result["parallel_efficiency_score"] <= 1.0
        assert 0.0 <= result["parallelizable_turn_ratio"] <= 1.0


class TestIntegration:
    def test_realistic_session_mixed_parallel_and_sequential(self):
        """Simulate a realistic session with both parallel-eligible and dependent turns."""
        turns = [
            # Independent reads (missed opportunity)
            _turn("Read", {"file_path": "/src/main.py"}, turn_index=0),
            _turn("Read", {"file_path": "/src/utils.py"}, turn_index=1),
            # Dependent: read then edit same file
            _turn("Read", {"file_path": "/src/config.py"}, turn_index=2),
            _turn("Edit", {"file_path": "/src/config.py"}, turn_index=3),
            # Independent greps (missed opportunity)
            _turn("Grep", {"pattern": "TODO", "path": "/src"}, turn_index=4),
            _turn("Grep", {"pattern": "FIXME", "path": "/tests"}, turn_index=5),
            # Dependent bash sequence
            _turn("Bash", {"command": "git add -A"}, turn_index=6),
            _turn("Bash", {"command": "git commit -m 'fix'"}, turn_index=7),
        ]
        result = analyze_session_parallel_missed_opportunity(turns)

        assert result["total_turns"] == 8
        # Turns 0-1 and 4-5 are independent pairs
        assert result["sequential_independent_pairs"] >= 2
        # Turns 2-3 (Read→Edit same file) and 6-7 (git state mods) should NOT be counted
        assert result["total_missed_opportunities"] >= 2
        assert 0.0 < result["parallel_efficiency_score"] < 1.0

    def test_all_return_keys_present(self):
        """Verify all expected keys are in the result."""
        result = analyze_session_parallel_missed_opportunity([])
        expected_keys = {
            "total_turns",
            "sequential_independent_pairs",
            "repeated_single_tool_sequences",
            "independent_task_launches",
            "total_missed_opportunities",
            "parallelizable_turn_ratio",
            "parallel_efficiency_score",
        }
        assert set(result.keys()) == expected_keys
