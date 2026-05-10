"""Tests for pack model cost attribution analyzer."""

from __future__ import annotations

import pytest

from src.synthesis.pack_model_cost_attribution import analyze_pack_model_cost_attribution


def _pack(tool_calls):
    """Build a minimal pack record list from a flat list of tool calls."""
    return [{"sessions": [{"messages": [{"tool_calls": tool_calls}]}]}]


def _multi_session_pack(session_tool_calls_list):
    """Build a pack with multiple sessions, each with its own tool calls."""
    sessions = [
        {"messages": [{"tool_calls": tcs}]} for tcs in session_tool_calls_list
    ]
    return [{"sessions": sessions}]


class TestInputValidation:
    def test_none_returns_empty(self):
        result = analyze_pack_model_cost_attribution(None)
        assert result["total_tool_calls"] == 0
        assert result["cost_efficiency_score"] == 1.0
        assert result["inefficiencies"] == []

    def test_empty_list_returns_empty(self):
        result = analyze_pack_model_cost_attribution([])
        assert result["total_tool_calls"] == 0
        assert result["cost_efficiency_score"] == 1.0

    def test_non_list_raises(self):
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_model_cost_attribution("not a list")


class TestStageClassification:
    def test_read_classified_as_exploration(self):
        records = _pack([{"tool_name": "Read", "file_path": "foo.py"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["stage_counts"]["exploration"] == 1

    def test_edit_classified_as_implementation(self):
        records = _pack([{"tool_name": "Edit", "file_path": "foo.py", "old_string": "a", "new_string": "b"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["stage_counts"]["implementation"] == 1

    def test_bash_test_classified_as_verification(self):
        records = _pack([{"tool_name": "Bash", "command": "pytest tests/"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["stage_counts"]["verification"] == 1

    def test_askuserquestion_classified_as_communication(self):
        records = _pack([{"tool_name": "AskUserQuestion", "question": "ok?"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["stage_counts"]["communication"] == 1

    def test_glob_classified_as_exploration(self):
        records = _pack([{"tool_name": "Glob", "pattern": "*.py"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["stage_counts"]["exploration"] == 1

    def test_grep_classified_as_exploration(self):
        records = _pack([{"tool_name": "Grep", "pattern": "foo"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["stage_counts"]["exploration"] == 1

    def test_write_classified_as_implementation(self):
        records = _pack([{"tool_name": "Write", "file_path": "f.py", "content": "x"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["stage_counts"]["implementation"] == 1

    def test_task_explore_classified_as_exploration(self):
        records = _pack([{"tool_name": "Task", "subagent_type": "Explore", "prompt": "find files"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["stage_counts"]["exploration"] == 1

    def test_bash_build_classified_as_verification(self):
        records = _pack([{"tool_name": "Bash", "command": "npm run build"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["stage_counts"]["verification"] == 1


class TestModelDetection:
    def test_opus_model_detected(self):
        records = _pack([{"tool_name": "Task", "model": "opus", "prompt": "analyze"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["model_counts"]["opus"] == 1

    def test_default_model_when_not_specified(self):
        records = _pack([{"tool_name": "Read", "file_path": "f.py"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["model_counts"]["default"] == 1

    def test_sonnet_model_detected(self):
        records = _pack([{"tool_name": "Task", "model": "sonnet", "prompt": "x"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["model_counts"]["sonnet"] == 1

    def test_haiku_model_detected(self):
        records = _pack([{"tool_name": "Task", "model": "haiku", "prompt": "x"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["model_counts"]["haiku"] == 1


class TestCostProxy:
    def test_opus_weighted_3x(self):
        records = _pack([{"tool_name": "Task", "model": "opus", "prompt": "do stuff"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["total_cost_proxy"] == 3.0

    def test_haiku_weighted_025x(self):
        records = _pack([{"tool_name": "Task", "model": "haiku", "prompt": "do stuff"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["total_cost_proxy"] == 0.25

    def test_sonnet_weighted_1x(self):
        records = _pack([{"tool_name": "Task", "model": "sonnet", "prompt": "x"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["total_cost_proxy"] == 1.0

    def test_default_weighted_1x(self):
        records = _pack([{"tool_name": "Read", "file_path": "f.py"}])
        result = analyze_pack_model_cost_attribution(records)
        assert result["total_cost_proxy"] == 1.0

    def test_stage_cost_proxy_accumulated(self):
        records = _pack([
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Read", "file_path": "b.py"},
            {"tool_name": "Edit", "file_path": "a.py", "old_string": "x", "new_string": "y"},
        ])
        result = analyze_pack_model_cost_attribution(records)
        assert result["stage_cost_proxy"]["exploration"] == 2.0
        assert result["stage_cost_proxy"]["implementation"] == 1.0
        assert result["total_cost_proxy"] == 3.0


class TestInefficiencies:
    def test_opus_for_exploration_flagged(self):
        records = _pack([
            {"tool_name": "Task", "model": "opus", "subagent_type": "Explore", "prompt": "find files"},
        ])
        result = analyze_pack_model_cost_attribution(records)
        assert len(result["inefficiencies"]) >= 1
        assert any("Opus used for exploration" in i for i in result["inefficiencies"])

    def test_redundant_reads_detected(self):
        records = _multi_session_pack([
            [{"tool_name": "Read", "file_path": "shared.py"}],
            [{"tool_name": "Read", "file_path": "shared.py"}],
        ])
        result = analyze_pack_model_cost_attribution(records)
        assert any("Redundant read" in i for i in result["inefficiencies"])

    def test_no_redundant_read_within_same_session(self):
        records = _pack([
            {"tool_name": "Read", "file_path": "f.py"},
            {"tool_name": "Read", "file_path": "f.py"},
        ])
        result = analyze_pack_model_cost_attribution(records)
        assert not any("Redundant read" in i for i in result["inefficiencies"])

    def test_inefficiencies_capped_at_10(self):
        # Create 15 opus exploration calls
        tool_calls = [
            {"tool_name": "Task", "model": "opus", "subagent_type": "Explore", "prompt": f"task {i}"}
            for i in range(15)
        ]
        records = _pack(tool_calls)
        result = analyze_pack_model_cost_attribution(records)
        assert len(result["inefficiencies"]) == 10


class TestScoring:
    def test_no_inefficiencies_perfect_score(self):
        records = _pack([
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Edit", "file_path": "a.py", "old_string": "x", "new_string": "y"},
        ])
        result = analyze_pack_model_cost_attribution(records)
        assert result["cost_efficiency_score"] == 1.0

    def test_inefficiencies_reduce_score(self):
        records = _pack([
            {"tool_name": "Task", "model": "opus", "subagent_type": "Explore", "prompt": "find"},
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Read", "file_path": "b.py"},
            {"tool_name": "Edit", "file_path": "a.py", "old_string": "x", "new_string": "y"},
        ])
        result = analyze_pack_model_cost_attribution(records)
        assert result["cost_efficiency_score"] < 1.0
        assert result["cost_efficiency_score"] >= 0.0

    def test_score_clamped_to_zero(self):
        # All opus exploration = all inefficient, many inefficiencies relative to calls
        tool_calls = [
            {"tool_name": "Task", "model": "opus", "subagent_type": "Explore", "prompt": f"t{i}"}
            for i in range(2)
        ]
        records = _pack(tool_calls)
        result = analyze_pack_model_cost_attribution(records)
        # 2 inefficiencies / 2 calls = 1.0 penalty -> score 0.0
        assert result["cost_efficiency_score"] == 0.0

    def test_empty_returns_perfect_score(self):
        result = analyze_pack_model_cost_attribution([])
        assert result["cost_efficiency_score"] == 1.0


class TestEdgeCases:
    def test_non_dict_records_skipped(self):
        result = analyze_pack_model_cost_attribution(["not a dict", 42])
        assert result["total_tool_calls"] == 0

    def test_missing_sessions_key(self):
        result = analyze_pack_model_cost_attribution([{"other": "data"}])
        assert result["total_tool_calls"] == 0

    def test_non_dict_tool_calls_skipped(self):
        records = _pack(["not a dict", 42])
        result = analyze_pack_model_cost_attribution(records)
        assert result["total_tool_calls"] == 0
