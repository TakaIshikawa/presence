"""Tests for session_read_efficiency_pattern analyzer."""

import pytest

from src.synthesis.session_read_efficiency_pattern import (
    analyze_session_read_efficiency_pattern,
)


def _turn(tool_name, tool_params=None, tool_result="", turn_index=0, **kwargs):
    return {
        "turn_index": turn_index,
        "tool_name": tool_name,
        "tool_params": tool_params or {},
        "tool_result": tool_result,
        "assistant_response": kwargs.get("assistant_response", ""),
        "is_error": False,
        "is_last_turn": False,
    }


class TestInputValidation:
    def test_none_returns_empty(self):
        result = analyze_session_read_efficiency_pattern(None)
        assert result["total_reads"] == 0
        assert result["read_efficiency_score"] == 1.0

    def test_empty_list_returns_empty(self):
        result = analyze_session_read_efficiency_pattern([])
        assert result["total_reads"] == 0
        assert result["read_efficiency_score"] == 1.0

    def test_non_list_raises(self):
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_read_efficiency_pattern("not a list")


class TestReadClassification:
    def test_targeted_read_with_offset_limit(self):
        records = [
            _turn("Read", {"file_path": "/a.py", "offset": 10, "limit": 50}),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        assert result["total_reads"] == 1
        assert result["targeted_reads"] == 1
        assert result["full_reads"] == 0
        assert result["targeted_read_ratio"] == 1.0

    def test_full_read_without_params(self):
        records = [
            _turn("Read", {"file_path": "/a.py"}),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        assert result["total_reads"] == 1
        assert result["targeted_reads"] == 0
        assert result["full_reads"] == 1
        assert result["targeted_read_ratio"] == 0.0

    def test_reread_detected(self):
        records = [
            _turn("Read", {"file_path": "/a.py"}, turn_index=0),
            _turn("Read", {"file_path": "/a.py"}, turn_index=1),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        assert result["total_reads"] == 2
        assert result["full_reads"] == 2
        assert result["re_reads"] == 1

    def test_targeted_read_with_limit_only(self):
        records = [
            _turn("Read", {"file_path": "/a.py", "limit": 30}),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        assert result["targeted_reads"] == 1
        assert result["avg_lines_per_read"] == 30.0


class TestPostEditReads:
    def test_targeted_post_edit_read(self):
        records = [
            _turn("Edit", {"file_path": "/a.py"}, turn_index=0),
            _turn("Read", {"file_path": "/a.py", "offset": 10, "limit": 30}, turn_index=1),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        assert result["post_edit_reads"] == 1
        assert result["targeted_post_edit_reads"] == 1
        assert result["full_post_edit_reads"] == 0

    def test_full_post_edit_read(self):
        records = [
            _turn("Write", {"file_path": "/a.py"}, turn_index=0),
            _turn("Read", {"file_path": "/a.py"}, turn_index=1),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        assert result["post_edit_reads"] == 1
        assert result["targeted_post_edit_reads"] == 0
        assert result["full_post_edit_reads"] == 1

    def test_read_beyond_2_turns_not_post_edit(self):
        records = [
            _turn("Edit", {"file_path": "/a.py"}, turn_index=0),
            _turn("Bash", {"command": "ls"}, turn_index=1),
            _turn("Bash", {"command": "pwd"}, turn_index=2),
            _turn("Read", {"file_path": "/a.py"}, turn_index=3),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        assert result["post_edit_reads"] == 0


class TestCacheBeforeRead:
    def test_cache_query_before_read_detected(self):
        records = [
            _turn("Bash", {"command": "/cache query /a.py"}, turn_index=0),
            _turn("Read", {"file_path": "/a.py", "offset": 1, "limit": 50}, turn_index=1),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        assert result["cache_before_read_count"] == 1


class TestScoring:
    def test_all_targeted_high_score(self):
        records = [
            _turn("Read", {"file_path": "/a.py", "offset": 0, "limit": 50}, turn_index=0),
            _turn("Read", {"file_path": "/b.py", "offset": 10, "limit": 30}, turn_index=1),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        # targeted_ratio=1.0 -> 0.4
        # no post-edit reads -> post_edit component = 0.3 * (0/1) = 0.0
        # no re-reads -> reread component = 0.3 * (1 - 0/2) = 0.3
        # total = 0.4 + 0.0 + 0.3 = 0.7
        assert result["read_efficiency_score"] == 0.7

    def test_all_full_reads_lower_score(self):
        records = [
            _turn("Read", {"file_path": "/a.py"}, turn_index=0),
            _turn("Read", {"file_path": "/b.py"}, turn_index=1),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        # targeted_ratio=0.0 -> 0.0
        # no post-edit reads -> 0.0
        # no re-reads -> 0.3
        # total = 0.3
        assert result["read_efficiency_score"] == 0.3

    def test_empty_perfect_score(self):
        result = analyze_session_read_efficiency_pattern([])
        assert result["read_efficiency_score"] == 1.0

    def test_mixed_with_rereads(self):
        records = [
            _turn("Read", {"file_path": "/a.py"}, turn_index=0),
            _turn("Read", {"file_path": "/a.py"}, turn_index=1),  # re-read
            _turn("Read", {"file_path": "/b.py", "limit": 50}, turn_index=2),
        ]
        result = analyze_session_read_efficiency_pattern(records)
        assert result["total_reads"] == 3
        assert result["targeted_reads"] == 1
        assert result["re_reads"] == 1
        # targeted_ratio = 1/3 ≈ 0.333
        assert result["targeted_read_ratio"] == 0.333
        # reread component = 0.3 * (1 - 1/3) = 0.2
        # score = 0.4*0.333 + 0 + 0.3*(1-1/3) = 0.133 + 0.2 = 0.333
        assert result["read_efficiency_score"] == 0.333
