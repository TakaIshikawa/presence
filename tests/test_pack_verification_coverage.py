"""Tests for pack_verification_coverage analyzer."""

from __future__ import annotations

import pytest

from src.synthesis.pack_verification_coverage import analyze_pack_verification_coverage


def _pack(messages):
    """Each message is a list of tool_call dicts."""
    return [{"sessions": [{"messages": [{"tool_calls": tc_list} for tc_list in messages]}]}]


class TestInputValidation:
    def test_none_returns_empty(self):
        result = analyze_pack_verification_coverage(None)
        assert result["total_messages"] == 0
        assert result["verification_coverage_score"] == 1.0

    def test_empty_list_returns_empty(self):
        result = analyze_pack_verification_coverage([])
        assert result["total_messages"] == 0
        assert result["verification_coverage_score"] == 1.0

    def test_non_list_raises(self):
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_verification_coverage("not a list")


class TestTurnClassification:
    def test_edit_is_implementation(self):
        pack = _pack([
            [{"tool_name": "Edit", "file_path": "foo.py", "old_string": "a", "new_string": "b"}],
        ])
        result = analyze_pack_verification_coverage(pack)
        assert result["implementation_turns"] == 1

    def test_pytest_bash_is_verification(self):
        pack = _pack([
            [{"tool_name": "Bash", "command": "pytest tests/"}],
        ])
        result = analyze_pack_verification_coverage(pack)
        assert result["verification_turns"] == 1

    def test_npm_test_is_verification(self):
        pack = _pack([
            [{"tool_name": "Bash", "command": "npm test"}],
        ])
        result = analyze_pack_verification_coverage(pack)
        assert result["verification_turns"] == 1


class TestUnverifiedEdits:
    def test_edit_without_verification_within_5_turns(self):
        # Edit followed by 6 non-verification messages -> unverified
        messages = [
            [{"tool_name": "Edit", "file_path": "a.py", "old_string": "x", "new_string": "y"}],
        ] + [
            [{"tool_name": "Read", "file_path": "b.py"}] for _ in range(6)
        ]
        pack = _pack(messages)
        result = analyze_pack_verification_coverage(pack)
        assert result["unverified_edits"] == 1

    def test_edit_followed_by_test_not_unverified(self):
        messages = [
            [{"tool_name": "Edit", "file_path": "a.py", "old_string": "x", "new_string": "y"}],
            [{"tool_name": "Bash", "command": "pytest"}],
        ]
        pack = _pack(messages)
        result = analyze_pack_verification_coverage(pack)
        assert result["unverified_edits"] == 0


class TestLateVerification:
    def test_all_tests_at_end_flagged(self):
        # 10 messages: 8 edits, then 2 verification at end (last 20%)
        messages = [
            [{"tool_name": "Edit", "file_path": "a.py", "old_string": "x", "new_string": "y"}]
            for _ in range(8)
        ] + [
            [{"tool_name": "Bash", "command": "pytest"}],
            [{"tool_name": "Bash", "command": "npm run build"}],
        ]
        pack = _pack(messages)
        result = analyze_pack_verification_coverage(pack)
        assert result["late_verification"] is True

    def test_tests_distributed_not_flagged(self):
        # Verification interspersed throughout
        messages = [
            [{"tool_name": "Edit", "file_path": "a.py", "old_string": "x", "new_string": "y"}],
            [{"tool_name": "Bash", "command": "pytest"}],
            [{"tool_name": "Edit", "file_path": "b.py", "old_string": "x", "new_string": "y"}],
            [{"tool_name": "Bash", "command": "pytest"}],
            [{"tool_name": "Edit", "file_path": "c.py", "old_string": "x", "new_string": "y"}],
            [{"tool_name": "Bash", "command": "pytest"}],
        ]
        pack = _pack(messages)
        result = analyze_pack_verification_coverage(pack)
        assert result["late_verification"] is False


class TestTestSpecificity:
    def test_targeted_test_detected(self):
        pack = _pack([
            [{"tool_name": "Bash", "command": "pytest tests/test_foo.py"}],
        ])
        result = analyze_pack_verification_coverage(pack)
        assert result["targeted_tests"] == 1
        assert result["broad_tests"] == 0

    def test_broad_test_detected(self):
        pack = _pack([
            [{"tool_name": "Bash", "command": "pytest"}],
        ])
        result = analyze_pack_verification_coverage(pack)
        assert result["broad_tests"] == 1
        assert result["targeted_tests"] == 0


class TestScoring:
    def test_balanced_verification_good_score(self):
        # 3 edits each followed by targeted test -> good score
        messages = []
        for _ in range(3):
            messages.append(
                [{"tool_name": "Edit", "file_path": "a.py", "old_string": "x", "new_string": "y"}]
            )
            messages.append(
                [{"tool_name": "Bash", "command": "pytest tests/test_a.py"}]
            )
        pack = _pack(messages)
        result = analyze_pack_verification_coverage(pack)
        assert result["verification_coverage_score"] >= 0.8

    def test_no_verification_low_score(self):
        # Only edits, no verification
        messages = [
            [{"tool_name": "Edit", "file_path": "a.py", "old_string": "x", "new_string": "y"}]
            for _ in range(5)
        ]
        pack = _pack(messages)
        result = analyze_pack_verification_coverage(pack)
        assert result["verification_coverage_score"] < 0.5

    def test_empty_perfect_score(self):
        result = analyze_pack_verification_coverage([])
        assert result["verification_coverage_score"] == 1.0
