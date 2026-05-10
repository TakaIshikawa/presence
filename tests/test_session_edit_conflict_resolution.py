"""Tests for session Edit conflict resolution and old_string retry strategy analyzer."""

import pytest

from synthesis.session_edit_conflict_resolution import SessionEditConflictResolutionAnalyzer


@pytest.fixture
def analyzer():
    return SessionEditConflictResolutionAnalyzer()


# --- Input validation ---


def test_none_input_returns_perfect(analyzer):
    result = analyzer.analyze(None)
    assert result["resolution_score"] == 1.0
    assert result["anti_patterns"] == []


def test_empty_list_returns_perfect(analyzer):
    result = analyzer.analyze([])
    assert result["resolution_score"] == 1.0


def test_non_list_input_raises(analyzer):
    with pytest.raises(ValueError, match="records must be a list"):
        analyzer.analyze({"messages": []})


# --- All edits succeed ---


def test_all_edits_succeed_perfect_score(analyzer):
    """Session with all edits succeeding → perfect score."""
    records = [
        {
            "session_id": "s1",
            "messages": [
                {
                    "tool_calls": [
                        {"tool_name": "Edit", "file_path": "/a.py", "old_string": "x", "success": True},
                        {"tool_name": "Edit", "file_path": "/b.py", "old_string": "y", "success": True},
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["edit_success_rate"] == 1.0
    assert result["resolution_score"] == 1.0
    assert result["anti_patterns"] == []


# --- Failure then Read then retry ---


def test_failure_read_retry_good_recovery(analyzer):
    """Failure → Read → successful retry → good recovery_rate."""
    records = [
        {
            "session_id": "s1",
            "messages": [
                {
                    "tool_calls": [
                        {"tool_name": "Edit", "file_path": "/a.py", "old_string": "old", "success": False},
                    ]
                },
                {
                    "tool_calls": [
                        {"tool_name": "Read", "file_path": "/a.py"},
                    ]
                },
                {
                    "tool_calls": [
                        {"tool_name": "Edit", "file_path": "/a.py", "old_string": "new_old", "success": True},
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["recovery_rate"] == 1.0
    assert result["read_before_retry_rate"] == 1.0
    assert "blind_retry" not in result["anti_patterns"]


# --- Blind retry anti-pattern ---


def test_blind_retry_without_read_detected(analyzer):
    """Retrying same failed Edit without reading → anti-pattern detected."""
    records = [
        {
            "session_id": "s1",
            "messages": [
                {
                    "tool_calls": [
                        {"tool_name": "Edit", "file_path": "/a.py", "old_string": "same", "success": False},
                    ]
                },
                {
                    "tool_calls": [
                        {"tool_name": "Edit", "file_path": "/a.py", "old_string": "same", "success": False},
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert "blind_retry" in result["anti_patterns"]


# --- Give up ---


def test_give_up_detected(analyzer):
    """Failure without recovery → give_up anti-pattern."""
    records = [
        {
            "session_id": "s1",
            "messages": [
                {
                    "tool_calls": [
                        {"tool_name": "Edit", "file_path": "/a.py", "old_string": "x", "success": False},
                    ]
                },
                {
                    "tool_calls": [
                        {"tool_name": "Bash", "command": "echo done"},
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert "give_up" in result["anti_patterns"]
    assert result["recovery_rate"] == 0.0


# --- Excessive retries ---


def test_excessive_retries_detected(analyzer):
    """More than 3 retries on same edit → excessive retry anti-pattern."""
    records = [
        {
            "session_id": "s1",
            "messages": [
                {"tool_calls": [{"tool_name": "Edit", "file_path": "/a.py", "old_string": "v1", "success": False}]},
                {"tool_calls": [{"tool_name": "Edit", "file_path": "/a.py", "old_string": "v2", "success": False}]},
                {"tool_calls": [{"tool_name": "Edit", "file_path": "/a.py", "old_string": "v3", "success": False}]},
                {"tool_calls": [{"tool_name": "Edit", "file_path": "/a.py", "old_string": "v4", "success": False}]},
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert "excessive_retries" in result["anti_patterns"]
    assert result["resolution_score"] < 1.0
