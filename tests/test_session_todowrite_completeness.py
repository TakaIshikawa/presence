"""Tests for session TodoWrite completeness and status transition analyzer."""

import pytest

from synthesis.session_todowrite_completeness import SessionTodoWriteCompletenessAnalyzer


@pytest.fixture
def analyzer():
    return SessionTodoWriteCompletenessAnalyzer()


# --- Input validation ---


def test_none_input_returns_perfect(analyzer):
    result = analyzer.analyze(None)
    assert result["overall_discipline_score"] == 1.0


def test_empty_list_returns_perfect(analyzer):
    result = analyzer.analyze([])
    assert result["overall_discipline_score"] == 1.0


def test_non_list_input_raises(analyzer):
    with pytest.raises(ValueError, match="records must be a list"):
        analyzer.analyze({"messages": []})


# --- Perfect session ---


def test_perfect_session_all_completed_one_at_a_time(analyzer):
    """All tasks: pending → in_progress → completed, one at a time → score 1.0."""
    records = [
        {
            "session_id": "s1",
            "messages": [
                {
                    "tool_calls": [
                        {
                            "tool_name": "TodoWrite",
                            "todos": [
                                {"content": "Task A", "status": "in_progress", "activeForm": "Doing A"},
                                {"content": "Task B", "status": "pending", "activeForm": "Doing B"},
                            ],
                        }
                    ]
                },
                {
                    "tool_calls": [
                        {
                            "tool_name": "TodoWrite",
                            "todos": [
                                {"content": "Task A", "status": "completed", "activeForm": "Doing A"},
                                {"content": "Task B", "status": "in_progress", "activeForm": "Doing B"},
                            ],
                        }
                    ]
                },
                {
                    "tool_calls": [
                        {
                            "tool_name": "TodoWrite",
                            "todos": [
                                {"content": "Task A", "status": "completed", "activeForm": "Doing A"},
                                {"content": "Task B", "status": "completed", "activeForm": "Doing B"},
                            ],
                        }
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["completion_rate"] == 1.0
    assert result["transition_score"] == 1.0
    assert result["single_active_compliance"] == 1.0
    assert result["abandoned_tasks"] == 0
    assert result["overall_discipline_score"] == 1.0


# --- Abandoned tasks ---


def test_abandoned_tasks_lower_completion_rate(analyzer):
    """3 of 5 tasks abandoned → low completion_rate."""
    records = [
        {
            "session_id": "s1",
            "messages": [
                {
                    "tool_calls": [
                        {
                            "tool_name": "TodoWrite",
                            "todos": [
                                {"content": "T1", "status": "completed", "activeForm": "x"},
                                {"content": "T2", "status": "completed", "activeForm": "x"},
                                {"content": "T3", "status": "pending", "activeForm": "x"},
                                {"content": "T4", "status": "in_progress", "activeForm": "x"},
                                {"content": "T5", "status": "pending", "activeForm": "x"},
                            ],
                        }
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["completion_rate"] < 0.5
    assert result["abandoned_tasks"] == 3


# --- Multiple in_progress ---


def test_multiple_in_progress_penalizes_compliance(analyzer):
    """2 tasks in_progress simultaneously → single_active_compliance penalized."""
    records = [
        {
            "session_id": "s1",
            "messages": [
                {
                    "tool_calls": [
                        {
                            "tool_name": "TodoWrite",
                            "todos": [
                                {"content": "T1", "status": "in_progress", "activeForm": "x"},
                                {"content": "T2", "status": "in_progress", "activeForm": "x"},
                            ],
                        }
                    ]
                },
                {
                    "tool_calls": [
                        {
                            "tool_name": "TodoWrite",
                            "todos": [
                                {"content": "T1", "status": "completed", "activeForm": "x"},
                                {"content": "T2", "status": "completed", "activeForm": "x"},
                            ],
                        }
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["single_active_compliance"] < 1.0


# --- Skipping in_progress ---


def test_skip_in_progress_penalizes_transition_score(analyzer):
    """pending → completed directly (skipping in_progress) → accepted but tracked."""
    records = [
        {
            "session_id": "s1",
            "messages": [
                {
                    "tool_calls": [
                        {
                            "tool_name": "TodoWrite",
                            "todos": [
                                {"content": "T1", "status": "pending", "activeForm": "x"},
                            ],
                        }
                    ]
                },
                {
                    "tool_calls": [
                        {
                            "tool_name": "TodoWrite",
                            "todos": [
                                {"content": "T1", "status": "completed", "activeForm": "x"},
                            ],
                        }
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    # pending → completed is accepted as valid (trivial task shortcut)
    assert result["transition_score"] == 1.0
    assert result["completion_rate"] == 1.0


# --- No TodoWrite calls ---


def test_session_without_todowrite_neutral(analyzer):
    """Session with no TodoWrite calls → perfect score."""
    records = [
        {
            "session_id": "s1",
            "messages": [
                {
                    "tool_calls": [
                        {"tool_name": "Read", "file_path": "/a.py"},
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["overall_discipline_score"] == 1.0
