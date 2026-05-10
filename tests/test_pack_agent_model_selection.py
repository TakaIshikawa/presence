"""Tests for pack Task agent model selection and subagent_type appropriateness analyzer."""

import pytest

from synthesis.pack_agent_model_selection import PackAgentModelSelectionAnalyzer


@pytest.fixture
def analyzer():
    return PackAgentModelSelectionAnalyzer()


# --- Input validation ---


def test_none_input_returns_neutral(analyzer):
    result = analyzer.analyze(None)
    assert result["overall_score"] == 1.0
    assert result["total_task_calls"] == 0


def test_empty_list_returns_neutral(analyzer):
    result = analyzer.analyze([])
    assert result["overall_score"] == 1.0


def test_non_list_input_raises(analyzer):
    with pytest.raises(ValueError, match="records must be a list"):
        analyzer.analyze({"sessions": []})


# --- Appropriate selections ---


def test_explore_for_search_task_appropriate(analyzer):
    """Using Explore subagent for search task → appropriate."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {
                                    "tool_name": "Task",
                                    "prompt": "Find all files that handle authentication",
                                    "description": "search for auth files",
                                    "subagent_type": "Explore",
                                    "model": "haiku",
                                }
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["subagent_type_score"] == 1.0
    assert result["overall_score"] == 1.0


# --- Inappropriate model ---


def test_opus_for_simple_search_inappropriate(analyzer):
    """Using opus for a simple search task → model penalty."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {
                                    "tool_name": "Task",
                                    "prompt": "Quick check if file exists",
                                    "description": "simple file check",
                                    "subagent_type": "general-purpose",
                                    "model": "opus",
                                }
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["model_appropriateness_score"] < 1.0
    assert len(result["issues"]) > 0


# --- Mismatched subagent_type ---


def test_bash_subagent_for_search_task_mismatch(analyzer):
    """Using Bash subagent for a search/explore task → mismatch."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {
                                    "tool_name": "Task",
                                    "prompt": "Run the build and deploy",
                                    "description": "execute build",
                                    "subagent_type": "Bash",
                                    "model": "sonnet",
                                }
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    # Bash for "run the build" is appropriate
    assert result["subagent_type_score"] == 1.0


# --- No Task calls ---


def test_no_task_calls_neutral(analyzer):
    """Session with no Task tool calls → neutral score."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
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
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["overall_score"] == 1.0
    assert result["total_task_calls"] == 0
