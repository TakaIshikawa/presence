"""Tests for pack context window pressure and summarization trigger analyzer."""

import pytest

from synthesis.pack_context_window_pressure import PackContextWindowPressureAnalyzer


@pytest.fixture
def analyzer():
    return PackContextWindowPressureAnalyzer()


# --- Input validation ---


def test_none_input_returns_neutral(analyzer):
    result = analyzer.analyze(None)
    assert result["pressure_score"] == 1.0


def test_empty_list_returns_neutral(analyzer):
    result = analyzer.analyze([])
    assert result["pressure_score"] == 1.0


def test_non_list_input_raises(analyzer):
    with pytest.raises(ValueError, match="records must be a list"):
        analyzer.analyze({"sessions": []})


# --- Low pressure session ---


def test_low_pressure_session_high_score(analyzer):
    """Short session with targeted reads → high pressure_score."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {"tool_calls": [{"tool_name": "Read", "file_path": "/a.py", "limit": 30}]},
                        {"tool_calls": [{"tool_name": "Edit", "file_path": "/a.py"}]},
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["pressure_score"] > 0.8
    assert result["summarization_triggers"] == 0
    assert result["large_read_ratio"] == 0.0


# --- High pressure session ---


def test_high_pressure_session_low_score(analyzer):
    """Long session with many full-file reads → low pressure_score."""
    messages = [
        {"tool_calls": [{"tool_name": "Read", "file_path": f"/file_{i}.py"}]}
        for i in range(60)
    ]

    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": messages,
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["pressure_score"] < 0.3
    assert result["summarization_triggers"] >= 1
    assert result["large_read_ratio"] == 1.0


# --- Summarization trigger ---


def test_summarization_trigger_detected(analyzer):
    """Session with >50 messages triggers summarization detection."""
    messages = [{"tool_calls": []} for _ in range(55)]

    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {"session_id": "s1", "messages": messages},
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["summarization_triggers"] == 1
