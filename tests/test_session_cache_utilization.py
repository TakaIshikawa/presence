"""Tests for session cache utilization and snapshot discipline analyzer."""

import pytest

from synthesis.session_cache_utilization import SessionCacheUtilizationAnalyzer


@pytest.fixture
def analyzer():
    return SessionCacheUtilizationAnalyzer()


# --- Input validation ---


def test_none_input_returns_neutral(analyzer):
    result = analyzer.analyze(None)
    assert result["utilization_score"] == 1.0
    assert result["missed_opportunities"] == 0


def test_empty_list_returns_neutral(analyzer):
    result = analyzer.analyze([])
    assert result["utilization_score"] == 1.0


def test_non_list_input_raises(analyzer):
    with pytest.raises(ValueError, match="records must be a list"):
        analyzer.analyze({"messages": []})


# --- Baseline mode ---


def test_baseline_mode_returns_neutral(analyzer):
    """Baseline mode session → neutral score regardless of behavior."""
    records = [
        {
            "session_id": "s1",
            "optimization_mode": "baseline",
            "messages": [
                {
                    "tool_calls": [
                        {"tool_name": "Read", "file_path": "/a.py"},
                    ]
                },
                {
                    "tool_calls": [
                        {"tool_name": "Read", "file_path": "/a.py"},
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["utilization_score"] == 1.0
    assert result["mode"] == "baseline"


# --- Good cache usage ---


def test_optimized_session_with_no_rereads_perfect_score(analyzer):
    """Session with no re-reads → perfect score (no cache needed)."""
    records = [
        {
            "session_id": "s1",
            "optimization_mode": "optimized",
            "messages": [
                {
                    "tool_calls": [
                        {"tool_name": "Read", "file_path": "/a.py"},
                        {"tool_name": "Read", "file_path": "/b.py"},
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["utilization_score"] == 1.0
    assert result["missed_opportunities"] == 0
    assert result["mode"] == "optimized"


# --- Poor cache usage ---


def test_optimized_session_ignoring_cache_low_score(analyzer):
    """Optimized session with re-reads but no cache → low score."""
    records = [
        {
            "session_id": "s1",
            "optimization_mode": "optimized",
            "messages": [
                {
                    "tool_calls": [
                        {"tool_name": "Read", "file_path": "/a.py"},
                    ]
                },
                {
                    "tool_calls": [
                        {"tool_name": "Read", "file_path": "/a.py"},
                    ]
                },
                {
                    "tool_calls": [
                        {"tool_name": "Read", "file_path": "/a.py"},
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["utilization_score"] < 0.5
    assert result["missed_opportunities"] >= 2
    assert result["mode"] == "optimized"


# --- Mixed usage ---


def test_mixed_usage_patterns(analyzer):
    """Session with some cache queries before re-reads."""
    records = [
        {
            "session_id": "s1",
            "optimization_mode": "optimized",
            "messages": [
                {
                    "tool_calls": [
                        {"tool_name": "Read", "file_path": "/a.py"},
                    ]
                },
                {
                    "tool_calls": [
                        {"tool_name": "Bash", "command": "/cache query /a.py"},
                    ]
                },
                {
                    "tool_calls": [
                        {"tool_name": "Read", "file_path": "/a.py"},
                    ]
                },
            ],
        }
    ]

    result = analyzer.analyze(records)

    # The re-read of /a.py should be recognized as having cache query
    assert result["mode"] == "optimized"
    # Cache was queried before the re-read
    assert result["missed_opportunities"] == 0
