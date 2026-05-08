"""Tests for terminal session cleanup analysis."""

import pytest

from synthesis.terminal_session_cleanup import analyze_terminal_session_cleanup


def test_closed_sessions_are_counted_cleanly():
    report = analyze_terminal_session_cleanup(
        [
            {"session_id": "1", "tool": "terminal", "action": "started", "command": "npm run dev"},
            {"session_id": "1", "tool": "terminal", "action": "closed"},
        ]
    )

    assert report["opened_session_count"] == 1
    assert report["closed_session_count"] == 1
    assert report["still_running_session_count"] == 0


def test_still_running_sessions_are_reported():
    report = analyze_terminal_session_cleanup(
        [{"session_id": "1", "tool": "terminal", "status": "running", "command": "npm run dev"}]
    )

    assert report["still_running_session_count"] == 1
    assert report["unresolved_server_risk_count"] == 1
    assert report["examples"][0]["session_id"] == "1"


def test_server_url_mention_reduces_unresolved_risk_without_hiding_running_count():
    report = analyze_terminal_session_cleanup(
        [
            {"session_id": "1", "tool": "terminal", "status": "running", "command": "npm run dev"},
            {"final_answer": "Server is available at http://localhost:5173."},
        ]
    )

    assert report["still_running_session_count"] == 1
    assert report["final_answer_mentioned_sessions"] == 1
    assert report["unresolved_server_risk_count"] == 0


def test_multiple_sessions_use_deterministic_keys():
    report = analyze_terminal_session_cleanup(
        [
            {"tool": "terminal", "action": "started", "command": "python -m http.server"},
            {"session_id": "named", "tool": "terminal", "action": "started", "command": "vite"},
        ]
    )

    assert report["opened_session_count"] == 2
    assert [example["session_id"] for example in report["examples"]] == ["0", "named"]


def test_unknown_events_are_ignored():
    report = analyze_terminal_session_cleanup([{"tool": "notebook", "action": "note"}])

    assert report["opened_session_count"] == 0


def test_invalid_input_raises():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_terminal_session_cleanup({"tool": "terminal"})
