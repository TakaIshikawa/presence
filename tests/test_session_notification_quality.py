"""Tests for session notification quality analyzer."""

from __future__ import annotations

from synthesis.session_notification_quality import analyze_session_notification_quality


def _msg(role: str, text: str, tool_calls: list | None = None) -> dict:
    m: dict = {"role": role, "text": text}
    if tool_calls is not None:
        m["tool_calls"] = tool_calls
    return m


def _session(messages: list[dict]) -> dict:
    return {"messages": messages}


def test_empty_records_returns_defaults():
    result = analyze_session_notification_quality([])
    assert result["total_sessions"] == 0
    assert result["sessions_analyzed"] == 0
    assert result["notification_quality_score"] == 1.0


def test_none_records_returns_defaults():
    result = analyze_session_notification_quality(None)
    assert result["total_sessions"] == 0


def test_invalid_records_raises():
    try:
        analyze_session_notification_quality("not a list")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass


def test_clear_ending_detected():
    session = _session([
        _msg("user", "Fix the bug"),
        _msg("assistant", "Working on it.", [{"tool": "Edit"}]),
        _msg("assistant", "All tests pass. The fix is ready."),
    ])
    result = analyze_session_notification_quality([session])

    assert result["sessions_with_clear_ending"] == 1
    assert result["clear_ending_rate"] == 1.0


def test_unclear_ending_detected():
    session = _session([
        _msg("user", "Fix the bug"),
        _msg("assistant", "Let me check.", [{"tool": "Read"}]),
    ])
    result = analyze_session_notification_quality([session])

    assert result["sessions_with_clear_ending"] == 0
    assert result["clear_ending_rate"] == 0.0


def test_error_explanation_detected():
    session = _session([
        _msg("user", "Build the project"),
        _msg("assistant", "The build failed with a TypeError in auth.py line 42. "
             "The issue is that the function expects a string but receives None. "
             "I'll fix this by adding a None check."),
        _msg("assistant", "All tests pass. Changes committed successfully."),
    ])
    result = analyze_session_notification_quality([session])

    assert result["sessions_with_error_explanation"] == 1
    assert result["error_explanation_rate"] == 1.0


def test_progress_signaling_in_long_session():
    messages = [_msg("user", "Implement the feature")]
    # Create a long session with 10+ tool calls
    for i in range(12):
        messages.append(
            _msg("assistant", f"Now working on step {i+1}.", [{"tool": "Edit"}])
        )
    messages.append(_msg("assistant", "Done. All tests pass."))

    result = analyze_session_notification_quality([_session(messages)])

    assert result["long_sessions_with_progress"] == 1
    assert result["progress_signaling_rate"] == 1.0


def test_no_progress_in_long_session():
    messages = [_msg("user", "Implement the feature")]
    for _ in range(12):
        messages.append(_msg("assistant", ".", [{"tool": "Edit"}]))
    messages.append(_msg("assistant", "Done."))

    result = analyze_session_notification_quality([_session(messages)])

    assert result["long_sessions_with_progress"] == 0


def test_actionable_outcome_detected():
    session = _session([
        _msg("user", "Create the config"),
        _msg("assistant", "Created `config.py` with the settings. "
             "Run `python config.py` to verify."),
    ])
    result = analyze_session_notification_quality([session])

    assert result["sessions_with_actionable_outcomes"] == 1
    assert result["actionable_outcome_rate"] == 1.0


def test_communication_tool_ratio():
    session = _session([
        _msg("user", "Fix it"),
        _msg("assistant", "Looking at the issue.", [{"tool": "Read"}, {"tool": "Read"}]),
        _msg("assistant", "Found the problem."),
        _msg("assistant", "Fixed it.", [{"tool": "Edit"}]),
        _msg("assistant", "All tests pass."),
    ])
    result = analyze_session_notification_quality([session])

    # 4 text messages from assistant, 3 tool calls
    assert abs(result["avg_communication_tool_ratio"] - 4 / 3) < 0.01


def test_quality_score_between_zero_and_one():
    session = _session([
        _msg("user", "Do something"),
        _msg("assistant", "Working on it.", [{"tool": "Bash"}]),
    ])
    result = analyze_session_notification_quality([session])

    assert 0.0 <= result["notification_quality_score"] <= 1.0


def test_multiple_sessions_aggregated():
    s1 = _session([
        _msg("user", "Task 1"),
        _msg("assistant", "All tests pass. Changes committed successfully."),
    ])
    s2 = _session([
        _msg("user", "Task 2"),
        _msg("assistant", "Trailing off..."),
    ])
    result = analyze_session_notification_quality([s1, s2])

    assert result["total_sessions"] == 2
    assert result["sessions_analyzed"] == 2
    assert result["sessions_with_clear_ending"] == 1
    assert result["clear_ending_rate"] == 0.5


def test_session_without_messages_not_analyzed():
    result = analyze_session_notification_quality([{"messages": []}])
    assert result["total_sessions"] == 1
    assert result["sessions_analyzed"] == 0
