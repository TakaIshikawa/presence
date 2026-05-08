"""Tests for session tool result acknowledgement analyzer."""

import pytest

from synthesis.session_tool_result_acknowledgement import (
    AgentResponse,
    ToolCall,
    analyze_session_tool_result_acknowledgement,
)


def test_empty_input_returns_stable_zero_state():
    result = analyze_session_tool_result_acknowledgement([], [])

    assert result.metrics.total_tool_calls == 0
    assert result.metrics.acknowledged_calls == 0
    assert result.metrics.silent_drops == 0
    assert result.metrics.repeated_calls == 0
    assert result.metrics.acknowledgement_rate == 0.0
    assert "No tool calls" in result.insights[0]


def test_tool_call_with_acknowledgement():
    tool_calls = [
        ToolCall(
            turn_index=1,
            tool_type="read",
            tool_args={"file_path": "test.py"},
            result_summary="File contains 100 lines",
        )
    ]
    agent_responses = [
        AgentResponse(
            turn_index=2,
            content="The file contains 100 lines. Let me analyze it.",
        )
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert result.metrics.total_tool_calls == 1
    assert result.metrics.acknowledged_calls == 1
    assert result.metrics.silent_drops == 0
    assert result.metrics.acknowledgement_rate == 100.0


def test_tool_call_without_acknowledgement():
    tool_calls = [
        ToolCall(
            turn_index=1,
            tool_type="grep",
            tool_args={"pattern": "error"},
            result_summary="Found 5 matches",
        )
    ]
    agent_responses = [
        AgentResponse(
            turn_index=2,
            content="Let me continue with the next step.",
        )
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert result.metrics.total_tool_calls == 1
    assert result.metrics.acknowledged_calls == 0
    assert result.metrics.silent_drops == 1
    assert result.metrics.acknowledgement_rate == 0.0


def test_tool_call_with_no_following_agent_response():
    tool_calls = [
        ToolCall(
            turn_index=5,
            tool_type="bash",
            tool_args={"command": "pytest"},
            result_summary="All tests passed",
        )
    ]
    agent_responses = [
        AgentResponse(turn_index=3, content="Running tests now.")
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert result.metrics.total_tool_calls == 1
    assert result.metrics.acknowledged_calls == 0
    assert result.metrics.silent_drops == 1
    assert len(result.examples) == 1
    assert result.examples[0].agent_response_turn is None


def test_repeated_identical_tool_calls():
    tool_calls = [
        ToolCall(1, "read", {"file_path": "test.py"}, "File contents"),
        ToolCall(3, "read", {"file_path": "test.py"}, "File contents"),  # Repeated
    ]
    agent_responses = [
        AgentResponse(2, "I see the file."),
        AgentResponse(4, "Let me read it again."),
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert result.metrics.total_tool_calls == 2
    assert result.metrics.repeated_calls == 1
    assert result.metrics.repeated_call_rate == 50.0


def test_repeated_calls_must_be_within_recent_window():
    tool_calls = [
        ToolCall(1, "read", {"file_path": "test.py"}, "File contents"),
        ToolCall(10, "read", {"file_path": "test.py"}, "File contents"),  # Too far
    ]
    agent_responses = [
        AgentResponse(2, "I see the read result."),
        AgentResponse(11, "I see another read result."),
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    # Should not be considered repeated due to distance
    assert result.metrics.repeated_calls == 0


def test_multiple_tool_calls_mixed_acknowledgement():
    tool_calls = [
        ToolCall(1, "read", {"file_path": "a.py"}, "Content A"),
        ToolCall(3, "grep", {"pattern": "error"}, "Found errors"),
        ToolCall(5, "bash", {"command": "test"}, "All passed"),
    ]
    agent_responses = [
        AgentResponse(2, "The file content shows X."),  # Acknowledges read
        AgentResponse(4, "Moving forward now."),  # Ignores grep
        AgentResponse(6, "Tests passed successfully."),  # Acknowledges bash
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert result.metrics.total_tool_calls == 3
    assert result.metrics.acknowledged_calls == 2
    assert result.metrics.silent_drops == 1
    assert result.metrics.acknowledgement_rate == pytest.approx(66.67, rel=0.01)


def test_by_tool_type_metrics():
    tool_calls = [
        ToolCall(1, "read", {"file_path": "a.py"}, "Content"),
        ToolCall(3, "read", {"file_path": "b.py"}, "Content"),
        ToolCall(5, "bash", {"command": "test"}, "Passed"),
    ]
    agent_responses = [
        AgentResponse(2, "I see the read result."),
        AgentResponse(4, "Ignoring this."),
        AgentResponse(6, "Tests look good."),
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert "read" in result.metrics.by_tool_type
    assert "bash" in result.metrics.by_tool_type
    assert result.metrics.by_tool_type["read"]["total"] == 2
    assert result.metrics.by_tool_type["bash"]["total"] == 1


def test_acknowledgement_with_tool_type_mention():
    tool_calls = [
        ToolCall(1, "grep", {"pattern": "TODO"}, "Found 10 TODOs"),
    ]
    agent_responses = [
        AgentResponse(2, "The grep found some items."),
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert result.metrics.acknowledged_calls == 1


def test_acknowledgement_with_result_overlap():
    tool_calls = [
        ToolCall(
            1,
            "bash",
            {"command": "pytest tests/"},
            result_summary="collected 25 items passed all tests successfully",
        ),
    ]
    agent_responses = [
        AgentResponse(
            2,
            content="All tests passed successfully with 25 items collected.",
        ),
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert result.metrics.acknowledged_calls == 1


def test_acknowledgement_with_keywords():
    tool_calls = [
        ToolCall(1, "read", {"file_path": "config.json"}, "Configuration data"),
    ]
    agent_responses = [
        AgentResponse(2, "The output shows the configuration is valid."),
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert result.metrics.acknowledged_calls == 1


def test_examples_capped_at_five():
    tool_calls = [
        ToolCall(i * 2, "read", {"file_path": f"file{i}.py"}, "Content")
        for i in range(10)
    ]
    agent_responses = []  # No acknowledgements

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert len(result.examples) <= 5


def test_insights_include_silent_drops():
    tool_calls = [
        ToolCall(1, "read", {"file_path": "test.py"}, "Content"),
    ]
    agent_responses = [
        AgentResponse(2, "Moving on to something else."),
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert any("dropped" in insight.lower() or "ignored" in insight.lower() for insight in result.insights)


def test_insights_include_repeated_calls():
    tool_calls = [
        ToolCall(1, "bash", {"command": "test"}, "Result"),
        ToolCall(3, "bash", {"command": "test"}, "Result"),
    ]
    agent_responses = [
        AgentResponse(2, "Test complete."),
        AgentResponse(4, "Test complete."),
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    assert any("repeated" in insight.lower() for insight in result.insights)


def test_insights_include_worst_tool_type():
    tool_calls = [
        ToolCall(1, "read", {"file_path": "a.py"}, "Content"),
        ToolCall(3, "read", {"file_path": "b.py"}, "Content"),
        ToolCall(5, "read", {"file_path": "c.py"}, "Content"),
        ToolCall(7, "bash", {"command": "test"}, "Passed"),
    ]
    agent_responses = [
        AgentResponse(2, "Unrelated comment."),
        AgentResponse(4, "Unrelated comment."),
        AgentResponse(6, "Unrelated comment."),
        AgentResponse(8, "Tests passed."),
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    # 'read' should be identified as worst with 0% acknowledgement
    assert any("read" in insight.lower() for insight in result.insights)


@pytest.mark.parametrize(
    ("tool_calls", "message"),
    [
        ("not_a_list", "list or tuple"),
        ([{"turn": 1}], "ToolCall"),
        ([ToolCall(-1, "read", {}, "")], "non-negative"),
        ([ToolCall(1, "", {}, "")], "not be empty"),
        (
            [ToolCall(2, "read", {}, ""), ToolCall(1, "read", {}, "")],
            "strictly increasing",
        ),
    ],
)
def test_invalid_tool_calls_raise_errors(tool_calls, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_tool_result_acknowledgement(tool_calls, [])


@pytest.mark.parametrize(
    ("agent_responses", "message"),
    [
        ("not_a_list", "list or tuple"),
        ([{"turn": 1}], "AgentResponse"),
        ([AgentResponse(-1, "content")], "non-negative"),
        ([AgentResponse(1, "")], "not be empty"),
        (
            [AgentResponse(2, "a"), AgentResponse(1, "b")],
            "strictly increasing",
        ),
    ],
)
def test_invalid_agent_responses_raise_errors(agent_responses, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_tool_result_acknowledgement([], agent_responses)


def test_tool_signature_for_different_tool_types():
    # Different signatures for different tools
    tool_calls = [
        ToolCall(1, "read", {"file_path": "a.py"}, "Content"),
        ToolCall(3, "read", {"file_path": "b.py"}, "Content"),  # Different file
        ToolCall(5, "bash", {"command": "test"}, "Passed"),
    ]
    agent_responses = [
        AgentResponse(2, "I see the read."),
        AgentResponse(4, "I see another read."),
        AgentResponse(6, "Test result noted."),
    ]

    result = analyze_session_tool_result_acknowledgement(tool_calls, agent_responses)

    # Should not detect repeated calls as they have different signatures
    assert result.metrics.repeated_calls == 0
