"""Shared input contract tests for recent analyzer modules."""

import pytest

from engagement.agent_preference_learning import analyze_agent_preference_learning
from engagement.error_message_clarity import analyze_error_message_clarity
from synthesis.conversation_turn_depth import analyze_conversation_turn_depth
from synthesis.tool_call_batching_patterns import analyze_tool_call_batching_patterns


@pytest.mark.parametrize(
    "analyzer,empty_input,zero_key",
    [
        (analyze_error_message_clarity, [], ("actionability_distribution", "total_count")),
        (analyze_tool_call_batching_patterns, [], ("batching_stats", "total_batches")),
        (analyze_agent_preference_learning, [], ("optimization_adoption_rate", "baseline_sessions")),
        (analyze_conversation_turn_depth, [], ("turn_depth_stats", "max_depth")),
    ],
)
def test_empty_inputs_return_documented_empty_reports(analyzer, empty_input, zero_key):
    report = analyzer(empty_input)

    section, key = zero_key
    assert report[section][key] == 0


@pytest.mark.parametrize(
    "analyzer,error",
    [
        (analyze_error_message_clarity, "errors must be a sequence"),
        (analyze_tool_call_batching_patterns, "batches must be a sequence"),
        (analyze_agent_preference_learning, "sessions must be a sequence"),
        (analyze_conversation_turn_depth, "turns must be a sequence"),
    ],
)
def test_none_inputs_raise_clear_value_errors(analyzer, error):
    with pytest.raises(ValueError, match=error):
        analyzer(None)


@pytest.mark.parametrize(
    "analyzer,input_value,error",
    [
        (analyze_error_message_clarity, [object()], "errors must contain ErrorMessage"),
        (analyze_tool_call_batching_patterns, [object()], "batches must contain ToolBatch"),
        (analyze_agent_preference_learning, [object()], "sessions must contain SessionBehavior"),
        (analyze_conversation_turn_depth, [object()], "turns must contain ConversationTurn"),
    ],
)
def test_malformed_items_raise_clear_value_errors(analyzer, input_value, error):
    with pytest.raises(ValueError, match=error):
        analyzer(input_value)
