"""Tests for context rehydration gap analyzer."""

import pytest

from engagement.context_rehydration_gaps import (
    ContextTurn,
    analyze_context_rehydration_gaps,
)


def test_empty_turns_return_low_zero_state():
    result = analyze_context_rehydration_gaps([])

    assert result.metrics.total_turns == 0
    assert result.metrics.repeated_file_reads == 0
    assert result.metrics.repeated_clarification_asks == 0
    assert result.metrics.resumed_session_gaps == 0
    assert result.severity == "low"


def test_none_input_raises_value_error():
    with pytest.raises(ValueError, match="turns must be a list or tuple"):
        analyze_context_rehydration_gaps(None)


def test_minimal_valid_turn_returns_expected_metrics():
    result = analyze_context_rehydration_gaps([ContextTurn(0)])

    assert result.metrics.total_turns == 1
    assert result.metrics.unnecessary_rediscovery_ratio == 0.0
    assert result.top_repeated_context_keys == ()
    assert result.severity == "low"


def test_efficient_reuse_has_low_severity():
    result = analyze_context_rehydration_gaps(
        [
            ContextTurn(0, file_reads=("src/a.py",)),
            ContextTurn(1, file_reads=("src/b.py",), clarification_asks=("What next?",)),
        ]
    )

    assert result.metrics.unnecessary_rediscovery_ratio == 0.0
    assert result.severity == "low"


def test_repeated_reads_are_separate_from_clarification_asks():
    result = analyze_context_rehydration_gaps(
        [
            ContextTurn(0, file_reads=("src/a.py",), clarification_asks=("What is the goal?",)),
            ContextTurn(1, file_reads=("src/a.py",), clarification_asks=("What is the goal?",)),
        ]
    )

    assert result.metrics.repeated_file_reads == 1
    assert result.metrics.repeated_clarification_asks == 1
    assert "src/a.py" in result.top_repeated_context_keys
    assert "what is the goal?" in result.top_repeated_context_keys
    assert result.severity == "high"


def test_resumed_session_without_summary_counts_gap():
    result = analyze_context_rehydration_gaps(
        [
            ContextTurn(0, resumed_session=True, has_summary=False),
            ContextTurn(1),
            ContextTurn(2),
            ContextTurn(3),
        ]
    )

    assert result.metrics.resumed_session_gaps == 1
    assert result.metrics.unnecessary_rediscovery_ratio == 0.25
    assert result.severity == "moderate"


def test_repeated_user_clarifications_can_drive_moderate_severity():
    result = analyze_context_rehydration_gaps(
        [
            ContextTurn(0, clarification_asks=("status?",)),
            ContextTurn(1, clarification_asks=("status?",)),
            ContextTurn(2),
            ContextTurn(3),
        ]
    )

    assert result.metrics.repeated_clarification_asks == 1
    assert result.severity == "moderate"


@pytest.mark.parametrize(
    "turns",
    [
        "bad",
        [{"turn_index": 0}],
        [ContextTurn(-1)],
        [ContextTurn(1), ContextTurn(1)],
        [ContextTurn(0, file_reads="src/a.py")],
        [ContextTurn(0, clarification_asks=(1,))],
        [ContextTurn(0, resumed_session="yes")],
    ],
)
def test_malformed_turns_raise_value_error(turns):
    with pytest.raises(ValueError, match="turns|turn_index|file_reads|clarification_asks|booleans"):
        analyze_context_rehydration_gaps(turns)
