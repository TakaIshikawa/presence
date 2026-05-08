"""Tests for context rehydration gap analyzer."""

import pytest

from engagement.context_rehydration_gaps import (
    ContextTurn,
    analyze_context_rehydration_gaps,
)


def test_empty_turns_return_low_zero_state():
    result = analyze_context_rehydration_gaps([])

    assert result.metrics.total_turns == 0
    assert result.severity == "low"
    assert result.repeated_context_counts == {}


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
    assert result.repeated_context_counts == {
        "src/a.py": 2,
        "what is the goal?": 2,
    }
    assert result.severity == "high"


def test_repeated_file_counts_include_observed_reads():
    result = analyze_context_rehydration_gaps(
        [
            ContextTurn(0, file_reads=("src/a.py", "src/b.py")),
            ContextTurn(1, file_reads=("src/a.py",)),
            ContextTurn(2, file_reads=("src/a.py", "src/b.py")),
        ]
    )

    assert result.repeated_context_counts == {"src/a.py": 3, "src/b.py": 2}
    assert result.top_repeated_context_keys == ("src/a.py", "src/b.py")


def test_repeated_ask_counts_are_whitespace_and_case_normalized():
    result = analyze_context_rehydration_gaps(
        [
            ContextTurn(0, clarification_asks=("  What   next? ",)),
            ContextTurn(1, clarification_asks=("what next?",)),
            ContextTurn(2, clarification_asks=("WHAT NEXT?",)),
        ]
    )

    assert result.repeated_context_counts == {"what next?": 3}


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
    with pytest.raises(ValueError):
        analyze_context_rehydration_gaps(turns)
