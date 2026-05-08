"""Tests for session context refresh cadence analyzer."""

import pytest

from synthesis.session_context_refresh_cadence import (
    MARKER_CONSTRAINT_RECAP,
    MARKER_FILE_CONTEXT_RECAP,
    MARKER_NONE,
    MARKER_PLAN_RECAP,
    QUALITY_NO_SESSION,
    QUALITY_STALE,
    QUALITY_STRONG,
    STALE_CONTEXT_GAP_TURNS,
    ContextRefreshTurn,
    analyze_session_context_refresh_cadence,
)


def test_no_turns_returns_no_session_result():
    result = analyze_session_context_refresh_cadence([])

    assert result.metrics.total_turns == 0
    assert result.quality == QUALITY_NO_SESSION
    assert "No session" in result.insights[0]


def test_single_refresh_event_returns_deterministic_report():
    result = analyze_session_context_refresh_cadence(
        [ContextRefreshTurn(0, 100, (MARKER_CONSTRAINT_RECAP,))]
    )

    assert result.metrics.total_turns == 1
    assert result.metrics.refresh_turns == 1
    assert result.metrics.average_turns_between_refreshes == 0.0
    assert result.metrics.longest_refresh_gap == 0
    assert result.metrics.stale_context_windows == 0
    assert result.quality == QUALITY_STRONG


def test_regular_refresh_cadence_is_strong():
    result = analyze_session_context_refresh_cadence(
        [
            ContextRefreshTurn(0, 100, (MARKER_PLAN_RECAP,)),
            ContextRefreshTurn(2, 100, (MARKER_CONSTRAINT_RECAP,)),
            ContextRefreshTurn(4, 100, (MARKER_PLAN_RECAP,)),
        ]
    )

    assert result.metrics.refresh_turns == 3
    assert result.metrics.average_turns_between_refreshes == 2.0
    assert result.metrics.longest_refresh_gap == 2
    assert result.quality == QUALITY_STRONG


def test_long_gaps_count_stale_windows():
    result = analyze_session_context_refresh_cadence(
        [
            ContextRefreshTurn(0, 100, (MARKER_PLAN_RECAP,)),
            ContextRefreshTurn(6, 100, (MARKER_NONE,)),
            ContextRefreshTurn(10, 100, (MARKER_FILE_CONTEXT_RECAP,)),
        ]
    )

    assert result.metrics.longest_refresh_gap == 10
    assert result.metrics.stale_context_windows == 1
    assert result.quality != QUALITY_STRONG
    assert any("Longest refresh gap" in insight for insight in result.insights)


def test_marker_specific_counts_and_dominant_marker():
    result = analyze_session_context_refresh_cadence(
        [
            ContextRefreshTurn(0, 100, (MARKER_PLAN_RECAP, MARKER_FILE_CONTEXT_RECAP)),
            ContextRefreshTurn(1, 100, (MARKER_PLAN_RECAP,)),
        ]
    )

    assert result.metrics.marker_counts == (
        (MARKER_FILE_CONTEXT_RECAP, 1),
        (MARKER_PLAN_RECAP, 2),
    )
    assert any(MARKER_PLAN_RECAP in insight for insight in result.insights)


def test_no_refreshes_are_stale():
    result = analyze_session_context_refresh_cadence(
        [ContextRefreshTurn(index, 100, (MARKER_NONE,)) for index in range(5)]
    )

    assert result.metrics.refresh_turns == 0
    assert result.quality == QUALITY_STALE


def test_only_non_refresh_events_are_classified_explicitly():
    result = analyze_session_context_refresh_cadence(
        [ContextRefreshTurn(index, 100, (MARKER_NONE,)) for index in range(3)]
    )

    assert result.metrics.marker_counts == ()
    assert result.metrics.refresh_turns == 0
    assert result.quality == QUALITY_STALE
    assert "0 of 3 turns" in result.insights[0]


def test_exact_stale_threshold_gap_is_not_stale():
    result = analyze_session_context_refresh_cadence(
        [
            ContextRefreshTurn(0, 100, (MARKER_PLAN_RECAP,)),
            ContextRefreshTurn(STALE_CONTEXT_GAP_TURNS, 100, (MARKER_FILE_CONTEXT_RECAP,)),
        ]
    )

    assert result.metrics.longest_refresh_gap == STALE_CONTEXT_GAP_TURNS
    assert result.metrics.stale_context_windows == 0
    assert result.quality == QUALITY_STRONG


@pytest.mark.parametrize(
    ("turns", "message"),
    [
        ("bad", "list or tuple"),
        ([{"turn_index": 0}], "ContextRefreshTurn"),
        ([ContextRefreshTurn(-1, 1)], "turn_index"),
        ([ContextRefreshTurn(0, -1)], "token_estimate"),
        ([ContextRefreshTurn(0, 1, ("unknown",))], "unsupported"),
        ([ContextRefreshTurn(0, 1, (MARKER_NONE, MARKER_PLAN_RECAP))], "cannot be combined"),
        ([ContextRefreshTurn(1, 1), ContextRefreshTurn(1, 1)], "unique"),
    ],
)
def test_invalid_turn_data_raises_clear_errors(turns, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_context_refresh_cadence(turns)
