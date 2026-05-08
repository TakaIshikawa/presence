"""Tests for session grep optimization analyzer."""

import pytest

from synthesis.session_grep_optimization import (
    EVENT_GREP,
    EVENT_READ,
    GrepOptimizationEvent,
    analyze_session_grep_optimization,
)


def test_empty_events_returns_zero_metrics():
    result = analyze_session_grep_optimization([])

    assert result.metrics.total_greps == 0
    assert result.metrics.total_opportunity_count == 0
    assert "No events provided" in result.insights[0]


def test_no_grep_events():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.total_greps == 0
    assert "No grep events found" in result.insights[0]


def test_efficient_grep_usage():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="def calculate",
            result_count=5,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=2,
            pattern="class User",
            result_count=3,
            type_filter="py",
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.total_greps == 2
    assert result.metrics.total_opportunity_count == 0


def test_broad_pattern_detected():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="error",
            result_count=150,
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.broad_pattern_count == 1
    assert result.metrics.total_opportunity_count == 1
    opp = result.opportunities[0]
    assert opp.issue_type == "broad_pattern"
    assert opp.estimated_token_savings > 0
    assert "150 results" in opp.suggestion


def test_repeated_pattern_detected():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="def foo",
            result_count=10,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=3,
            pattern="def foo",
            result_count=10,
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.repeated_pattern_count == 1
    opp = result.opportunities[0]
    assert opp.issue_type == "repeated_pattern"
    assert "turn 1" in opp.suggestion


def test_repeated_pattern_normalization():
    """Test that similar patterns are detected even with different formatting."""
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="def foo",
            result_count=10,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=2,
            pattern='"DEF FOO"',  # Different case and quotes
            result_count=10,
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.repeated_pattern_count == 1


def test_repeated_pattern_far_apart_not_flagged():
    """Repeated patterns more than 10 turns apart should not be flagged."""
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="def foo",
            result_count=10,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=15,
            pattern="def foo",
            result_count=10,
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.repeated_pattern_count == 0


def test_grep_read_inefficiency():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="class Foo",
            result_count=2,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/foo.py",
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.grep_read_inefficiency_count == 1
    opp = result.opportunities[0]
    assert opp.issue_type == "grep_read_inefficiency"
    assert "targeted read" in opp.suggestion


def test_grep_read_not_flagged_for_many_results():
    """Grep with many results followed by read is not grep-read inefficiency."""
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="error",
            result_count=50,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/foo.py",
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.grep_read_inefficiency_count == 0


def test_grep_read_not_flagged_if_far_apart():
    """Grep and read separated by other greps should only flag the closest one."""
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="class Foo",
            result_count=2,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=2,
            pattern="other",
            result_count=5,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=3,
            pattern="another",
            result_count=5,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=4,
            pattern="more",
            result_count=5,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_READ,
            turn_index=5,
            file_path="src/foo.py",
        ),
    ]

    result = analyze_session_grep_optimization(events)

    # Only the last grep (turn 4) should be flagged since it's immediately before the read
    # Earlier greps are blocked by intervening grep events
    assert result.metrics.grep_read_inefficiency_count == 1
    assert result.opportunities[0].grep_turn == 4


def test_missing_filter_for_py_files():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="import .py",
            result_count=50,
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.missing_filter_count == 1
    opp = result.opportunities[0]
    assert opp.issue_type == "missing_filter"
    assert "py" in opp.suggestion.lower()


def test_no_missing_filter_when_filter_present():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="import",
            result_count=50,
            type_filter="py",
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.missing_filter_count == 0


def test_no_missing_filter_for_low_results():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="def foo.py",
            result_count=5,
        ),
    ]

    result = analyze_session_grep_optimization(events)

    # Low result count, so filter wouldn't help much
    assert result.metrics.missing_filter_count == 0


def test_multiple_opportunity_types():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="error",
            result_count=150,  # Broad pattern
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=2,
            pattern="error",
            result_count=150,  # Repeated pattern
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=3,
            pattern="class Foo",
            result_count=2,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_READ,
            turn_index=4,
            file_path="src/foo.py",  # Grep-read inefficiency
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=5,
            pattern="import .py",
            result_count=60,  # Missing filter
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.broad_pattern_count >= 1
    assert result.metrics.repeated_pattern_count >= 1
    assert result.metrics.grep_read_inefficiency_count >= 1
    assert result.metrics.missing_filter_count >= 1
    assert result.metrics.total_opportunity_count >= 4


def test_opportunities_sorted_by_savings():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="small",
            result_count=105,  # Small savings
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=2,
            pattern="large",
            result_count=500,  # Large savings
        ),
    ]

    result = analyze_session_grep_optimization(events)

    # Should be sorted by estimated savings (descending)
    assert len(result.opportunities) == 2
    assert result.opportunities[0].estimated_token_savings > result.opportunities[1].estimated_token_savings


def test_total_savings_estimation():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="error",
            result_count=200,
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.estimated_total_savings > 0
    assert "token savings" in " ".join(result.insights).lower()


@pytest.mark.parametrize(
    ("events", "error_message"),
    [
        ("not a list", "events must be a list or tuple"),
        ([{"type": "grep"}], "GrepOptimizationEvent"),
        (
            [
                GrepOptimizationEvent(
                    event_type="invalid",
                    turn_index=0,
                )
            ],
            "invalid event_type",
        ),
        (
            [
                GrepOptimizationEvent(
                    event_type=EVENT_GREP,
                    turn_index="not_int",
                )
            ],
            "must be an integer",
        ),
        (
            [
                GrepOptimizationEvent(
                    event_type=EVENT_GREP,
                    turn_index=-1,
                )
            ],
            "non-negative",
        ),
        (
            [
                GrepOptimizationEvent(
                    event_type=EVENT_GREP,
                    turn_index=5,
                    pattern="foo",
                ),
                GrepOptimizationEvent(
                    event_type=EVENT_GREP,
                    turn_index=3,
                    pattern="bar",
                ),
            ],
            "ordered",
        ),
        (
            [
                GrepOptimizationEvent(
                    event_type=EVENT_GREP,
                    turn_index=0,
                    pattern="",
                )
            ],
            "non-empty pattern",
        ),
        (
            [
                GrepOptimizationEvent(
                    event_type=EVENT_GREP,
                    turn_index=0,
                    pattern="foo",
                    result_count="not_int",
                )
            ],
            "integer result_count",
        ),
        (
            [
                GrepOptimizationEvent(
                    event_type=EVENT_GREP,
                    turn_index=0,
                    pattern="foo",
                    result_count=-1,
                )
            ],
            "non-negative",
        ),
    ],
)
def test_invalid_events_raise_value_error(events, error_message):
    with pytest.raises(ValueError, match=error_message):
        analyze_session_grep_optimization(events)


def test_insights_generation():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="error",
            result_count=200,
        ),
    ]

    result = analyze_session_grep_optimization(events)

    insights_text = " ".join(result.insights)
    assert "100+ results" in insights_text


def test_glob_filter_prevents_missing_filter_flag():
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="import .py",
            result_count=50,
            glob_filter="**/*.py",
        ),
    ]

    result = analyze_session_grep_optimization(events)

    assert result.metrics.missing_filter_count == 0


def test_file_extension_extraction():
    """Test that various file extension formats are recognized."""
    events = [
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=1,
            pattern="test.*\\.ts",
            result_count=30,
        ),
        GrepOptimizationEvent(
            event_type=EVENT_GREP,
            turn_index=2,
            pattern="*.js files",
            result_count=30,
        ),
    ]

    result = analyze_session_grep_optimization(events)

    # Both should be flagged as missing filter
    assert result.metrics.missing_filter_count == 2
