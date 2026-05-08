"""Tests for session edit tool patterns analyzer."""

import pytest

from synthesis.session_edit_tool_patterns import (
    EVENT_EDIT,
    EVENT_READ,
    ISSUE_EDIT_FAILED,
    ISSUE_NO_RECENT_READ,
    ISSUE_SMALL_MATCH,
    ISSUE_UNNECESSARY_REPLACE_ALL,
    EditPatternEvent,
    analyze_session_edit_tool_patterns,
)


def test_empty_events_returns_zero_metrics():
    result = analyze_session_edit_tool_patterns([])

    assert result.metrics.total_edits == 0
    assert result.metrics.total_issue_count == 0
    assert "No events provided" in result.insights[0]


def test_no_edit_events():
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    assert result.metrics.total_edits == 0
    assert "No edit events found" in result.insights[0]


def test_safe_edits_with_context():
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=2,
            file_path="src/foo.py",
            old_string="def old_function(): pass",
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    assert result.metrics.total_edits == 1
    assert result.metrics.total_issue_count == 0
    assert result.metrics.safe_edit_rate == 1.0


def test_small_match_string_detected():
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=2,
            file_path="src/foo.py",
            old_string="x = 1",  # 5 chars
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    assert result.metrics.small_match_count == 1
    assert result.metrics.total_issue_count == 1
    issue = result.issues[0]
    assert issue.issue_type == ISSUE_SMALL_MATCH
    assert issue.old_string_length == 5


def test_edit_without_recent_read():
    events = [
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=1,
            file_path="src/foo.py",
            old_string="def function(): pass",
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    assert result.metrics.no_recent_read_count == 1
    issue = result.issues[0]
    assert issue.issue_type == ISSUE_NO_RECENT_READ
    assert issue.had_recent_read is False


def test_recent_read_within_5_turns():
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=6,
            file_path="src/foo.py",
            old_string="def function(): pass",
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    # Turn 6 - turn 1 = 5 turns, should be considered recent
    assert result.metrics.no_recent_read_count == 0


def test_stale_read_beyond_5_turns():
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=7,
            file_path="src/foo.py",
            old_string="def function(): pass",
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    # Turn 7 - turn 1 = 6 turns, considered stale
    assert result.metrics.no_recent_read_count == 1


def test_failed_edit():
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=2,
            file_path="src/foo.py",
            old_string="def function(): pass",
            edit_succeeded=False,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    assert result.metrics.failed_edit_count == 1
    issue = result.issues[0]
    assert issue.issue_type == ISSUE_EDIT_FAILED


def test_unnecessary_replace_all():
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=2,
            file_path="src/foo.py",
            old_string="x" * 60,  # Long specific string
            replace_all=True,
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    assert result.metrics.unnecessary_replace_all_count == 1
    issue = result.issues[0]
    assert issue.issue_type == ISSUE_UNNECESSARY_REPLACE_ALL


def test_appropriate_replace_all():
    """replace_all with short string is appropriate, not flagged."""
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=2,
            file_path="src/foo.py",
            old_string="x = 1",  # Short string
            replace_all=True,
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    # Should flag small match but not unnecessary replace_all
    assert result.metrics.unnecessary_replace_all_count == 0
    assert result.metrics.small_match_count == 1


def test_multiple_issues_same_edit():
    """One edit can trigger multiple issues."""
    events = [
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=1,
            file_path="src/foo.py",
            old_string="x = 1",  # Small match
            edit_succeeded=False,  # Failed
            # No recent read
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    # Should flag: small match, no recent read, and failed edit
    assert result.metrics.total_issue_count == 3
    assert result.metrics.small_match_count == 1
    assert result.metrics.no_recent_read_count == 1
    assert result.metrics.failed_edit_count == 1


def test_multiple_files_tracked_independently():
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/a.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=2,
            file_path="src/a.py",
            old_string="code in a.py",
            edit_succeeded=True,
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=3,
            file_path="src/b.py",  # Different file, no read
            old_string="code in b.py",
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    # First edit is safe, second has no recent read
    assert result.metrics.total_edits == 2
    assert result.metrics.no_recent_read_count == 1


def test_safe_edit_rate_calculation():
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=2,
            file_path="src/foo.py",
            old_string="long enough string to be safe",
            edit_succeeded=True,
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=3,
            file_path="src/foo.py",
            old_string="x",  # Small match
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    # 1 out of 2 edits has issues
    assert result.metrics.safe_edit_rate == 0.5


def test_issues_limited_to_10():
    """Only first 10 issues should be returned."""
    events = [
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=i,
            file_path="src/foo.py",
            old_string="x",  # Small match
            edit_succeeded=True,
        )
        for i in range(1, 16)
    ]

    result = analyze_session_edit_tool_patterns(events)

    # 15 edits, all with issues, but only 10 returned
    assert result.metrics.total_issue_count == 30  # Each has 2 issues (small + no read)
    assert len(result.issues) == 10


@pytest.mark.parametrize(
    ("events", "error_message"),
    [
        ("not a list", "events must be a list or tuple"),
        ([{"type": "edit"}], "EditPatternEvent"),
        (
            [
                EditPatternEvent(
                    event_type="invalid",
                    turn_index=0,
                    file_path="foo.py",
                )
            ],
            "invalid event_type",
        ),
        (
            [
                EditPatternEvent(
                    event_type=EVENT_EDIT,
                    turn_index="not_int",
                    file_path="foo.py",
                )
            ],
            "must be an integer",
        ),
        (
            [
                EditPatternEvent(
                    event_type=EVENT_EDIT,
                    turn_index=-1,
                    file_path="foo.py",
                )
            ],
            "non-negative",
        ),
        (
            [
                EditPatternEvent(
                    event_type=EVENT_EDIT,
                    turn_index=5,
                    file_path="foo.py",
                ),
                EditPatternEvent(
                    event_type=EVENT_EDIT,
                    turn_index=3,
                    file_path="bar.py",
                ),
            ],
            "ordered",
        ),
        (
            [
                EditPatternEvent(
                    event_type=EVENT_EDIT,
                    turn_index=0,
                    file_path="",
                )
            ],
            "non-empty file_path",
        ),
        (
            [
                EditPatternEvent(
                    event_type=EVENT_EDIT,
                    turn_index=0,
                    file_path="foo.py",
                    old_string=123,
                )
            ],
            "string old_string",
        ),
    ],
)
def test_invalid_events_raise_value_error(events, error_message):
    with pytest.raises(ValueError, match=error_message):
        analyze_session_edit_tool_patterns(events)


def test_insights_generation():
    events = [
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=1,
            file_path="src/foo.py",
            old_string="x",
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    insights_text = " ".join(result.insights)
    assert "pattern issue" in insights_text.lower()
    assert "small match" in insights_text.lower()


def test_all_safe_edits_insight():
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=2,
            file_path="src/foo.py",
            old_string="long safe match string",
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    assert "All edits appear safe" in result.insights[0]


def test_low_safe_edit_rate_insight():
    events = [
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=1,
            file_path="src/foo.py",
            old_string="x",
            edit_succeeded=True,
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=2,
            file_path="src/foo.py",
            old_string="y",
            edit_succeeded=False,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    insights_text = " ".join(result.insights)
    assert "Low safe edit rate" in insights_text


def test_empty_old_string_not_flagged_as_small():
    """Empty old_string should not be flagged as small match."""
    events = [
        EditPatternEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
        ),
        EditPatternEvent(
            event_type=EVENT_EDIT,
            turn_index=2,
            file_path="src/foo.py",
            old_string="",
            edit_succeeded=True,
        ),
    ]

    result = analyze_session_edit_tool_patterns(events)

    # Empty string is not > 0, so not flagged as small
    assert result.metrics.small_match_count == 0
