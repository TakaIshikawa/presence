"""Tests for session read redundancy analyzer."""

import pytest

from synthesis.session_read_redundancy import (
    EVENT_MODIFICATION,
    EVENT_READ,
    ReadRedundancyEvent,
    analyze_session_read_redundancy,
)


def test_empty_events_returns_zero_metrics():
    result = analyze_session_read_redundancy([])

    assert result.metrics.total_reads == 0
    assert result.metrics.duplicate_reads == 0
    assert result.metrics.redundancy_rate == 0.0
    assert result.redundancy_detected is False
    assert "No events provided" in result.insights[0]


def test_single_read_no_redundancy():
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
            content="def foo(): pass",
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.total_reads == 1
    assert result.metrics.duplicate_reads == 0
    assert result.metrics.redundancy_rate == 0.0
    assert result.redundancy_detected is False


def test_reads_with_edits_between_no_redundancy():
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
            content="def foo(): pass",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_MODIFICATION,
            turn_index=2,
            file_path="src/foo.py",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=3,
            file_path="src/foo.py",
            content="def foo(): return 42",  # Different content
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.total_reads == 2
    assert result.metrics.duplicate_reads == 0
    assert result.metrics.redundancy_rate == 0.0


def test_duplicate_reads_without_edits():
    content = "def foo(): pass"
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
            content=content,
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/foo.py",
            content=content,
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.total_reads == 2
    assert result.metrics.duplicate_reads == 1
    assert result.metrics.redundancy_rate == 0.5
    assert result.metrics.unique_files_with_duplicates == 1
    assert len(result.duplicate_pairs) == 1
    assert result.duplicate_pairs[0].file_path == "src/foo.py"
    assert result.duplicate_pairs[0].read_count == 2


def test_multiple_duplicate_reads():
    content = "x = 1"
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
            content=content,
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/foo.py",
            content=content,
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=3,
            file_path="src/foo.py",
            content=content,
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.total_reads == 3
    assert result.metrics.duplicate_reads == 2
    assert result.metrics.redundancy_rate == pytest.approx(0.667, abs=0.01)
    duplicate_pair = result.duplicate_pairs[0]
    assert duplicate_pair.read_count == 3


def test_partial_redundancy():
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/a.py",
            content="a = 1",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/a.py",
            content="a = 1",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=3,
            file_path="src/b.py",
            content="b = 2",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=4,
            file_path="src/c.py",
            content="c = 3",
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.total_reads == 4
    assert result.metrics.duplicate_reads == 1
    assert result.metrics.redundancy_rate == 0.25
    assert result.metrics.unique_files_with_duplicates == 1


def test_high_redundancy_rate_detected():
    content = "x = 1"
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
            content=content,
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/foo.py",
            content=content,
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=3,
            file_path="src/foo.py",
            content=content,
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.redundancy_detected is True
    assert "High redundancy rate" in " ".join(result.insights)


def test_empty_file_content():
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/empty.py",
            content="",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/empty.py",
            content="",
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.total_reads == 2
    assert result.metrics.duplicate_reads == 1


def test_modification_resets_duplicate_tracking():
    content_v1 = "x = 1"
    content_v2 = "x = 2"

    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
            content=content_v1,
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/foo.py",
            content=content_v1,
        ),
        ReadRedundancyEvent(
            event_type=EVENT_MODIFICATION,
            turn_index=3,
            file_path="src/foo.py",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=4,
            file_path="src/foo.py",
            content=content_v2,
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=5,
            file_path="src/foo.py",
            content=content_v2,
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.total_reads == 4
    # First duplicate at turn 2, second duplicate at turn 5
    assert result.metrics.duplicate_reads == 2


def test_content_hash_provided():
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
            content="def foo(): pass",
            content_hash="abc123",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/foo.py",
            content="def foo(): pass",
            content_hash="abc123",
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.duplicate_reads == 1


def test_different_files_no_cross_contamination():
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/a.py",
            content="a = 1",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/b.py",
            content="a = 1",  # Same content but different file
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.duplicate_reads == 0


def test_wasted_tokens_estimation():
    content = "x = 1\ny = 2\nz = 3"  # ~15 characters
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/foo.py",
            content=content,
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/foo.py",
            content=content,
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.wasted_tokens > 0
    # Should be roughly content length / 4
    assert result.metrics.wasted_tokens >= 3


def test_duplicate_pairs_sorted_by_token_estimate():
    events = [
        # File A: small content, 2 reads
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/a.py",
            content="x = 1",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/a.py",
            content="x = 1",
        ),
        # File B: large content, 2 reads
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=3,
            file_path="src/b.py",
            content="x = 1\n" * 100,  # Much larger
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=4,
            file_path="src/b.py",
            content="x = 1\n" * 100,
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert len(result.duplicate_pairs) == 2
    # Largest wasted tokens should be first
    assert result.duplicate_pairs[0].file_path == "src/b.py"
    assert result.duplicate_pairs[0].token_estimate > result.duplicate_pairs[1].token_estimate


@pytest.mark.parametrize(
    ("events", "error_message"),
    [
        ("not a list", "events must be a list or tuple"),
        ([{"type": "read"}], "ReadRedundancyEvent"),
        (
            [
                ReadRedundancyEvent(
                    event_type="invalid",
                    turn_index=0,
                    file_path="foo.py",
                )
            ],
            "invalid event_type",
        ),
        (
            [
                ReadRedundancyEvent(
                    event_type=EVENT_READ,
                    turn_index="not_int",
                    file_path="foo.py",
                )
            ],
            "must be an integer",
        ),
        (
            [
                ReadRedundancyEvent(
                    event_type=EVENT_READ,
                    turn_index=-1,
                    file_path="foo.py",
                )
            ],
            "non-negative",
        ),
        (
            [
                ReadRedundancyEvent(
                    event_type=EVENT_READ,
                    turn_index=5,
                    file_path="foo.py",
                ),
                ReadRedundancyEvent(
                    event_type=EVENT_READ,
                    turn_index=3,
                    file_path="bar.py",
                ),
            ],
            "ordered",
        ),
        (
            [
                ReadRedundancyEvent(
                    event_type=EVENT_READ,
                    turn_index=0,
                    file_path="",
                )
            ],
            "non-empty file_path",
        ),
        (
            [
                ReadRedundancyEvent(
                    event_type=EVENT_READ,
                    turn_index=0,
                    file_path="foo.py",
                    content=123,
                )
            ],
            "string content",
        ),
        (
            [
                ReadRedundancyEvent(
                    event_type=EVENT_MODIFICATION,
                    turn_index=0,
                    file_path="",
                )
            ],
            "non-empty file_path",
        ),
    ],
)
def test_invalid_events_raise_value_error(events, error_message):
    with pytest.raises(ValueError, match=error_message):
        analyze_session_read_redundancy(events)


def test_multiple_files_with_redundancy():
    events = [
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=1,
            file_path="src/a.py",
            content="a = 1",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=2,
            file_path="src/a.py",
            content="a = 1",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=3,
            file_path="src/b.py",
            content="b = 2",
        ),
        ReadRedundancyEvent(
            event_type=EVENT_READ,
            turn_index=4,
            file_path="src/b.py",
            content="b = 2",
        ),
    ]

    result = analyze_session_read_redundancy(events)

    assert result.metrics.unique_files_with_duplicates == 2
    assert len(result.duplicate_pairs) == 2
