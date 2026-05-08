"""Tests for session read optimization analyzer."""

import pytest

from synthesis.session_read_optimization import (
    EditOperation,
    ReadOperation,
    SessionReadOptimization,
    analyze_session_read_optimization,
)


def test_empty_reads_returns_zero_state():
    result = analyze_session_read_optimization([], [])

    assert result.metrics.total_reads == 0
    assert result.metrics.targeted_reads == 0
    assert result.metrics.full_reads == 0
    assert result.metrics.targeted_read_percentage == 0.0
    assert "No read operations" in result.insights[0]


def test_targeted_read_with_offset_and_limit():
    reads = [
        ReadOperation(1, "file.py", offset=100, limit=50, bytes_read=500),
    ]

    result = analyze_session_read_optimization(reads, [])

    assert result.metrics.total_reads == 1
    assert result.metrics.targeted_reads == 1
    assert result.metrics.full_reads == 0
    assert result.metrics.targeted_read_percentage == 100.0


def test_targeted_read_with_only_offset():
    reads = [
        ReadOperation(1, "file.py", offset=100, limit=None, bytes_read=1000),
    ]

    result = analyze_session_read_optimization(reads, [])

    assert result.metrics.targeted_reads == 1


def test_targeted_read_with_only_limit():
    reads = [
        ReadOperation(1, "file.py", offset=None, limit=100, bytes_read=1000),
    ]

    result = analyze_session_read_optimization(reads, [])

    assert result.metrics.targeted_reads == 1


def test_full_read_without_offset_or_limit():
    reads = [
        ReadOperation(1, "file.py", offset=None, limit=None, bytes_read=5000),
    ]

    result = analyze_session_read_optimization(reads, [])

    assert result.metrics.targeted_reads == 0
    assert result.metrics.full_reads == 1
    assert result.metrics.targeted_read_percentage == 0.0


def test_repeated_reads_detected():
    reads = [
        ReadOperation(1, "file.py", None, None, 1000),
        ReadOperation(3, "file.py", None, None, 1000),  # Repeated
    ]

    result = analyze_session_read_optimization(reads, [])

    assert result.metrics.repeated_reads == 1
    assert result.metrics.repeated_read_rate == 50.0


def test_wasteful_read_after_small_edit():
    edits = [
        EditOperation(2, "file.py", edit_size_bytes=100),  # Small edit
    ]
    reads = [
        ReadOperation(1, "file.py", None, None, 5000),
        ReadOperation(3, "file.py", None, None, 5000),  # Full read after small edit
    ]

    result = analyze_session_read_optimization(reads, edits)

    assert result.metrics.wasteful_reads == 1
    assert result.metrics.wasteful_read_rate == 50.0


def test_no_wasteful_read_after_large_edit():
    edits = [
        EditOperation(2, "file.py", edit_size_bytes=1000),  # Large edit
    ]
    reads = [
        ReadOperation(1, "file.py", None, None, 5000),
        ReadOperation(3, "file.py", None, None, 5000),  # Full read after large edit
    ]

    result = analyze_session_read_optimization(reads, edits)

    # Not wasteful because edit was large
    assert result.metrics.wasteful_reads == 0


def test_no_wasteful_read_if_targeted():
    edits = [
        EditOperation(2, "file.py", edit_size_bytes=100),  # Small edit
    ]
    reads = [
        ReadOperation(1, "file.py", None, None, 5000),
        ReadOperation(3, "file.py", offset=100, limit=50, bytes_read=500),  # Targeted read
    ]

    result = analyze_session_read_optimization(reads, edits)

    # Not wasteful because read is targeted
    assert result.metrics.wasteful_reads == 0


def test_average_bytes_per_read():
    reads = [
        ReadOperation(1, "file1.py", None, None, 1000),
        ReadOperation(2, "file2.py", None, None, 2000),
        ReadOperation(3, "file3.py", None, None, 3000),
    ]

    result = analyze_session_read_optimization(reads, [])

    # Average: (1000 + 2000 + 3000) / 3 = 2000
    assert result.metrics.average_bytes_per_read == 2000.0


def test_bytes_read_per_edit():
    reads = [
        ReadOperation(1, "file.py", None, None, 3000),
        ReadOperation(3, "file.py", None, None, 2000),
    ]
    edits = [
        EditOperation(2, "file.py", edit_size_bytes=100),
    ]

    result = analyze_session_read_optimization(reads, edits)

    # Total bytes: 5000, edits: 1, ratio: 5000.0
    assert result.metrics.bytes_read_per_edit == 5000.0


def test_multiple_files_tracked_separately():
    reads = [
        ReadOperation(1, "file1.py", None, None, 1000),
        ReadOperation(2, "file2.py", None, None, 1000),
        ReadOperation(3, "file1.py", None, None, 1000),  # Repeated for file1
    ]

    result = analyze_session_read_optimization(reads, [])

    # Only file1 has repeated reads
    assert result.metrics.repeated_reads == 1


def test_examples_capped_at_five():
    reads = [
        ReadOperation(i * 2, "file.py", None, None, 1000)
        for i in range(10)
    ]

    result = analyze_session_read_optimization(reads, [])

    assert len(result.examples) <= 5


def test_low_targeted_read_adoption_insight():
    reads = [
        ReadOperation(1, "file1.py", None, None, 1000),
        ReadOperation(2, "file2.py", None, None, 1000),
        ReadOperation(3, "file3.py", None, None, 1000),
        ReadOperation(4, "file4.py", None, None, 1000),
        ReadOperation(5, "file5.py", None, None, 1000),
    ]

    result = analyze_session_read_optimization(reads, [])

    # 0% targeted < 50%
    low_adoption = [i for i in result.insights if "low targeted read adoption" in i.lower()]
    assert len(low_adoption) > 0


def test_high_average_read_size_insight():
    reads = [
        ReadOperation(1, "file.py", None, None, 15000),
    ]

    result = analyze_session_read_optimization(reads, [])

    # 15000 bytes > 10000 threshold
    high_size = [i for i in result.insights if "high average read size" in i.lower()]
    assert len(high_size) > 0


def test_excellent_efficiency_insight():
    reads = [
        ReadOperation(i, f"file{i}.py", offset=0, limit=100, bytes_read=100)
        for i in range(1, 11)
    ]

    result = analyze_session_read_optimization(reads, [])

    # 100% targeted, 0% wasteful, 0% repeated (each file read once)
    excellence = [i for i in result.insights if "excellent read efficiency" in i.lower()]
    assert len(excellence) > 0


def test_repeated_read_insight():
    reads = [
        ReadOperation(1, "file.py", None, None, 1000),
        ReadOperation(2, "file.py", None, None, 1000),
    ]

    result = analyze_session_read_optimization(reads, [])

    repeated = [i for i in result.insights if "repeated reads" in i.lower()]
    assert len(repeated) > 0


def test_wasteful_read_insight():
    edits = [
        EditOperation(2, "file.py", edit_size_bytes=100),
    ]
    reads = [
        ReadOperation(1, "file.py", None, None, 5000),
        ReadOperation(3, "file.py", None, None, 5000),
    ]

    result = analyze_session_read_optimization(reads, edits)

    wasteful = [i for i in result.insights if "full reads immediately after small edits" in i.lower()]
    assert len(wasteful) > 0


@pytest.mark.parametrize(
    ("reads", "message"),
    [
        ("not_a_list", "list or tuple"),
        ([{"turn": 1}], "ReadOperation"),
        ([ReadOperation(-1, "file.py", None, None, 0)], "non-negative"),
        ([ReadOperation(1, "", None, None, 0)], "not be empty"),
        ([ReadOperation(1, "file.py", None, None, -100)], "non-negative"),
        (
            [ReadOperation(2, "a", None, None, 0), ReadOperation(1, "b", None, None, 0)],
            "strictly increasing",
        ),
    ],
)
def test_invalid_reads_raise_errors(reads, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_read_optimization(reads, [])


@pytest.mark.parametrize(
    ("edits", "message"),
    [
        ("not_a_list", "list or tuple"),
        ([{"turn": 1}], "EditOperation"),
        ([EditOperation(-1, "file.py", 0)], "non-negative"),
        ([EditOperation(1, "", 0)], "not be empty"),
        ([EditOperation(1, "file.py", -100)], "non-negative"),
        (
            [EditOperation(2, "a", 0), EditOperation(1, "b", 0)],
            "strictly increasing",
        ),
    ],
)
def test_invalid_edits_raise_errors(edits, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_read_optimization([], edits)


def test_mixed_targeted_and_full_reads():
    reads = [
        ReadOperation(1, "file1.py", offset=0, limit=50, bytes_read=500),
        ReadOperation(2, "file2.py", None, None, 5000),
        ReadOperation(3, "file3.py", offset=100, limit=50, bytes_read=500),
    ]

    result = analyze_session_read_optimization(reads, [])

    assert result.metrics.targeted_reads == 2
    assert result.metrics.full_reads == 1
    assert result.metrics.targeted_read_percentage == pytest.approx(66.67, rel=0.01)


def test_edit_before_first_read_considered_wasteful():
    edits = [
        EditOperation(1, "file.py", edit_size_bytes=100),  # Small edit
    ]
    reads = [
        ReadOperation(2, "file.py", None, None, 5000),  # Full read after small edit
    ]

    result = analyze_session_read_optimization(reads, edits)

    # Full read after small edit is wasteful
    assert result.metrics.wasteful_reads == 1


def test_no_edits_no_wasteful_reads():
    reads = [
        ReadOperation(1, "file.py", None, None, 5000),
        ReadOperation(2, "file.py", None, None, 5000),
    ]

    result = analyze_session_read_optimization(reads, [])

    # Without edits, can't determine wastefulness
    assert result.metrics.wasteful_reads == 0
