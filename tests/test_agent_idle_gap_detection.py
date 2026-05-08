"""Tests for agent idle gap detection."""

from engagement.agent_idle_gap_detection import analyze_agent_idle_gaps


def test_empty_input_returns_empty_report():
    report = analyze_agent_idle_gaps([])

    assert report["event_count"] == 0
    assert report["max_gap_seconds"] == 0.0
    assert report["flagged_intervals"] == []


def test_sorted_events_compute_gaps():
    report = analyze_agent_idle_gaps(
        [
            {"timestamp": "2026-01-01T00:00:00Z", "label": "start"},
            {"timestamp": "2026-01-01T00:10:00Z", "label": "resume"},
        ],
        threshold_seconds=300,
    )

    assert report["event_count"] == 2
    assert report["max_gap_seconds"] == 600.0
    assert report["average_gap_seconds"] == 600.0
    assert report["flagged_gap_count"] == 1
    assert report["flagged_intervals"][0]["start_event"]["label"] == "start"


def test_unsorted_epoch_events_are_normalized():
    report = analyze_agent_idle_gaps(
        [{"timestamp": 1000, "event": "b"}, {"timestamp": 0, "event": "a"}],
        threshold_seconds=500,
    )

    assert report["max_gap_seconds"] == 1000.0
    assert report["flagged_intervals"][0]["start_event"]["label"] == "a"
    assert report["flagged_intervals"][0]["end_event"]["label"] == "b"


def test_invalid_timestamps_are_skipped():
    report = analyze_agent_idle_gaps(
        [{"timestamp": "bad"}, {"timestamp": "2026-01-01T00:00:00Z"}]
    )

    assert report["event_count"] == 1
    assert report["invalid_event_count"] == 1
    assert report["gap_count"] == 0


def test_single_event_has_no_gaps():
    report = analyze_agent_idle_gaps([{"timestamp": "2026-01-01T00:00:00Z"}])

    assert report["event_count"] == 1
    assert report["average_gap_seconds"] == 0.0
    assert report["flagged_intervals"] == []


def test_configurable_threshold_controls_flags():
    events = [
        {"timestamp": "2026-01-01T00:00:00Z"},
        {"timestamp": "2026-01-01T00:02:00Z"},
    ]

    assert analyze_agent_idle_gaps(events, threshold_seconds=60)["flagged_gap_count"] == 1
    assert analyze_agent_idle_gaps(events, threshold_seconds=180)["flagged_gap_count"] == 0


def test_flagged_gap_label_counts_repeated_transitions():
    report = analyze_agent_idle_gaps(
        [
            {"timestamp": "2026-01-01T00:00:00Z", "label": "tool"},
            {"timestamp": "2026-01-01T00:10:00Z", "label": "assistant"},
            {"timestamp": "2026-01-01T00:11:00Z", "label": "tool"},
            {"timestamp": "2026-01-01T00:25:00Z", "label": "assistant"},
        ],
        threshold_seconds=300,
    )

    assert report["flagged_gap_count"] == 2
    assert report["flagged_gap_label_counts"] == {"tool -> assistant": 2}


def test_flagged_gap_label_counts_uses_empty_label_fallback_and_skips_unflagged():
    report = analyze_agent_idle_gaps(
        [
            {"timestamp": "2026-01-01T00:00:00Z"},
            {"timestamp": "2026-01-01T00:10:00Z", "label": "resume"},
            {"timestamp": "2026-01-01T00:11:00Z"},
        ],
        threshold_seconds=300,
    )

    assert report["flagged_gap_label_counts"] == {" -> resume": 1}
