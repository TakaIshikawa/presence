"""Tests for agent uncertainty resolution analyzer."""

import pytest

from engagement.agent_uncertainty_resolution import (
    QUALITY_STRONG,
    QUALITY_WEAK,
    SEVERITY_HIGH,
    SEVERITY_LOW,
    SEVERITY_MEDIUM,
    UncertaintyEvent,
    analyze_agent_uncertainty_resolution,
)


def test_empty_input_returns_zero_state():
    result = analyze_agent_uncertainty_resolution([])

    assert result.metrics.total_uncertainties == 0
    assert result.metrics.resolution_rate == 0.0
    assert "No uncertainty" in result.insights[0]


def test_fully_resolved_uncertainties_are_strong():
    result = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(0, "api", SEVERITY_LOW, 1, "docs"),
            UncertaintyEvent(2, "test", SEVERITY_MEDIUM, 4, "pytest"),
        ]
    )

    assert result.metrics.resolved_count == 2
    assert result.metrics.unresolved_count == 0
    assert result.metrics.resolution_rate == 1.0
    assert result.quality == QUALITY_STRONG


def test_unresolved_high_severity_degrades_quality():
    result = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(0, "api", SEVERITY_HIGH),
            UncertaintyEvent(1, "test", SEVERITY_LOW, 2, "pytest"),
        ]
    )

    assert result.metrics.high_severity_unresolved_count == 1
    assert result.quality == QUALITY_WEAK
    assert any("high-severity" in insight for insight in result.insights)


def test_mixed_source_distribution_is_sorted():
    result = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(0, "api", SEVERITY_LOW, 1, "docs"),
            UncertaintyEvent(2, "api", SEVERITY_LOW, 3, "docs"),
            UncertaintyEvent(4, "test", SEVERITY_LOW, 5, "tests"),
        ]
    )

    assert result.metrics.source_distribution == (("docs", 2), ("tests", 1))


def test_latency_rounding():
    result = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(0, "a", SEVERITY_LOW, 1, "docs"),
            UncertaintyEvent(2, "b", SEVERITY_LOW, 6, "tests"),
            UncertaintyEvent(7, "c", SEVERITY_LOW, 9, "logs"),
        ]
    )

    assert result.metrics.average_resolution_latency == 2.33


@pytest.mark.parametrize(
    ("events", "message"),
    [
        ("bad", "list or tuple"),
        ([{"turn_index": 0}], "UncertaintyEvent"),
        ([UncertaintyEvent(-1, "api", SEVERITY_LOW)], "turn_index"),
        ([UncertaintyEvent(0, "api", "urgent")], "unsupported severity"),
        ([UncertaintyEvent(3, "api", SEVERITY_LOW, 2, "docs")], "at or after"),
        ([UncertaintyEvent(0, "api", SEVERITY_LOW, 1, 123)], "resolution_source"),
    ],
)
def test_invalid_input_validation(events, message):
    with pytest.raises(ValueError, match=message):
        analyze_agent_uncertainty_resolution(events)
