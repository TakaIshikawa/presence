"""Tests for agent uncertainty resolution analysis."""

import pytest

from engagement.agent_uncertainty_resolution import (
    QUALITY_CRITICAL,
    QUALITY_MODERATE,
    QUALITY_NO_UNCERTAINTIES,
    QUALITY_STRONG,
    QUALITY_WEAK,
    SEVERITY_HIGH,
    SEVERITY_LOW,
    SEVERITY_MEDIUM,
    AgentUncertaintyResolutionMetrics,
    ResolutionSourceCount,
    UncertaintyEvent,
    analyze_agent_uncertainty_resolution,
)


def test_empty_input_returns_zero_state_with_insight():
    analysis = analyze_agent_uncertainty_resolution([])

    assert analysis.metrics == AgentUncertaintyResolutionMetrics(
        total_uncertainties=0,
        resolved_count=0,
        unresolved_count=0,
        resolution_rate=0.0,
        average_resolution_latency=0.0,
        high_severity_unresolved_count=0,
        source_distribution=(),
    )
    assert analysis.quality_tier == QUALITY_NO_UNCERTAINTIES
    assert analysis.quality == QUALITY_NO_UNCERTAINTIES
    assert analysis.insights == ("No uncertainty events supplied - nothing to resolve.",)


def test_fully_resolved_uncertainties_are_strong_quality():
    analysis = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(
                turn_index=1,
                uncertainty_type="api_contract",
                severity=SEVERITY_MEDIUM,
                resolved_turn_index=2,
                resolution_source="official_docs",
            ),
            UncertaintyEvent(
                turn_index=3,
                uncertainty_type="test_status",
                severity=SEVERITY_LOW,
                resolved_turn_index=5,
                resolution_source="pytest",
            ),
        ]
    )

    assert analysis.metrics.total_uncertainties == 2
    assert analysis.metrics.resolved_count == 2
    assert analysis.metrics.unresolved_count == 0
    assert analysis.metrics.resolution_rate == 1.0
    assert analysis.metrics.average_resolution_latency == 1.5
    assert analysis.quality_tier == QUALITY_STRONG
    assert analysis.quality == QUALITY_STRONG
    assert "Resolved 2 of 2 uncertainties (100.0%)." in analysis.insights


def test_unresolved_high_severity_degrades_quality():
    analysis = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(
                turn_index=1,
                uncertainty_type="dependency_behavior",
                severity=SEVERITY_MEDIUM,
                resolved_turn_index=2,
                resolution_source="local_test",
            ),
            UncertaintyEvent(
                turn_index=3,
                uncertainty_type="data_loss_risk",
                severity=SEVERITY_HIGH,
            ),
            UncertaintyEvent(
                turn_index=4,
                uncertainty_type="edge_case",
                severity=SEVERITY_LOW,
                resolved_turn_index=5,
                resolution_source="code_read",
            ),
        ]
    )

    assert analysis.metrics.resolution_rate == 0.667
    assert analysis.metrics.high_severity_unresolved_count == 1
    assert analysis.quality_tier == QUALITY_CRITICAL
    assert any(
        "high-severity uncertainties remained unresolved" in insight
        for insight in analysis.insights
    )


def test_mixed_source_distribution_is_deterministic():
    analysis = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(1, "contract", SEVERITY_MEDIUM, 2, "docs"),
            UncertaintyEvent(2, "regression", SEVERITY_MEDIUM, 4, "tests"),
            UncertaintyEvent(5, "api", SEVERITY_LOW, 6, "docs"),
        ]
    )

    assert analysis.metrics.source_distribution == (
        ResolutionSourceCount(source="docs", count=2),
        ResolutionSourceCount(source="tests", count=1),
    )
    assert analysis.metrics.source_distribution == (("docs", 2), ("tests", 1))
    assert any("Most resolutions came from docs" in insight for insight in analysis.insights)


def test_latency_rounding_and_moderate_quality_threshold():
    analysis = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(1, "first", SEVERITY_LOW, 3, "logs"),
            UncertaintyEvent(4, "second", SEVERITY_MEDIUM, 7, "tests"),
            UncertaintyEvent(8, "third", SEVERITY_LOW, 12, "docs"),
            UncertaintyEvent(13, "fourth", SEVERITY_LOW),
        ]
    )

    assert analysis.metrics.resolution_rate == 0.75
    assert analysis.metrics.average_resolution_latency == 3.0
    assert analysis.quality_tier == QUALITY_MODERATE


def test_low_resolution_rate_is_weak_quality():
    analysis = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(0, "api", SEVERITY_LOW, 1, "docs"),
            UncertaintyEvent(2, "test", SEVERITY_LOW),
            UncertaintyEvent(3, "logs", SEVERITY_LOW),
        ]
    )

    assert analysis.quality_tier == QUALITY_WEAK
    assert analysis.quality == QUALITY_WEAK


@pytest.mark.parametrize(
    ("events", "message"),
    [
        ("bad", "events must be a list or tuple"),
        ([object()], "events must contain UncertaintyEvent instances"),
        (
            [UncertaintyEvent(-1, "contract", SEVERITY_LOW)],
            "turn_index must be a non-negative integer",
        ),
        (
            [
                UncertaintyEvent(2, "later", SEVERITY_LOW),
                UncertaintyEvent(1, "earlier", SEVERITY_LOW),
            ],
            "turn_index values must be ordered",
        ),
        (
            [UncertaintyEvent(1, "", SEVERITY_LOW)],
            "uncertainty_type must be a non-empty string",
        ),
        ([UncertaintyEvent(1, "contract", "urgent")], "unsupported severity"),
        (
            [UncertaintyEvent(3, "contract", SEVERITY_MEDIUM, 2, "docs")],
            "resolved_turn_index must be greater than or equal to turn_index",
        ),
        (
            [UncertaintyEvent(1, "contract", SEVERITY_MEDIUM, 2, 123)],
            "resolution_source must be a string",
        ),
        (
            [UncertaintyEvent(1, "contract", SEVERITY_MEDIUM, 2, "")],
            "resolution_source must be non-empty",
        ),
        (
            [UncertaintyEvent(1, "contract", SEVERITY_MEDIUM, 2, None)],
            "resolved_turn_index and resolution_source must be provided together",
        ),
    ],
)
def test_invalid_inputs_raise_clear_errors(events, message):
    with pytest.raises(ValueError, match=message):
        analyze_agent_uncertainty_resolution(events)
