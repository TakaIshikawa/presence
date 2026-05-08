"""Tests for agent uncertainty resolution analysis."""

import pytest

from engagement.agent_uncertainty_resolution import (
    QUALITY_CRITICAL,
    QUALITY_MODERATE,
    QUALITY_NO_UNCERTAINTIES,
    QUALITY_STRONG,
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
    assert analysis.insights == ("No uncertainty events supplied - nothing to resolve.",)


def test_fully_resolved_uncertainties_are_strong_quality():
    analysis = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(
                turn_index=1,
                uncertainty_type="api_contract",
                severity="medium",
                resolved_turn_index=2,
                resolution_source="official_docs",
            ),
            UncertaintyEvent(
                turn_index=3,
                uncertainty_type="test_status",
                severity="low",
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
    assert "Resolved 2 of 2 uncertainties (100.0%)." in analysis.insights


def test_unresolved_high_severity_degrades_quality():
    analysis = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(
                turn_index=1,
                uncertainty_type="dependency_behavior",
                severity="medium",
                resolved_turn_index=2,
                resolution_source="local_test",
            ),
            UncertaintyEvent(
                turn_index=3,
                uncertainty_type="data_loss_risk",
                severity="high",
            ),
            UncertaintyEvent(
                turn_index=4,
                uncertainty_type="edge_case",
                severity="low",
                resolved_turn_index=5,
                resolution_source="code_read",
            ),
        ]
    )

    assert analysis.metrics.resolution_rate == 0.667
    assert analysis.metrics.high_severity_unresolved_count == 1
    assert analysis.quality_tier == QUALITY_CRITICAL
    assert any("high-severity uncertainties remained unresolved" in insight for insight in analysis.insights)


def test_mixed_source_distribution_is_deterministic():
    analysis = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(1, "contract", "medium", 2, "docs"),
            UncertaintyEvent(2, "regression", "medium", 4, "tests"),
            UncertaintyEvent(5, "api", "low", 6, "docs"),
        ]
    )

    assert analysis.metrics.source_distribution == (
        ResolutionSourceCount(source="docs", count=2),
        ResolutionSourceCount(source="tests", count=1),
    )
    assert any("Most resolutions came from docs" in insight for insight in analysis.insights)


def test_latency_rounding_and_moderate_quality_threshold():
    analysis = analyze_agent_uncertainty_resolution(
        [
            UncertaintyEvent(1, "first", "low", 3, "logs"),
            UncertaintyEvent(4, "second", "medium", 7, "tests"),
            UncertaintyEvent(8, "third", "low", 12, "docs"),
            UncertaintyEvent(13, "fourth", "low"),
        ]
    )

    assert analysis.metrics.resolution_rate == 0.75
    assert analysis.metrics.average_resolution_latency == 3.0
    assert analysis.quality_tier == QUALITY_MODERATE


@pytest.mark.parametrize(
    ("events", "message"),
    [
        ("bad", "events must be a list or tuple"),
        ([object()], "events must contain UncertaintyEvent instances"),
        ([UncertaintyEvent(-1, "contract", "low")], "turn_index must be a non-negative integer"),
        (
            [UncertaintyEvent(2, "later", "low"), UncertaintyEvent(1, "earlier", "low")],
            "turn_index values must be ordered",
        ),
        ([UncertaintyEvent(1, "", "low")], "uncertainty_type must be a non-empty string"),
        ([UncertaintyEvent(1, "contract", "urgent")], "unsupported severity"),
        (
            [UncertaintyEvent(3, "contract", "medium", 2, "docs")],
            "resolved_turn_index must be greater than or equal to turn_index",
        ),
        (
            [UncertaintyEvent(1, "contract", "medium", 2, 123)],
            "resolution_source must be a string",
        ),
        (
            [UncertaintyEvent(1, "contract", "medium", 2, "")],
            "resolution_source must be non-empty",
        ),
        (
            [UncertaintyEvent(1, "contract", "medium", 2, None)],
            "resolved_turn_index and resolution_source must be provided together",
        ),
    ],
)
def test_invalid_inputs_raise_clear_errors(events, message):
    with pytest.raises(ValueError, match=message):
        analyze_agent_uncertainty_resolution(events)
