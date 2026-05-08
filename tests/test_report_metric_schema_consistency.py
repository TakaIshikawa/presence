"""Schema consistency tests for representative analyzer metric reports."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone

from engagement.content_quality_score import calculate_content_quality_score
from synthesis.idea_realization_lag import IdeaRealizationChain, analyze_idea_realization_lag
from synthesis.session_context_retention import SessionTurn, analyze_session_context_retention
from synthesis.session_tool_usage_density import calculate_session_tool_usage_density


def _jsonable(value):
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def test_session_tool_usage_density_schema_is_stable_and_serializable():
    report = _jsonable(calculate_session_tool_usage_density(["Read", "Edit"], 4))

    assert {"tools_per_turn", "tool_diversity_index", "workflow_pattern", "metrics"} <= report.keys()
    assert isinstance(report["tools_per_turn"], float)
    json.dumps(report)


def test_session_context_retention_schema_is_stable_and_serializable():
    report = _jsonable(
        analyze_session_context_retention(
            [SessionTurn(0, None, ["api"]), SessionTurn(1, 0, ["api", "tests"])]
        )
    )

    assert {"metrics", "coherence_score", "quality_tier", "insights"} <= report.keys()
    assert isinstance(report["coherence_score"], float)
    json.dumps(report)


def test_idea_realization_lag_schema_is_stable_and_serializable():
    now = datetime.now(timezone.utc)
    report = _jsonable(
        analyze_idea_realization_lag(
            [IdeaRealizationChain("idea", now, now + timedelta(days=2), 2.0)]
        )
    )

    assert {"lag_distribution", "velocity", "tier_counts", "orphaned_count"} <= report.keys()
    assert isinstance(report["orphaned_count"], int)
    json.dumps(report)


def test_content_quality_score_schema_is_stable_and_serializable():
    report = _jsonable(
        calculate_content_quality_score(
            views=100,
            likes=5,
            replies=2,
            shares=1,
            reply_depth_avg=2.0,
            published_at=datetime.now(timezone.utc),
            engagement_variance=0.1,
        )
    )

    assert {"score", "tier", "metrics", "component_scores"} <= report.keys()
    assert isinstance(report["score"], float)
    json.dumps(report)
