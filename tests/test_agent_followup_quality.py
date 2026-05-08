"""Tests for agent follow-up quality analysis."""

import pytest

from synthesis.agent_followup_quality import analyze_agent_followup_quality


def test_empty_input_returns_zero_metrics():
    report = analyze_agent_followup_quality([])

    assert report["total_records"] == 0
    assert report["quality_percentage"] == 0.0


def test_actionable_followup_with_owner_counts_as_quality():
    report = analyze_agent_followup_quality(
        [{"id": "a", "owner": "codex", "followups": ["Rerun pytest after dependency update"]}]
    )

    assert report["actionable_followups"] == 1
    assert report["weak_followups"] == 0


def test_vague_followup_is_reported():
    report = analyze_agent_followup_quality([{"id": "b", "owner": "codex", "followups": ["Maybe later"]}])

    assert report["weak_followups"] == 1
    assert report["examples"][0]["reason"] == "not_actionable"


def test_missing_owner_is_counted():
    report = analyze_agent_followup_quality([{"id": "c", "followups": ["Next verify pytest"]}])

    assert report["missing_owner_count"] == 1
    assert report["examples"][0]["reason"] == "missing_owner"


def test_examples_are_limited():
    report = analyze_agent_followup_quality([{"id": str(i), "followups": ["Maybe later"]} for i in range(7)])

    assert len(report["examples"]) == 5


def test_non_list_input_raises():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_agent_followup_quality({"followups": []})
