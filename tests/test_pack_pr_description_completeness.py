from __future__ import annotations

import pytest

from synthesis.pack_pr_description_completeness import (
    analyze_pack_pr_description_completeness,
)


def test_empty_input_returns_zeroed_metrics():
    result = analyze_pack_pr_description_completeness([])

    assert result["total_descriptions"] == 0
    assert result["complete_descriptions"] == 0
    assert result["completeness_rate_percent"] == 0.0
    assert all(value == 0 for value in result["component_coverage"].values())
    assert result["examples"] == []


def test_rejects_non_list_input():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_pr_description_completeness({"summary": "done"})


def test_scores_complete_description_from_pr_body():
    records = [
        {
            "pack_id": "p1",
            "pr_body": """
            Summary: adds analyzer outputs.
            Changed files: src/synthesis/foo.py and tests/test_foo.py.
            Tests: pytest tests/test_foo.py -q passed.
            User-visible behavior: CLI output now includes totals.
            Risk/Rollback: low risk, revert commit to rollback.
            Follow-up: none.
            """,
        }
    ]

    result = analyze_pack_pr_description_completeness(records)

    assert result["total_descriptions"] == 1
    assert result["complete_descriptions"] == 1
    assert result["completeness_rate_percent"] == 100.0
    assert result["missing_component_counts"] == {
        "summary": 0,
        "test_evidence": 0,
        "changed_files": 0,
        "user_visible_behavior": 0,
        "risk_rollback": 0,
        "follow_up_items": 0,
    }


def test_accepts_merge_request_final_answer_and_summary_fields():
    records = [
        {
            "pack_id": "p1",
            "merge_request_body": "Summary: changed the API behavior.",
            "final_answer": "Tests passed with pytest. Risk is low.",
            "summary": "Follow-up: add docs later.",
            "changed_files": ["src/api.py"],
        },
        {"pack_id": "p2", "final_answer": "Implemented the helper."},
    ]

    result = analyze_pack_pr_description_completeness(records)

    assert result["total_descriptions"] == 2
    assert result["complete_descriptions"] == 1
    assert result["completeness_rate_percent"] == 50.0
    assert result["component_coverage"]["summary"] == 100.0
    assert result["missing_component_counts"]["test_evidence"] == 1
    assert result["examples"][0]["pack_id"] == "p2"
