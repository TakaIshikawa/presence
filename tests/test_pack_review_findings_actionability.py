from __future__ import annotations

import pytest

from synthesis.pack_review_findings_actionability import (
    analyze_pack_review_findings_actionability,
)


def test_empty_input_returns_zeroed_metrics():
    result = analyze_pack_review_findings_actionability(None)

    assert result == {
        "total_findings": 0,
        "actionable_findings": 0,
        "low_actionability_findings": 0,
        "actionability_rate_percent": 0.0,
        "missing_component_counts": {
            "file_reference": 0,
            "line_reference": 0,
            "failure_mode": 0,
            "severity": 0,
            "suggested_fix": 0,
        },
        "examples": [],
    }


def test_rejects_non_list_input():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_review_findings_actionability({"findings": []})


def test_scores_actionable_and_low_actionability_findings():
    records = [
        {
            "pack_id": "p1",
            "findings": [
                {
                    "file": "src/app.py",
                    "line": 42,
                    "severity": "high",
                    "message": "This fails when payload is missing.",
                    "suggested_fix": "Add a guard before indexing.",
                },
                "Looks odd.",
            ],
        }
    ]

    result = analyze_pack_review_findings_actionability(records)

    assert result["total_findings"] == 2
    assert result["actionable_findings"] == 1
    assert result["low_actionability_findings"] == 1
    assert result["actionability_rate_percent"] == 50.0
    assert result["missing_component_counts"]["file_reference"] == 1
    assert result["examples"][0]["pack_id"] == "p1"


def test_accepts_common_finding_keys_and_text_references():
    records = [
        {
            "pack_id": "p1",
            "review_findings": [
                "High: src/parser.py:12 breaks empty input; fix by returning [] early."
            ],
            "comments": [
                "Medium issue in tests/test_parser.py line 9: missing assertion causes regression; add expected output check."
            ],
            "issues": [],
        }
    ]

    result = analyze_pack_review_findings_actionability(records)

    assert result["total_findings"] == 2
    assert result["actionable_findings"] == 2
    assert result["missing_component_counts"] == {
        "file_reference": 0,
        "line_reference": 0,
        "failure_mode": 0,
        "severity": 0,
        "suggested_fix": 0,
    }
