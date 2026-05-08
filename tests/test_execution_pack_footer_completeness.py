"""Tests for execution pack footer completeness analysis."""

import pytest

from synthesis.execution_pack_footer_completeness import analyze_execution_pack_footer_completeness


COMPLETE_FOOTER = """
Outcome: completed
Changed files: src/example.py
Verification: uv run pytest tests/test_example.py
Residual risk: none remaining
"""


def test_complete_footer_scores_one_hundred_percent():
    report = analyze_execution_pack_footer_completeness([{"id": "a", "final_message": COMPLETE_FOOTER}])

    assert report["complete_records"] == 1
    assert report["average_completeness_percentage"] == 100.0
    assert report["weak_examples"] == []


def test_missing_footer_reports_all_fields():
    report = analyze_execution_pack_footer_completeness([{"id": "missing"}])

    assert report["complete_records"] == 0
    assert report["weak_examples"][0]["missing_fields"] == [
        "outcome",
        "changed_files",
        "verification",
        "residual_risk",
    ]


def test_partial_footer_reports_exact_missing_sections():
    report = analyze_execution_pack_footer_completeness(
        [{"id": "partial", "summary": "Outcome: completed\nVerification: pytest"}]
    )

    assert report["record_summaries"][0]["missing_fields"] == ["changed_files", "residual_risk"]
    assert report["average_completeness_percentage"] == 50.0


def test_alternate_field_names_are_scanned():
    report = analyze_execution_pack_footer_completeness([{"id": "b", "batch_footer": COMPLETE_FOOTER}])

    assert report["complete_records"] == 1


def test_multiple_sessions_score_average_percentage():
    report = analyze_execution_pack_footer_completeness(
        [
            {"id": "a", "final_message": COMPLETE_FOOTER},
            {"id": "b", "final_message": "Outcome: completed\nVerification: pytest"},
        ]
    )

    assert report["total_records"] == 2
    assert report["average_completeness_percentage"] == 75.0


def test_invalid_top_level_input_raises():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_execution_pack_footer_completeness({"final_message": COMPLETE_FOOTER})
