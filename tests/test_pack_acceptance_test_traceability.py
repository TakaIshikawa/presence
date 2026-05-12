from __future__ import annotations

import pytest

from synthesis.pack_acceptance_test_traceability import (
    analyze_pack_acceptance_test_traceability,
)


def test_empty_input_returns_zeroed_metrics():
    result = analyze_pack_acceptance_test_traceability(None)

    assert result == {
        "total_criteria": 0,
        "traced_criteria": 0,
        "untraced_criteria": 0,
        "traceability_rate_percent": 0.0,
        "signal_type_counts": {
            "test_name": 0,
            "test_file": 0,
            "command_output": 0,
            "summary": 0,
        },
        "examples": [],
    }


def test_rejects_non_list_input():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_acceptance_test_traceability({"acceptanceCriteria": []})


def test_traces_criteria_to_multiple_signal_types():
    records = [
        {
            "task_id": "t1",
            "acceptanceCriteria": [
                "Reports failure reason counts",
                "Handles empty input",
                "Raises ValueError for non-list input",
            ],
            "test_names": ["test_reports_failure_reason_counts"],
            "test_files": ["tests/test_empty_input.py"],
            "command_output": "test_non_list_input_raises_valueerror passed",
            "summary": "Implemented failure reason reporting and empty input behavior.",
        }
    ]

    result = analyze_pack_acceptance_test_traceability(records)

    assert result["total_criteria"] == 3
    assert result["traced_criteria"] == 3
    assert result["untraced_criteria"] == 0
    assert result["traceability_rate_percent"] == 100.0
    assert result["signal_type_counts"]["test_name"] == 1
    assert result["signal_type_counts"]["test_file"] == 1
    assert result["signal_type_counts"]["command_output"] == 1
    assert result["signal_type_counts"]["summary"] == 2


def test_reports_untraced_criteria_examples():
    records = [
        {
            "task_id": "t2",
            "acceptance_criteria": [{"criterion": "Exports CSV download"}],
            "test_names": ["test_json_payload"],
            "summary": "Verified JSON output.",
        }
    ]

    result = analyze_pack_acceptance_test_traceability(records)

    assert result["total_criteria"] == 1
    assert result["traced_criteria"] == 0
    assert result["examples"] == [
        {
            "task_id": "t2",
            "criterion": "Exports CSV download",
            "missing_signal": True,
        }
    ]
