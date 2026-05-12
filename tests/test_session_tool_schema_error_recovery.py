from __future__ import annotations

import pytest

from synthesis.session_tool_schema_error_recovery import (
    analyze_session_tool_schema_error_recovery,
)


def test_empty_input_returns_zeroed_metrics():
    assert analyze_session_tool_schema_error_recovery(None) == {
        "schema_error_count": 0,
        "recovered_count": 0,
        "unrecovered_count": 0,
        "recovery_rate_percent": 0.0,
        "error_type_counts": {
            "invalid_json": 0,
            "missing_required_parameter": 0,
            "wrong_parameter_type": 0,
            "unknown_tool": 0,
            "schema_validation_failure": 0,
        },
        "examples": [],
    }


def test_rejects_non_positive_recovery_window():
    with pytest.raises(ValueError, match="recovery_window must be positive"):
        analyze_session_tool_schema_error_recovery([], recovery_window=0)


def test_rejects_non_list_input():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_tool_schema_error_recovery({"tool": "read"})


def test_counts_recovered_schema_error_for_same_tool_within_window():
    records = [
        {
            "session_id": "s1",
            "tool_calls": [
                {
                    "turn_index": 2,
                    "tool": "functions.exec_command",
                    "status": "error",
                    "error": "missing required parameter: cmd",
                },
                {"turn_index": 3, "tool": "functions.exec_command", "status": "completed", "output": "ok"},
            ],
        }
    ]

    result = analyze_session_tool_schema_error_recovery(records)

    assert result["schema_error_count"] == 1
    assert result["recovered_count"] == 1
    assert result["unrecovered_count"] == 0
    assert result["recovery_rate_percent"] == 100.0
    assert result["error_type_counts"]["missing_required_parameter"] == 1
    assert result["examples"][0]["recovered"] is True
    assert result["examples"][0]["recovery_turn_index"] == 3


def test_classifies_common_error_types_and_unrecovered_window():
    records = [
        {"turn_index": 1, "tool": "read", "status": "error", "error": "Invalid JSON in tool arguments"},
        {"turn_index": 5, "tool": "read", "status": "completed", "result": "too late"},
        {"turn_index": 6, "tool": "write", "error": "invalid type: expected type string"},
        {"turn_index": 7, "tool": "made_up", "error": "Unknown tool: made_up"},
        {"turn_index": 8, "tool": "search", "error": "Schema validation error: invalid arguments"},
    ]

    result = analyze_session_tool_schema_error_recovery(records, recovery_window=2)

    assert result["schema_error_count"] == 4
    assert result["recovered_count"] == 0
    assert result["unrecovered_count"] == 4
    assert result["error_type_counts"] == {
        "invalid_json": 1,
        "missing_required_parameter": 0,
        "wrong_parameter_type": 1,
        "unknown_tool": 1,
        "schema_validation_failure": 1,
    }


def test_recovers_same_command_intent_without_tool_name():
    records = [
        {"turn_index": 1, "command": "read_file {path: 123}", "error": "field required: path must be a string"},
        {"turn_index": 2, "command": "read_file {path: 'src/app.py'}", "status": "completed", "result": "content"},
    ]

    result = analyze_session_tool_schema_error_recovery(records)

    assert result["schema_error_count"] == 1
    assert result["recovered_count"] == 1
    assert result["error_type_counts"]["wrong_parameter_type"] == 1
