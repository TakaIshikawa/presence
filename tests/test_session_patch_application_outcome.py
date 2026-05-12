from __future__ import annotations

import pytest

from synthesis.session_patch_application_outcome import (
    analyze_session_patch_application_outcome,
)


def test_empty_input_returns_zeroed_metrics():
    assert analyze_session_patch_application_outcome(None) == {
        "total_sessions": 0,
        "patch_attempts": 0,
        "successful_patches": 0,
        "failed_patches": 0,
        "failure_rate_percent": 0.0,
        "failure_reason_counts": {
            "context_mismatch": 0,
            "grammar_format_error": 0,
            "missing_file": 0,
            "permission_denial": 0,
            "unknown": 0,
        },
        "examples": [],
    }
    assert analyze_session_patch_application_outcome([])["patch_attempts"] == 0


def test_rejects_non_list_input():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_patch_application_outcome({"tool": "apply_patch"})


def test_counts_successes_failures_and_sessions():
    records = [
        {"session_id": "s1", "tool": "apply_patch", "success": True},
        {"session_id": "s1", "tool": "apply_patch", "success": False, "stderr": "Hunk did not match"},
        {"session_id": "s2", "tool": "edit", "status": "failed", "error": "No such file: src/missing.py"},
        {"session_id": "s3", "tool": "bash", "stdout": "pytest passed"},
    ]

    result = analyze_session_patch_application_outcome(records)

    assert result["total_sessions"] == 3
    assert result["patch_attempts"] == 3
    assert result["successful_patches"] == 1
    assert result["failed_patches"] == 2
    assert result["failure_rate_percent"] == 66.67
    assert result["failure_reason_counts"]["context_mismatch"] == 1
    assert result["failure_reason_counts"]["missing_file"] == 1


def test_classifies_common_failure_reasons_and_limits_examples():
    records = [
        {"session_id": "s1", "tool": "apply_patch", "success": False, "stderr": "Permission denied"},
        {"session_id": "s2", "tool": "apply_patch", "success": False, "stderr": "invalid patch format"},
        {"session_id": "s3", "tool": "apply_patch", "success": False, "stderr": "unexpected failure"},
        {"session_id": "s4", "tool": "apply_patch", "success": False, "stderr": "patch failed"},
        {"session_id": "s5", "tool": "apply_patch", "success": False, "stderr": "file not found"},
        {"session_id": "s6", "tool": "apply_patch", "success": False, "stderr": "grammar error"},
    ]

    result = analyze_session_patch_application_outcome(records)

    assert result["failure_reason_counts"] == {
        "context_mismatch": 1,
        "grammar_format_error": 2,
        "missing_file": 1,
        "permission_denial": 1,
        "unknown": 1,
    }
    assert len(result["examples"]) == 5
    assert result["examples"][0] == {
        "session_id": "s1",
        "reason": "permission_denial",
        "tool": "apply_patch",
    }


def test_detects_nested_tool_calls_and_patch_text():
    records = [
        {
            "session_id": "s1",
            "tool_calls": [
                {"name": "bash", "success": True},
                {"name": "functions.apply_patch", "status": "completed"},
                {"name": "bash", "success": False, "input": "*** Begin Patch\nbroken", "stderr": "parse error"},
            ],
        }
    ]

    result = analyze_session_patch_application_outcome(records)

    assert result["patch_attempts"] == 2
    assert result["successful_patches"] == 1
    assert result["failed_patches"] == 1
    assert result["failure_reason_counts"]["grammar_format_error"] == 1
