from __future__ import annotations

import pytest

from synthesis.session_test_failure_localization import (
    analyze_session_test_failure_localization,
)


def test_empty_input_returns_zeroed_metrics():
    assert analyze_session_test_failure_localization(None) == {
        "failed_test_commands": 0,
        "localized_failures": 0,
        "unlocalized_failures": 0,
        "localization_rate_percent": 0.0,
        "localization_signal_counts": {
            "failing_file": 0,
            "test_name": 0,
            "traceback_file": 0,
            "assertion_text": 0,
        },
        "examples": [],
    }


def test_rejects_non_list_input():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_test_failure_localization({"tool": "bash"})


def test_localizes_pytest_failure_by_file_and_test_name_before_edit():
    records = [
        {
            "session_id": "s1",
            "tool_calls": [
                {
                    "tool": "bash",
                    "command": "pytest tests/test_widget.py -q",
                    "exit_code": 1,
                    "stdout": "FAILED tests/test_widget.py::test_renders_name\nE AssertionError: assert 'Bob' == 'Alice'",
                },
                {"tool": "bash", "command": "rg test_renders_name tests/test_widget.py"},
                {"tool": "functions.apply_patch", "input": "*** Begin Patch\n"},
            ],
        }
    ]

    result = analyze_session_test_failure_localization(records)

    assert result["failed_test_commands"] == 1
    assert result["localized_failures"] == 1
    assert result["unlocalized_failures"] == 0
    assert result["localization_rate_percent"] == 100.0
    assert result["localization_signal_counts"]["failing_file"] == 1
    assert result["localization_signal_counts"]["test_name"] == 1
    assert result["examples"][0]["localized"] is True


def test_counts_unlocalized_when_edit_happens_before_read():
    records = [
        {"tool": "bash", "command": "pytest", "exit_code": 1, "stderr": "FAILED tests/test_api.py::test_create_user"},
        {"tool": "functions.apply_patch", "input": "*** Begin Patch\n"},
        {"tool": "bash", "command": "rg test_create_user tests/test_api.py"},
    ]

    result = analyze_session_test_failure_localization(records)

    assert result["failed_test_commands"] == 1
    assert result["localized_failures"] == 0
    assert result["unlocalized_failures"] == 1
    assert result["examples"][0]["localized"] is False


def test_recognizes_traceback_file_and_assertion_snippet_searches():
    records = [
        {
            "tool": "bash",
            "command": "python -m pytest tests/test_service.py",
            "exit_code": 1,
            "stderr": "\n".join(
                [
                    'File "src/service.py", line 12, in build',
                    "E AssertionError: assert response.status_code == 201",
                ]
            ),
        },
        {"tool": "bash", "command": "sed -n '1,80p' src/service.py"},
        {"tool": "bash", "command": "rg \"response.status_code\" src tests"},
    ]

    result = analyze_session_test_failure_localization(records)

    assert result["localized_failures"] == 1
    assert result["localization_signal_counts"]["traceback_file"] == 1
    assert result["localization_signal_counts"]["assertion_text"] == 1


def test_ignores_passing_test_commands():
    result = analyze_session_test_failure_localization(
        [{"tool": "bash", "command": "pytest tests/test_ok.py -q", "exit_code": 0, "stdout": "1 passed"}]
    )

    assert result["failed_test_commands"] == 0
