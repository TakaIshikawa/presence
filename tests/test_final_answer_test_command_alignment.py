"""Tests for final answer test command alignment analyzer."""

import pytest

from synthesis.final_answer_test_command_alignment import (
    analyze_final_answer_test_command_alignment,
)


def test_empty_input_returns_zeroed_metrics():
    report = analyze_final_answer_test_command_alignment([])

    assert report["total_sessions"] == 0
    assert report["wrong_path_count"] == 0
    assert report["missing_coverage_count"] == 0
    assert report["generic_command_count"] == 0
    assert report["issue_percentage"] == 0.0
    assert report["examples"] == []


def test_aligned_test_command_has_no_issues():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py"],
            "testCommand": "pytest tests/test_foo.py",
        }
    ])

    assert report["total_sessions"] == 1
    assert report["wrong_path_count"] == 0
    assert report["missing_coverage_count"] == 0
    assert report["generic_command_count"] == 0
    assert report["examples"] == []


def test_wrong_path_flags_issue():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py"],
            "testCommand": "pytest tests/test_bar.py",
        }
    ])

    assert report["wrong_path_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "wrong_path"


def test_missing_coverage_flags_issue():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py", "src/bar.py"],
            "testCommand": "pytest tests/test_foo.py",
        }
    ])

    assert report["missing_coverage_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "missing_coverage"
    assert "bar.py" in report["examples"][0]["details"]


def test_generic_command_for_specific_changes_flags_issue():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py"],
            "testCommand": "pytest",
        }
    ])

    assert report["generic_command_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "generic_command"


def test_generic_command_for_many_changes_does_not_flag():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/a.py", "src/b.py", "src/c.py", "src/d.py"],
            "testCommand": "pytest",
        }
    ])

    assert report["generic_command_count"] == 0


def test_test_files_in_changed_files_are_ignored():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py", "tests/test_foo.py"],
            "testCommand": "pytest tests/test_foo.py",
        }
    ])

    assert report["missing_coverage_count"] == 0


def test_missing_test_command_skips_analysis():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py"],
        }
    ])

    assert report["total_sessions"] == 1
    assert report["wrong_path_count"] == 0


def test_missing_changed_files_skips_analysis():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "testCommand": "pytest tests/test_foo.py",
        }
    ])

    assert report["total_sessions"] == 1
    assert report["wrong_path_count"] == 0


def test_multiple_sessions_analyzed_independently():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py"],
            "testCommand": "pytest tests/test_foo.py",
        },
        {
            "session_id": "sess-2",
            "changed_files": ["src/bar.py"],
            "testCommand": "pytest tests/test_wrong.py",
        }
    ])

    assert report["total_sessions"] == 2
    assert report["wrong_path_count"] == 1


def test_examples_capped_at_five():
    records = []
    for i in range(10):
        records.append({
            "session_id": f"sess-{i}",
            "changed_files": ["src/foo.py"],
            "testCommand": "pytest tests/test_bar.py",
        })

    report = analyze_final_answer_test_command_alignment(records)

    assert report["wrong_path_count"] == 10
    assert len(report["examples"]) == 5


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_final_answer_test_command_alignment({"session_id": "sess-1"})


def test_none_input_returns_zeroed_metrics():
    report = analyze_final_answer_test_command_alignment(None)

    assert report["total_sessions"] == 0


def test_non_dict_records_are_skipped():
    report = analyze_final_answer_test_command_alignment([
        "not a dict",
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py"],
            "testCommand": "pytest tests/test_foo.py",
        }
    ])

    assert report["total_sessions"] == 1


def test_nested_final_answer_structure():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py"],
            "finalAnswer": {"testCommand": "pytest tests/test_foo.py"},
        }
    ])

    assert report["total_sessions"] == 1
    assert report["wrong_path_count"] == 0


def test_issue_percentage_calculation():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py"],
            "testCommand": "pytest tests/test_wrong.py",
        },
        {
            "session_id": "sess-2",
            "changed_files": ["src/bar.py"],
            "testCommand": "pytest tests/test_bar.py",
        }
    ])

    assert report["total_sessions"] == 2
    assert report["wrong_path_count"] == 1
    assert report["issue_percentage"] == 50.0


def test_partial_overlap_allowed():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.py", "src/bar.py"],
            "testCommand": "pytest tests/test_foo.py tests/test_bar.py tests/test_extra.py",
        }
    ])

    # Has overlap with changed files, so not flagged as wrong_path
    assert report["wrong_path_count"] == 0


def test_npm_test_considered_generic():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/foo.js"],
            "testCommand": "npm test",
        }
    ])

    assert report["generic_command_count"] == 1


def test_typescript_file_patterns():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/Button.ts"],
            "testCommand": "jest tests/Button.test.ts",
        }
    ])

    assert report["wrong_path_count"] == 0
    assert report["missing_coverage_count"] == 0


def test_nested_module_paths():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["src/components/Button.py"],
            "testCommand": "pytest tests/test_components_Button.py",
        }
    ])

    assert report["wrong_path_count"] == 0


def test_whitespace_handling():
    report = analyze_final_answer_test_command_alignment([
        {
            "session_id": "sess-1",
            "changed_files": ["  src/foo.py  "],
            "testCommand": "  pytest   tests/test_foo.py  ",
        }
    ])

    assert report["wrong_path_count"] == 0
