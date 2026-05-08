"""Tests for execution pack expected file drift analysis."""

import pytest

from synthesis.execution_pack_expected_file_drift import analyze_execution_pack_expected_file_drift


def test_empty_input_returns_zeroed_metrics():
    report = analyze_execution_pack_expected_file_drift([])

    assert report["task_count"] == 0
    assert report["tasks_with_unexpected_files"] == 0
    assert report["unexpected_file_count"] == 0
    assert report["missing_expected_file_count"] == 0
    assert report["drift_rate"] == 0.0
    assert report["examples"] == []


def test_tasks_with_changed_files_entirely_inside_expected_files_are_in_scope():
    report = analyze_execution_pack_expected_file_drift([
        {
            "task_id": "task-001",
            "expected_files": ["src/foo.py", "src/bar.py"],
            "changed_files": ["src/foo.py", "src/bar.py"],
        },
        {
            "task_id": "task-002",
            "expected_files": ["tests/test_foo.py"],
            "changed_files": ["tests/test_foo.py"],
        },
    ])

    assert report["task_count"] == 2
    assert report["tasks_with_unexpected_files"] == 0
    assert report["unexpected_file_count"] == 0
    assert report["drift_rate"] == 0.0


def test_tasks_with_changed_files_outside_expected_files_report_unexpected_files():
    report = analyze_execution_pack_expected_file_drift([
        {
            "task_id": "task-001",
            "expected_files": ["src/foo.py"],
            "changed_files": ["src/foo.py", "src/bar.py", "src/baz.py"],
        }
    ])

    assert report["tasks_with_unexpected_files"] == 1
    assert report["unexpected_file_count"] == 2
    assert report["drift_rate"] == 100.0
    assert report["examples"][0]["task_id"] == "task-001"
    assert report["examples"][0]["unexpected_files"] == ["src/bar.py", "src/baz.py"]


def test_tasks_with_expected_files_never_changed_report_missing_expected_files():
    report = analyze_execution_pack_expected_file_drift([
        {
            "task_id": "task-001",
            "expected_files": ["src/foo.py", "tests/test_foo.py"],
            "changed_files": ["src/foo.py"],
        }
    ])

    assert report["missing_expected_file_count"] == 1
    assert report["examples"][0]["task_id"] == "task-001"
    assert report["examples"][0]["missing_expected_files"] == ["tests/test_foo.py"]
    assert report["examples"][0]["unexpected_files"] == []


def test_duplicate_file_paths_are_normalized():
    report = analyze_execution_pack_expected_file_drift([
        {
            "task_id": "task-001",
            "expected_files": ["src/foo.py", "src/foo.py", "src/bar.py"],
            "changed_files": ["src/foo.py", "src/foo.py"],
        }
    ])

    assert report["task_count"] == 1
    assert report["missing_expected_file_count"] == 1
    assert report["examples"][0]["missing_expected_files"] == ["src/bar.py"]


def test_missing_task_id_raises_value_error():
    with pytest.raises(ValueError, match="missing task_id"):
        analyze_execution_pack_expected_file_drift([
            {
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py"],
            }
        ])


def test_empty_task_id_raises_value_error():
    with pytest.raises(ValueError, match="missing task_id"):
        analyze_execution_pack_expected_file_drift([
            {
                "task_id": "",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py"],
            }
        ])


def test_whitespace_only_task_id_raises_value_error():
    with pytest.raises(ValueError, match="missing task_id"):
        analyze_execution_pack_expected_file_drift([
            {
                "task_id": "   ",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py"],
            }
        ])


def test_non_mapping_record_raises_value_error():
    with pytest.raises(ValueError, match="record at index 0 is not a dictionary"):
        analyze_execution_pack_expected_file_drift(["not a mapping"])


def test_non_list_input_raises_value_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_execution_pack_expected_file_drift({"task_id": "task-001"})


def test_non_sequence_expected_files_raises_value_error():
    with pytest.raises(ValueError, match="invalid expected_files type"):
        analyze_execution_pack_expected_file_drift([
            {
                "task_id": "task-001",
                "expected_files": 123,
                "changed_files": [],
            }
        ])


def test_non_sequence_changed_files_raises_value_error():
    with pytest.raises(ValueError, match="invalid changed_files type"):
        analyze_execution_pack_expected_file_drift([
            {
                "task_id": "task-001",
                "expected_files": [],
                "changed_files": {"not": "a list"},
            }
        ])


def test_non_string_items_in_expected_files_raises_value_error():
    with pytest.raises(ValueError, match="non-string item in expected_files"):
        analyze_execution_pack_expected_file_drift([
            {
                "task_id": "task-001",
                "expected_files": ["src/foo.py", 123],
                "changed_files": [],
            }
        ])


def test_non_string_items_in_changed_files_raises_value_error():
    with pytest.raises(ValueError, match="non-string item in changed_files"):
        analyze_execution_pack_expected_file_drift([
            {
                "task_id": "task-001",
                "expected_files": [],
                "changed_files": ["src/foo.py", None],
            }
        ])


def test_examples_are_capped_at_five():
    records = [
        {
            "task_id": f"task-{i:03d}",
            "expected_files": ["expected.py"],
            "changed_files": ["unexpected.py"],
        }
        for i in range(7)
    ]

    report = analyze_execution_pack_expected_file_drift(records)

    assert report["tasks_with_unexpected_files"] == 7
    assert len(report["examples"]) == 5
    assert [ex["task_id"] for ex in report["examples"]] == [
        "task-000",
        "task-001",
        "task-002",
        "task-003",
        "task-004",
    ]


def test_missing_expected_and_unexpected_files_in_same_task():
    report = analyze_execution_pack_expected_file_drift([
        {
            "task_id": "task-001",
            "expected_files": ["src/foo.py", "tests/test_foo.py"],
            "changed_files": ["src/foo.py", "src/bar.py"],
        }
    ])

    assert report["tasks_with_unexpected_files"] == 1
    assert report["unexpected_file_count"] == 1
    assert report["missing_expected_file_count"] == 1
    assert report["examples"][0]["unexpected_files"] == ["src/bar.py"]
    assert report["examples"][0]["missing_expected_files"] == ["tests/test_foo.py"]


def test_none_expected_files_treated_as_empty_list():
    report = analyze_execution_pack_expected_file_drift([
        {
            "task_id": "task-001",
            "expected_files": None,
            "changed_files": ["src/foo.py"],
        }
    ])

    assert report["unexpected_file_count"] == 1
    assert report["examples"][0]["unexpected_files"] == ["src/foo.py"]


def test_none_changed_files_treated_as_empty_list():
    report = analyze_execution_pack_expected_file_drift([
        {
            "task_id": "task-001",
            "expected_files": ["src/foo.py"],
            "changed_files": None,
        }
    ])

    assert report["missing_expected_file_count"] == 1
    assert report["examples"][0]["missing_expected_files"] == ["src/foo.py"]


def test_string_expected_files_converted_to_list():
    report = analyze_execution_pack_expected_file_drift([
        {
            "task_id": "task-001",
            "expected_files": "src/foo.py",
            "changed_files": ["src/foo.py"],
        }
    ])

    assert report["task_count"] == 1
    assert report["tasks_with_unexpected_files"] == 0


def test_string_changed_files_converted_to_list():
    report = analyze_execution_pack_expected_file_drift([
        {
            "task_id": "task-001",
            "expected_files": ["src/foo.py"],
            "changed_files": "src/foo.py",
        }
    ])

    assert report["task_count"] == 1
    assert report["missing_expected_file_count"] == 0


def test_drift_rate_calculation():
    report = analyze_execution_pack_expected_file_drift([
        {
            "task_id": "task-001",
            "expected_files": ["src/foo.py"],
            "changed_files": ["src/foo.py", "src/bar.py"],
        },
        {
            "task_id": "task-002",
            "expected_files": ["src/baz.py"],
            "changed_files": ["src/baz.py"],
        },
        {
            "task_id": "task-003",
            "expected_files": ["src/qux.py"],
            "changed_files": ["src/qux.py"],
        },
    ])

    assert report["tasks_with_unexpected_files"] == 1
    assert report["task_count"] == 3
    assert report["drift_rate"] == 33.33
