"""Tests for execution pack expected file drift analysis."""

import pytest

from synthesis.execution_pack_expected_file_drift import (
    analyze_execution_pack_expected_file_drift,
)


def test_empty_input_returns_zeroed_metrics():
    report = analyze_execution_pack_expected_file_drift([])

    assert report["task_count"] == 0
    assert report["tasks_with_unexpected_files"] == 0
    assert report["unexpected_file_count"] == 0
    assert report["missing_expected_file_count"] == 0
    assert report["drift_rate"] == 0.0
    assert report["examples"] == []


def test_changed_files_entirely_inside_expected_files():
    report = analyze_execution_pack_expected_file_drift(
        [
            {
                "task_id": "task1",
                "expected_files": ["src/main.py", "tests/test_main.py"],
                "changed_files": ["src/main.py", "tests/test_main.py"],
            }
        ]
    )

    assert report["task_count"] == 1
    assert report["tasks_with_unexpected_files"] == 0
    assert report["unexpected_file_count"] == 0
    assert report["missing_expected_file_count"] == 0
    assert report["drift_rate"] == 0.0
    assert report["examples"] == []


def test_changed_files_outside_expected_files():
    report = analyze_execution_pack_expected_file_drift(
        [
            {
                "task_id": "task1",
                "expected_files": ["src/main.py"],
                "changed_files": ["src/main.py", "src/utils.py", "README.md"],
            }
        ]
    )

    assert report["task_count"] == 1
    assert report["tasks_with_unexpected_files"] == 1
    assert report["unexpected_file_count"] == 2
    assert report["missing_expected_file_count"] == 0
    assert report["drift_rate"] == 100.0
    assert len(report["examples"]) == 1
    assert set(report["examples"][0]["unexpected_files"]) == {"src/utils.py", "README.md"}
    assert report["examples"][0]["missing_expected_files"] == []


def test_expected_files_never_changed():
    report = analyze_execution_pack_expected_file_drift(
        [
            {
                "task_id": "task1",
                "expected_files": ["src/main.py", "tests/test_main.py"],
                "changed_files": ["src/utils.py"],
            }
        ]
    )

    assert report["task_count"] == 1
    assert report["tasks_with_unexpected_files"] == 1
    assert report["unexpected_file_count"] == 1
    assert report["missing_expected_file_count"] == 2
    assert len(report["examples"]) == 1
    assert report["examples"][0]["unexpected_files"] == ["src/utils.py"]
    assert set(report["examples"][0]["missing_expected_files"]) == {
        "src/main.py",
        "tests/test_main.py",
    }


def test_duplicate_file_paths_normalized():
    report = analyze_execution_pack_expected_file_drift(
        [
            {
                "task_id": "task1",
                "expected_files": ["src/main.py", "src/main.py"],
                "changed_files": ["src/main.py", "src/main.py", "src/utils.py"],
            }
        ]
    )

    assert report["unexpected_file_count"] == 1  # Only utils.py is unexpected
    assert report["missing_expected_file_count"] == 0


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list of task dictionaries"):
        analyze_execution_pack_expected_file_drift({"task_id": "task1"})


def test_non_mapping_record_raises_error():
    with pytest.raises(ValueError, match="records must be a list of task dictionaries"):
        analyze_execution_pack_expected_file_drift(["not a dict"])


def test_missing_task_id_raises_error():
    with pytest.raises(ValueError, match="task_id must be a non-empty string"):
        analyze_execution_pack_expected_file_drift(
            [
                {
                    "expected_files": ["src/main.py"],
                    "changed_files": ["src/main.py"],
                }
            ]
        )


def test_empty_task_id_raises_error():
    with pytest.raises(ValueError, match="task_id must be a non-empty string"):
        analyze_execution_pack_expected_file_drift(
            [
                {
                    "task_id": "",
                    "expected_files": ["src/main.py"],
                    "changed_files": ["src/main.py"],
                }
            ]
        )


def test_non_sequence_expected_files_raises_error():
    with pytest.raises(ValueError, match="expected_files and changed_files must be sequences"):
        analyze_execution_pack_expected_file_drift(
            [
                {
                    "task_id": "task1",
                    "expected_files": "src/main.py",
                    "changed_files": ["src/main.py"],
                }
            ]
        )


def test_non_sequence_changed_files_raises_error():
    with pytest.raises(ValueError, match="expected_files and changed_files must be sequences"):
        analyze_execution_pack_expected_file_drift(
            [
                {
                    "task_id": "task1",
                    "expected_files": ["src/main.py"],
                    "changed_files": "src/main.py",
                }
            ]
        )


def test_non_string_file_path_raises_error():
    with pytest.raises(ValueError, match="file paths must be strings"):
        analyze_execution_pack_expected_file_drift(
            [
                {
                    "task_id": "task1",
                    "expected_files": ["src/main.py", 123],
                    "changed_files": ["src/main.py"],
                }
            ]
        )


def test_none_expected_files_treated_as_empty():
    report = analyze_execution_pack_expected_file_drift(
        [
            {
                "task_id": "task1",
                "expected_files": None,
                "changed_files": ["src/main.py"],
            }
        ]
    )

    assert report["unexpected_file_count"] == 1
    assert report["missing_expected_file_count"] == 0


def test_none_changed_files_treated_as_empty():
    report = analyze_execution_pack_expected_file_drift(
        [
            {
                "task_id": "task1",
                "expected_files": ["src/main.py"],
                "changed_files": None,
            }
        ]
    )

    assert report["unexpected_file_count"] == 0
    assert report["missing_expected_file_count"] == 1


def test_examples_capped_at_five():
    records = [
        {
            "task_id": f"task{i}",
            "expected_files": ["src/main.py"],
            "changed_files": ["src/utils.py"],
        }
        for i in range(7)
    ]

    report = analyze_execution_pack_expected_file_drift(records)

    assert report["tasks_with_unexpected_files"] == 7
    assert len(report["examples"]) == 5


def test_multiple_tasks_with_mixed_drift():
    report = analyze_execution_pack_expected_file_drift(
        [
            {
                "task_id": "task1",
                "expected_files": ["src/main.py"],
                "changed_files": ["src/main.py"],
            },
            {
                "task_id": "task2",
                "expected_files": ["src/utils.py"],
                "changed_files": ["src/utils.py", "README.md"],
            },
            {
                "task_id": "task3",
                "expected_files": ["src/config.py", "tests/test_config.py"],
                "changed_files": ["src/config.py"],
            },
        ]
    )

    assert report["task_count"] == 3
    assert report["tasks_with_unexpected_files"] == 2
    assert report["unexpected_file_count"] == 1  # README.md
    assert report["missing_expected_file_count"] == 1  # tests/test_config.py
    assert report["drift_rate"] == 66.67
    assert len(report["examples"]) == 2


def test_partial_overlap():
    report = analyze_execution_pack_expected_file_drift(
        [
            {
                "task_id": "task1",
                "expected_files": ["src/a.py", "src/b.py", "src/c.py"],
                "changed_files": ["src/b.py", "src/c.py", "src/d.py"],
            }
        ]
    )

    assert report["unexpected_file_count"] == 1  # src/d.py
    assert report["missing_expected_file_count"] == 1  # src/a.py
    assert len(report["examples"]) == 1
    assert report["examples"][0]["unexpected_files"] == ["src/d.py"]
    assert report["examples"][0]["missing_expected_files"] == ["src/a.py"]


def test_empty_string_file_paths_filtered():
    report = analyze_execution_pack_expected_file_drift(
        [
            {
                "task_id": "task1",
                "expected_files": ["src/main.py", "", "  "],
                "changed_files": ["src/main.py"],
            }
        ]
    )

    assert report["unexpected_file_count"] == 0
    assert report["missing_expected_file_count"] == 0
