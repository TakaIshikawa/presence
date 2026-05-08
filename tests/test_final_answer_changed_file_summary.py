"""Tests for final answer changed file summary analyzer."""

import pytest

from synthesis.final_answer_changed_file_summary import (
    analyze_final_answer_changed_file_summary,
)


def test_empty_changed_files_returns_zeroed_metrics():
    report = analyze_final_answer_changed_file_summary([], "Files modified successfully")

    assert report["changed_file_count"] == 0
    assert report["mentioned_file_count"] == 0
    assert report["omitted_file_count"] == 0
    assert report["mention_rate"] == 0.0
    assert report["omitted_files"] == []


def test_full_path_mentions_count_as_mentioned():
    report = analyze_final_answer_changed_file_summary(
        ["src/utils.py", "tests/test_utils.py"],
        "Updated src/utils.py and tests/test_utils.py with the new logic.",
    )

    assert report["changed_file_count"] == 2
    assert report["mentioned_file_count"] == 2
    assert report["omitted_file_count"] == 0
    assert report["mention_rate"] == 100.0
    assert report["omitted_files"] == []


def test_basename_only_mentions_count_as_mentioned():
    report = analyze_final_answer_changed_file_summary(
        ["src/analysis/validator.py", "tests/test_validator.py"],
        "Modified validator.py and test_validator.py to add the new checks.",
    )

    assert report["changed_file_count"] == 2
    assert report["mentioned_file_count"] == 2
    assert report["omitted_file_count"] == 0
    assert report["mention_rate"] == 100.0


def test_unmentioned_changed_files_are_listed_in_stable_sorted_order():
    report = analyze_final_answer_changed_file_summary(
        ["src/z_last.py", "src/a_first.py", "src/middle.py"],
        "Updated the core module",
    )

    assert report["omitted_file_count"] == 3
    assert report["omitted_files"] == ["src/a_first.py", "src/middle.py", "src/z_last.py"]


def test_mixed_mentioned_and_omitted_files():
    report = analyze_final_answer_changed_file_summary(
        ["src/config.py", "src/parser.py", "tests/test_config.py"],
        "Updated config.py and test_config.py with new logic",
    )

    assert report["changed_file_count"] == 3
    assert report["mentioned_file_count"] == 2  # config.py and test_config.py
    assert report["omitted_file_count"] == 1
    assert report["mention_rate"] == 66.67
    assert report["omitted_files"] == ["src/parser.py"]


def test_whitespace_normalization_in_final_answer():
    report = analyze_final_answer_changed_file_summary(
        ["src/handler.py"],
        "Modified   src/handler.py   with\n\nmultiple   spaces",
    )

    assert report["mentioned_file_count"] == 1
    assert report["omitted_file_count"] == 0


def test_empty_final_answer():
    report = analyze_final_answer_changed_file_summary(
        ["src/example.py"],
        "",
    )

    assert report["mentioned_file_count"] == 0
    assert report["omitted_file_count"] == 1
    assert report["omitted_files"] == ["src/example.py"]


def test_none_changed_files_treated_as_empty():
    report = analyze_final_answer_changed_file_summary(None, "No changes made")

    assert report["changed_file_count"] == 0
    assert report["mentioned_file_count"] == 0


def test_string_changed_files_raises_error():
    with pytest.raises(ValueError, match="changed_files must be a list, not a string"):
        analyze_final_answer_changed_file_summary("src/utils.py", "Updated utils")


def test_non_list_changed_files_raises_error():
    with pytest.raises(ValueError, match="changed_files must be a list"):
        analyze_final_answer_changed_file_summary({"file": "src/utils.py"}, "Updated utils")


def test_non_string_final_answer_raises_error():
    with pytest.raises(ValueError, match="final_answer must be a string"):
        analyze_final_answer_changed_file_summary(["src/utils.py"], None)

    with pytest.raises(ValueError, match="final_answer must be a string"):
        analyze_final_answer_changed_file_summary(["src/utils.py"], 123)


def test_changed_files_with_non_string_items_raises_error():
    with pytest.raises(ValueError, match="changed_files must contain only strings"):
        analyze_final_answer_changed_file_summary(["src/utils.py", 123, "tests/test.py"], "Updated files")


def test_empty_strings_in_changed_files_are_filtered():
    report = analyze_final_answer_changed_file_summary(
        ["src/utils.py", "", "  ", "tests/test.py"],
        "Updated utils.py and test.py",
    )

    assert report["changed_file_count"] == 2
    assert report["mentioned_file_count"] == 2
