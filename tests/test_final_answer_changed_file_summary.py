"""Tests for final answer changed file summary analysis."""

import pytest

from synthesis.final_answer_changed_file_summary import (
    analyze_final_answer_changed_file_summary,
)


def test_empty_changed_files_returns_zeroed_metrics():
    report = analyze_final_answer_changed_file_summary([], "Some final answer text")

    assert report["changed_file_count"] == 0
    assert report["mentioned_file_count"] == 0
    assert report["omitted_file_count"] == 0
    assert report["mention_rate"] == 0.0
    assert report["omitted_files"] == []


def test_full_path_mentions_count_as_mentioned():
    report = analyze_final_answer_changed_file_summary(
        ["src/main.py", "tests/test_main.py"],
        "Updated src/main.py and added tests/test_main.py",
    )

    assert report["changed_file_count"] == 2
    assert report["mentioned_file_count"] == 2
    assert report["omitted_file_count"] == 0
    assert report["mention_rate"] == 100.0
    assert report["omitted_files"] == []


def test_basename_only_mentions_count_as_mentioned():
    report = analyze_final_answer_changed_file_summary(
        ["src/utils/helper.py", "tests/test_helper.py"],
        "Modified helper.py and test_helper.py",
    )

    assert report["changed_file_count"] == 2
    assert report["mentioned_file_count"] == 2
    assert report["omitted_file_count"] == 0
    assert report["mention_rate"] == 100.0
    assert report["omitted_files"] == []


def test_unmentioned_changed_files_listed_in_stable_sorted_order():
    report = analyze_final_answer_changed_file_summary(
        ["z_file.py", "a_file.py", "m_file.py"],
        "Updated something",
    )

    assert report["changed_file_count"] == 3
    assert report["mentioned_file_count"] == 0
    assert report["omitted_file_count"] == 3
    assert report["mention_rate"] == 0.0
    assert report["omitted_files"] == ["a_file.py", "m_file.py", "z_file.py"]


def test_partial_mentions():
    report = analyze_final_answer_changed_file_summary(
        ["src/main.py", "src/utils.py", "tests/test_main.py"],
        "Updated main.py and test_main.py",
    )

    assert report["changed_file_count"] == 3
    assert report["mentioned_file_count"] == 2  # main.py and test_main.py
    assert report["omitted_file_count"] == 1
    assert report["mention_rate"] == 66.67
    assert report["omitted_files"] == ["src/utils.py"]


def test_whitespace_normalization_in_final_answer():
    report = analyze_final_answer_changed_file_summary(
        ["src/main.py"],
        "Updated   src/main.py   with\n\nmultiple    spaces",
    )

    assert report["mentioned_file_count"] == 1
    assert report["omitted_file_count"] == 0


def test_duplicate_file_paths_are_normalized():
    report = analyze_final_answer_changed_file_summary(
        ["src/main.py", "src/main.py", "tests/test.py"],
        "Updated main.py",
    )

    assert report["changed_file_count"] == 2  # Duplicates removed
    assert report["mentioned_file_count"] == 1
    assert report["omitted_file_count"] == 1


def test_invalid_changed_files_non_list_raises_error():
    with pytest.raises(ValueError, match="changed_files must be a list of file paths"):
        analyze_final_answer_changed_file_summary("not a list", "Some text")


def test_invalid_changed_files_non_string_item_raises_error():
    with pytest.raises(ValueError, match="changed_files must be a list of file paths"):
        analyze_final_answer_changed_file_summary(
            ["valid.py", 123, "another.py"],
            "Some text",
        )


def test_invalid_final_answer_non_string_raises_error():
    with pytest.raises(ValueError, match="final_answer must be a string"):
        analyze_final_answer_changed_file_summary(["file.py"], 123)


def test_empty_final_answer_string():
    report = analyze_final_answer_changed_file_summary(
        ["src/main.py"],
        "",
    )

    assert report["changed_file_count"] == 1
    assert report["mentioned_file_count"] == 0
    assert report["omitted_file_count"] == 1


def test_none_changed_files_treated_as_empty_list():
    report = analyze_final_answer_changed_file_summary(None, "Some text")

    assert report["changed_file_count"] == 0
    assert report["mentioned_file_count"] == 0
    assert report["omitted_file_count"] == 0


def test_empty_string_file_paths_are_filtered():
    report = analyze_final_answer_changed_file_summary(
        ["src/main.py", "", "  ", "tests/test.py"],
        "Updated main.py",
    )

    assert report["changed_file_count"] == 2  # Empty strings filtered
    assert report["mentioned_file_count"] == 1


def test_basename_collision_handling():
    # If multiple files have the same basename, mentioning the basename
    # should count for all of them
    report = analyze_final_answer_changed_file_summary(
        ["src/utils.py", "lib/utils.py"],
        "Modified utils.py",
    )

    assert report["mentioned_file_count"] == 2
    assert report["omitted_file_count"] == 0
