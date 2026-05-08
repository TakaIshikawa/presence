"""Tests for final answer changed file summary analysis."""

import pytest

from synthesis.final_answer_changed_file_summary import analyze_final_answer_changed_file_summary


def test_empty_changed_files_returns_zeroed_metrics():
    report = analyze_final_answer_changed_file_summary([], "some final answer text")

    assert report["changed_file_count"] == 0
    assert report["mentioned_file_count"] == 0
    assert report["omitted_file_count"] == 0
    assert report["mention_rate"] == 0.0
    assert report["omitted_files"] == []


def test_full_path_mentions_count_as_mentioned():
    report = analyze_final_answer_changed_file_summary(
        ["src/foo.py", "tests/test_foo.py"],
        "I modified src/foo.py and tests/test_foo.py to add the feature.",
    )

    assert report["mentioned_file_count"] == 2
    assert report["omitted_file_count"] == 0
    assert report["mention_rate"] == 100.0


def test_basename_only_mentions_count_as_mentioned():
    report = analyze_final_answer_changed_file_summary(
        ["src/components/Button.tsx", "tests/Button.test.tsx"],
        "Updated Button.tsx and added tests in Button.test.tsx",
    )

    assert report["mentioned_file_count"] == 2
    assert report["omitted_file_count"] == 0
    assert report["mention_rate"] == 100.0


def test_unmentioned_changed_files_are_listed_in_stable_sorted_order():
    report = analyze_final_answer_changed_file_summary(
        ["z_file.py", "a_file.py", "m_file.py"],
        "Made some changes",
    )

    assert report["omitted_file_count"] == 3
    assert report["omitted_files"] == ["a_file.py", "m_file.py", "z_file.py"]


def test_partial_mentions_are_detected():
    report = analyze_final_answer_changed_file_summary(
        ["src/foo.py", "src/bar.py", "src/baz.py"],
        "Updated foo.py and bar.py",
    )

    assert report["mentioned_file_count"] == 2
    assert report["omitted_file_count"] == 1
    assert report["omitted_files"] == ["src/baz.py"]


def test_repeated_whitespace_in_final_answer_is_normalized():
    report = analyze_final_answer_changed_file_summary(
        ["src/foo.py"],
        "Updated   src/foo.py\n\nwith   multiple   spaces",
    )

    assert report["mentioned_file_count"] == 1


def test_none_changed_files_treated_as_empty():
    report = analyze_final_answer_changed_file_summary(None, "some text")

    assert report["changed_file_count"] == 0
    assert report["omitted_files"] == []


def test_none_final_answer_treated_as_empty_string():
    report = analyze_final_answer_changed_file_summary(["src/foo.py"], None)

    assert report["omitted_file_count"] == 1
    assert report["omitted_files"] == ["src/foo.py"]


def test_invalid_changed_files_type_raises_value_error():
    with pytest.raises(ValueError, match="changed_files must be a list"):
        analyze_final_answer_changed_file_summary(123, "text")


def test_non_string_items_in_changed_files_raises_value_error():
    with pytest.raises(ValueError, match="changed_files must contain only strings"):
        analyze_final_answer_changed_file_summary(["src/foo.py", 123], "text")


def test_non_string_final_answer_raises_value_error():
    with pytest.raises(ValueError, match="final_answer must be a string"):
        analyze_final_answer_changed_file_summary(["src/foo.py"], 123)


def test_duplicate_file_paths_are_normalized():
    report = analyze_final_answer_changed_file_summary(
        ["src/foo.py", "src/foo.py", "src/bar.py"],
        "Updated foo.py",
    )

    assert report["changed_file_count"] == 2
    assert report["mentioned_file_count"] == 1
    assert report["omitted_files"] == ["src/bar.py"]


def test_empty_string_changed_files_are_filtered():
    report = analyze_final_answer_changed_file_summary(
        ["src/foo.py", "", "  ", "src/bar.py"],
        "Updated foo.py",
    )

    assert report["changed_file_count"] == 2
    assert report["mentioned_file_count"] == 1


def test_single_string_changed_files_converted_to_list():
    report = analyze_final_answer_changed_file_summary(
        "src/foo.py",
        "Updated src/foo.py",
    )

    assert report["changed_file_count"] == 1
    assert report["mentioned_file_count"] == 1


def test_empty_string_changed_files_returns_empty():
    report = analyze_final_answer_changed_file_summary("", "some text")

    assert report["changed_file_count"] == 0


def test_whitespace_only_changed_files_returns_empty():
    report = analyze_final_answer_changed_file_summary("   ", "some text")

    assert report["changed_file_count"] == 0


def test_mention_rate_calculation():
    report = analyze_final_answer_changed_file_summary(
        ["a.py", "b.py", "c.py"],
        "Updated a.py and b.py",
    )

    assert report["mention_rate"] == 66.67


def test_case_sensitive_matching():
    report = analyze_final_answer_changed_file_summary(
        ["src/Foo.py"],
        "Updated src/foo.py",
    )

    # Should not match due to case difference
    assert report["omitted_file_count"] == 1


def test_basename_collision_detection():
    # Both files have same basename but different paths
    report = analyze_final_answer_changed_file_summary(
        ["src/foo.py", "tests/foo.py"],
        "Updated foo.py",
    )

    # Both should be counted as mentioned since they share the basename
    assert report["mentioned_file_count"] == 2
    assert report["omitted_file_count"] == 0


def test_newlines_normalized_in_final_answer():
    report = analyze_final_answer_changed_file_summary(
        ["src/foo.py"],
        "I updated\nsrc/foo.py\nwith changes",
    )

    assert report["mentioned_file_count"] == 1


def test_tabs_normalized_in_final_answer():
    report = analyze_final_answer_changed_file_summary(
        ["src/foo.py"],
        "I\tupdated\tsrc/foo.py",
    )

    assert report["mentioned_file_count"] == 1
