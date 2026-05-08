"""Tests for execution pack file ownership overlap analysis."""

import pytest

from synthesis.pack_file_ownership import analyze_pack_file_ownership


def test_no_overlap_reports_no_conflicts():
    report = analyze_pack_file_ownership(
        [
            {"title": "A", "executionPack": "pack", "expectedFiles": ["src/a.py"]},
            {"title": "B", "executionPack": "pack", "expectedFiles": ["src/b.py"]},
        ]
    )

    assert report["overlapping_file_count"] == 0
    assert report["conflict_pairs"] == []


def test_two_task_overlap_in_same_pack_creates_conflict_pair():
    report = analyze_pack_file_ownership(
        [
            {"title": "A", "executionPack": "pack", "expectedFiles": ["src/a.py"]},
            {"title": "B", "executionPack": "pack", "expectedFiles": ["src/a.py"]},
        ]
    )

    assert report["per_pack"]["pack"]["overlapping_file_count"] == 1
    assert report["conflict_pairs"] == [{"pack": "pack", "tasks": ["A", "B"], "files": ["src/a.py"]}]


def test_repeated_file_names_are_normalized_per_task():
    report = analyze_pack_file_ownership(
        [{"title": "A", "executionPack": "pack", "expectedFiles": ["./src/a.py", "src/a.py"]}]
    )

    assert report["per_pack"]["pack"]["overlapping_file_count"] == 0


def test_tasks_in_different_packs_do_not_conflict():
    report = analyze_pack_file_ownership(
        [
            {"title": "A", "executionPack": {"key": "one"}, "expectedFiles": ["src/a.py"]},
            {"title": "B", "executionPack": {"key": "two"}, "expectedFiles": ["src/a.py"]},
        ]
    )

    assert report["overlapping_file_count"] == 0
    assert report["conflict_pairs"] == []


def test_missing_pack_key_falls_back_to_unknown():
    report = analyze_pack_file_ownership([{"title": "A", "expectedFiles": ["src/a.py"]}])

    assert "unknown" in report["per_pack"]


def test_high_risk_file_requires_three_tasks():
    report = analyze_pack_file_ownership(
        [
            {"title": "A", "executionPack": "pack", "expectedFiles": ["src/a.py"]},
            {"title": "B", "executionPack": "pack", "expectedFiles": ["src/a.py"]},
            {"title": "C", "executionPack": "pack", "expectedFiles": ["src/a.py"]},
        ]
    )

    assert report["high_risk_files"][0]["file"] == "src/a.py"
    assert report["high_risk_files"][0]["task_count"] == 3


def test_malformed_expected_files_and_invalid_top_level_input():
    report = analyze_pack_file_ownership([{"title": "A", "executionPack": "pack", "expectedFiles": 42}])

    assert report["per_pack"]["pack"]["overlapping_file_count"] == 0

    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_file_ownership({"title": "A"})
