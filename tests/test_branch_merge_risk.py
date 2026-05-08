"""Tests for branch merge risk analysis."""

from typing import cast

import pytest

from synthesis.branch_merge_risk import BranchTouchRecord, analyze_branch_merge_risk


def _record_with_invalid_changed_files(changed_files: object) -> BranchTouchRecord:
    record = BranchTouchRecord("a", ("src/foo.py",))
    object.__setattr__(record, "changed_files", changed_files)
    return record


def test_independent_branches_with_companion_tests_are_low_risk():
    report = analyze_branch_merge_risk(
        [
            BranchTouchRecord("a", ("src/foo.py",), ("tests/test_foo.py",)),
            BranchTouchRecord("b", ("src/bar.py",), ("tests/test_bar.py",)),
        ]
    )

    assert report.risk_label == "low"
    assert report.test_coverage_ratio == 1.0


def test_shared_hotspots_increase_risk():
    report = analyze_branch_merge_risk(
        [
            BranchTouchRecord("a", ("src/foo.py",), ("tests/test_foo.py",)),
            BranchTouchRecord("b", ("src/foo.py",), ("tests/test_foo.py",)),
        ]
    )

    assert report.shared_file_hotspots == (("src/foo.py", 2),)
    assert report.risk_label == "medium"


def test_missing_companion_tests_increase_risk():
    report = analyze_branch_merge_risk([BranchTouchRecord("a", ("src/foo.py",), ())])

    assert report.test_coverage_ratio == 0.0
    assert report.risk_label == "high"


def test_generated_artifact_touches_are_impact_hints():
    report = analyze_branch_merge_risk([BranchTouchRecord("a", ("coverage/report.json",), ())])

    assert report.impact_hint_branches == 1
    assert report.risk_label == "medium"


def test_hotspot_sorting_is_deterministic_for_equal_counts():
    report = analyze_branch_merge_risk(
        [
            BranchTouchRecord("a", ("src/z.py", "src/a.py"), ("tests/test_z.py", "tests/test_a.py")),
            BranchTouchRecord("b", ("src/z.py", "src/a.py"), ("tests/test_z.py", "tests/test_a.py")),
        ]
    )

    assert report.shared_file_hotspots == (("src/a.py", 2), ("src/z.py", 2))


def test_invalid_records_raise_value_error():
    malformed_record: object = {"branch_name": "a"}

    with pytest.raises(ValueError, match="BranchTouchRecord"):
        analyze_branch_merge_risk(cast(list[BranchTouchRecord], [malformed_record]))
    with pytest.raises(ValueError, match="changed_files"):
        analyze_branch_merge_risk([_record_with_invalid_changed_files(["src/foo.py"])])
