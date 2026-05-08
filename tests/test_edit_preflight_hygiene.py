"""Tests for edit preflight hygiene analysis."""

import pytest

from synthesis.edit_preflight_hygiene import analyze_edit_preflight_hygiene


def test_clean_read_before_edit_flow():
    report = analyze_edit_preflight_hygiene(
        [
            {"session_id": "s1", "tool": "cat", "path": "src/app.py"},
            {"session_id": "s1", "tool": "apply_patch", "path": "src/app.py"},
        ]
    )

    assert report["total_edits"] == 1
    assert report["preflighted_edits"] == 1
    assert report["edit_before_read_violations"] == 0


def test_edit_before_read_violation_is_reported():
    report = analyze_edit_preflight_hygiene([{"session_id": "s1", "tool": "edit", "path": "src/app.py"}])

    assert report["edit_before_read_violations"] == 1
    assert report["examples"][0] == {
        "session_id": "s1",
        "path": "src/app.py",
        "event_index": 0,
        "reason": "edit_before_read",
    }


def test_multi_file_edits_report_one_violation_per_unread_file():
    report = analyze_edit_preflight_hygiene(
        [
            {"session_id": "s1", "tool": "rg", "path": "src/a.py"},
            {"session_id": "s1", "tool": "apply_patch", "files": ["src/a.py", "src/b.py"]},
        ]
    )

    assert report["total_edits"] == 2
    assert report["preflighted_edits"] == 1
    assert report["edit_before_read_violations"] == 1
    assert report["examples"][0]["path"] == "src/b.py"


def test_read_history_is_scoped_by_session():
    report = analyze_edit_preflight_hygiene(
        [
            {"session_id": "s1", "tool": "cat", "path": "src/app.py"},
            {"session_id": "s2", "tool": "write", "path": "src/app.py"},
        ]
    )

    assert report["session_counts"]["s2"]["edit_before_read_violations"] == 1


def test_missing_paths_are_counted():
    report = analyze_edit_preflight_hygiene([{"session_id": "s1", "tool": "apply_patch"}])

    assert report["missing_path_count"] == 1
    assert report["total_edits"] == 0
    assert report["examples"][0]["reason"] == "missing_path"


def test_invalid_input_raises():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_edit_preflight_hygiene({"tool": "apply_patch"})
