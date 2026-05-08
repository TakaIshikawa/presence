"""Tests for session edit operation efficiency analyzer."""

import pytest

from synthesis.session_edit_operation_efficiency import (
    analyze_session_edit_operation_efficiency,
    _calculate_efficiency_score,
    _rate_efficiency,
)


class TestAnalyzeSessionEditOperationEfficiency:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_edit_operation_efficiency([])

        assert result["total_operations"] == 0
        assert result["edit_count"] == 0
        assert result["write_count"] == 0
        assert result["new_file_count"] == 0
        assert result["edit_efficiency_score"] == 0.0
        assert result["efficiency_rating"] == "empty"

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_edit_operation_efficiency(None)
        assert result["total_operations"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_edit_operation_efficiency("not a list")

    def test_all_edits_perfect_efficiency(self):
        """Verify all edit operations result in perfect efficiency."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "edit", "file_path": "src/foo.py", "is_new_file": False, "turn_index": 0},
            {"operation": "edit", "file_path": "src/bar.py", "is_new_file": False, "turn_index": 1},
            {"operation": "edit", "file_path": "src/baz.py", "is_new_file": False, "turn_index": 2},
        ])

        assert result["total_operations"] == 3
        assert result["edit_count"] == 3
        assert result["write_count"] == 0
        assert result["edit_efficiency_score"] == 1.0
        assert result["efficiency_rating"] == "perfect"

    def test_all_writes_low_efficiency(self):
        """Verify all write operations result in low efficiency."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "write", "file_path": "src/foo.py", "is_new_file": False, "turn_index": 0},
            {"operation": "write", "file_path": "src/bar.py", "is_new_file": False, "turn_index": 1},
        ])

        assert result["edit_count"] == 0
        assert result["write_count"] == 2
        assert result["edit_efficiency_score"] == 0.0
        assert result["efficiency_rating"] == "low"

    def test_mixed_edits_and_writes(self):
        """Verify mixed operations calculate correctly."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "edit", "file_path": "src/foo.py", "is_new_file": False},
            {"operation": "edit", "file_path": "src/bar.py", "is_new_file": False},
            {"operation": "edit", "file_path": "src/baz.py", "is_new_file": False},
            {"operation": "write", "file_path": "src/qux.py", "is_new_file": False},
        ])

        assert result["total_operations"] == 4
        assert result["edit_count"] == 3
        assert result["write_count"] == 1
        assert result["edit_efficiency_score"] == 0.75
        assert result["efficiency_rating"] == "high"

    def test_new_files_excluded_from_efficiency(self):
        """Verify new file creations are excluded from efficiency calculation."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "write", "file_path": "src/new.py", "is_new_file": True},
            {"operation": "write", "file_path": "src/another_new.py", "is_new_file": True},
            {"operation": "edit", "file_path": "src/existing.py", "is_new_file": False},
        ])

        assert result["new_file_count"] == 2
        assert result["total_operations"] == 1  # Only existing file edit
        assert result["edit_count"] == 1
        assert result["edit_efficiency_score"] == 1.0

    def test_high_efficiency_rating(self):
        """Verify high efficiency rating (75-99%)."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "edit", "file_path": "a", "is_new_file": False},
            {"operation": "edit", "file_path": "b", "is_new_file": False},
            {"operation": "edit", "file_path": "c", "is_new_file": False},
            {"operation": "write", "file_path": "d", "is_new_file": False},
        ])

        assert result["edit_efficiency_score"] == 0.75
        assert result["efficiency_rating"] == "high"

    def test_medium_efficiency_rating(self):
        """Verify medium efficiency rating (50-74%)."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "edit", "file_path": "a", "is_new_file": False},
            {"operation": "edit", "file_path": "b", "is_new_file": False},
            {"operation": "write", "file_path": "c", "is_new_file": False},
            {"operation": "write", "file_path": "d", "is_new_file": False},
        ])

        assert result["edit_efficiency_score"] == 0.5
        assert result["efficiency_rating"] == "medium"

    def test_low_efficiency_rating(self):
        """Verify low efficiency rating (<50%)."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "edit", "file_path": "a", "is_new_file": False},
            {"operation": "write", "file_path": "b", "is_new_file": False},
            {"operation": "write", "file_path": "c", "is_new_file": False},
            {"operation": "write", "file_path": "d", "is_new_file": False},
        ])

        assert result["edit_efficiency_score"] == 0.25
        assert result["efficiency_rating"] == "low"

    def test_case_insensitive_operation(self):
        """Verify operation names are case-insensitive."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "EDIT", "file_path": "a", "is_new_file": False},
            {"operation": "Write", "file_path": "b", "is_new_file": False},
        ])

        assert result["edit_count"] == 1
        assert result["write_count"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_edit_operation_efficiency([
            "not a dict",
            {"operation": "edit", "file_path": "a", "is_new_file": False},
        ])

        assert result["total_operations"] == 1

    def test_missing_operation_skipped(self):
        """Verify records without operation are skipped."""
        result = analyze_session_edit_operation_efficiency([
            {"file_path": "a", "is_new_file": False},
            {"operation": "edit", "file_path": "b", "is_new_file": False},
        ])

        assert result["total_operations"] == 1


class TestCalculateEfficiencyScore:
    """Test efficiency score calculation helper."""

    def test_zero_total_returns_zero(self):
        """Verify zero total returns 0.0."""
        assert _calculate_efficiency_score(5, 0) == 0.0

    def test_all_edits_returns_one(self):
        """Verify all edits returns 1.0."""
        assert _calculate_efficiency_score(10, 10) == 1.0

    def test_no_edits_returns_zero(self):
        """Verify no edits returns 0.0."""
        assert _calculate_efficiency_score(0, 10) == 0.0

    def test_partial_edits_calculated_correctly(self):
        """Verify partial edits calculate correctly."""
        assert _calculate_efficiency_score(3, 4) == 0.75

    def test_result_rounded_to_three_decimals(self):
        """Verify result is rounded to 3 decimal places."""
        assert _calculate_efficiency_score(1, 3) == 0.333


class TestRateEfficiency:
    """Test efficiency rating helper."""

    def test_empty_rating(self):
        """Verify empty rating for zero total."""
        assert _rate_efficiency(0.0, 0) == "empty"

    def test_perfect_rating(self):
        """Verify perfect rating for score 1.0."""
        assert _rate_efficiency(1.0, 10) == "perfect"

    def test_high_rating(self):
        """Verify high rating for score >= 0.75."""
        assert _rate_efficiency(0.75, 10) == "high"
        assert _rate_efficiency(0.9, 10) == "high"

    def test_medium_rating(self):
        """Verify medium rating for score 0.5-0.74."""
        assert _rate_efficiency(0.5, 10) == "medium"
        assert _rate_efficiency(0.74, 10) == "medium"

    def test_low_rating(self):
        """Verify low rating for score < 0.5."""
        assert _rate_efficiency(0.49, 10) == "low"
        assert _rate_efficiency(0.1, 10) == "low"


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_efficient_workflow(self):
        """Simulate efficient workflow with targeted edits."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "edit", "file_path": "src/main.py", "is_new_file": False},
            {"operation": "edit", "file_path": "src/utils.py", "is_new_file": False},
            {"operation": "edit", "file_path": "tests/test_main.py", "is_new_file": False},
            {"operation": "write", "file_path": "new_file.py", "is_new_file": True},
        ])

        assert result["efficiency_rating"] == "perfect"
        assert result["new_file_count"] == 1

    def test_inefficient_workflow(self):
        """Simulate inefficient workflow with full rewrites."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "write", "file_path": "src/main.py", "is_new_file": False},
            {"operation": "write", "file_path": "src/utils.py", "is_new_file": False},
            {"operation": "write", "file_path": "tests/test_main.py", "is_new_file": False},
        ])

        assert result["efficiency_rating"] == "low"
        assert result["edit_efficiency_score"] == 0.0

    def test_mixed_new_and_existing_files(self):
        """Simulate mixed new file creation and existing file edits."""
        result = analyze_session_edit_operation_efficiency([
            {"operation": "write", "file_path": "new1.py", "is_new_file": True},
            {"operation": "write", "file_path": "new2.py", "is_new_file": True},
            {"operation": "edit", "file_path": "existing1.py", "is_new_file": False},
            {"operation": "edit", "file_path": "existing2.py", "is_new_file": False},
            {"operation": "write", "file_path": "existing3.py", "is_new_file": False},
        ])

        assert result["new_file_count"] == 2
        assert result["total_operations"] == 3
        assert result["edit_count"] == 2
        assert result["edit_efficiency_score"] == pytest.approx(0.667, abs=0.001)
