"""Tests for session Edit precision analyzer."""

import pytest

from synthesis.session_edit_precision import (
    EditCall,
    EditPrecisionMetrics,
    Finding,
    analyze_session_edit_precision,
)


class TestAnalyzeSessionEditPrecision:
    """Test main analyzer function."""

    def test_empty_edits_returns_zero_metrics(self):
        """Verify empty edits returns zero metrics."""
        metrics, findings = analyze_session_edit_precision([])
        assert metrics.total_edits == 0
        assert metrics.uniqueness_rate == 0.0
        assert len(findings) == 0

    def test_single_perfect_edit(self):
        """Verify perfect edit produces no findings."""
        edits = [
            EditCall(
                turn_index=1,
                file_path="src/main.py",
                old_string="def foo():",
                new_string="def foo(x):",
                replace_all=False,
                had_prior_read=True,
                old_string_match_count=1,
                has_indentation_error=False,
                has_line_number_prefix=False,
                new_string_is_empty=False,
            )
        ]
        metrics, findings = analyze_session_edit_precision(edits)
        assert metrics.total_edits == 1
        assert metrics.unique_old_string_count == 1
        assert metrics.uniqueness_rate == 100.0
        assert len(findings) == 0

    def test_ambiguous_old_string_warning(self):
        """Verify warning for ambiguous old_string."""
        edits = [
            EditCall(
                turn_index=2,
                file_path="src/test.py",
                old_string="pass",
                new_string="return True",
                replace_all=False,
                had_prior_read=True,
                old_string_match_count=5,
                has_indentation_error=False,
                has_line_number_prefix=False,
                new_string_is_empty=False,
            )
        ]
        metrics, findings = analyze_session_edit_precision(edits)
        assert metrics.ambiguous_old_string_count == 1
        uniqueness_findings = [f for f in findings if f.category == "old_string_uniqueness"]
        assert len(uniqueness_findings) >= 1
        assert "5 locations" in uniqueness_findings[0].message

    def test_no_match_old_string_critical(self):
        """Verify critical finding for no match."""
        edits = [
            EditCall(
                turn_index=3,
                file_path="src/app.py",
                old_string="nonexistent code",
                new_string="new code",
                replace_all=False,
                had_prior_read=True,
                old_string_match_count=0,
                has_indentation_error=False,
                has_line_number_prefix=False,
                new_string_is_empty=False,
            )
        ]
        metrics, findings = analyze_session_edit_precision(edits)
        assert metrics.no_match_old_string_count == 1
        uniqueness_findings = [f for f in findings if f.category == "old_string_uniqueness"]
        assert uniqueness_findings[0].severity == "critical"

    def test_indentation_error_critical(self):
        """Verify critical finding for indentation error."""
        edits = [
            EditCall(
                turn_index=4,
                file_path="src/util.py",
                old_string="def bar():",
                new_string="def bar(y):",
                replace_all=False,
                had_prior_read=True,
                old_string_match_count=1,
                has_indentation_error=True,
                has_line_number_prefix=False,
                new_string_is_empty=False,
            )
        ]
        metrics, findings = analyze_session_edit_precision(edits)
        assert metrics.indentation_error_count == 1
        context_findings = [f for f in findings if f.category == "context_preservation"]
        assert len(context_findings) >= 1
        assert context_findings[0].severity == "critical"

    def test_line_number_prefix_critical(self):
        """Verify critical finding for line number prefix."""
        edits = [
            EditCall(
                turn_index=5,
                file_path="src/data.py",
                old_string="10→    return x",
                new_string="return y",
                replace_all=False,
                had_prior_read=True,
                old_string_match_count=1,
                has_indentation_error=False,
                has_line_number_prefix=True,
                new_string_is_empty=False,
            )
        ]
        metrics, findings = analyze_session_edit_precision(edits)
        assert metrics.line_number_prefix_count == 1
        prefix_findings = [f for f in findings if f.category == "line_number_prefix"]
        assert prefix_findings[0].severity == "critical"

    def test_empty_new_string_warning(self):
        """Verify warning for empty new_string."""
        edits = [
            EditCall(
                turn_index=6,
                file_path="src/clean.py",
                old_string="# TODO: implement",
                new_string="",
                replace_all=False,
                had_prior_read=True,
                old_string_match_count=1,
                has_indentation_error=False,
                has_line_number_prefix=False,
                new_string_is_empty=True,
            )
        ]
        metrics, findings = analyze_session_edit_precision(edits)
        assert metrics.empty_new_string_count == 1
        validity_findings = [f for f in findings if f.category == "new_string_validity"]
        assert validity_findings[0].severity == "warning"

    def test_missing_prior_read_critical(self):
        """Verify critical finding for missing prior Read."""
        edits = [
            EditCall(
                turn_index=7,
                file_path="src/blind.py",
                old_string="old code",
                new_string="new code",
                replace_all=False,
                had_prior_read=False,
                old_string_match_count=1,
                has_indentation_error=False,
                has_line_number_prefix=False,
                new_string_is_empty=False,
            )
        ]
        metrics, findings = analyze_session_edit_precision(edits)
        assert metrics.read_before_edit_count == 0
        assert metrics.read_before_edit_rate == 0.0
        discipline_findings = [f for f in findings if f.category == "read_before_edit_discipline"]
        assert discipline_findings[0].severity == "critical"

    def test_uniqueness_rate_calculation(self):
        """Verify uniqueness rate calculation."""
        edits = [
            EditCall(turn_index=1, file_path="a.py", old_string="x", new_string="y",
                     replace_all=False, had_prior_read=True, old_string_match_count=1,
                     has_indentation_error=False, has_line_number_prefix=False, new_string_is_empty=False),
            EditCall(turn_index=2, file_path="b.py", old_string="z", new_string="w",
                     replace_all=False, had_prior_read=True, old_string_match_count=1,
                     has_indentation_error=False, has_line_number_prefix=False, new_string_is_empty=False),
            EditCall(turn_index=3, file_path="c.py", old_string="a", new_string="b",
                     replace_all=False, had_prior_read=True, old_string_match_count=5,
                     has_indentation_error=False, has_line_number_prefix=False, new_string_is_empty=False),
        ]
        metrics, findings = analyze_session_edit_precision(edits)
        # 2 unique out of 3 = 66.67%
        assert metrics.uniqueness_rate == 66.67

    def test_invalid_edits_type_raises_error(self):
        """Verify invalid edits type raises ValueError."""
        with pytest.raises(ValueError, match="must be a list or tuple"):
            analyze_session_edit_precision("not a list")

    def test_invalid_edit_instance_raises_error(self):
        """Verify invalid edit instance raises ValueError."""
        with pytest.raises(ValueError, match="EditCall instance"):
            analyze_session_edit_precision([{"file_path": "test.py"}])
