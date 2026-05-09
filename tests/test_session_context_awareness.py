"""Tests for session context awareness analyzer."""

import pytest

from synthesis.session_context_awareness import analyze_session_context_awareness


class TestAnalyzeSessionContextAwareness:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_context_awareness([])
        assert result["total_tool_calls"] == 0
        assert result["redundant_read_count"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_context_awareness(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_context_awareness("not a list")

    def test_single_read_not_redundant(self):
        """Verify single read is not redundant."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "file_path": "main.py"}
        ])
        assert result["read_call_count"] == 1
        assert result["redundant_read_count"] == 0

    def test_redundant_read_detected(self):
        """Verify redundant re-reads are detected."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "file_path": "main.py"},
            {"tool_name": "Read", "file_path": "main.py"},
        ])
        assert result["redundant_read_count"] == 1

    def test_context_reference_tracking(self):
        """Verify context reference tracking."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "references_prior_context": True},
        ])
        assert result["context_references"] == 1
