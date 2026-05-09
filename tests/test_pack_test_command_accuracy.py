"""Tests for pack test command accuracy analyzer."""

import pytest

from synthesis.pack_test_command_accuracy import analyze_pack_test_command_accuracy


class TestAnalyzePackTestCommandAccuracy:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_test_command_accuracy([])
        assert result["total_tasks"] == 0
        assert result["accuracy_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_test_command_accuracy(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_test_command_accuracy("not a list")

    def test_valid_pytest_command(self):
        """Verify valid pytest command detection."""
        result = analyze_pack_test_command_accuracy([
            {"task_id": "task1", "test_command": "pytest tests/test_foo.py"}
        ])
        assert result["valid_commands"] == 1
        assert result["invalid_syntax_count"] == 0

    def test_invalid_command_syntax(self):
        """Verify invalid command syntax detection."""
        result = analyze_pack_test_command_accuracy([
            {"task_id": "task1", "test_command": "invalid command"}
        ])
        assert result["invalid_syntax_count"] == 1

    def test_anti_pattern_detection(self):
        """Verify anti-pattern detection."""
        result = analyze_pack_test_command_accuracy([
            {"task_id": "task1", "test_command": "cd /tmp && pytest"}
        ])
        assert result["anti_patterns_count"] == 1
