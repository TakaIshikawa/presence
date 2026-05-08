"""Tests for session agent tool distribution analyzer."""

import pytest

from synthesis.session_agent_tool_distribution import analyze_session_agent_tool_distribution


class TestAnalyzeSessionAgentToolDistribution:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_agent_tool_distribution([])

        assert result["total_tool_calls"] == 0
        assert result["tool_distribution"] == {}
        assert result["tool_percentages"] == {}
        assert result["anomalies"] == []
        assert result["tool_diversity"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_agent_tool_distribution(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_agent_tool_distribution("not a list")

    def test_single_tool_call_counted(self):
        """Verify single tool call is counted correctly."""
        result = analyze_session_agent_tool_distribution([
            {"tool_name": "Read", "turn_index": 0}
        ])

        assert result["total_tool_calls"] == 1
        assert result["tool_distribution"]["Read"] == 1
        assert result["tool_percentages"]["Read"] == 100.0
        assert result["tool_diversity"] == 1

    def test_multiple_same_tool_calls(self):
        """Verify multiple calls to same tool."""
        result = analyze_session_agent_tool_distribution([
            {"tool_name": "Read", "turn_index": 0},
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
        ])

        assert result["tool_distribution"]["Read"] == 3
        assert result["tool_diversity"] == 1

    def test_multiple_different_tools(self):
        """Verify distribution across multiple tools."""
        result = analyze_session_agent_tool_distribution([
            {"tool_name": "Read", "turn_index": 0},
            {"tool_name": "Write", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Bash", "turn_index": 3},
        ])

        assert result["total_tool_calls"] == 4
        assert result["tool_diversity"] == 4
        assert result["tool_distribution"]["Read"] == 1
        assert result["tool_distribution"]["Write"] == 1

    def test_percentage_calculation(self):
        """Verify percentage calculation."""
        result = analyze_session_agent_tool_distribution([
            {"tool_name": "Read", "turn_index": 0},
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Write", "turn_index": 2},
            {"tool_name": "Edit", "turn_index": 3},
        ])

        assert result["tool_percentages"]["Read"] == 50.0
        assert result["tool_percentages"]["Write"] == 25.0
        assert result["tool_percentages"]["Edit"] == 25.0

    def test_write_before_read_anomaly(self):
        """Verify write before read anomaly detection."""
        result = analyze_session_agent_tool_distribution([
            {"tool_name": "Write", "turn_index": 0},
            {"tool_name": "Read", "turn_index": 1},
        ])

        assert "write_before_read" in result["anomalies"]

    def test_writes_without_reads_anomaly(self):
        """Verify writes without reads anomaly detection."""
        result = analyze_session_agent_tool_distribution([
            {"tool_name": "Write", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
        ])

        assert "writes_without_reads" in result["anomalies"]

    def test_excessive_grep_anomaly(self):
        """Verify excessive grep anomaly detection."""
        result = analyze_session_agent_tool_distribution([
            {"tool_name": "Grep", "turn_index": i}
            for i in range(10)
        ])

        assert "excessive_grep" in result["anomalies"]

    def test_normal_session_no_anomalies(self):
        """Verify normal session has no anomalies."""
        result = analyze_session_agent_tool_distribution([
            {"tool_name": "Read", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Bash", "turn_index": 2},
        ])

        assert result["anomalies"] == []

    def test_balanced_usage_pattern(self):
        """Verify balanced tool usage."""
        tools = ["Read"] * 5 + ["Write"] * 3 + ["Edit"] * 2 + ["Bash"] * 2
        records = [{"tool_name": tool, "turn_index": i} for i, tool in enumerate(tools)]

        result = analyze_session_agent_tool_distribution(records)

        assert result["total_tool_calls"] == 12
        assert result["tool_diversity"] == 4
        assert result["tool_distribution"]["Read"] == 5

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_agent_tool_distribution([
            "not a dict",
            {"tool_name": "Read", "turn_index": 0},
        ])

        assert result["total_tool_calls"] == 1

    def test_missing_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_agent_tool_distribution([
            {"turn_index": 0},
            {"tool_name": "Read", "turn_index": 1},
        ])

        assert result["total_tool_calls"] == 1

    def test_case_insensitive_tool_names(self):
        """Verify tool names are handled case-insensitively for anomalies."""
        result = analyze_session_agent_tool_distribution([
            {"tool_name": "read", "turn_index": 0},
            {"tool_name": "WRITE", "turn_index": 1},
        ])

        # Should not trigger writes_without_reads since we had a read
        assert "writes_without_reads" not in result["anomalies"]
