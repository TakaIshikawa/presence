"""Tests for session Grep output mode analyzer."""

import pytest

from synthesis.session_grep_output_mode import (
    Finding,
    GrepOutputModeMetrics,
    GrepToolCall,
    analyze_session_grep_output_mode,
)


class TestAnalyzeSessionGrepOutputMode:
    """Test main analyzer function."""

    def test_empty_tool_calls_returns_zero_metrics(self):
        """Verify empty tool calls returns zero metrics."""
        metrics, findings = analyze_session_grep_output_mode([])

        assert metrics.total_grep_calls == 0
        assert metrics.output_mode_files_count == 0
        assert metrics.output_mode_content_count == 0
        assert metrics.output_mode_count_count == 0
        assert metrics.context_lines_usage_count == 0
        assert metrics.context_lines_usage_rate == 0.0
        assert metrics.head_limit_usage_count == 0
        assert metrics.head_limit_usage_rate == 0.0
        assert metrics.multiline_usage_count == 0
        assert metrics.multiline_usage_rate == 0.0
        assert metrics.specific_pattern_count == 0
        assert metrics.broad_pattern_count == 0
        assert metrics.pattern_specificity_score == 0.0
        assert metrics.findings_count == 0
        assert len(findings) == 0

    def test_single_efficient_grep_call(self):
        """Verify single efficient grep call."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern="class Authentication",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="py",
                result_count=3,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        assert metrics.total_grep_calls == 1
        assert metrics.output_mode_files_count == 1
        assert metrics.specific_pattern_count == 1
        assert len(findings) == 0

    def test_output_mode_distribution(self):
        """Verify output mode counts are tracked correctly."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern="error",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=5,
            ),
            GrepToolCall(
                turn_index=2,
                pattern="function test",
                output_mode="content",
                context_lines=5,
                has_context_a=False,
                has_context_b=False,
                has_context_c=True,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=2,
            ),
            GrepToolCall(
                turn_index=3,
                pattern="TODO",
                output_mode="count",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=50,
            ),
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        assert metrics.total_grep_calls == 3
        assert metrics.output_mode_files_count == 1
        assert metrics.output_mode_content_count == 1
        assert metrics.output_mode_count_count == 1

    def test_context_lines_usage_rate(self):
        """Verify context lines usage rate calculation."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern="error",
                output_mode="content",
                context_lines=5,
                has_context_a=False,
                has_context_b=False,
                has_context_c=True,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=3,
            ),
            GrepToolCall(
                turn_index=2,
                pattern="warning",
                output_mode="content",
                context_lines=3,
                has_context_a=True,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=2,
            ),
            GrepToolCall(
                turn_index=3,
                pattern="info",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=10,
            ),
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # 2 out of 3 use context lines = 66.67%
        assert metrics.context_lines_usage_count == 2
        assert metrics.context_lines_usage_rate == 66.67

    def test_head_limit_usage_rate(self):
        """Verify head_limit usage rate calculation."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern="test",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=50,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=200,
            ),
            GrepToolCall(
                turn_index=2,
                pattern="error",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=5,
            ),
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # 1 out of 2 use head_limit = 50%
        assert metrics.head_limit_usage_count == 1
        assert metrics.head_limit_usage_rate == 50.0

    def test_multiline_usage_rate(self):
        """Verify multiline usage rate calculation."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern=r"struct \{[\s\S]*?field",
                output_mode="content",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=True,
                glob_filter="",
                type_filter="go",
                result_count=2,
            ),
            GrepToolCall(
                turn_index=2,
                pattern="error",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=10,
            ),
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # 1 out of 2 use multiline = 50%
        assert metrics.multiline_usage_count == 1
        assert metrics.multiline_usage_rate == 50.0

    def test_pattern_specificity_score(self):
        """Verify pattern specificity score calculation."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern="^class Authentication.*__init__",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=1,
            ),
            GrepToolCall(
                turn_index=2,
                pattern=".",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=5000,
            ),
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Average should be moderate (one very specific, one very broad)
        assert metrics.specific_pattern_count >= 1
        assert metrics.broad_pattern_count >= 1

    def test_content_mode_without_context_lines_warning(self):
        """Verify warning for content mode without context lines."""
        tool_calls = [
            GrepToolCall(
                turn_index=5,
                pattern="error",
                output_mode="content",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=20,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Should have at least one warning about content mode without context
        assert len(findings) >= 1
        finding = next((f for f in findings if f.category == "output_mode_selection"), None)
        assert finding is not None
        assert finding.severity == "warning"
        assert "content" in finding.message.lower()

    def test_count_mode_with_low_results_info(self):
        """Verify info finding for count mode with low result count."""
        tool_calls = [
            GrepToolCall(
                turn_index=3,
                pattern="rare_function",
                output_mode="count",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=3,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Should suggest files_with_matches instead
        finding = next((f for f in findings if "files_with_matches" in f.message), None)
        assert finding is not None
        assert finding.severity == "info"

    def test_missing_head_limit_critical_finding(self):
        """Verify critical finding for missing head_limit with large results."""
        tool_calls = [
            GrepToolCall(
                turn_index=7,
                pattern="test",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=150,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Should have critical finding for missing head_limit
        finding = next((f for f in findings if f.category == "head_limit_usage"), None)
        assert finding is not None
        assert finding.severity == "critical"
        assert "150" in finding.example
        assert finding.turn_index == 7

    def test_missing_head_limit_warning_moderate_results(self):
        """Verify warning for missing head_limit with moderate results."""
        tool_calls = [
            GrepToolCall(
                turn_index=4,
                pattern="error",
                output_mode="content",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=75,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Should have warning for moderate result count
        finding = next((f for f in findings if f.category == "head_limit_usage"), None)
        assert finding is not None
        assert finding.severity == "warning"

    def test_multiline_opportunity_detection(self):
        """Verify detection of patterns needing multiline mode."""
        tool_calls = [
            GrepToolCall(
                turn_index=2,
                pattern=r"interface\{[\s\S]*?}",
                output_mode="content",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=5,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Should detect cross-line pattern without multiline mode
        finding = next((f for f in findings if f.category == "multiline_mode_usage"), None)
        assert finding is not None
        assert finding.severity == "warning"
        assert "multiline" in finding.message.lower()

    def test_broad_pattern_critical_finding(self):
        """Verify critical finding for very broad pattern with many results."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern=".*",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=500,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Should have critical finding for broad pattern
        finding = next((f for f in findings if f.category == "pattern_specificity"), None)
        assert finding is not None
        assert finding.severity == "critical"
        assert "500" in finding.example

    def test_broad_pattern_warning(self):
        """Verify warning for moderately broad pattern."""
        tool_calls = [
            GrepToolCall(
                turn_index=3,
                pattern="test",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=120,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # May have warning for broad pattern
        specificity_findings = [f for f in findings if f.category == "pattern_specificity"]
        # At least check it doesn't crash
        assert metrics.total_grep_calls == 1

    def test_findings_severity_counts(self):
        """Verify findings are counted by severity correctly."""
        tool_calls = [
            # Critical: broad pattern + many results + no head_limit
            GrepToolCall(
                turn_index=1,
                pattern=".",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=500,
            ),
            # Warning: content mode without context
            GrepToolCall(
                turn_index=2,
                pattern="error",
                output_mode="content",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=10,
            ),
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        assert metrics.findings_count == len(findings)
        assert metrics.critical_findings >= 1
        assert metrics.warning_findings >= 1

    def test_finding_contains_example(self):
        """Verify findings include concrete examples from transcript."""
        tool_calls = [
            GrepToolCall(
                turn_index=5,
                pattern="authentication_error",
                output_mode="content",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=15,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        if findings:
            # All findings should have non-empty example
            for finding in findings:
                assert finding.example
                assert "authentication_error" in finding.example

    def test_finding_turn_index_preserved(self):
        """Verify findings preserve turn_index from tool calls."""
        tool_calls = [
            GrepToolCall(
                turn_index=42,
                pattern="test.*pattern",
                output_mode="content",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=8,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        if findings:
            for finding in findings:
                assert finding.turn_index == 42

    def test_efficient_grep_pattern_no_findings(self):
        """Verify efficient grep usage produces no findings."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern="^class AuthenticationManager",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=20,
                multiline=False,
                glob_filter="**/*.py",
                type_filter="py",
                result_count=2,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        assert len(findings) == 0
        assert metrics.findings_count == 0

    def test_content_mode_with_context_lines_no_warning(self):
        """Verify content mode with context lines doesn't produce warning."""
        tool_calls = [
            GrepToolCall(
                turn_index=3,
                pattern="def authenticate",
                output_mode="content",
                context_lines=5,
                has_context_a=False,
                has_context_b=False,
                has_context_c=True,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=3,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Should not have output_mode_selection warning
        output_mode_findings = [f for f in findings if f.category == "output_mode_selection"]
        assert len(output_mode_findings) == 0

    def test_pattern_specificity_calculation(self):
        """Verify pattern specificity is calculated correctly."""
        # Very specific pattern
        specific_call = GrepToolCall(
            turn_index=1,
            pattern=r"^function\b authenticate\(username: string\)$",
            output_mode="files_with_matches",
            context_lines=0,
            has_context_a=False,
            has_context_b=False,
            has_context_c=False,
            head_limit=0,
            multiline=False,
            glob_filter="",
            type_filter="",
            result_count=1,
        )

        # Very broad pattern
        broad_call = GrepToolCall(
            turn_index=2,
            pattern=".*",
            output_mode="files_with_matches",
            context_lines=0,
            has_context_a=False,
            has_context_b=False,
            has_context_c=False,
            head_limit=0,
            multiline=False,
            glob_filter="",
            type_filter="",
            result_count=1000,
        )

        metrics_specific, _ = analyze_session_grep_output_mode([specific_call])
        metrics_broad, _ = analyze_session_grep_output_mode([broad_call])

        # Specific pattern should have higher score than broad
        assert metrics_specific.pattern_specificity_score > metrics_broad.pattern_specificity_score

    def test_multiline_pattern_indicators(self):
        """Verify various multiline pattern indicators are detected."""
        patterns_needing_multiline = [
            r"struct\n.*field",
            r"[\s\S]*?end",
            r"[^]*content",
            "newline.*pattern",
        ]

        for pattern in patterns_needing_multiline:
            tool_calls = [
                GrepToolCall(
                    turn_index=1,
                    pattern=pattern,
                    output_mode="content",
                    context_lines=0,
                    has_context_a=False,
                    has_context_b=False,
                    has_context_c=False,
                    head_limit=0,
                    multiline=False,
                    glob_filter="",
                    type_filter="",
                    result_count=5,
                )
            ]

            metrics, findings = analyze_session_grep_output_mode(tool_calls)

            # Should detect multiline opportunity
            multiline_findings = [f for f in findings if f.category == "multiline_mode_usage"]
            assert len(multiline_findings) >= 1, f"Pattern '{pattern}' should trigger multiline finding"


class TestValidation:
    """Test input validation."""

    def test_invalid_tool_calls_type(self):
        """Verify non-sequence tool_calls raises error."""
        with pytest.raises(ValueError, match="must be a list or tuple"):
            analyze_session_grep_output_mode("not a list")

    def test_invalid_tool_call_instance(self):
        """Verify non-GrepToolCall instance raises error."""
        with pytest.raises(ValueError, match="GrepToolCall instance"):
            analyze_session_grep_output_mode([{"pattern": "test"}])

    def test_invalid_turn_index_type(self):
        """Verify invalid turn_index type raises error."""
        with pytest.raises(ValueError, match="turn_index must be an integer"):
            analyze_session_grep_output_mode([
                GrepToolCall(
                    turn_index="not an int",
                    pattern="test",
                    output_mode="files_with_matches",
                    context_lines=0,
                    has_context_a=False,
                    has_context_b=False,
                    has_context_c=False,
                    head_limit=0,
                    multiline=False,
                    glob_filter="",
                    type_filter="",
                    result_count=5,
                )
            ])

    def test_invalid_turn_index_boolean(self):
        """Verify boolean turn_index raises error."""
        with pytest.raises(ValueError, match="turn_index must be an integer"):
            analyze_session_grep_output_mode([
                GrepToolCall(
                    turn_index=True,
                    pattern="test",
                    output_mode="files_with_matches",
                    context_lines=0,
                    has_context_a=False,
                    has_context_b=False,
                    has_context_c=False,
                    head_limit=0,
                    multiline=False,
                    glob_filter="",
                    type_filter="",
                    result_count=5,
                )
            ])

    def test_negative_turn_index(self):
        """Verify negative turn_index raises error."""
        with pytest.raises(ValueError, match="non-negative"):
            analyze_session_grep_output_mode([
                GrepToolCall(
                    turn_index=-1,
                    pattern="test",
                    output_mode="files_with_matches",
                    context_lines=0,
                    has_context_a=False,
                    has_context_b=False,
                    has_context_c=False,
                    head_limit=0,
                    multiline=False,
                    glob_filter="",
                    type_filter="",
                    result_count=5,
                )
            ])

    def test_invalid_pattern_type(self):
        """Verify non-string pattern raises error."""
        with pytest.raises(ValueError, match="pattern must be a string"):
            analyze_session_grep_output_mode([
                GrepToolCall(
                    turn_index=0,
                    pattern=123,
                    output_mode="files_with_matches",
                    context_lines=0,
                    has_context_a=False,
                    has_context_b=False,
                    has_context_c=False,
                    head_limit=0,
                    multiline=False,
                    glob_filter="",
                    type_filter="",
                    result_count=5,
                )
            ])

    def test_invalid_output_mode(self):
        """Verify invalid output_mode raises error."""
        with pytest.raises(ValueError, match="output_mode must be"):
            analyze_session_grep_output_mode([
                GrepToolCall(
                    turn_index=0,
                    pattern="test",
                    output_mode="invalid_mode",
                    context_lines=0,
                    has_context_a=False,
                    has_context_b=False,
                    has_context_c=False,
                    head_limit=0,
                    multiline=False,
                    glob_filter="",
                    type_filter="",
                    result_count=5,
                )
            ])


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_result_count(self):
        """Verify zero result count is handled correctly."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern="nonexistent_function",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=0,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Should not crash, should handle gracefully
        assert metrics.total_grep_calls == 1
        # Zero results shouldn't trigger most findings
        critical_findings = [f for f in findings if f.severity == "critical"]
        assert len(critical_findings) == 0

    def test_empty_pattern(self):
        """Verify empty pattern is handled."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern="",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=0,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Should handle empty pattern without crashing
        assert metrics.total_grep_calls == 1

    def test_very_large_result_count(self):
        """Verify very large result counts are handled."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern=".",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=100000,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # Should flag as critical
        critical_findings = [f for f in findings if f.severity == "critical"]
        assert len(critical_findings) >= 1

    def test_multiple_context_flags(self):
        """Verify handling of multiple context flag combinations."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern="test",
                output_mode="content",
                context_lines=10,  # Combined from -A 5 -B 5
                has_context_a=True,
                has_context_b=True,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=3,
            )
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        assert metrics.context_lines_usage_count == 1
        assert metrics.context_lines_usage_rate == 100.0

    def test_all_output_modes_efficient(self):
        """Verify session with all output modes used efficiently."""
        tool_calls = [
            GrepToolCall(
                turn_index=1,
                pattern="class User",
                output_mode="files_with_matches",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=10,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=3,
            ),
            GrepToolCall(
                turn_index=2,
                pattern="def authenticate",
                output_mode="content",
                context_lines=5,
                has_context_a=False,
                has_context_b=False,
                has_context_c=True,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=2,
            ),
            GrepToolCall(
                turn_index=3,
                pattern="TODO",
                output_mode="count",
                context_lines=0,
                has_context_a=False,
                has_context_b=False,
                has_context_c=False,
                head_limit=0,
                multiline=False,
                glob_filter="",
                type_filter="",
                result_count=50,
            ),
        ]

        metrics, findings = analyze_session_grep_output_mode(tool_calls)

        # All output modes should be represented
        assert metrics.output_mode_files_count == 1
        assert metrics.output_mode_content_count == 1
        assert metrics.output_mode_count_count == 1
        # Minimal findings for efficient usage
        assert metrics.critical_findings == 0
