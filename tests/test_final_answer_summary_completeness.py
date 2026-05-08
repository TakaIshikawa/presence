"""Tests for final answer summary completeness analyzer."""

import pytest

from synthesis.final_answer_summary_completeness import analyze_final_answer_summary_completeness


class TestAnalyzeFinalAnswerSummaryCompleteness:
    """Test main analyzer function."""

    def test_empty_records_returns_zeroed_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_final_answer_summary_completeness([])

        assert result["total_sessions"] == 0
        assert result["has_completion_statement"] == 0
        assert result["references_changed_files"] == 0
        assert result["includes_verification_results"] == 0
        assert result["provides_next_steps"] == 0
        assert result["complete_answer_count"] == 0
        assert result["minimal_summary_count"] == 0
        assert result["verbose_summary_count"] == 0
        assert result["well_balanced_count"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_final_answer_summary_completeness(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_final_answer_summary_completeness("not a list")

    def test_completion_statement_detected(self):
        """Verify completion statement detection."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Task completed successfully. All tests pass.",
            }
        ])

        assert result["has_completion_statement"] == 1

    def test_file_references_detected(self):
        """Verify file reference detection."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Created src/analyzer.py and tests/test_analyzer.py",
            }
        ])

        assert result["references_changed_files"] == 1

    def test_verification_results_detected(self):
        """Verify verification results detection."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Implementation complete. All tests pass and lint is clean.",
            }
        ])

        assert result["includes_verification_results"] == 1

    def test_next_steps_detected(self):
        """Verify next steps detection."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Completed. Next steps: review the changes and deploy to production.",
            }
        ])

        assert result["provides_next_steps"] == 1

    def test_complete_answer_all_elements(self):
        """Verify complete answer with all elements."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Task completed successfully. Created src/analyzer.py and tests/test_analyzer.py. "
                                "All tests pass with 100% coverage. Next steps: review and merge.",
                "changed_files": ["src/analyzer.py", "tests/test_analyzer.py"],
                "verification_passed": True,
            }
        ])

        assert result["has_completion_statement"] == 1
        assert result["references_changed_files"] == 1
        assert result["includes_verification_results"] == 1
        assert result["provides_next_steps"] == 1
        assert result["complete_answer_count"] == 1

    def test_minimal_summary_no_context(self):
        """Verify minimal summary detection."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Done.",
            }
        ])

        assert result["minimal_summary_count"] == 1

    def test_empty_final_answer(self):
        """Verify empty final answer is treated as minimal."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "",
            }
        ])

        assert result["minimal_summary_count"] == 1

    def test_file_count_reference(self):
        """Verify file count reference detection."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Modified 3 files to implement the feature.",
            }
        ])

        assert result["references_changed_files"] == 1

    def test_changed_files_list_reference(self):
        """Verify changed_files list is referenced."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Updated analyzer.py as requested.",
                "changed_files": ["analyzer.py"],
            }
        ])

        assert result["references_changed_files"] == 1

    def test_verification_with_explicit_flag(self):
        """Verify verification detection with explicit flag."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Implementation complete with full test coverage.",
                "verification_passed": True,
            }
        ])

        assert result["includes_verification_results"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_final_answer_summary_completeness([
            "not a dict",
            {
                "session_id": "session1",
                "final_answer": "Task completed.",
            },
        ])

        assert result["total_sessions"] == 1

    def test_mixed_answer_quality(self):
        """Verify mixed answer quality detection."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Task completed. Created analyzer.py. Tests pass. Next: review.",
            },
            {
                "session_id": "session2",
                "final_answer": "Done.",
            },
            {
                "session_id": "session3",
                "final_answer": "Fixed the issue in src/main.py and tests pass.",
            },
        ])

        assert result["total_sessions"] == 3
        assert result["complete_answer_count"] >= 1
        assert result["minimal_summary_count"] >= 1

    def test_case_insensitive_pattern_matching(self):
        """Verify pattern matching is case-insensitive."""
        result = analyze_final_answer_summary_completeness([
            {"session_id": "s1", "final_answer": "TASK COMPLETED. ALL TESTS PASS."},
        ])

        assert result["has_completion_statement"] == 1
        assert result["includes_verification_results"] == 1

    def test_optimal_complete_answer(self):
        """Verify optimal complete answer pattern."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Successfully implemented the authentication analyzer in src/synthesis/auth_analyzer.py "
                                "with comprehensive tests in tests/test_auth_analyzer.py. All 25 tests pass with 100% coverage. "
                                "Type checking with mypy passes. You can now integrate this analyzer into the main pipeline.",
                "changed_files": ["src/synthesis/auth_analyzer.py", "tests/test_auth_analyzer.py"],
                "verification_passed": True,
            }
        ])

        assert result["has_completion_statement"] == 1
        assert result["references_changed_files"] == 1
        assert result["includes_verification_results"] == 1
        assert result["provides_next_steps"] == 1
        assert result["complete_answer_count"] == 1

    def test_anti_pattern_minimal_answer(self):
        """Verify anti-pattern of minimal answer."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Done.",
            }
        ])

        assert result["has_completion_statement"] == 0
        assert result["references_changed_files"] == 0
        assert result["includes_verification_results"] == 0
        assert result["provides_next_steps"] == 0
        assert result["minimal_summary_count"] == 1

    def test_various_completion_phrases(self):
        """Verify various completion phrase patterns."""
        completion_phrases = [
            "Task completed successfully",
            "Implementation finished",
            "Successfully created the analyzer",
            "Ready for review",
        ]

        for phrase in completion_phrases:
            result = analyze_final_answer_summary_completeness([
                {"session_id": "s1", "final_answer": phrase}
            ])
            assert result["has_completion_statement"] == 1, f"Should detect completion in: {phrase}"

    def test_various_verification_phrases(self):
        """Verify various verification phrase patterns."""
        verification_phrases = [
            "All tests pass",
            "pytest passed with 100% coverage",
            "Build successful",
            "mypy type checking clean",
            "ruff check passed",
        ]

        for phrase in verification_phrases:
            result = analyze_final_answer_summary_completeness([
                {"session_id": "s1", "final_answer": phrase}
            ])
            assert result["includes_verification_results"] == 1, f"Should detect verification in: {phrase}"

    def test_various_next_steps_phrases(self):
        """Verify various next steps phrase patterns."""
        next_steps_phrases = [
            "Next steps: review the code",
            "Follow-up: test in staging",
            "You can now deploy",
            "To use this feature, run the command",
            "Consider adding more tests",
        ]

        for phrase in next_steps_phrases:
            result = analyze_final_answer_summary_completeness([
                {"session_id": "s1", "final_answer": phrase}
            ])
            assert result["provides_next_steps"] == 1, f"Should detect next steps in: {phrase}"

    def test_summary_ratio_calculation_action_focused(self):
        """Verify action-focused answers have low ratio."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Created analyzer. Implemented tests. Fixed bugs. Updated docs.",
            }
        ])

        # Action-focused = low ratio
        assert result["avg_summary_to_action_ratio"] < 50.0

    def test_summary_ratio_calculation_explanation_focused(self):
        """Verify explanation-focused answers have high ratio."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Because the system needed improvement, however we had to consider several factors. "
                                "Therefore, although challenging, we essentially decided to generally refactor everything.",
            }
        ])

        # Explanation-focused = high ratio
        assert result["avg_summary_to_action_ratio"] > 50.0

    def test_well_balanced_answer(self):
        """Verify well-balanced answer detection."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "Successfully implemented the analyzer in src/main.py and added comprehensive tests. "
                                "All tests pass and verification succeeded. You can now integrate with the pipeline.",
            }
        ])

        # Should have balanced ratio and multiple elements (3+ completeness score)
        assert result["well_balanced_count"] >= 1 or result["complete_answer_count"] >= 1

    def test_whitespace_handling(self):
        """Verify whitespace in final_answer is stripped."""
        result = analyze_final_answer_summary_completeness([
            {
                "session_id": "session1",
                "final_answer": "  Task completed.  ",
            }
        ])

        assert result["has_completion_statement"] == 1

    def test_zero_denominator_in_averages(self):
        """Verify zero denominator in average calculations."""
        result = analyze_final_answer_summary_completeness([])

        assert result["avg_summary_to_action_ratio"] == 0.0
