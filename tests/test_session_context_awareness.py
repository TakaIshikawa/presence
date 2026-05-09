"""Tests for session context awareness and state tracking analyzer."""

import pytest

from synthesis.session_context_awareness import analyze_session_context_awareness


class TestAnalyzeSessionContextAwareness:
    """Test main analyzer function."""

    def test_empty_session(self):
        """Test analyzer with no records."""
        result = analyze_session_context_awareness([])
        assert result["total_turns"] == 0
        assert result["total_tool_calls"] == 0
        assert result["context_awareness_score"] == 1.0
        assert result["efficiency_score"] == 1.0

    def test_none_input(self):
        """Test analyzer with None input."""
        result = analyze_session_context_awareness(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type(self):
        """Test analyzer rejects non-list input."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_context_awareness("not a list")

    def test_non_mapping_records_handled(self):
        """Test that non-mapping records are handled gracefully."""
        result = analyze_session_context_awareness([
            "invalid",
            123,
            None,
            {"tool_name": "Read", "file_path": "test.py"},
        ])
        assert result["total_turns"] == 4
        assert result["total_tool_calls"] == 1


class TestFileExploration:
    """Test file read and re-exploration patterns."""

    def test_single_file_read_not_redundant(self):
        """Test single read is not redundant."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "file_path": "main.py"},
        ])
        assert result["read_call_count"] == 1
        assert result["unique_files_read"] == 1
        assert result["redundant_read_count"] == 0
        assert result["redundant_read_rate"] == 0.0

    def test_redundant_read_same_file_unchanged(self):
        """Test re-reading unchanged file is redundant."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "file_path": "main.py"},
            {"tool_name": "Read", "file_path": "main.py", "file_was_edited": False},
        ])
        assert result["read_call_count"] == 2
        assert result["redundant_read_count"] == 1
        assert result["redundant_read_rate"] == 50.0

    def test_reread_after_edit_not_redundant(self):
        """Test re-reading after edit is not redundant."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "file_path": "main.py"},
            {"tool_name": "Edit", "file_path": "main.py"},
            {"tool_name": "Read", "file_path": "main.py", "file_was_edited": True},
        ])
        assert result["redundant_read_count"] == 0

    def test_multiple_files_read_counts(self):
        """Test counting unique files and multi-reads."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Read", "file_path": "b.py"},
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Read", "file_path": "c.py"},
            {"tool_name": "Read", "file_path": "a.py"},
        ])
        assert result["unique_files_read"] == 3
        assert result["files_read_multiple_times"] == 1  # Only a.py
        assert result["max_file_read_count"] == 3  # a.py read 3 times

    def test_write_tool_marks_file_edited(self):
        """Test Write tool marks file as edited."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "file_path": "new.py"},
            {"tool_name": "Write", "file_path": "new.py"},
            {"tool_name": "Read", "file_path": "new.py"},
        ])
        assert result["redundant_read_count"] == 0


class TestSearchPatterns:
    """Test grep search pattern redundancy detection."""

    def test_unique_grep_patterns_counted(self):
        """Test unique grep patterns are counted."""
        result = analyze_session_context_awareness([
            {"tool_name": "Grep", "pattern": "def.*function"},
            {"tool_name": "Grep", "pattern": "class.*Model"},
        ])
        assert result["grep_call_count"] == 2
        assert result["unique_grep_patterns"] == 2
        assert result["duplicate_grep_count"] == 0

    def test_duplicate_grep_pattern_detected(self):
        """Test duplicate grep patterns are detected."""
        result = analyze_session_context_awareness([
            {"tool_name": "Grep", "pattern": "TODO"},
            {"tool_name": "Grep", "pattern": "FIXME"},
            {"tool_name": "Grep", "pattern": "TODO"},
        ])
        assert result["grep_call_count"] == 3
        assert result["unique_grep_patterns"] == 2
        assert result["duplicate_grep_count"] == 1
        assert result["duplicate_grep_rate"] == 33.33

    def test_multiple_duplicate_greps(self):
        """Test multiple duplicate grep patterns."""
        result = analyze_session_context_awareness([
            {"tool_name": "Grep", "pattern": "error"},
            {"tool_name": "Grep", "pattern": "error"},
            {"tool_name": "Grep", "pattern": "warning"},
            {"tool_name": "Grep", "pattern": "error"},
            {"tool_name": "Grep", "pattern": "warning"},
        ])
        assert result["duplicate_grep_count"] == 3  # 2 for "error", 1 for "warning"


class TestUserInteraction:
    """Test repeated question detection."""

    def test_unique_questions_counted(self):
        """Test unique questions are counted."""
        result = analyze_session_context_awareness([
            {"tool_name": "AskUserQuestion", "question": "Which library to use?"},
            {"tool_name": "AskUserQuestion", "question": "What API version?"},
        ])
        assert result["askuser_call_count"] == 2
        assert result["repeated_questions_count"] == 0

    def test_repeated_question_detected(self):
        """Test repeated questions are detected."""
        result = analyze_session_context_awareness([
            {"tool_name": "AskUserQuestion", "question": "Which approach?"},
            {"tool_name": "AskUserQuestion", "question": "What color?"},
            {"tool_name": "AskUserQuestion", "question": "Which approach?"},
        ])
        assert result["askuser_call_count"] == 3
        assert result["repeated_questions_count"] == 1
        assert result["repeated_question_rate"] == 33.33

    def test_question_normalization_case_insensitive(self):
        """Test question matching is case insensitive."""
        result = analyze_session_context_awareness([
            {"tool_name": "AskUserQuestion", "question": "Which Library?"},
            {"tool_name": "AskUserQuestion", "question": "which library?"},
        ])
        assert result["repeated_questions_count"] == 1


class TestContextUsage:
    """Test context reference and citation tracking."""

    def test_context_references_counted(self):
        """Test context references are counted."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "references_prior_context": True},
            {"tool_name": "Grep", "references_prior_context": True},
            {"tool_name": "Edit", "references_prior_context": False},
        ])
        assert result["context_references"] == 2
        assert result["context_reference_rate"] == 66.67

    def test_prior_result_citations_counted(self):
        """Test prior result citations are counted."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "cites_prior_result": True},
            {"tool_name": "Grep", "cites_prior_result": False},
            {"tool_name": "Edit", "cites_prior_result": True},
        ])
        assert result["prior_result_citations"] == 2
        assert result["citation_rate"] == 66.67

    def test_high_context_usage_scenario(self):
        """Test scenario with high context awareness."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read"},
            {"tool_name": "Grep", "references_prior_context": True, "cites_prior_result": True},
            {"tool_name": "Edit", "references_prior_context": True, "cites_prior_result": True},
            {"tool_name": "Read", "references_prior_context": True, "cites_prior_result": True},
        ])
        assert result["context_reference_rate"] == 75.0
        assert result["citation_rate"] == 75.0


class TestLostContext:
    """Test lost context detection."""

    def test_repeated_requests_counted(self):
        """Test repeated requests are counted."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "repeats_prior_request": True},
            {"tool_name": "Grep", "repeats_prior_request": True},
        ])
        assert result["repeated_request_count"] == 2

    def test_lost_context_instances_counted(self):
        """Test lost context instances are counted."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read"},
            {"tool_name": "Grep", "lost_context_indicator": True},
            {"tool_name": "Edit"},
            {"tool_name": "Read", "lost_context_indicator": True},
        ])
        assert result["lost_context_instances"] == 2
        assert result["lost_context_rate"] == 50.0

    def test_no_lost_context(self):
        """Test session without lost context."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read"},
            {"tool_name": "Grep"},
            {"tool_name": "Edit"},
        ])
        assert result["lost_context_instances"] == 0
        assert result["lost_context_rate"] == 0.0


class TestStateConsistency:
    """Test state tracking and consistency."""

    def test_working_directory_changes_counted(self):
        """Test working directory changes are counted."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "working_directory": "/project"},
            {"tool_name": "Grep", "working_directory": "/project"},
            {"tool_name": "Edit", "working_directory": "/project/src"},
            {"tool_name": "Read", "working_directory": "/project"},
        ])
        assert result["working_directory_changes"] == 2

    def test_state_inconsistencies_counted(self):
        """Test state inconsistencies are counted."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "state_inconsistency": True},
            {"tool_name": "Grep", "state_inconsistency": False},
            {"tool_name": "Edit", "state_inconsistency": True},
            {"tool_name": "Read", "state_inconsistency": False},
        ])
        assert result["state_inconsistencies"] == 2

    def test_state_consistency_score_perfect(self):
        """Test perfect state consistency score."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read"},
            {"tool_name": "Grep"},
            {"tool_name": "Edit"},
        ])
        assert result["state_consistency_score"] == 1.0

    def test_state_consistency_score_with_errors(self):
        """Test state consistency score with errors."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "state_inconsistency": True},
            {"tool_name": "Grep"},
            {"tool_name": "Edit"},
            {"tool_name": "Read"},
        ])
        # 1/4 = 25% inconsistency rate, should reduce score
        assert result["state_consistency_score"] < 1.0


class TestAntiPatterns:
    """Test anti-pattern detection and scoring."""

    def test_total_anti_patterns_calculation(self):
        """Test total anti-patterns are summed correctly."""
        result = analyze_session_context_awareness([
            # Redundant read
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Read", "file_path": "a.py"},
            # Duplicate grep
            {"tool_name": "Grep", "pattern": "TODO"},
            {"tool_name": "Grep", "pattern": "TODO"},
            # Repeated question
            {"tool_name": "AskUserQuestion", "question": "Which?"},
            {"tool_name": "AskUserQuestion", "question": "Which?"},
            # Lost context
            {"tool_name": "Read", "lost_context_indicator": True},
            # State inconsistency
            {"tool_name": "Edit", "state_inconsistency": True},
        ])
        assert result["total_anti_patterns"] == 5
        # 5 anti-patterns / 8 tool calls = 62.5%
        assert result["anti_pattern_rate"] == 62.5

    def test_no_anti_patterns(self):
        """Test session without anti-patterns."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Grep", "pattern": "TODO"},
            {"tool_name": "Edit", "file_path": "a.py"},
        ])
        assert result["total_anti_patterns"] == 0
        assert result["anti_pattern_rate"] == 0.0


class TestContextAwarenessScore:
    """Test overall context awareness score calculation."""

    def test_high_context_awareness_score(self):
        """Test high score for good context awareness."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read"},
            {
                "tool_name": "Grep",
                "references_prior_context": True,
                "cites_prior_result": True,
            },
            {
                "tool_name": "Edit",
                "references_prior_context": True,
                "cites_prior_result": True,
            },
            {
                "tool_name": "Read",
                "references_prior_context": True,
                "cites_prior_result": True,
            },
        ])
        # High context reference rate (75%), high citation rate (75%)
        # No lost context, perfect state consistency
        assert result["context_awareness_score"] >= 0.8

    def test_low_context_awareness_score(self):
        """Test low score for poor context awareness."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "lost_context_indicator": True},
            {"tool_name": "Grep", "lost_context_indicator": True},
            {"tool_name": "Edit", "state_inconsistency": True},
            {"tool_name": "Read", "lost_context_indicator": True},
        ])
        # No context references, no citations, high lost context (75%)
        assert result["context_awareness_score"] < 0.5


class TestEfficiencyScore:
    """Test efficiency score calculation."""

    def test_high_efficiency_score(self):
        """Test high score for efficient exploration."""
        result = analyze_session_context_awareness([
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Grep", "pattern": "TODO"},
            {"tool_name": "Edit", "file_path": "a.py"},
            {"tool_name": "Read", "file_path": "b.py"},
        ])
        # No redundant reads, no duplicate greps, no repeated questions
        assert result["efficiency_score"] >= 0.9

    def test_low_efficiency_score(self):
        """Test low score for inefficient exploration."""
        result = analyze_session_context_awareness([
            # Redundant reads
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Read", "file_path": "a.py"},
            # Duplicate greps
            {"tool_name": "Grep", "pattern": "TODO"},
            {"tool_name": "Grep", "pattern": "TODO"},
            # Repeated questions
            {"tool_name": "AskUserQuestion", "question": "Which?"},
            {"tool_name": "AskUserQuestion", "question": "Which?"},
        ])
        # High redundancy across all dimensions
        assert result["efficiency_score"] < 0.5


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_division_safety(self):
        """Test that zero division is handled gracefully."""
        result = analyze_session_context_awareness([
            {"tool_name": "Bash"},  # Not Read, Grep, or AskUserQuestion
        ])
        assert result["redundant_read_rate"] == 0.0
        assert result["duplicate_grep_rate"] == 0.0
        assert result["repeated_question_rate"] == 0.0

    def test_case_insensitive_tool_names(self):
        """Test tool name matching is case insensitive."""
        result = analyze_session_context_awareness([
            {"tool_name": "read", "file_path": "a.py"},
            {"tool_name": "READ", "file_path": "b.py"},
            {"tool_name": "Read", "file_path": "c.py"},
        ])
        assert result["read_call_count"] == 3

    def test_missing_optional_fields(self):
        """Test handling of records with missing optional fields."""
        result = analyze_session_context_awareness([
            {
                "tool_name": "Read",
                # Missing: file_path, references_prior_context, etc.
            },
        ])
        # Should not crash, should use defaults
        assert result["total_tool_calls"] == 1


class TestRealWorldScenarios:
    """Test realistic session scenarios."""

    def test_ideal_context_aware_workflow(self):
        """Test ideal workflow with good context awareness."""
        result = analyze_session_context_awareness([
            # Initial exploration
            {"tool_name": "Read", "file_path": "main.py"},
            {"tool_name": "Grep", "pattern": "def.*process"},
            # Context-aware follow-up
            {
                "tool_name": "Edit",
                "file_path": "main.py",
                "references_prior_context": True,
                "cites_prior_result": True,
            },
            # Re-read after edit (not redundant)
            {
                "tool_name": "Read",
                "file_path": "main.py",
                "file_was_edited": True,
                "references_prior_context": True,
            },
        ])
        assert result["redundant_read_count"] == 0
        assert result["context_reference_rate"] == 50.0
        assert result["context_awareness_score"] >= 0.6
        assert result["efficiency_score"] >= 0.9

    def test_poor_context_awareness_workflow(self):
        """Test workflow with poor context awareness."""
        result = analyze_session_context_awareness([
            # Redundant exploration
            {"tool_name": "Read", "file_path": "main.py"},
            {"tool_name": "Read", "file_path": "main.py"},
            {"tool_name": "Grep", "pattern": "TODO"},
            {"tool_name": "Grep", "pattern": "TODO"},
            # Lost context
            {"tool_name": "AskUserQuestion", "question": "Which?"},
            {
                "tool_name": "AskUserQuestion",
                "question": "Which?",
                "lost_context_indicator": True,
            },
        ])
        assert result["redundant_read_count"] == 1
        assert result["duplicate_grep_count"] == 1
        assert result["repeated_questions_count"] == 1
        assert result["lost_context_instances"] == 1
        assert result["total_anti_patterns"] == 4
        assert result["context_awareness_score"] < 0.5
        assert result["efficiency_score"] <= 0.55  # Allow small margin

    def test_mixed_quality_session(self):
        """Test session with mixed context awareness quality."""
        result = analyze_session_context_awareness([
            # Good: unique reads
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Read", "file_path": "b.py"},
            # Bad: redundant read of b.py (unchanged)
            {"tool_name": "Read", "file_path": "b.py"},
            # Good: context reference
            {
                "tool_name": "Edit",
                "file_path": "a.py",
                "references_prior_context": True,
            },
            # Good: unique grep
            {"tool_name": "Grep", "pattern": "error"},
        ])
        assert result["redundant_read_count"] == 1
        assert result["context_reference_rate"] == 20.0
        assert 0.4 <= result["context_awareness_score"] <= 0.8
        assert 0.6 <= result["efficiency_score"] <= 0.95
