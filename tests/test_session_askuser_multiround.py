"""Tests for session AskUserQuestion multi-turn conversation analyzer."""

import pytest

from synthesis.session_askuser_multiround import analyze_session_askuser_multiround


class TestAnalyzeSessionAskUserMultiRound:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_askuser_multiround([])

        assert result["total_turns"] == 0
        assert result["askuser_invocations"] == 0
        assert result["total_questions_asked"] == 0
        assert result["avg_questions_per_invocation"] == 0.0
        assert result["sessions_with_multiple_rounds"] is False
        assert result["multi_round_session_percentage"] == 0.0
        assert result["time_between_rounds_seconds"] == []
        assert result["avg_time_between_rounds_seconds"] == 0.0
        assert result["min_time_between_rounds_seconds"] == 0.0
        assert result["max_time_between_rounds_seconds"] == 0.0
        assert result["session_completed"] is False
        assert result["session_failed"] is False
        assert result["correlation_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_askuser_multiround(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_askuser_multiround("not a list")

    def test_single_question_round(self):
        """Verify single AskUserQuestion invocation."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "Read",
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {"question": "Which approach should we use?"},
                    {"question": "What is your preference?"},
                ],
                "timestamp": 1000.0,
            },
            {
                "turn_index": 2,
                "tool_name": "Edit",
            },
        ])

        assert result["total_turns"] == 3
        assert result["askuser_invocations"] == 1
        assert result["total_questions_asked"] == 2
        assert result["avg_questions_per_invocation"] == 2.0
        assert result["sessions_with_multiple_rounds"] is False
        assert result["multi_round_session_percentage"] == 0.0

    def test_multiple_question_rounds(self):
        """Verify multiple AskUserQuestion invocations."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Question 1?"}],
                "timestamp": 1000.0,
            },
            {
                "turn_index": 1,
                "tool_name": "Edit",
            },
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Question 2?"}],
                "timestamp": 1050.0,
            },
            {
                "turn_index": 3,
                "tool_name": "Write",
            },
            {
                "turn_index": 4,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Question 3?"}],
                "timestamp": 1120.0,
            },
        ])

        assert result["askuser_invocations"] == 3
        assert result["total_questions_asked"] == 3
        assert result["sessions_with_multiple_rounds"] is True
        assert result["multi_round_session_percentage"] == 100.0

    def test_time_between_rounds_calculation(self):
        """Verify time intervals between question rounds."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?"}],
                "timestamp": 1000.0,
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?"}],
                "timestamp": 1050.0,
            },
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q3?"}],
                "timestamp": 1120.0,
            },
        ])

        # Intervals: 1050-1000=50, 1120-1050=70
        assert result["time_between_rounds_seconds"] == [50.0, 70.0]
        assert result["avg_time_between_rounds_seconds"] == 60.0
        assert result["min_time_between_rounds_seconds"] == 50.0
        assert result["max_time_between_rounds_seconds"] == 70.0

    def test_no_timestamps_provided(self):
        """Verify handling when timestamps are missing."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?"}],
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?"}],
            },
        ])

        assert result["askuser_invocations"] == 2
        assert result["time_between_rounds_seconds"] == []
        assert result["avg_time_between_rounds_seconds"] == 0.0

    def test_partial_timestamps(self):
        """Verify handling when only some records have timestamps."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?"}],
                "timestamp": 1000.0,
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?"}],
                # No timestamp
            },
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q3?"}],
                "timestamp": 1100.0,
            },
        ])

        # Only records with timestamps included
        assert result["time_between_rounds_seconds"] == [100.0]

    def test_session_completed_success(self):
        """Verify session completion tracking."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?"}],
            },
            {
                "turn_index": 1,
                "tool_name": "Edit",
                "session_completed": True,
            },
        ])

        assert result["session_completed"] is True
        assert result["session_failed"] is False

    def test_session_failed(self):
        """Verify session failure tracking."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?"}],
            },
            {
                "turn_index": 1,
                "tool_name": "Edit",
                "session_failed": True,
            },
        ])

        assert result["session_completed"] is False
        assert result["session_failed"] is True

    def test_correlation_score_no_questions_completed(self):
        """Verify correlation score when no questions asked but completed."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "session_completed": True,
            },
        ])

        # Autonomous completion
        assert result["correlation_score"] == 0.5

    def test_correlation_score_no_questions_failed(self):
        """Verify correlation score when no questions asked and failed."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "session_failed": True,
            },
        ])

        # Should have asked for help
        assert result["correlation_score"] == -0.5

    def test_correlation_score_few_questions_completed(self):
        """Verify correlation score with few questions and success."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?"}],
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?"}],
            },
            {
                "turn_index": 2,
                "tool_name": "Write",
                "session_completed": True,
            },
        ])

        # 2 invocations + success = strong positive correlation
        assert result["correlation_score"] == 0.9

    def test_correlation_score_many_questions_completed(self):
        """Verify correlation score with many questions and success."""
        result = analyze_session_askuser_multiround([
            *[
                {
                    "turn_index": i,
                    "tool_name": "AskUserQuestion",
                    "questions": [{"question": f"Q{i}?"}],
                }
                for i in range(7)
            ],
            {
                "turn_index": 7,
                "tool_name": "Write",
                "session_completed": True,
            },
        ])

        # 7 invocations + success = weak positive correlation
        assert result["correlation_score"] == 0.3

    def test_correlation_score_questions_failed(self):
        """Verify correlation score with questions and failure."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?"}],
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?"}],
            },
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q3?"}],
            },
            {
                "turn_index": 3,
                "tool_name": "Write",
                "session_failed": True,
            },
        ])

        # 3 invocations + failure = moderate negative correlation
        assert result["correlation_score"] == -0.7

    def test_correlation_score_many_questions_failed(self):
        """Verify correlation score with many questions and failure."""
        result = analyze_session_askuser_multiround([
            *[
                {
                    "turn_index": i,
                    "tool_name": "AskUserQuestion",
                    "questions": [{"question": f"Q{i}?"}],
                }
                for i in range(10)
            ],
            {
                "turn_index": 10,
                "tool_name": "Write",
                "session_failed": True,
            },
        ])

        # 10 invocations + failure = strong negative correlation
        assert result["correlation_score"] == -0.9

    def test_multiple_questions_per_invocation(self):
        """Verify tracking of multiple questions in single invocation."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {"question": "Q1?"},
                    {"question": "Q2?"},
                    {"question": "Q3?"},
                ],
            },
        ])

        assert result["askuser_invocations"] == 1
        assert result["total_questions_asked"] == 3
        assert result["avg_questions_per_invocation"] == 3.0

    def test_mixed_question_counts_per_invocation(self):
        """Verify average calculation with varying question counts."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?"}],
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {"question": "Q2?"},
                    {"question": "Q3?"},
                    {"question": "Q4?"},
                ],
            },
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {"question": "Q5?"},
                    {"question": "Q6?"},
                ],
            },
        ])

        assert result["askuser_invocations"] == 3
        assert result["total_questions_asked"] == 6
        # (1 + 3 + 2) / 3 = 2.0
        assert result["avg_questions_per_invocation"] == 2.0

    def test_empty_questions_list(self):
        """Verify handling of empty questions list."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [],
            },
        ])

        assert result["askuser_invocations"] == 1
        assert result["total_questions_asked"] == 0

    def test_missing_questions_field(self):
        """Verify handling when questions field is missing."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                # No questions field
            },
        ])

        assert result["askuser_invocations"] == 1
        assert result["total_questions_asked"] == 0

    def test_non_list_questions_field(self):
        """Verify handling when questions field is not a list."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": "not a list",
            },
        ])

        assert result["askuser_invocations"] == 1
        assert result["total_questions_asked"] == 0

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "ASKUSERQUESTION",
                "questions": [{"question": "Q?"}],
            },
            {
                "turn_index": 1,
                "tool_name": "askuserquestion",
                "questions": [{"question": "Q?"}],
            },
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?"}],
            },
        ])

        assert result["askuser_invocations"] == 3

    def test_non_askuser_tools_ignored(self):
        """Verify non-AskUserQuestion tools are ignored."""
        result = analyze_session_askuser_multiround([
            {"turn_index": 0, "tool_name": "Read"},
            {"turn_index": 1, "tool_name": "Edit"},
            {"turn_index": 2, "tool_name": "Write"},
            {
                "turn_index": 3,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?"}],
            },
            {"turn_index": 4, "tool_name": "Bash"},
        ])

        assert result["total_turns"] == 5
        assert result["askuser_invocations"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_askuser_multiround([
            "not a dict",
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?"}],
            },
            None,
            42,
        ])

        assert result["total_turns"] == 1
        assert result["askuser_invocations"] == 1

    def test_whitespace_handling_in_tool_names(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "  AskUserQuestion  ",
                "questions": [{"question": "Q?"}],
            }
        ])

        assert result["askuser_invocations"] == 1

    def test_session_outcome_updates_with_last_record(self):
        """Verify session outcome is determined by last record."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "session_completed": True,
            },
            {
                "turn_index": 1,
                "tool_name": "Edit",
                "session_completed": False,
                "session_failed": True,
            },
        ])

        # Last record wins
        assert result["session_completed"] is False
        assert result["session_failed"] is True

    def test_timestamps_sorted_chronologically(self):
        """Verify timestamps are sorted before interval calculation."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?"}],
                "timestamp": 1100.0,
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?"}],
                "timestamp": 1000.0,
            },
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q3?"}],
                "timestamp": 1050.0,
            },
        ])

        # Sorted: 1000, 1050, 1100
        # Intervals: 50, 50
        assert result["time_between_rounds_seconds"] == [50.0, 50.0]
        assert result["avg_time_between_rounds_seconds"] == 50.0

    def test_zero_timestamp_ignored(self):
        """Verify zero timestamps are ignored."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?"}],
                "timestamp": 0,
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?"}],
                "timestamp": 1000.0,
            },
        ])

        assert len(result["time_between_rounds_seconds"]) == 0

    def test_negative_timestamp_ignored(self):
        """Verify negative timestamps are ignored."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?"}],
                "timestamp": -100.0,
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?"}],
                "timestamp": 1000.0,
            },
        ])

        assert len(result["time_between_rounds_seconds"]) == 0

    def test_string_timestamp_parsed(self):
        """Verify string timestamps are parsed."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?"}],
                "timestamp": "1000.0",
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?"}],
                "timestamp": "1050.0",
            },
        ])

        assert result["time_between_rounds_seconds"] == [50.0]

    def test_no_session_outcome_neutral_correlation(self):
        """Verify correlation score is neutral when no outcome info."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?"}],
            },
        ])

        assert result["correlation_score"] == 0.0

    def test_single_invocation_no_intervals(self):
        """Verify single invocation produces no time intervals."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?"}],
                "timestamp": 1000.0,
            },
        ])

        assert result["time_between_rounds_seconds"] == []
        assert result["avg_time_between_rounds_seconds"] == 0.0

    def test_comprehensive_multi_round_session(self):
        """Verify comprehensive multi-round session with all features."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "Read",
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {"question": "Which approach?"},
                    {"question": "What format?"},
                ],
                "timestamp": 1000.0,
            },
            {
                "turn_index": 2,
                "tool_name": "Edit",
            },
            {
                "turn_index": 3,
                "tool_name": "Write",
            },
            {
                "turn_index": 4,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {"question": "Should we proceed?"},
                ],
                "timestamp": 1065.0,
            },
            {
                "turn_index": 5,
                "tool_name": "Bash",
            },
            {
                "turn_index": 6,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {"question": "Any other changes?"},
                    {"question": "Commit now?"},
                    {"question": "Push to remote?"},
                ],
                "timestamp": 1150.0,
                "session_completed": True,
            },
        ])

        assert result["total_turns"] == 7
        assert result["askuser_invocations"] == 3
        assert result["total_questions_asked"] == 6
        assert result["avg_questions_per_invocation"] == 2.0
        assert result["sessions_with_multiple_rounds"] is True
        assert result["multi_round_session_percentage"] == 100.0
        # Intervals: 1065-1000=65, 1150-1065=85
        assert result["time_between_rounds_seconds"] == [65.0, 85.0]
        assert result["avg_time_between_rounds_seconds"] == 75.0
        assert result["min_time_between_rounds_seconds"] == 65.0
        assert result["max_time_between_rounds_seconds"] == 85.0
        assert result["session_completed"] is True
        # 3 invocations + success = moderate positive correlation
        assert result["correlation_score"] == 0.6
