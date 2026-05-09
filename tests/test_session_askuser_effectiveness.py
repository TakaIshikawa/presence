"""Tests for session AskUserQuestion effectiveness analyzer."""

import pytest

from synthesis.session_askuser_effectiveness import analyze_session_askuser_effectiveness


class TestAnalyzeSessionAskUserEffectiveness:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_askuser_effectiveness([])

        assert result["total_turns"] == 0
        assert result["total_questions_asked"] == 0
        assert result["turns_with_questions"] == 0
        assert result["questions_per_turn"] == 0.0
        assert result["total_question_count"] == 0
        assert result["avg_questions_per_call"] == 0.0
        assert result["multiselect_questions"] == 0
        assert result["single_choice_questions"] == 0
        assert result["multiselect_ratio"] == 0.0
        assert result["questions_with_responses"] == 0
        assert result["response_rate"] == 0.0
        assert result["avg_response_latency_seconds"] == 0.0
        assert result["questions_leading_to_completion"] == 0
        assert result["questions_leading_to_abandonment"] == 0
        assert result["completion_correlation_rate"] == 0.0
        assert result["avg_options_per_question"] == 0.0
        assert result["avg_description_length"] == 0.0
        assert result["question_clarity_score"] == 0.0
        assert result["overall_effectiveness_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_askuser_effectiveness(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_askuser_effectiveness("not a list")

    def test_single_question_single_choice(self):
        """Verify single AskUserQuestion with single-choice question."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which approach should we use?",
                        "multiSelect": False,
                        "options": [
                            {"label": "Option A", "description": "Description A"},
                            {"label": "Option B", "description": "Description B"},
                        ],
                    }
                ],
                "user_responded": True,
                "response_latency_seconds": 30.0,
                "task_completed": True,
                "task_abandoned": False,
            }
        ])

        assert result["total_questions_asked"] == 1
        assert result["total_question_count"] == 1
        assert result["single_choice_questions"] == 1
        assert result["multiselect_questions"] == 0
        assert result["multiselect_ratio"] == 0.0
        assert result["questions_with_responses"] == 1
        assert result["response_rate"] == 100.0
        assert result["avg_response_latency_seconds"] == 30.0
        assert result["questions_leading_to_completion"] == 1
        assert result["completion_correlation_rate"] == 100.0

    def test_single_question_multiselect(self):
        """Verify single AskUserQuestion with multiSelect question."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which features do you want?",
                        "multiSelect": True,
                        "options": [
                            {"label": "Feature 1", "description": "First feature"},
                            {"label": "Feature 2", "description": "Second feature"},
                            {"label": "Feature 3", "description": "Third feature"},
                        ],
                    }
                ],
                "user_responded": True,
            }
        ])

        assert result["multiselect_questions"] == 1
        assert result["single_choice_questions"] == 0
        assert result["multiselect_ratio"] == 100.0
        assert result["avg_options_per_question"] == 3.0

    def test_multiple_questions_in_one_call(self):
        """Verify AskUserQuestion with multiple questions."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Question 1?",
                        "multiSelect": False,
                        "options": [
                            {"label": "A", "description": "Option A"},
                            {"label": "B", "description": "Option B"},
                        ],
                    },
                    {
                        "question": "Question 2?",
                        "multiSelect": True,
                        "options": [
                            {"label": "X", "description": "Option X"},
                            {"label": "Y", "description": "Option Y"},
                        ],
                    },
                ],
                "user_responded": True,
            }
        ])

        assert result["total_questions_asked"] == 1
        assert result["total_question_count"] == 2
        assert result["avg_questions_per_call"] == 2.0
        assert result["single_choice_questions"] == 1
        assert result["multiselect_questions"] == 1
        assert result["multiselect_ratio"] == 50.0

    def test_questions_leading_to_abandonment(self):
        """Verify questions that lead to task abandonment."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Should we proceed?",
                        "multiSelect": False,
                        "options": [
                            {"label": "Yes", "description": "Continue"},
                            {"label": "No", "description": "Stop"},
                        ],
                    }
                ],
                "user_responded": True,
                "task_completed": False,
                "task_abandoned": True,
            }
        ])

        assert result["questions_leading_to_abandonment"] == 1
        assert result["questions_leading_to_completion"] == 0
        assert result["completion_correlation_rate"] == 0.0

    def test_mixed_completion_outcomes(self):
        """Verify mixed completion and abandonment outcomes."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?", "multiSelect": False, "options": []}],
                "task_completed": True,
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?", "multiSelect": False, "options": []}],
                "task_completed": True,
            },
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q3?", "multiSelect": False, "options": []}],
                "task_abandoned": True,
            },
        ])

        assert result["questions_leading_to_completion"] == 2
        assert result["questions_leading_to_abandonment"] == 1
        # 2/3 = 66.67%
        assert result["completion_correlation_rate"] == 66.67

    def test_no_user_response(self):
        """Verify questions without user responses."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
                "user_responded": False,
            }
        ])

        assert result["questions_with_responses"] == 0
        assert result["response_rate"] == 0.0

    def test_response_latency_tracking(self):
        """Verify response latency is tracked."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q1?", "multiSelect": False, "options": []}],
                "response_latency_seconds": 10.0,
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q2?", "multiSelect": False, "options": []}],
                "response_latency_seconds": 20.0,
            },
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q3?", "multiSelect": False, "options": []}],
                "response_latency_seconds": 30.0,
            },
        ])

        # (10 + 20 + 30) / 3 = 20.0
        assert result["avg_response_latency_seconds"] == 20.0

    def test_option_count_tracking(self):
        """Verify option count per question is tracked."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Q1?",
                        "multiSelect": False,
                        "options": [
                            {"label": "A", "description": "Option A"},
                            {"label": "B", "description": "Option B"},
                        ],
                    },
                    {
                        "question": "Q2?",
                        "multiSelect": False,
                        "options": [
                            {"label": "X", "description": "Option X"},
                            {"label": "Y", "description": "Option Y"},
                            {"label": "Z", "description": "Option Z"},
                            {"label": "W", "description": "Option W"},
                        ],
                    },
                ],
            }
        ])

        # (2 + 4) / 2 = 3.0
        assert result["avg_options_per_question"] == 3.0

    def test_description_length_tracking(self):
        """Verify description length is tracked."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Q?",
                        "multiSelect": False,
                        "options": [
                            {"label": "A", "description": "Short"},  # 5 chars
                            {"label": "B", "description": "A much longer description"},  # 25 chars
                        ],
                    }
                ],
            }
        ])

        # (5 + 25) / 2 = 15.0
        assert result["avg_description_length"] == 15.0

    def test_questions_per_turn_calculation(self):
        """Verify questions per turn calculation."""
        result = analyze_session_askuser_effectiveness([
            # Turn 0: No question
            {"turn_index": 0, "tool_name": "Read"},
            # Turn 1: Question
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
            },
            # Turn 2: No question
            {"turn_index": 2, "tool_name": "Edit"},
            # Turn 3: Question
            {
                "turn_index": 3,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
            },
        ])

        assert result["total_turns"] == 4
        assert result["turns_with_questions"] == 2
        # 2 questions / 2 turns with questions = 1.0
        assert result["questions_per_turn"] == 1.0

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "ASKUSERQUESTION",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
            },
            {
                "turn_index": 1,
                "tool_name": "askuserquestion",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
            },
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
            },
        ])

        assert result["total_questions_asked"] == 3

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_askuser_effectiveness([
            "not a dict",
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
            },
        ])

        # Only dict records counted in total_turns
        assert result["total_turns"] == 1
        assert result["total_questions_asked"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_askuser_effectiveness([
            {"turn_index": 0},
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
            },
        ])

        assert result["total_turns"] == 2
        assert result["total_questions_asked"] == 1

    def test_whitespace_handling_in_tool_names(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "  AskUserQuestion  ",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
            }
        ])

        assert result["total_questions_asked"] == 1

    def test_optimal_pattern_high_clarity(self):
        """Verify optimal pattern with high clarity score."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which library should we use?",
                        "multiSelect": False,
                        "options": [
                            {
                                "label": "Library A",
                                "description": "A well-tested library with comprehensive documentation and active maintenance",
                            },
                            {
                                "label": "Library B",
                                "description": "A newer library with modern features and excellent performance",
                            },
                            {
                                "label": "Library C",
                                "description": "A lightweight library with minimal dependencies and simple API",
                            },
                        ],
                    }
                ],
                "user_responded": True,
                "response_latency_seconds": 45.0,
                "task_completed": True,
            }
        ])

        assert result["avg_options_per_question"] == 3.0
        # Each description ~80-90 chars
        assert result["avg_description_length"] > 50
        assert result["question_clarity_score"] > 80.0
        # High effectiveness with 1 question in 1 turn (100% frequency hurts autonomy)
        assert result["overall_effectiveness_score"] > 0.5

    def test_anti_pattern_excessive_questions(self):
        """Verify anti-pattern of excessive questions."""
        # Create many questions across many turns
        records = []
        for i in range(20):
            records.append({
                "turn_index": i,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": f"Q{i}?", "multiSelect": False, "options": []}],
            })

        result = analyze_session_askuser_effectiveness(records)

        assert result["total_questions_asked"] == 20
        assert result["total_turns"] == 20
        # Low autonomy score due to high question frequency
        assert result["overall_effectiveness_score"] < 0.3

    def test_anti_pattern_unclear_questions(self):
        """Verify anti-pattern of unclear questions."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "What?",
                        "multiSelect": False,
                        "options": [
                            {"label": "A", "description": "x"},  # Too short
                            {"label": "B", "description": "y"},
                        ],
                    }
                ],
            }
        ])

        # Very short descriptions lower clarity score
        assert result["avg_description_length"] == 1.0
        # Clarity still gets points for having questions and 2 options
        assert result["question_clarity_score"] < 70.0

    def test_anti_pattern_too_many_options(self):
        """Verify anti-pattern of too many options."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Choose one:",
                        "multiSelect": False,
                        "options": [
                            {"label": f"Option {i}", "description": f"Description {i}"}
                            for i in range(10)
                        ],
                    }
                ],
            }
        ])

        assert result["avg_options_per_question"] == 10.0
        # Clarity score penalized for too many options
        assert result["question_clarity_score"] < 80.0

    def test_effectiveness_score_components(self):
        """Verify effectiveness score calculation components."""
        # Good pattern: few questions, high completion, high clarity
        result = analyze_session_askuser_effectiveness([
            # 1 question in 20 turns = 5% frequency (good autonomy)
            *[{"turn_index": i, "tool_name": "Read"} for i in range(19)],
            {
                "turn_index": 19,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which approach?",
                        "multiSelect": False,
                        "options": [
                            {
                                "label": "Approach A",
                                "description": "Well-documented approach with proven track record",
                            },
                            {
                                "label": "Approach B",
                                "description": "Modern approach with better performance characteristics",
                            },
                        ],
                    }
                ],
                "user_responded": True,
                "response_latency_seconds": 30.0,
                "task_completed": True,
            },
        ])

        assert result["total_turns"] == 20
        assert result["total_questions_asked"] == 1
        # High autonomy (few questions)
        # High completion (100%)
        # High response rate (100%)
        # Good clarity
        assert result["overall_effectiveness_score"] > 0.8

    def test_multiselect_false_default(self):
        """Verify multiSelect defaults to False when not specified."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Q?",
                        # multiSelect not specified
                        "options": [
                            {"label": "A", "description": "Option A"},
                            {"label": "B", "description": "Option B"},
                        ],
                    }
                ],
            }
        ])

        assert result["single_choice_questions"] == 1
        assert result["multiselect_questions"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields are handled gracefully."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Q?",
                        "multiSelect": False,
                        # No options
                    }
                ],
                # No user_responded, response_latency_seconds, task_completed, etc.
            }
        ])

        assert result["total_questions_asked"] == 1
        assert result["questions_with_responses"] == 0
        assert result["avg_response_latency_seconds"] == 0.0
        assert result["questions_leading_to_completion"] == 0

    def test_empty_questions_list(self):
        """Verify empty questions list is handled."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [],
            }
        ])

        assert result["total_questions_asked"] == 1
        assert result["total_question_count"] == 0

    def test_malformed_questions_skipped(self):
        """Verify malformed questions in list are skipped."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    "not a dict",
                    {
                        "question": "Q?",
                        "multiSelect": False,
                        "options": [{"label": "A", "description": "Opt A"}],
                    },
                ],
            }
        ])

        assert result["total_question_count"] == 2
        assert result["single_choice_questions"] == 1  # Only valid question counted

    def test_malformed_options_skipped(self):
        """Verify malformed options are skipped."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Q?",
                        "multiSelect": False,
                        "options": [
                            "not a dict",
                            {"label": "A", "description": "Opt A"},
                        ],
                    }
                ],
            }
        ])

        assert result["avg_options_per_question"] == 2.0
        # Only valid option's description counted
        assert result["avg_description_length"] == 5.0

    def test_zero_response_latency_ignored(self):
        """Verify zero response latency is not included in average."""
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
                "response_latency_seconds": 0,  # Should be ignored
            },
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
                "response_latency_seconds": 30.0,
            },
        ])

        # Only non-zero latency counted
        assert result["avg_response_latency_seconds"] == 30.0

    def test_clarity_score_boundary_conditions(self):
        """Verify clarity score boundary conditions."""
        # Perfect clarity: 3 options, 100 char descriptions
        result = analyze_session_askuser_effectiveness([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Q?",
                        "multiSelect": False,
                        "options": [
                            {"label": "A", "description": "x" * 100},
                            {"label": "B", "description": "y" * 100},
                            {"label": "C", "description": "z" * 100},
                        ],
                    }
                ],
            }
        ])

        assert result["avg_options_per_question"] == 3.0
        assert result["avg_description_length"] == 100.0
        # Should be near perfect
        assert result["question_clarity_score"] == 100.0

    def test_non_askuser_tools_ignored(self):
        """Verify non-AskUserQuestion tools are ignored."""
        result = analyze_session_askuser_effectiveness([
            {"turn_index": 0, "tool_name": "Read"},
            {"turn_index": 1, "tool_name": "Edit"},
            {"turn_index": 2, "tool_name": "Write"},
            {
                "turn_index": 3,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Q?", "multiSelect": False, "options": []}],
            },
            {"turn_index": 4, "tool_name": "Bash"},
        ])

        assert result["total_turns"] == 5
        assert result["total_questions_asked"] == 1
        assert result["turns_with_questions"] == 1
