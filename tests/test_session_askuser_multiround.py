"""Tests for session AskUserQuestion multi-turn conversation flow analyzer."""

import pytest

from synthesis.session_askuser_multiround import (
    analyze_session_askuser_multiround,
    _is_exitplanmode_confusion,
    _evaluate_option_quality,
    _is_multiselect_appropriate,
)


class TestAnalyzeSessionAskUserMultiRound:
    """Test main analyzer function."""

    def test_empty_session(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_askuser_multiround([])

        assert result["total_turns"] == 0
        assert result["askuser_invocations"] == 0
        assert result["questions_per_session"] == 0
        assert result["batch_efficiency_rate"] == 0.0
        assert result["early_planning_rate"] == 0.0
        assert result["option_quality_score"] == 0.0
        assert result["multiselect_appropriate_rate"] == 0.0
        assert result["exitplanmode_confusion_rate"] == 0.0

    def test_none_input(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_askuser_multiround(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_askuser_multiround("not a list")

    def test_single_question_sequential(self):
        """Verify single question per call (sequential pattern)."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which approach should we use?",
                        "options": [
                            {"label": "Approach A", "description": "Use method A for implementation"},
                            {"label": "Approach B", "description": "Use method B for implementation"},
                        ],
                        "multiSelect": False,
                    }
                ],
                "in_plan_mode": True,
            }
        ])

        assert result["askuser_invocations"] == 1
        assert result["total_questions_asked"] == 1
        assert result["sequential_questions"] == 1
        assert result["batched_questions"] == 0
        assert result["batch_efficiency_rate"] == 0.0

    def test_multiple_questions_batched(self):
        """Verify multiple questions in single call (batched pattern)."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which library?",
                        "options": [
                            {"label": "Library A", "description": "Fast and lightweight"},
                            {"label": "Library B", "description": "Feature-rich"},
                        ],
                        "multiSelect": False,
                    },
                    {
                        "question": "Which features to enable?",
                        "options": [
                            {"label": "Feature 1", "description": "Enable caching"},
                            {"label": "Feature 2", "description": "Enable logging"},
                            {"label": "Feature 3", "description": "Enable monitoring"},
                        ],
                        "multiSelect": True,
                    },
                ],
                "in_plan_mode": True,
            }
        ])

        assert result["total_questions_asked"] == 2
        assert result["batched_questions"] == 2
        assert result["batch_efficiency_rate"] == 100.0

    def test_mixed_batching_efficiency(self):
        """Verify batch efficiency calculation with mixed patterns."""
        result = analyze_session_askuser_multiround([
            # Sequential question
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Question 1?", "options": [{"label": "A", "description": "Option A"}]}],
            },
            # Batched questions
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {"question": "Question 2?", "options": [{"label": "A", "description": "Option A"}]},
                    {"question": "Question 3?", "options": [{"label": "A", "description": "Option A"}]},
                ],
            },
        ])

        # 1 sequential + 2 batched = 3 total, 2 batched = 66.67%
        assert result["sequential_questions"] == 1
        assert result["batched_questions"] == 2
        assert result["batch_efficiency_rate"] == 66.67

    def test_early_planning_rate(self):
        """Verify early planning rate calculation."""
        result = analyze_session_askuser_multiround([
            # Early planning question
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Plan question?", "options": [{"label": "A", "description": "Desc"}]}],
                "in_plan_mode": True,
            },
            # Mid-execution question
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Execution question?", "options": [{"label": "A", "description": "Desc"}]}],
                "in_plan_mode": False,
                "after_implementation_start": True,
            },
            # Early planning question (before implementation)
            {
                "turn_index": 2,
                "tool_name": "AskUserQuestion",
                "questions": [{"question": "Another plan question?", "options": [{"label": "A", "description": "Desc"}]}],
                "after_implementation_start": False,
            },
        ])

        # 2 early planning, 1 mid-execution = 66.67%
        assert result["early_planning_questions"] == 2
        assert result["mid_execution_questions"] == 1
        assert result["early_planning_rate"] == 66.67

    def test_option_quality_score_good(self):
        """Verify option quality score with good options."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which approach?",
                        "options": [
                            {"label": "Approach A", "description": "Use Redis for caching with TTL support"},
                            {"label": "Approach B", "description": "Use in-memory cache for faster access"},
                            {"label": "Approach C", "description": "Use file-based cache for persistence"},
                        ],
                    }
                ],
            }
        ])

        # 3 options (ideal), good labels and descriptions
        assert result["option_quality_score"] > 0.8

    def test_option_quality_score_poor(self):
        """Verify option quality score with poor options."""
        result = analyze_session_askuser_multiround([
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "What to do?",
                        "options": [
                            {"label": "This is a very long label with way too many words that makes it hard to read", "description": ""},
                            {"label": "", "description": "Empty label"},
                        ],
                    }
                ],
            }
        ])

        # Poor labels and descriptions
        assert result["option_quality_score"] < 0.65

    def test_multiselect_appropriate_rate(self):
        """Verify multiselect appropriateness detection."""
        result = analyze_session_askuser_multiround([
            # Appropriate multiSelect (features)
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which features to enable?",
                        "options": [
                            {"label": "Enable caching", "description": "Cache API responses"},
                            {"label": "Enable logging", "description": "Log all requests"},
                        ],
                        "multiSelect": True,
                    }
                ],
            },
            # Appropriate single select (approach)
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which approach?",
                        "options": [
                            {"label": "Approach A", "description": "Method A"},
                            {"label": "Approach B", "description": "Method B"},
                        ],
                        "multiSelect": False,
                    }
                ],
            },
        ])

        # Both appropriate = 100%
        assert result["multiselect_total"] == 2
        assert result["multiselect_appropriate"] == 2
        assert result["multiselect_appropriate_rate"] == 100.0

    def test_multiselect_inappropriate(self):
        """Verify detection of inappropriate multiSelect usage."""
        result = analyze_session_askuser_multiround([
            # Inappropriate: approach question with multiSelect
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which approach to use?",
                        "options": [
                            {"label": "Approach A", "description": "Use method A"},
                            {"label": "Approach B", "description": "Use method B"},
                        ],
                        "multiSelect": True,  # Should be False (mutually exclusive)
                    }
                ],
            },
        ])

        assert result["multiselect_appropriate"] == 0
        assert result["multiselect_appropriate_rate"] == 0.0

    def test_exitplanmode_confusion_detection(self):
        """Verify ExitPlanMode confusion anti-pattern detection."""
        result = analyze_session_askuser_multiround([
            # Anti-pattern: asking 'should I proceed'
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Is this plan okay? Should I proceed with implementation?",
                        "options": [
                            {"label": "Yes", "description": "Proceed"},
                            {"label": "No", "description": "Revise plan"},
                        ],
                    }
                ],
            },
            # Normal question
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which database to use?",
                        "options": [
                            {"label": "PostgreSQL", "description": "Use PostgreSQL"},
                            {"label": "MongoDB", "description": "Use MongoDB"},
                        ],
                    }
                ],
            },
        ])

        # 1 out of 2 invocations = 50%
        assert result["exitplanmode_confusion_count"] == 1
        assert result["exitplanmode_confusion_rate"] == 50.0

    def test_realistic_good_pattern(self):
        """Verify realistic good usage pattern."""
        result = analyze_session_askuser_multiround([
            # Early planning with batched questions
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Which authentication method?",
                        "options": [
                            {"label": "JWT", "description": "Use JSON Web Tokens for stateless auth"},
                            {"label": "Sessions", "description": "Use server-side sessions with cookies"},
                        ],
                        "multiSelect": False,
                    },
                    {
                        "question": "Which features to include?",
                        "options": [
                            {"label": "2FA", "description": "Two-factor authentication"},
                            {"label": "OAuth", "description": "Social login support"},
                            {"label": "Email verification", "description": "Verify user emails"},
                        ],
                        "multiSelect": True,
                    },
                ],
                "in_plan_mode": True,
            }
        ])

        # Should show good patterns
        assert result["batch_efficiency_rate"] == 100.0  # All batched
        assert result["early_planning_rate"] == 100.0  # During planning
        assert result["option_quality_score"] > 0.8  # Good quality
        assert result["multiselect_appropriate_rate"] == 100.0  # Appropriate usage
        assert result["exitplanmode_confusion_rate"] == 0.0  # No anti-patterns

    def test_realistic_poor_pattern(self):
        """Verify realistic poor usage pattern."""
        result = analyze_session_askuser_multiround([
            # Sequential question during execution
            {
                "turn_index": 0,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Is this okay?",
                        "options": [
                            {"label": "Yes", "description": ""},
                            {"label": "No", "description": ""},
                        ],
                    }
                ],
                "after_implementation_start": True,
            },
            # Another sequential question
            {
                "turn_index": 1,
                "tool_name": "AskUserQuestion",
                "questions": [
                    {
                        "question": "Should I proceed?",
                        "options": [
                            {"label": "Yes", "description": ""},
                            {"label": "No", "description": ""},
                        ],
                    }
                ],
                "after_implementation_start": True,
            },
        ])

        # Should show poor patterns
        assert result["batch_efficiency_rate"] == 0.0  # All sequential
        assert result["early_planning_rate"] == 0.0  # During execution
        assert result["option_quality_score"] < 0.8  # Poor quality (empty descriptions)
        assert result["exitplanmode_confusion_rate"] == 50.0  # 1 out of 2


class TestHelperFunctions:
    """Test helper functions."""

    def test_is_exitplanmode_confusion_positive(self):
        """Verify detection of ExitPlanMode confusion phrases."""
        assert _is_exitplanmode_confusion("Should I proceed with this plan?")
        assert _is_exitplanmode_confusion("Is this plan okay?")
        assert _is_exitplanmode_confusion("Can I proceed?")
        assert _is_exitplanmode_confusion("Does this look good?")

    def test_is_exitplanmode_confusion_negative(self):
        """Verify normal questions not flagged."""
        assert not _is_exitplanmode_confusion("Which approach should we use?")
        assert not _is_exitplanmode_confusion("What database to choose?")

    def test_evaluate_option_quality_ideal(self):
        """Verify ideal option quality."""
        options = [
            {"label": "Option A", "description": "Clear description of option A"},
            {"label": "Option B", "description": "Clear description of option B"},
            {"label": "Option C", "description": "Clear description of option C"},
        ]
        score = _evaluate_option_quality(options)
        assert score > 0.8

    def test_evaluate_option_quality_poor(self):
        """Verify poor option quality."""
        options = [
            {"label": "", "description": ""},
        ]
        score = _evaluate_option_quality(options)
        assert score < 0.3

    def test_is_multiselect_appropriate_features(self):
        """Verify multiselect appropriate for features."""
        question = "Which features to enable?"
        options = [
            {"label": "Enable caching", "description": "Cache responses"},
            {"label": "Enable logging", "description": "Log requests"},
        ]
        assert _is_multiselect_appropriate(question, options, True)
        assert not _is_multiselect_appropriate(question, options, False)

    def test_is_multiselect_appropriate_approach(self):
        """Verify single select appropriate for approaches."""
        question = "Which approach to use?"
        options = [
            {"label": "Approach A", "description": "Use method A"},
            {"label": "Approach B", "description": "Use method B"},
        ]
        assert not _is_multiselect_appropriate(question, options, True)
        assert _is_multiselect_appropriate(question, options, False)
