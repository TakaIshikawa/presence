"""Tests for session notification and communication pattern analyzer."""

import pytest

from synthesis.session_notification_pattern import analyze_session_notification_pattern


class TestAnalyzeSessionNotificationPattern:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_notification_pattern([])

        assert result["total_turns"] == 0
        assert result["bash_task_calls"] == 0
        assert result["bash_task_with_description"] == 0
        assert result["tool_description_clarity_score"] == 0.0
        assert result["tool_results_count"] == 0
        assert result["results_with_summary"] == 0
        assert result["result_communication_ratio"] == 0.0
        assert result["emoji_usage_count"] == 0
        assert result["turns_with_commentary"] == 0
        assert result["thinking_aloud_ratio"] == 0.0
        assert result["websearch_calls"] == 0
        assert result["websearch_with_sources"] == 0
        assert result["sources_citation_compliance"] == 0.0
        assert result["communication_quality_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_notification_pattern(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_notification_pattern("not a list")

    def test_bash_with_clear_description(self):
        """Verify Bash call with clear description is tracked."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Bash",
                "tool_description": "Run tests for user module",
            }
        ])

        assert result["bash_task_calls"] == 1
        assert result["bash_task_with_description"] == 1
        assert result["tool_description_clarity_score"] == 100.0

    def test_bash_without_description(self):
        """Verify Bash call without description is tracked."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Bash",
                "tool_description": "",
            }
        ])

        assert result["bash_task_calls"] == 1
        assert result["bash_task_with_description"] == 0
        assert result["tool_description_clarity_score"] == 0.0

    def test_task_with_description(self):
        """Verify Task call with description is tracked."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Task",
                "tool_description": "Search codebase for errors",
            }
        ])

        assert result["bash_task_calls"] == 1
        assert result["bash_task_with_description"] == 1

    def test_multiple_bash_task_calls_partial_descriptions(self):
        """Verify partial description coverage tracking."""
        result = analyze_session_notification_pattern([
            {"turn_index": 0, "tool_name": "Bash", "tool_description": "Clear desc"},
            {"turn_index": 1, "tool_name": "Task", "tool_description": ""},
            {"turn_index": 2, "tool_name": "Bash", "tool_description": "Another clear"},
            {"turn_index": 3, "tool_name": "Task", "tool_description": ""},
        ])

        assert result["bash_task_calls"] == 4
        assert result["bash_task_with_description"] == 2
        # 2/4 = 50%
        assert result["tool_description_clarity_score"] == 50.0

    def test_tool_result_with_summary(self):
        """Verify tool result with user-facing summary is tracked."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "tool_result": "File content here...",
                "assistant_response": "The file shows that the configuration is correct.",
            }
        ])

        assert result["tool_results_count"] == 1
        assert result["results_with_summary"] == 1
        assert result["result_communication_ratio"] == 100.0

    def test_tool_result_without_summary(self):
        """Verify tool result without summary is tracked."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "tool_result": "File content here...",
                "assistant_response": "",
            }
        ])

        assert result["tool_results_count"] == 1
        assert result["results_with_summary"] == 0
        assert result["result_communication_ratio"] == 0.0

    def test_multiple_results_partial_summaries(self):
        """Verify partial summary coverage tracking."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Grep",
                "tool_result": "Match found",
                "assistant_response": "I found 3 matches in the codebase.",
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "tool_result": "Content",
                "assistant_response": "",
            },
            {
                "turn_index": 2,
                "tool_name": "Bash",
                "tool_result": "Success",
                "assistant_response": "The command completed successfully.",
            },
        ])

        assert result["tool_results_count"] == 3
        assert result["results_with_summary"] == 2
        # 2/3 = 66.67%
        assert result["result_communication_ratio"] == 66.67

    def test_emoji_usage_detection(self):
        """Verify emoji usage is detected."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Edit",
                "assistant_response": "Updated the file! 🎉 Looking good! ✨",
            }
        ])

        assert result["emoji_usage_count"] >= 1

    def test_no_emoji_usage(self):
        """Verify sessions without emojis."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Edit",
                "assistant_response": "Updated the file. Looking good.",
            }
        ])

        assert result["emoji_usage_count"] == 0

    def test_thinking_aloud_detection(self):
        """Verify thinking aloud commentary is detected."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "assistant_text_before_tools": "Let me read the configuration file to understand the settings.",
            }
        ])

        assert result["turns_with_commentary"] == 1
        # 1/1 = 100%
        assert result["thinking_aloud_ratio"] == 100.0

    def test_thinking_aloud_too_short_ignored(self):
        """Verify very short text before tools is not counted."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "assistant_text_before_tools": "Reading",  # Too short
            }
        ])

        assert result["turns_with_commentary"] == 0

    def test_mixed_thinking_aloud_patterns(self):
        """Verify mixed thinking aloud patterns."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Read",
                "assistant_text_before_tools": "Let me check the file contents first.",
            },
            {
                "turn_index": 1,
                "tool_name": "Edit",
                "assistant_text_before_tools": "",
            },
            {
                "turn_index": 2,
                "tool_name": "Write",
                "assistant_text_before_tools": "",
            },
        ])

        assert result["total_turns"] == 3
        assert result["turns_with_commentary"] == 1
        # 1/3 = 33.33%
        assert result["thinking_aloud_ratio"] == 33.33

    def test_websearch_with_sources_citation(self):
        """Verify WebSearch with Sources section is tracked."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "WebSearch",
                "assistant_response": """
Found relevant information.

Sources:
- [Article Title](https://example.com/article)
                """,
            }
        ])

        assert result["websearch_calls"] == 1
        assert result["websearch_with_sources"] == 1
        assert result["sources_citation_compliance"] == 100.0

    def test_websearch_without_sources_citation(self):
        """Verify WebSearch without Sources section is tracked."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "WebSearch",
                "assistant_response": "Found some information but no citation.",
            }
        ])

        assert result["websearch_calls"] == 1
        assert result["websearch_with_sources"] == 0
        assert result["sources_citation_compliance"] == 0.0

    def test_multiple_websearch_partial_citations(self):
        """Verify partial citation compliance tracking."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "WebSearch",
                "assistant_response": "Results.\n\nSources:\n- [Link](https://example.com)",
            },
            {
                "turn_index": 1,
                "tool_name": "WebSearch",
                "assistant_response": "More results without citation.",
            },
            {
                "turn_index": 2,
                "tool_name": "WebSearch",
                "assistant_response": "## Sources\n- [Article](https://test.com)",
            },
        ])

        assert result["websearch_calls"] == 3
        assert result["websearch_with_sources"] == 2
        # 2/3 = 66.67%
        assert result["sources_citation_compliance"] == 66.67

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_notification_pattern([
            {"turn_index": 0, "tool_name": "BASH", "tool_description": "Test"},
            {"turn_index": 1, "tool_name": "task", "tool_description": "Search"},
            {"turn_index": 2, "tool_name": "WebSearch", "assistant_response": "Sources:\n- [L](https://x.com)"},
        ])

        assert result["bash_task_calls"] == 2
        assert result["websearch_calls"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_notification_pattern([
            "not a dict",
            {"turn_index": 0, "tool_name": "Bash", "tool_description": "Test"},
        ])

        assert result["total_turns"] == 1
        assert result["bash_task_calls"] == 1

    def test_optimal_communication_pattern(self):
        """Verify optimal communication pattern scores highly."""
        result = analyze_session_notification_pattern([
            # Good mix: 5 out of 10 turns have commentary (50%)
            *[
                {
                    "turn_index": i,
                    "tool_name": "Bash" if i % 2 == 0 else "Read",
                    "tool_description": "Clear description here",
                    "tool_result": "Result",
                    "assistant_response": "The command completed successfully.",
                    "assistant_text_before_tools": "Let me check this." if i < 5 else "",
                }
                for i in range(10)
            ]
        ])

        assert result["tool_description_clarity_score"] == 100.0
        assert result["result_communication_ratio"] == 100.0
        assert result["emoji_usage_count"] == 0
        assert result["thinking_aloud_ratio"] == 50.0
        # High quality score
        assert result["communication_quality_score"] >= 0.85

    def test_anti_pattern_missing_descriptions(self):
        """Verify anti-pattern of missing tool descriptions."""
        result = analyze_session_notification_pattern([
            {"turn_index": 0, "tool_name": "Bash", "tool_description": ""},
            {"turn_index": 1, "tool_name": "Task", "tool_description": ""},
            {"turn_index": 2, "tool_name": "Bash", "tool_description": ""},
        ])

        assert result["tool_description_clarity_score"] == 0.0
        # Low score due to missing descriptions
        assert result["communication_quality_score"] < 0.3

    def test_anti_pattern_excessive_emojis(self):
        """Verify anti-pattern of excessive emoji usage."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Edit",
                "assistant_response": "Done! 🎉✨🚀💯👍🔥⭐️",
            }
        ])

        # Emoji detection may vary, but should detect at least 1
        assert result["emoji_usage_count"] >= 1
        # Penalized for emoji usage
        assert result["communication_quality_score"] < 0.8

    def test_anti_pattern_missing_result_summaries(self):
        """Verify anti-pattern of missing result summaries."""
        result = analyze_session_notification_pattern([
            {"turn_index": 0, "tool_name": "Read", "tool_result": "Content", "assistant_response": ""},
            {"turn_index": 1, "tool_name": "Grep", "tool_result": "Matches", "assistant_response": ""},
            {"turn_index": 2, "tool_name": "Bash", "tool_result": "Output", "assistant_response": ""},
        ])

        assert result["result_communication_ratio"] == 0.0
        # Low score due to no summaries
        assert result["communication_quality_score"] < 0.4

    def test_quality_score_components(self):
        """Verify quality score calculation components."""
        result = analyze_session_notification_pattern([
            {
                "turn_index": 0,
                "tool_name": "Bash",
                "tool_description": "Run comprehensive tests",
                "tool_result": "Tests passed",
                "assistant_response": "All tests completed successfully with no errors.",
                "assistant_text_before_tools": "Let me run the test suite.",
            },
            {
                "turn_index": 1,
                "tool_name": "WebSearch",
                "assistant_response": "Found docs.\n\nSources:\n- [Docs](https://example.com)",
            },
        ])

        # 100% description clarity (1/1)
        assert result["tool_description_clarity_score"] == 100.0
        # 100% result communication (1/1)
        assert result["result_communication_ratio"] == 100.0
        # 0 emojis
        assert result["emoji_usage_count"] == 0
        # 50% thinking aloud (1/2)
        assert result["thinking_aloud_ratio"] == 50.0
        # 100% citation compliance (1/1)
        assert result["sources_citation_compliance"] == 100.0
        # High overall quality
        assert result["communication_quality_score"] > 0.9

    def test_non_bash_task_tools_ignored(self):
        """Verify non-Bash/Task tools don't affect description clarity."""
        result = analyze_session_notification_pattern([
            {"turn_index": 0, "tool_name": "Read"},
            {"turn_index": 1, "tool_name": "Edit"},
            {"turn_index": 2, "tool_name": "Bash", "tool_description": "Test command here"},
        ])

        assert result["bash_task_calls"] == 1
        assert result["bash_task_with_description"] == 1

    def test_comprehensive_session_scenario(self):
        """Verify comprehensive session with mixed patterns."""
        result = analyze_session_notification_pattern([
            # Turn 0: Bash with description, result with summary, commentary
            {
                "turn_index": 0,
                "tool_name": "Bash",
                "tool_description": "Install dependencies",
                "tool_result": "Installed 15 packages",
                "assistant_response": "Successfully installed all dependencies.",
                "assistant_text_before_tools": "Let me install the required packages.",
            },
            # Turn 1: Task without description
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_description": "",
            },
            # Turn 2: Read with result but no summary
            {
                "turn_index": 2,
                "tool_name": "Read",
                "tool_result": "Config content",
                "assistant_response": "",
            },
            # Turn 3: WebSearch without sources
            {
                "turn_index": 3,
                "tool_name": "WebSearch",
                "assistant_response": "Found some info",
            },
            # Turn 4: Edit with emoji
            {
                "turn_index": 4,
                "tool_name": "Edit",
                "assistant_response": "Updated! 🎉",
            },
        ])

        assert result["total_turns"] == 5
        # 1/2 = 50% description clarity
        assert result["bash_task_calls"] == 2
        assert result["bash_task_with_description"] == 1
        assert result["tool_description_clarity_score"] == 50.0
        # 1/2 = 50% result communication
        assert result["tool_results_count"] == 2
        assert result["results_with_summary"] == 1
        assert result["result_communication_ratio"] == 50.0
        # 1 emoji
        assert result["emoji_usage_count"] >= 1
        # 1/5 = 20% thinking aloud
        assert result["turns_with_commentary"] == 1
        assert result["thinking_aloud_ratio"] == 20.0
        # 0/1 = 0% citation compliance
        assert result["websearch_calls"] == 1
        assert result["websearch_with_sources"] == 0
        assert result["sources_citation_compliance"] == 0.0
        # Moderate quality score
        assert 0.2 < result["communication_quality_score"] < 0.6
