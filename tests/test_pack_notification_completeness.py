"""Tests for pack user notification completeness analyzer."""

import pytest

from synthesis.pack_notification_completeness import analyze_pack_notification_completeness


class TestAnalyzePackNotificationCompleteness:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty packs returns zero metrics."""
        result = analyze_pack_notification_completeness([])

        assert result["total_packs"] == 0
        assert result["total_sessions"] == 0
        assert result["sessions_with_final_message"] == 0
        assert result["notification_completeness_score"] == 0.0
        assert result["total_tool_errors"] == 0
        assert result["errors_escalated_to_user"] == 0
        assert result["error_escalation_rate"] == 0.0
        assert result["silent_failures"] == 0
        assert result["total_sessions_with_pr_creation"] == 0
        assert result["pr_creation_with_url_reported"] == 0
        assert result["missing_pr_urls"] == 0
        assert result["total_md_file_creations"] == 0
        assert result["proactive_md_creations"] == 0
        assert result["proactive_md_creation_rate"] == 0.0
        assert result["sessions_using_askuser"] == 0
        assert result["clarification_usage_rate"] == 0.0
        assert result["example_good_notification"] == {}
        assert result["example_silent_failure"] == {}
        assert result["example_missing_pr_url"] == {}
        assert result["example_proactive_md"] == {}

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_notification_completeness(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_notification_completeness("not a list")

    def test_session_with_final_message(self):
        """Verify detection of session ending with user-facing message."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "I've completed the task successfully.",
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_final_message"] == 1
        assert result["notification_completeness_score"] == 100.0

    def test_session_without_final_message(self):
        """Verify detection of session ending without user message."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "",  # No text content
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"}
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_final_message"] == 0
        assert result["notification_completeness_score"] == 0.0

    def test_error_escalation_to_user(self):
        """Verify detection of error escalated to user."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [{"tool_name": "Bash", "command": "pytest"}],
                                "tool_results": [{"exit_code": 1, "output": "Test failed"}]
                            },
                            {
                                "message_index": 1,
                                "role": "assistant",
                                "text_content": "The tests failed with the following error...",
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_tool_errors"] == 1
        assert result["errors_escalated_to_user"] == 1
        assert result["error_escalation_rate"] == 100.0
        assert result["silent_failures"] == 0

    def test_silent_failure_not_escalated(self):
        """Verify detection of silent failure (error not communicated)."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [{"tool_name": "Bash", "command": "pytest"}],
                                "tool_results": [{"exit_code": 1, "output": "Test failed"}]
                            }
                            # No follow-up message to user
                        ]
                    }
                ]
            }
        ])

        assert result["total_tool_errors"] == 1
        assert result["errors_escalated_to_user"] == 0
        assert result["error_escalation_rate"] == 0.0
        assert result["silent_failures"] == 1
        assert result["example_silent_failure"]["session_id"] == "session1"

    def test_pr_creation_with_url_reported(self):
        """Verify detection of PR creation with URL reporting."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "gh pr create --title 'Fix'"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "role": "assistant",
                                "text_content": "Created PR: https://github.com/user/repo/pull/123",
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_sessions_with_pr_creation"] == 1
        assert result["pr_creation_with_url_reported"] == 1
        assert result["missing_pr_urls"] == 0

    def test_pr_creation_without_url_reported(self):
        """Verify detection of PR creation without URL reporting."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "gh pr create --title 'Fix'"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "role": "assistant",
                                "text_content": "Done.",  # No URL
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_sessions_with_pr_creation"] == 1
        assert result["pr_creation_with_url_reported"] == 0
        assert result["missing_pr_urls"] == 1
        assert result["example_missing_pr_url"]["session_id"] == "session1"

    def test_proactive_md_creation_without_request(self):
        """Verify detection of proactive .md file creation."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "user",
                                "text_content": "Add a new feature",
                                "tool_calls": []
                            },
                            {
                                "message_index": 1,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "Write", "file_path": "README.md"}
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_md_file_creations"] == 1
        assert result["proactive_md_creations"] == 1
        assert result["proactive_md_creation_rate"] == 100.0
        assert result["example_proactive_md"]["session_id"] == "session1"

    def test_explicit_md_creation_request(self):
        """Verify .md creation with explicit user request not flagged."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "user",
                                "text_content": "Create a README for this project",
                                "tool_calls": []
                            },
                            {
                                "message_index": 1,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "Write", "file_path": "README.md"}
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_md_file_creations"] == 1
        assert result["proactive_md_creations"] == 0
        assert result["proactive_md_creation_rate"] == 0.0

    def test_askuser_question_usage(self):
        """Verify detection of AskUserQuestion usage."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "AskUserQuestion", "questions": []}
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["sessions_using_askuser"] == 1
        assert result["clarification_usage_rate"] == 100.0

    def test_session_without_askuser_usage(self):
        """Verify sessions not using AskUserQuestion."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "Done",
                                "tool_calls": [
                                    {"tool_name": "Write", "file_path": "a.py"}
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["sessions_using_askuser"] == 0
        assert result["clarification_usage_rate"] == 0.0

    def test_example_good_notification_captured(self):
        """Verify good notification example is captured."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "Task completed successfully. All tests pass.",
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["example_good_notification"]["session_id"] == "session1"
        assert "completed" in result["example_good_notification"]["final_message"].lower()

    def test_multiple_packs_aggregation(self):
        """Verify metrics are aggregated across multiple packs."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "Done",
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            },
            {
                "pack_id": "pack2",
                "sessions": [
                    {
                        "session_id": "session2",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "Complete",
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 2
        assert result["total_sessions"] == 2
        assert result["sessions_with_final_message"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_notification_completeness([
            "not a dict",
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "Done",
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_sessions"] == 1

    def test_realistic_good_session_pattern(self):
        """Verify realistic good session with proper notifications."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "user",
                                "text_content": "Add a new feature to handle errors",
                                "tool_calls": []
                            },
                            {
                                "message_index": 1,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "AskUserQuestion", "questions": []}
                                ]
                            },
                            {
                                "message_index": 2,
                                "role": "user",
                                "text_content": "Use try-catch approach",
                                "tool_calls": []
                            },
                            {
                                "message_index": 3,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "Write", "file_path": "error_handler.py"}
                                ]
                            },
                            {
                                "message_index": 4,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "pytest tests/test_error_handler.py"}
                                ],
                                "tool_results": [{"exit_code": 0, "output": "All tests passed"}]
                            },
                            {
                                "message_index": 5,
                                "role": "assistant",
                                "text_content": "Implementation complete. All tests pass. The error handler is ready.",
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["notification_completeness_score"] == 100.0
        assert result["sessions_using_askuser"] == 1
        assert result["error_escalation_rate"] == 0.0  # No errors
        assert result["proactive_md_creations"] == 0

    def test_realistic_poor_session_pattern(self):
        """Verify realistic poor session with missing notifications."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "user",
                                "text_content": "Fix the bug",
                                "tool_calls": []
                            },
                            {
                                "message_index": 1,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "Edit", "file_path": "main.py"}
                                ]
                            },
                            {
                                "message_index": 2,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "pytest"}
                                ],
                                "tool_results": [{"exit_code": 1, "output": "Error: Test failed"}]
                            },
                            # No message to user about error!
                            {
                                "message_index": 3,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [
                                    {"tool_name": "Write", "file_path": "CHANGELOG.md"}  # Proactive
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["notification_completeness_score"] == 0.0  # No final message
        assert result["silent_failures"] == 1  # Error not escalated
        assert result["proactive_md_creations"] == 1  # CHANGELOG created proactively
        assert result["sessions_using_askuser"] == 0  # No clarification

    def test_error_field_in_tool_result(self):
        """Verify detection of error via error field in tool result."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "",
                                "tool_calls": [{"tool_name": "Read", "file_path": "missing.py"}],
                                "tool_results": [{"error": "File not found"}]
                            },
                            {
                                "message_index": 1,
                                "role": "assistant",
                                "text_content": "The file is missing.",
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_tool_errors"] == 1
        assert result["errors_escalated_to_user"] == 1
