<<<<<<< HEAD
"""Tests for pack user notification completeness analyzer."""
=======
"""Tests for pack notification completeness and session boundary discipline analyzer."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E

import pytest

from synthesis.pack_notification_completeness import analyze_pack_notification_completeness


class TestAnalyzePackNotificationCompleteness:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty packs returns zero metrics."""
        result = analyze_pack_notification_completeness([])

        assert result["total_packs"] == 0
        assert result["total_sessions"] == 0
<<<<<<< HEAD
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
=======
        assert result["sessions_with_explicit_completion"] == 0
        assert result["sessions_without_completion"] == 0
        assert result["explicit_completion_rate"] == 0.0
        assert result["total_errors"] == 0
        assert result["errors_communicated"] == 0
        assert result["errors_silent"] == 0
        assert result["error_notification_rate"] == 0.0
        assert result["long_sessions"] == 0
        assert result["long_sessions_with_progress"] == 0
        assert result["progress_update_frequency"] == 0.0
        assert result["messages_with_markdown"] == 0
        assert result["total_messages"] == 0
        assert result["markdown_usage_consistency"] == 0.0
        assert result["tool_communication_violations"] == 0
        assert result["example_good_completion"] == {}
        assert result["example_poor_completion"] == {}
        assert result["example_good_error_notification"] == {}
        assert result["example_poor_error_handling"] == {}
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_notification_completeness(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_notification_completeness("not a list")

<<<<<<< HEAD
    def test_session_with_final_message(self):
        """Verify detection of session ending with user-facing message."""
=======
    def test_session_with_explicit_completion(self):
        """Verify session ending with clear completion summary."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "I've completed the task successfully.",
                                "tool_calls": []
                            }
                        ]
=======
                        "duration_minutes": 3,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "I'm working on implementing the feature.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 1,
                                "text": "Implementation complete. Successfully created the feature and all tests pass.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

        assert result["total_sessions"] == 1
<<<<<<< HEAD
        assert result["sessions_with_final_message"] == 1
        assert result["notification_completeness_score"] == 100.0

    def test_session_without_final_message(self):
        """Verify detection of session ending without user message."""
=======
        assert result["sessions_with_explicit_completion"] == 1
        assert result["sessions_without_completion"] == 0
        assert result["explicit_completion_rate"] == 100.0
        assert result["example_good_completion"]["session_id"] == "session1"

    def test_session_without_explicit_completion(self):
        """Verify session ending abruptly without outcome summary."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 2,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Reading the file.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 1,
                                "text": "I see.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

        assert result["total_sessions"] == 1
<<<<<<< HEAD
        assert result["sessions_with_final_message"] == 0
        assert result["notification_completeness_score"] == 0.0

    def test_error_escalation_to_user(self):
        """Verify detection of error escalated to user."""
=======
        assert result["sessions_with_explicit_completion"] == 0
        assert result["sessions_without_completion"] == 1
        assert result["explicit_completion_rate"] == 0.0
        assert result["example_poor_completion"]["session_id"] == "session1"

    def test_error_communicated_to_user(self):
        """Verify errors that are properly communicated to user."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 3,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Running tests.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 1,
                                "text": "The test failed with error: assertion failed.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [
                            {
                                "error_type": "test_failure",
                                "error_message": "assertion failed",
                                "turn_index": 1,
                                "was_communicated": True,
                            }
                        ],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

<<<<<<< HEAD
        assert result["total_tool_errors"] == 1
        assert result["errors_escalated_to_user"] == 1
        assert result["error_escalation_rate"] == 100.0
        assert result["silent_failures"] == 0

    def test_silent_failure_not_escalated(self):
        """Verify detection of silent failure (error not communicated)."""
=======
        assert result["total_errors"] == 1
        assert result["errors_communicated"] == 1
        assert result["errors_silent"] == 0
        assert result["error_notification_rate"] == 100.0
        assert result["example_good_error_notification"]["error_type"] == "test_failure"

    def test_error_not_communicated_to_user(self):
        """Verify errors that are silently handled without user notification."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 3,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Running build.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 1,
                                "text": "Continuing with next step.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [
                            {
                                "error_type": "build_error",
                                "error_message": "syntax error",
                                "turn_index": 0,
                                "was_communicated": False,
                            }
                        ],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

<<<<<<< HEAD
        assert result["total_sessions_with_pr_creation"] == 1
        assert result["pr_creation_with_url_reported"] == 1
        assert result["missing_pr_urls"] == 0

    def test_pr_creation_without_url_reported(self):
        """Verify detection of PR creation without URL reporting."""
=======
        assert result["total_errors"] == 1
        assert result["errors_communicated"] == 0
        assert result["errors_silent"] == 1
        assert result["error_notification_rate"] == 0.0
        assert result["example_poor_error_handling"]["error_type"] == "build_error"

    def test_long_session_with_progress_updates(self):
        """Verify long session (>5min) with regular progress updates."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 8,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Starting implementation of the feature.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 1,
                                "text": "Currently working on the core logic.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 2,
                                "text": "Processing the test cases now.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 3,
                                "text": "Done. All tests pass.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

<<<<<<< HEAD
        assert result["total_sessions_with_pr_creation"] == 1
        assert result["pr_creation_with_url_reported"] == 0
        assert result["missing_pr_urls"] == 1
        assert result["example_missing_pr_url"]["session_id"] == "session1"

    def test_proactive_md_creation_without_request(self):
        """Verify detection of proactive .md file creation."""
=======
        assert result["long_sessions"] == 1
        assert result["long_sessions_with_progress"] == 1
        assert result["progress_update_frequency"] == 100.0

    def test_long_session_without_progress_updates(self):
        """Verify long session without progress updates is flagged."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 10,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Reading files.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 1,
                                "text": "Finished.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

<<<<<<< HEAD
        assert result["total_md_file_creations"] == 1
        assert result["proactive_md_creations"] == 1
        assert result["proactive_md_creation_rate"] == 100.0
        assert result["example_proactive_md"]["session_id"] == "session1"

    def test_explicit_md_creation_request(self):
        """Verify .md creation with explicit user request not flagged."""
=======
        assert result["long_sessions"] == 1
        assert result["long_sessions_with_progress"] == 0
        assert result["progress_update_frequency"] == 0.0

    def test_short_session_not_counted_for_progress(self):
        """Verify short sessions (<5min) don't count toward progress metrics."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 3,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Quick fix applied.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
                    }
                ]
            }
        ])

        assert result["long_sessions"] == 0
        assert result["progress_update_frequency"] == 0.0

    def test_markdown_usage_consistency(self):
        """Verify markdown formatting usage is tracked."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "duration_minutes": 3,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "## Implementation\n\nI'll **implement** the following:\n- Feature A\n- Feature B",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 1,
                                "text": "Using `code` blocks for examples.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 2,
                                "text": "Plain text message.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

<<<<<<< HEAD
        assert result["total_md_file_creations"] == 1
        assert result["proactive_md_creations"] == 0
        assert result["proactive_md_creation_rate"] == 0.0

    def test_askuser_question_usage(self):
        """Verify detection of AskUserQuestion usage."""
=======
        assert result["total_messages"] == 3
        assert result["messages_with_markdown"] == 2
        assert result["markdown_usage_consistency"] == 66.67

    def test_tool_communication_violation_bash_echo(self):
        """Verify Bash echo used for user communication is flagged."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 2,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "",
                                "is_final_message": True,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "echo 'Processing your request...'",
                                    }
                                ],
                            },
                        ],
                        "errors_encountered": [],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

<<<<<<< HEAD
        assert result["sessions_using_askuser"] == 1
        assert result["clarification_usage_rate"] == 100.0

    def test_session_without_askuser_usage(self):
        """Verify sessions not using AskUserQuestion."""
=======
        assert result["tool_communication_violations"] == 1

    def test_technical_echo_not_violation(self):
        """Verify technical echo usage is not flagged as violation."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 2,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Writing config file.",
                                "is_final_message": False,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "echo 'export PATH=/usr/bin' >> ~/.bashrc",
                                    }
                                ],
                            },
                            {
                                "message_index": 1,
                                "text": "Checking variable.",
                                "is_final_message": True,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "echo $HOME",
                                    }
                                ],
                            },
                        ],
                        "errors_encountered": [],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

<<<<<<< HEAD
        assert result["example_good_notification"]["session_id"] == "session1"
        assert "completed" in result["example_good_notification"]["final_message"].lower()
=======
        # Technical echo (file redirect and variable echo) should not be violations
        assert result["tool_communication_violations"] == 0
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E

    def test_multiple_packs_aggregation(self):
        """Verify metrics are aggregated across multiple packs."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
                        "messages": [
                            {
                                "message_index": 0,
                                "role": "assistant",
                                "text_content": "Done",
                                "tool_calls": []
                            }
                        ]
=======
                        "duration_minutes": 3,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Successfully completed task.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            },
            {
                "pack_id": "pack2",
                "sessions": [
                    {
                        "session_id": "session2",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 4,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Task done.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
                    }
                ]
            },
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        ])

        assert result["total_packs"] == 2
        assert result["total_sessions"] == 2
<<<<<<< HEAD
        assert result["sessions_with_final_message"] == 2
=======
        assert result["sessions_with_explicit_completion"] == 2

    def test_multiple_sessions_per_pack(self):
        """Verify multiple sessions are aggregated within pack."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "duration_minutes": 2,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "First task complete.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
                    },
                    {
                        "session_id": "session2",
                        "duration_minutes": 3,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Second task.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
                    },
                ]
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_sessions"] == 2
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_notification_completeness([
            "not a dict",
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 2,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Done.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
                    }
                ]
            },
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        ])

        assert result["total_packs"] == 1
        assert result["total_sessions"] == 1

<<<<<<< HEAD
    def test_realistic_good_session_pattern(self):
        """Verify realistic good session with proper notifications."""
=======
    def test_missing_sessions_handled_gracefully(self):
        """Verify pack without sessions is handled."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                # Missing sessions
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_sessions"] == 0

    def test_missing_messages_handled_gracefully(self):
        """Verify session without messages is handled."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        # Missing messages
                    }
                ]
            }
        ])

        assert result["total_sessions"] == 1
        assert result["total_messages"] == 0

    def test_mixed_completion_rates(self):
        """Verify accurate calculation with mixed completion patterns."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "duration_minutes": 2,
                        "messages": [
                            {"message_index": 0, "text": "Done successfully.", "is_final_message": True, "tool_calls": []},
                        ],
                        "errors_encountered": [],
                    },
                    {
                        "session_id": "session2",
                        "duration_minutes": 2,
                        "messages": [
                            {"message_index": 0, "text": "Okay.", "is_final_message": True, "tool_calls": []},
                        ],
                        "errors_encountered": [],
                    },
                    {
                        "session_id": "session3",
                        "duration_minutes": 2,
                        "messages": [
                            {"message_index": 0, "text": "Finished implementation.", "is_final_message": True, "tool_calls": []},
                        ],
                        "errors_encountered": [],
                    },
                    {
                        "session_id": "session4",
                        "duration_minutes": 2,
                        "messages": [
                            {"message_index": 0, "text": "I see.", "is_final_message": True, "tool_calls": []},
                        ],
                        "errors_encountered": [],
                    },
                ]
            }
        ])

        assert result["total_sessions"] == 4
        assert result["sessions_with_explicit_completion"] == 2
        assert result["sessions_without_completion"] == 2
        assert result["explicit_completion_rate"] == 50.0

    def test_mixed_error_notification_rates(self):
        """Verify accurate calculation with mixed error notification."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "duration_minutes": 2,
                        "messages": [{"message_index": 0, "text": "Done.", "is_final_message": True, "tool_calls": []}],
                        "errors_encountered": [
                            {"error_type": "error1", "error_message": "msg1", "turn_index": 0, "was_communicated": True},
                            {"error_type": "error2", "error_message": "msg2", "turn_index": 0, "was_communicated": False},
                            {"error_type": "error3", "error_message": "msg3", "turn_index": 0, "was_communicated": True},
                            {"error_type": "error4", "error_message": "msg4", "turn_index": 0, "was_communicated": True},
                        ],
                    }
                ]
            }
        ])

        assert result["total_errors"] == 4
        assert result["errors_communicated"] == 3
        assert result["errors_silent"] == 1
        assert result["error_notification_rate"] == 75.0

    def test_realistic_good_communication_discipline(self):
        """Verify realistic session with good communication discipline."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "duration_minutes": 7,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "## Starting Implementation\n\nI'll implement the feature with the following steps:\n- Step 1\n- Step 2",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 1,
                                "text": "Currently working on implementing step 1.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 2,
                                "text": "Analyzing the results now.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 3,
                                "text": "## Summary\n\nSuccessfully implemented the feature. All tests pass and the code is committed.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

<<<<<<< HEAD
        assert result["notification_completeness_score"] == 100.0
        assert result["sessions_using_askuser"] == 1
        assert result["error_escalation_rate"] == 0.0  # No errors
        assert result["proactive_md_creations"] == 0

    def test_realistic_poor_session_pattern(self):
        """Verify realistic poor session with missing notifications."""
=======
        assert result["explicit_completion_rate"] == 100.0
        assert result["progress_update_frequency"] == 100.0
        assert result["markdown_usage_consistency"] == 50.0  # 2 out of 4 messages have markdown
        assert result["tool_communication_violations"] == 0

    def test_realistic_poor_communication_discipline(self):
        """Verify realistic session with poor communication discipline."""
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
<<<<<<< HEAD
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
=======
                        "duration_minutes": 8,
                        "messages": [
                            {
                                "message_index": 0,
                                "text": "Reading files.",
                                "is_final_message": False,
                                "tool_calls": [],
                            },
                            {
                                "message_index": 1,
                                "text": "",
                                "is_final_message": False,
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "echo 'Making changes...'"},
                                ],
                            },
                            {
                                "message_index": 2,
                                "text": "Continuing.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [
                            {"error_type": "build_error", "error_message": "syntax error", "turn_index": 1, "was_communicated": False},
                        ],
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
                    }
                ]
            }
        ])

<<<<<<< HEAD
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
=======
        assert result["explicit_completion_rate"] == 0.0
        assert result["error_notification_rate"] == 0.0
        assert result["progress_update_frequency"] == 0.0
        assert result["markdown_usage_consistency"] == 0.0
        assert result["tool_communication_violations"] == 1
>>>>>>> relay/claude-code/add-pack-notification-completeness-and-session-bou-01KR895E
