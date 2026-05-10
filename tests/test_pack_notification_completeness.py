"""Tests for pack notification completeness and session boundary discipline analyzer."""

import pytest

from synthesis.pack_notification_completeness import analyze_pack_notification_completeness


class TestAnalyzePackNotificationCompleteness:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty packs returns zero metrics."""
        result = analyze_pack_notification_completeness([])

        assert result["total_packs"] == 0
        assert result["total_sessions"] == 0
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

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_notification_completeness(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_notification_completeness("not a list")

    def test_session_with_explicit_completion(self):
        """Verify session ending with clear completion summary."""
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
                    }
                ]
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_explicit_completion"] == 1
        assert result["sessions_without_completion"] == 0
        assert result["explicit_completion_rate"] == 100.0
        assert result["example_good_completion"]["session_id"] == "session1"

    def test_session_without_explicit_completion(self):
        """Verify session ending abruptly without outcome summary."""
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
                    }
                ]
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_explicit_completion"] == 0
        assert result["sessions_without_completion"] == 1
        assert result["explicit_completion_rate"] == 0.0
        assert result["example_poor_completion"]["session_id"] == "session1"

    def test_error_communicated_to_user(self):
        """Verify errors that are properly communicated to user."""
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
                    }
                ]
            }
        ])

        assert result["total_errors"] == 1
        assert result["errors_communicated"] == 1
        assert result["errors_silent"] == 0
        assert result["error_notification_rate"] == 100.0
        assert result["example_good_error_notification"]["error_type"] == "test_failure"

    def test_error_not_communicated_to_user(self):
        """Verify errors that are silently handled without user notification."""
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
                    }
                ]
            }
        ])

        assert result["total_errors"] == 1
        assert result["errors_communicated"] == 0
        assert result["errors_silent"] == 1
        assert result["error_notification_rate"] == 0.0
        assert result["example_poor_error_handling"]["error_type"] == "build_error"

    def test_long_session_with_progress_updates(self):
        """Verify long session (>5min) with regular progress updates."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
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
                    }
                ]
            }
        ])

        assert result["long_sessions"] == 1
        assert result["long_sessions_with_progress"] == 1
        assert result["progress_update_frequency"] == 100.0

    def test_long_session_without_progress_updates(self):
        """Verify long session without progress updates is flagged."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
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
                    }
                ]
            }
        ])

        assert result["long_sessions"] == 1
        assert result["long_sessions_with_progress"] == 0
        assert result["progress_update_frequency"] == 0.0

    def test_short_session_not_counted_for_progress(self):
        """Verify short sessions (<5min) don't count toward progress metrics."""
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
                    }
                ]
            }
        ])

        assert result["total_messages"] == 3
        assert result["messages_with_markdown"] == 2
        assert result["markdown_usage_consistency"] == 66.67

    def test_tool_communication_violation_bash_echo(self):
        """Verify Bash echo used for user communication is flagged."""
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
                    }
                ]
            }
        ])

        assert result["tool_communication_violations"] == 1

    def test_technical_echo_not_violation(self):
        """Verify technical echo usage is not flagged as violation."""
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
                    }
                ]
            }
        ])

        # Technical echo (file redirect and variable echo) should not be violations
        assert result["tool_communication_violations"] == 0

    def test_multiple_packs_aggregation(self):
        """Verify metrics are aggregated across multiple packs."""
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
                                "text": "Successfully completed task.",
                                "is_final_message": True,
                                "tool_calls": [],
                            },
                        ],
                        "errors_encountered": [],
                    }
                ]
            },
            {
                "pack_id": "pack2",
                "sessions": [
                    {
                        "session_id": "session2",
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
        ])

        assert result["total_packs"] == 2
        assert result["total_sessions"] == 2
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

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_notification_completeness([
            "not a dict",
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
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
        ])

        assert result["total_packs"] == 1
        assert result["total_sessions"] == 1

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
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
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
                    }
                ]
            }
        ])

        assert result["explicit_completion_rate"] == 100.0
        assert result["progress_update_frequency"] == 100.0
        assert result["markdown_usage_consistency"] == 50.0  # 2 out of 4 messages have markdown
        assert result["tool_communication_violations"] == 0

    def test_realistic_poor_communication_discipline(self):
        """Verify realistic session with poor communication discipline."""
        result = analyze_pack_notification_completeness([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
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
                    }
                ]
            }
        ])

        assert result["explicit_completion_rate"] == 0.0
        assert result["error_notification_rate"] == 0.0
        assert result["progress_update_frequency"] == 0.0
        assert result["markdown_usage_consistency"] == 0.0
        assert result["tool_communication_violations"] == 1
