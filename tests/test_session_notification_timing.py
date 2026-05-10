"""Tests for session_notification_timing analyzer."""

from __future__ import annotations

import pytest

from src.synthesis.session_notification_timing import (
    analyze_session_notification_timing,
)


class TestInputValidation:
    """Test input handling and edge cases."""

    def test_none_input_returns_empty_result(self):
        result = analyze_session_notification_timing(None)
        assert result["total_turns"] == 0
        assert result["notification_completeness"] == 0.0
        assert result["summary_quality"] == 0.0
        assert result["session_boundary_discipline"] == 0.0

    def test_empty_list_returns_empty_result(self):
        result = analyze_session_notification_timing([])
        assert result["total_turns"] == 0
        assert result["task_completions"] == 0

    def test_non_list_raises_value_error(self):
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_notification_timing("not a list")

    def test_non_list_dict_raises_value_error(self):
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_notification_timing({"key": "value"})

    def test_non_mapping_records_are_skipped(self):
        result = analyze_session_notification_timing(["string", 42, None])
        assert result["total_turns"] == 0


class TestTaskCompletionNotifications:
    """Test detection of notifications after Task tool completions."""

    def test_task_with_notification(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Agent completed the search.",
                "assistant_response": "The search found 5 matching files in the project.",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["task_completions"] == 1
        assert result["task_completion_notifications"] == 1
        assert result["silent_task_consumptions"] == 0

    def test_task_without_notification(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Agent completed the search.",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["task_completions"] == 1
        assert result["task_completion_notifications"] == 0
        assert result["silent_task_consumptions"] == 1

    def test_task_with_short_unhelpful_response(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Found 10 files",
                "assistant_response": "Ok.",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["task_completion_notifications"] == 0
        assert result["silent_task_consumptions"] == 1

    def test_multiple_tasks_mixed_notifications(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Done",
                "assistant_response": "The exploration completed successfully. Here are the results.",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 2,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Done",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 3,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Done",
                "assistant_response": "I found the implementation in src/main.py.",
                "is_error": False,
                "is_last_turn": False,
            },
        ]
        result = analyze_session_notification_timing(records)
        assert result["task_completions"] == 3
        assert result["task_completion_notifications"] == 2
        assert result["silent_task_consumptions"] == 1


class TestBackgroundTaskNotifications:
    """Test detection of notifications for background tasks."""

    def test_background_task_with_notification(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {"run_in_background": True},
                "tool_result": "Build completed.",
                "assistant_response": "The background build completed successfully.",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["background_task_completions"] == 1
        assert result["background_task_notifications"] == 1

    def test_background_bash_with_notification(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"run_in_background": True, "command": "npm test"},
                "tool_result": "All tests passed",
                "assistant_response": "The tests have completed and are all passing.",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["background_task_completions"] == 1
        assert result["background_task_notifications"] == 1

    def test_background_task_silently_consumed(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {"run_in_background": True},
                "tool_result": "Agent finished.",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["background_task_completions"] == 1
        assert result["background_task_notifications"] == 0
        assert result["silent_task_consumptions"] == 1


class TestSessionBoundaryDiscipline:
    """Test detection of proper session boundary messages."""

    def test_last_turn_with_boundary_message(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "git commit -m 'fix'"},
                "tool_result": "committed",
                "assistant_response": "All done! The changes have been committed. Let me know if you need anything else.",
                "is_error": False,
                "is_last_turn": True,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["session_boundary_messages"] == 1
        assert result["session_boundary_discipline"] == 1.0

    def test_last_turn_without_boundary_message(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "ls"},
                "tool_result": "file.txt",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": True,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["session_boundary_messages"] == 0
        assert result["session_boundary_discipline"] == 0.0

    def test_non_last_turns_dont_affect_boundary(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "ls"},
                "tool_result": "files",
                "assistant_response": "Here are the files found in the directory.",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 2,
                "tool_name": "Bash",
                "tool_params": {"command": "echo done"},
                "tool_result": "done",
                "assistant_response": "The task is complete and all changes are ready.",
                "is_error": False,
                "is_last_turn": True,
            },
        ]
        result = analyze_session_notification_timing(records)
        assert result["session_boundary_messages"] == 1
        assert result["session_boundary_discipline"] == 1.0

    def test_boundary_message_with_next_steps(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Done",
                "assistant_response": "Implementation is finished. Next steps: run the full test suite and deploy.",
                "is_error": False,
                "is_last_turn": True,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["session_boundary_messages"] == 1


class TestErrorCommunication:
    """Test detection of error explanations to the user."""

    def test_error_with_explanation(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "npm test"},
                "tool_result": "FAILED: 3 tests failed with assertion errors",
                "assistant_response": "The tests failed because the expected output format changed. Let me fix the assertions.",
                "is_error": True,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["error_occurrences"] == 1
        assert result["error_explanations_count"] == 1

    def test_error_without_explanation(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "npm test"},
                "tool_result": "ERROR: Module not found",
                "assistant_response": "",
                "is_error": True,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["error_occurrences"] == 1
        assert result["error_explanations_count"] == 0

    def test_error_detected_from_tool_result(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "python script.py"},
                "tool_result": "Traceback (most recent call last):\n  File 'script.py'\nNameError: name 'x' is not defined",
                "assistant_response": "The error is caused by an undefined variable. I need to fix the import.",
                "is_error": False,  # Not marked but result contains error
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["error_occurrences"] == 1
        assert result["error_explanations_count"] == 1

    def test_multiple_errors_partial_explanations(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "cargo build"},
                "tool_result": "error[E0382]: use of moved value",
                "assistant_response": "The error means we need to clone the value before moving it.",
                "is_error": True,
                "is_last_turn": False,
            },
            {
                "turn_index": 2,
                "tool_name": "Bash",
                "tool_params": {"command": "cargo build"},
                "tool_result": "error[E0308]: mismatched types",
                "assistant_response": "",
                "is_error": True,
                "is_last_turn": False,
            },
        ]
        result = analyze_session_notification_timing(records)
        assert result["error_occurrences"] == 2
        assert result["error_explanations_count"] == 1


class TestNotificationCompletenessScore:
    """Test the notification_completeness score calculation."""

    def test_perfect_notification_score(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Done",
                "assistant_response": "The task completed and found 5 results.",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 2,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Done",
                "assistant_response": "Successfully returned the analysis results.",
                "is_error": False,
                "is_last_turn": False,
            },
        ]
        result = analyze_session_notification_timing(records)
        assert result["notification_completeness"] == 1.0

    def test_zero_notification_score(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Done",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 2,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Done",
                "assistant_response": "hi",
                "is_error": False,
                "is_last_turn": False,
            },
        ]
        result = analyze_session_notification_timing(records)
        assert result["notification_completeness"] == 0.0

    def test_partial_notification_score(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Done",
                "assistant_response": "The search completed and found matching code.",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 2,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Done",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": False,
            },
        ]
        result = analyze_session_notification_timing(records)
        assert result["notification_completeness"] == 0.5


class TestSummaryQuality:
    """Test summary_quality score evaluation."""

    def test_high_quality_summary(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Found 10 files matching the pattern.",
                "assistant_response": "The search completed successfully and found 10 matching files:\n- src/main.py\n- src/utils.py",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["summary_quality"] > 0.5

    def test_no_summary(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "ls"},
                "tool_result": "file1.txt\nfile2.txt",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["summary_quality"] == 0.0

    def test_concise_informative_summary(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "npm test"},
                "tool_result": "15 tests passed, 0 failed",
                "assistant_response": "All 15 tests passed successfully.",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["summary_quality"] > 0.4


class TestProgressUpdates:
    """Test detection of progress updates during long operations."""

    def test_long_operation_with_progress(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "npm install"},
                "tool_result": "added 500 packages",
                "assistant_response": "I'm now running npm install to install all dependencies.",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["long_operations"] == 1
        assert result["progress_updates"] == 1

    def test_long_operation_without_progress(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "pytest"},
                "tool_result": "10 passed",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["long_operations"] == 1
        assert result["progress_updates"] == 0

    def test_short_command_not_counted_as_long_operation(self):
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "ls -la"},
                "tool_result": "files listed",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": False,
            }
        ]
        result = analyze_session_notification_timing(records)
        assert result["long_operations"] == 0


class TestIntegration:
    """Integration tests with realistic multi-turn sessions."""

    def test_full_session_good_discipline(self):
        """Well-disciplined session with proper notifications."""
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {"subagent_type": "Explore"},
                "tool_result": "Found implementation in src/auth.py",
                "assistant_response": "I found the authentication implementation in src/auth.py.",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 2,
                "tool_name": "Bash",
                "tool_params": {"command": "npm run test"},
                "tool_result": "All 20 tests passed",
                "assistant_response": "Let me run the tests to verify the changes.",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 3,
                "tool_name": "Bash",
                "tool_params": {"command": "git commit -m 'fix auth'"},
                "tool_result": "[main abc123] fix auth",
                "assistant_response": "All changes have been committed. The authentication fix is complete and all tests are passing.",
                "is_error": False,
                "is_last_turn": True,
            },
        ]
        result = analyze_session_notification_timing(records)
        assert result["notification_completeness"] == 1.0
        assert result["session_boundary_discipline"] == 1.0
        assert result["summary_quality"] > 0.3

    def test_full_session_poor_discipline(self):
        """Session with poor notification discipline."""
        records = [
            {
                "turn_index": 1,
                "tool_name": "Task",
                "tool_params": {"subagent_type": "Explore"},
                "tool_result": "Found the code",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 2,
                "tool_name": "Task",
                "tool_params": {},
                "tool_result": "Refactoring complete",
                "assistant_response": "ok",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 3,
                "tool_name": "Bash",
                "tool_params": {"command": "git add ."},
                "tool_result": "",
                "assistant_response": "",
                "is_error": False,
                "is_last_turn": True,
            },
        ]
        result = analyze_session_notification_timing(records)
        assert result["notification_completeness"] == 0.0
        assert result["silent_task_consumptions"] == 2
        assert result["session_boundary_discipline"] == 0.0

    def test_session_with_errors_well_handled(self):
        """Session where errors are properly communicated."""
        records = [
            {
                "turn_index": 1,
                "tool_name": "Bash",
                "tool_params": {"command": "npm run build"},
                "tool_result": "ERROR: TypeScript compilation failed\nerror TS2345",
                "assistant_response": "The build failed because of a TypeScript type error. I need to fix the type mismatch in the component.",
                "is_error": True,
                "is_last_turn": False,
            },
            {
                "turn_index": 2,
                "tool_name": "Bash",
                "tool_params": {"command": "npm run build"},
                "tool_result": "Build successful",
                "assistant_response": "The build completed successfully after the fix.",
                "is_error": False,
                "is_last_turn": True,
            },
        ]
        result = analyze_session_notification_timing(records)
        assert result["error_occurrences"] == 1
        assert result["error_explanations_count"] == 1
        assert result["session_boundary_discipline"] == 1.0

    def test_mixed_tool_types_not_affecting_task_metrics(self):
        """Non-Task, non-background tools don't affect task notification metrics."""
        records = [
            {
                "turn_index": 1,
                "tool_name": "Read",
                "tool_params": {"file_path": "src/main.py"},
                "tool_result": "file contents",
                "assistant_response": "Here's the main file content.",
                "is_error": False,
                "is_last_turn": False,
            },
            {
                "turn_index": 2,
                "tool_name": "Edit",
                "tool_params": {},
                "tool_result": "File updated",
                "assistant_response": "I've updated the file. The change is complete.",
                "is_error": False,
                "is_last_turn": True,
            },
        ]
        result = analyze_session_notification_timing(records)
        assert result["task_completions"] == 0
        assert result["background_task_completions"] == 0
        assert result["notification_completeness"] == 0.0
        assert result["session_boundary_discipline"] == 1.0
