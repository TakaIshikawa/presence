"""Tests for pack notification completeness analyzer."""

import pytest

from synthesis.pack_notification_completeness import analyze_pack_notification_completeness


def test_empty_packs_returns_zeroed_metrics():
    result = analyze_pack_notification_completeness([])

    assert result["total_packs"] == 0
    assert result["total_sessions"] == 0
    assert result["sessions_with_explicit_completion"] == 0
    assert result["explicit_completion_rate"] == 0.0
    assert result["total_errors"] == 0
    assert result["error_notification_rate"] == 0.0


def test_none_input_treated_as_empty_list():
    result = analyze_pack_notification_completeness(None)

    assert result["total_packs"] == 0


def test_invalid_input_type_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_notification_completeness("not a list")


def test_session_with_explicit_completion_is_counted():
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
                            "role": "assistant",
                            "text": "Implementation complete. Tests pass.",
                            "is_final_message": True,
                            "tool_calls": [],
                        }
                    ],
                }
            ],
        }
    ])

    assert result["total_sessions"] == 1
    assert result["sessions_with_explicit_completion"] == 1
    assert result["sessions_without_completion"] == 0
    assert result["explicit_completion_rate"] == 100.0
    assert result["example_good_completion"]["session_id"] == "session1"


def test_structured_errors_track_communication_status():
    result = analyze_pack_notification_completeness([
        {
            "pack_id": "pack1",
            "sessions": [
                {
                    "session_id": "session1",
                    "messages": [],
                    "errors_encountered": [
                        {
                            "error_type": "test_failure",
                            "error_message": "assertion failed",
                            "was_communicated": True,
                        },
                        {
                            "error_type": "build_error",
                            "error_message": "syntax error",
                            "was_communicated": False,
                        },
                    ],
                }
            ],
        }
    ])

    assert result["total_errors"] == 2
    assert result["errors_communicated"] == 1
    assert result["errors_silent"] == 1
    assert result["error_notification_rate"] == 50.0


def test_long_session_progress_and_pr_url_reporting_are_tracked():
    result = analyze_pack_notification_completeness([
        {
            "pack_id": "pack1",
            "sessions": [
                {
                    "session_id": "session1",
                    "duration_minutes": 8,
                    "messages": [
                        {
                            "text": "Starting implementation.",
                            "tool_calls": [
                                {"tool_name": "Bash", "command": "gh pr create --title fix"}
                            ],
                        },
                        {
                            "text": "Done: https://github.com/example/repo/pull/1",
                            "role": "assistant",
                            "is_final_message": True,
                            "tool_calls": [],
                        },
                    ],
                }
            ],
        }
    ])

    assert result["long_sessions"] == 1
    assert result["long_sessions_with_progress"] == 1
    assert result["progress_update_frequency"] == 100.0
    assert result["total_sessions_with_pr_creation"] == 1
    assert result["pr_creation_with_url_reported"] == 1
    assert result["missing_pr_urls"] == 0
