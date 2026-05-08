"""Tests for session background task usage analyzer."""

import pytest

from synthesis.session_background_task_usage import analyze_session_background_task_usage


class TestAnalyzeSessionBackgroundTaskUsage:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_background_task_usage([])

        assert result["total_tool_calls"] == 0
        assert result["background_task_count"] == 0
        assert result["foreground_task_count"] == 0
        assert result["background_usage_rate"] == 0.0
        assert result["background_tool_distribution"] == []
        assert result["average_duration"] == 0.0
        assert result["completion_rate"] == 0.0
        assert result["abandoned_count"] == 0
        assert result["missed_opportunities"] == 0
        assert result["long_foreground_examples"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_background_task_usage(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_background_task_usage("not a list")

    def test_single_foreground_task(self):
        """Verify single foreground task is counted."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Read",
                "run_in_background": False,
            }
        ])

        assert result["total_tool_calls"] == 1
        assert result["background_task_count"] == 0
        assert result["foreground_task_count"] == 1
        assert result["background_usage_rate"] == 0.0

    def test_single_background_task(self):
        """Verify single background task is counted."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": True,
            }
        ])

        assert result["total_tool_calls"] == 1
        assert result["background_task_count"] == 1
        assert result["foreground_task_count"] == 0
        assert result["background_usage_rate"] == 100.0

    def test_mixed_background_and_foreground(self):
        """Verify mixed background and foreground tasks."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Read",
                "run_in_background": False,
            },
            {
                "tool_name": "Bash",
                "run_in_background": True,
            },
            {
                "tool_name": "Edit",
                "run_in_background": False,
            }
        ])

        assert result["total_tool_calls"] == 3
        assert result["background_task_count"] == 1
        assert result["foreground_task_count"] == 2
        assert result["background_usage_rate"] == 33.33

    def test_background_tool_distribution(self):
        """Verify background tool distribution is tracked."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": True,
            },
            {
                "tool_name": "Bash",
                "run_in_background": True,
            },
            {
                "tool_name": "Task",
                "run_in_background": True,
            }
        ])

        assert len(result["background_tool_distribution"]) == 2
        assert result["background_tool_distribution"][0]["tool_name"] == "Bash"
        assert result["background_tool_distribution"][0]["count"] == 2

    def test_duration_tracking(self):
        """Verify background task durations are tracked."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": True,
                "duration": 10.5,
            },
            {
                "tool_name": "Task",
                "run_in_background": True,
                "duration": 20.3,
            }
        ])

        # Average = (10.5 + 20.3) / 2 = 15.4
        assert result["average_duration"] == 15.4

    def test_completion_rate_all_completed(self):
        """Verify completion rate when all tasks complete."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
            },
            {
                "tool_name": "Task",
                "run_in_background": True,
                "completed": True,
            }
        ])

        assert result["completion_rate"] == 100.0
        assert result["abandoned_count"] == 0

    def test_completion_rate_partial(self):
        """Verify completion rate with partial completion."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
            },
            {
                "tool_name": "Task",
                "run_in_background": True,
                "completed": False,
            }
        ])

        assert result["completion_rate"] == 50.0
        assert result["abandoned_count"] == 1

    def test_abandoned_tasks_counted(self):
        """Verify abandoned tasks are counted."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": False,
            },
            {
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": False,
            }
        ])

        assert result["abandoned_count"] == 2

    def test_missed_opportunity_long_foreground_bash(self):
        """Verify long foreground Bash tasks flagged as missed opportunities."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration": 45.0,
            }
        ])

        assert result["missed_opportunities"] == 1
        assert len(result["long_foreground_examples"]) == 1
        assert result["long_foreground_examples"][0]["tool_name"] == "Bash"

    def test_short_foreground_not_flagged(self):
        """Verify short foreground tasks are not flagged."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration": 5.0,
            }
        ])

        assert result["missed_opportunities"] == 0

    def test_non_backgroundable_tool_not_flagged(self):
        """Verify non-backgroundable tools are not flagged."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Read",
                "run_in_background": False,
                "duration": 60.0,
            }
        ])

        assert result["missed_opportunities"] == 0

    def test_backgroundable_tools_detected(self):
        """Verify backgroundable tools are recognized."""
        backgroundable = ["Bash", "Task", "WebFetch", "WebSearch"]
        for tool in backgroundable:
            result = analyze_session_background_task_usage([
                {
                    "tool_name": tool,
                    "run_in_background": False,
                    "duration": 45.0,
                }
            ])
            assert result["missed_opportunities"] == 1

    def test_non_backgroundable_tools_not_detected(self):
        """Verify non-backgroundable tools are not flagged."""
        non_backgroundable = ["Read", "Write", "Edit", "Grep", "Glob"]
        for tool in non_backgroundable:
            result = analyze_session_background_task_usage([
                {
                    "tool_name": tool,
                    "run_in_background": False,
                    "duration": 45.0,
                }
            ])
            assert result["missed_opportunities"] == 0

    def test_long_foreground_examples_limited(self):
        """Verify long foreground examples are limited to 3."""
        records = []
        for i in range(10):
            records.append({
                "tool_name": "Bash",
                "run_in_background": False,
                "duration": 45.0,
                "turn_index": i,
            })

        result = analyze_session_background_task_usage(records)

        assert result["missed_opportunities"] == 10
        assert len(result["long_foreground_examples"]) == 3

    def test_tool_distribution_limited_to_five(self):
        """Verify tool distribution is limited to 5 entries."""
        records = []
        for i in range(10):
            records.append({
                "tool_name": f"Tool{i}",
                "run_in_background": True,
            })

        result = analyze_session_background_task_usage(records)

        assert len(result["background_tool_distribution"]) <= 5

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_background_task_usage([
            "not a dict",
            {
                "tool_name": "Bash",
                "run_in_background": True,
            }
        ])

        assert result["total_tool_calls"] == 1

    def test_missing_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_background_task_usage([
            {
                "run_in_background": True,
            },
            {
                "tool_name": "Bash",
                "run_in_background": True,
            }
        ])

        assert result["total_tool_calls"] == 1

    def test_missing_duration_handled(self):
        """Verify missing duration is handled gracefully."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": True,
            }
        ])

        assert result["average_duration"] == 0.0

    def test_missing_completed_field_not_counted(self):
        """Verify missing completed field doesn't affect completion rate."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": True,
            }
        ])

        # No completion info, so completion rate is 0/1 = 0%
        assert result["completion_rate"] == 0.0
        assert result["abandoned_count"] == 0

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "BASH",
                "run_in_background": False,
                "duration": 45.0,
            }
        ])

        assert result["missed_opportunities"] == 1

    def test_zero_denominator_handled(self):
        """Verify zero denominator in percentage calculations."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Read",
                "run_in_background": False,
            }
        ])

        assert result["background_usage_rate"] == 0.0
        assert result["completion_rate"] == 0.0

    def test_boolean_duration_ignored(self):
        """Verify boolean values for duration are ignored."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": True,
                "duration": True,
            }
        ])

        assert result["average_duration"] == 0.0

    def test_negative_duration_accepted(self):
        """Verify negative duration values are accepted as-is."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": True,
                "duration": -5.0,
            }
        ])

        # Negative duration is unusual but accepted
        assert result["average_duration"] == -5.0

    def test_exactly_30_seconds_not_flagged(self):
        """Verify exactly 30 seconds is not flagged as missed opportunity."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration": 30.0,
            }
        ])

        assert result["missed_opportunities"] == 0

    def test_30_point_1_seconds_flagged(self):
        """Verify > 30 seconds is flagged as missed opportunity."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration": 30.1,
            }
        ])

        assert result["missed_opportunities"] == 1

    def test_turn_index_included_in_examples(self):
        """Verify turn_index is included in long foreground examples."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration": 45.0,
                "turn_index": 5,
            }
        ])

        assert result["long_foreground_examples"][0]["turn_index"] == 5

    def test_missing_turn_index_defaults_to_zero(self):
        """Verify missing turn_index defaults to 0."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration": 45.0,
            }
        ])

        assert result["long_foreground_examples"][0]["turn_index"] == 0

    def test_all_background_100_percent_usage(self):
        """Verify 100% background usage when all tasks are background."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True},
            {"tool_name": "Task", "run_in_background": True},
        ])

        assert result["background_usage_rate"] == 100.0

    def test_heavy_background_usage_pattern(self):
        """Verify session with heavy background usage."""
        records = []
        for i in range(10):
            records.append({
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
                "duration": 10.0 + i,
            })

        result = analyze_session_background_task_usage(records)

        assert result["background_task_count"] == 10
        assert result["completion_rate"] == 100.0
        assert result["average_duration"] == 14.5  # (10+11+...+19) / 10
