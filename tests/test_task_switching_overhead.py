"""Tests for task switching overhead analyzer."""

import pytest
from datetime import datetime, timedelta, timezone
from math import inf, nan

from synthesis.task_switching_overhead import (
    TaskSwitch,
    SwitchingMetrics,
    TaskSwitchingOverhead,
    analyze_task_switching_overhead,
    _calculate_avg_interval,
    _count_rapid_switches,
    _count_focused_periods,
    _count_unique_tasks,
    _classify_overhead_tier,
    CONTEXT_SWITCH_COST_MINUTES,
    RAPID_SWITCH_THRESHOLD_MINUTES,
    FOCUSED_WORK_THRESHOLD_MINUTES,
    TIER_EFFICIENT,
    TIER_MODERATE,
    TIER_FRAGMENTED,
    TIER_CHAOTIC,
    THRESHOLD_MODERATE,
    THRESHOLD_FRAGMENTED,
    THRESHOLD_CHAOTIC,
)


class TestTaskSwitch:
    """Test TaskSwitch dataclass."""

    def test_create_switch_basic(self):
        """Verify switch can be created with basic fields."""
        now = datetime.now(timezone.utc)
        switch = TaskSwitch(
            from_task="task1",
            to_task="task2",
            timestamp=now,
        )
        assert switch.from_task == "task1"
        assert switch.to_task == "task2"
        assert switch.timestamp == now
        assert switch.interval_minutes is None

    def test_create_switch_with_interval(self):
        """Verify switch can be created with interval."""
        now = datetime.now(timezone.utc)
        switch = TaskSwitch(
            from_task="task1",
            to_task="task2",
            timestamp=now,
            interval_minutes=15.5,
        )
        assert switch.interval_minutes == 15.5

    def test_switch_frozen(self):
        """Verify switch is immutable."""
        now = datetime.now(timezone.utc)
        switch = TaskSwitch("task1", "task2", now)
        with pytest.raises(AttributeError):
            switch.from_task = "task3"


class TestCalculateAvgInterval:
    """Test average interval calculation."""

    def test_empty_switches(self):
        """Verify empty switches returns zero."""
        assert _calculate_avg_interval([]) == 0.0

    def test_switches_without_intervals(self):
        """Verify switches without intervals returns zero."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now),
            TaskSwitch("b", "c", now),
        ]
        assert _calculate_avg_interval(switches) == 0.0

    def test_single_interval(self):
        """Verify single interval calculation."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=10.0),
        ]
        assert _calculate_avg_interval(switches) == 10.0

    def test_multiple_intervals(self):
        """Verify average of multiple intervals."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=10.0),
            TaskSwitch("b", "c", now, interval_minutes=20.0),
            TaskSwitch("c", "d", now, interval_minutes=30.0),
        ]
        assert _calculate_avg_interval(switches) == 20.0

    def test_mixed_intervals(self):
        """Verify calculation with some None intervals."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=15.0),
            TaskSwitch("b", "c", now),  # None interval
            TaskSwitch("c", "d", now, interval_minutes=25.0),
        ]
        assert _calculate_avg_interval(switches) == 20.0


class TestCountRapidSwitches:
    """Test rapid switch counting."""

    def test_empty_switches(self):
        """Verify empty switches returns zero."""
        assert _count_rapid_switches([]) == 0

    def test_no_rapid_switches(self):
        """Verify no rapid switches when all intervals are long."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=10.0),
            TaskSwitch("b", "c", now, interval_minutes=20.0),
        ]
        assert _count_rapid_switches(switches) == 0

    def test_all_rapid_switches(self):
        """Verify counting when all switches are rapid."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=2.0),
            TaskSwitch("b", "c", now, interval_minutes=3.0),
            TaskSwitch("c", "d", now, interval_minutes=4.0),
        ]
        assert _count_rapid_switches(switches) == 3

    def test_mixed_rapid_switches(self):
        """Verify counting with mix of rapid and normal switches."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=3.0),  # Rapid
            TaskSwitch("b", "c", now, interval_minutes=15.0),  # Normal
            TaskSwitch("c", "d", now, interval_minutes=2.0),  # Rapid
            TaskSwitch("d", "e", now, interval_minutes=30.0),  # Normal
        ]
        assert _count_rapid_switches(switches) == 2

    def test_threshold_boundary(self):
        """Verify exact threshold behavior."""
        now = datetime.now(timezone.utc)
        # Just below threshold = rapid
        switch_rapid = TaskSwitch("a", "b", now, interval_minutes=4.9)
        # At threshold = not rapid
        switch_at = TaskSwitch("b", "c", now, interval_minutes=5.0)
        # Above threshold = not rapid
        switch_above = TaskSwitch("c", "d", now, interval_minutes=5.1)

        assert _count_rapid_switches([switch_rapid]) == 1
        assert _count_rapid_switches([switch_at]) == 0
        assert _count_rapid_switches([switch_above]) == 0


class TestCountFocusedPeriods:
    """Test focused period counting."""

    def test_empty_switches_short_session(self):
        """Verify short session with no switches has no focused periods."""
        assert _count_focused_periods([], 20.0) == 0

    def test_empty_switches_long_session(self):
        """Verify long session with no switches counts as focused."""
        assert _count_focused_periods([], 60.0) == 1

    def test_no_focused_periods(self):
        """Verify no focused periods when all intervals are short."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=10.0),
            TaskSwitch("b", "c", now, interval_minutes=15.0),
            TaskSwitch("c", "d", now, interval_minutes=20.0),
        ]
        assert _count_focused_periods(switches, 60.0) == 0

    def test_all_focused_periods(self):
        """Verify counting when all periods are focused."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=35.0),
            TaskSwitch("b", "c", now, interval_minutes=40.0),
            TaskSwitch("c", "d", now, interval_minutes=45.0),
        ]
        assert _count_focused_periods(switches, 150.0) == 3

    def test_mixed_focused_periods(self):
        """Verify counting with mix of focused and short periods."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=40.0),  # Focused
            TaskSwitch("b", "c", now, interval_minutes=10.0),  # Short
            TaskSwitch("c", "d", now, interval_minutes=35.0),  # Focused
            TaskSwitch("d", "e", now, interval_minutes=5.0),  # Short
        ]
        assert _count_focused_periods(switches, 100.0) == 2

    def test_threshold_boundary(self):
        """Verify exact threshold behavior."""
        now = datetime.now(timezone.utc)
        # Just below threshold = not focused
        switch_below = TaskSwitch("a", "b", now, interval_minutes=29.9)
        # At threshold = focused
        switch_at = TaskSwitch("b", "c", now, interval_minutes=30.0)
        # Above threshold = focused
        switch_above = TaskSwitch("c", "d", now, interval_minutes=30.1)

        assert _count_focused_periods([switch_below], 100.0) == 0
        assert _count_focused_periods([switch_at], 100.0) == 1
        assert _count_focused_periods([switch_above], 100.0) == 1


class TestCountUniqueTasks:
    """Test unique task counting."""

    def test_empty_switches(self):
        """Verify empty switches returns 1 (implied single task)."""
        assert _count_unique_tasks([]) == 1

    def test_single_switch(self):
        """Verify single switch has 2 unique tasks."""
        now = datetime.now(timezone.utc)
        switches = [TaskSwitch("task1", "task2", now)]
        assert _count_unique_tasks(switches) == 2

    def test_linear_task_sequence(self):
        """Verify linear sequence of different tasks."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now),
            TaskSwitch("b", "c", now),
            TaskSwitch("c", "d", now),
        ]
        # Unique: a, b, c, d
        assert _count_unique_tasks(switches) == 4

    def test_task_revisiting(self):
        """Verify revisiting tasks counts correctly."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now),
            TaskSwitch("b", "c", now),
            TaskSwitch("c", "a", now),  # Back to a
            TaskSwitch("a", "b", now),  # Back to b
        ]
        # Unique: a, b, c (3 tasks despite multiple switches)
        assert _count_unique_tasks(switches) == 3

    def test_alternating_tasks(self):
        """Verify alternating between two tasks."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("x", "y", now),
            TaskSwitch("y", "x", now),
            TaskSwitch("x", "y", now),
            TaskSwitch("y", "x", now),
        ]
        # Only 2 unique tasks
        assert _count_unique_tasks(switches) == 2


class TestClassifyOverheadTier:
    """Test overhead tier classification."""

    def test_efficient_tier(self):
        """Verify efficient tier classification."""
        assert _classify_overhead_tier(0.0) == TIER_EFFICIENT
        assert _classify_overhead_tier(10.0) == TIER_EFFICIENT
        assert _classify_overhead_tier(14.9) == TIER_EFFICIENT

    def test_moderate_tier(self):
        """Verify moderate tier classification."""
        assert _classify_overhead_tier(15.0) == TIER_MODERATE
        assert _classify_overhead_tier(20.0) == TIER_MODERATE
        assert _classify_overhead_tier(29.9) == TIER_MODERATE

    def test_fragmented_tier(self):
        """Verify fragmented tier classification."""
        assert _classify_overhead_tier(30.0) == TIER_FRAGMENTED
        assert _classify_overhead_tier(40.0) == TIER_FRAGMENTED
        assert _classify_overhead_tier(49.9) == TIER_FRAGMENTED

    def test_chaotic_tier(self):
        """Verify chaotic tier classification."""
        assert _classify_overhead_tier(50.0) == TIER_CHAOTIC
        assert _classify_overhead_tier(75.0) == TIER_CHAOTIC
        assert _classify_overhead_tier(100.0) == TIER_CHAOTIC


class TestAnalyzeTaskSwitchingOverhead:
    """Test complete task switching overhead analysis."""

    def test_no_switches_efficient(self):
        """Verify no switches produces efficient result."""
        result = analyze_task_switching_overhead([], 60.0)
        assert result.metrics.total_switches == 0
        assert result.metrics.overhead_percentage == 0.0
        assert result.overhead_tier == TIER_EFFICIENT
        assert "single-task" in result.insights[0].lower()

    def test_few_switches_efficient(self):
        """Verify few switches with good intervals is efficient."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=45.0),
            TaskSwitch("b", "c", now, interval_minutes=40.0),
        ]
        result = analyze_task_switching_overhead(switches, 120.0)
        # 2 switches * 5 min = 10 min overhead / 120 min = 8.33%
        assert result.overhead_tier == TIER_EFFICIENT
        assert result.metrics.total_switches == 2

    def test_moderate_switching(self):
        """Verify moderate switching pattern."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=15.0),
            TaskSwitch("b", "c", now, interval_minutes=20.0),
            TaskSwitch("c", "d", now, interval_minutes=25.0),
            TaskSwitch("d", "e", now, interval_minutes=20.0),
        ]
        result = analyze_task_switching_overhead(switches, 100.0)
        # 4 switches * 5 min = 20 min / 100 min = 20%
        assert result.overhead_tier == TIER_MODERATE

    def test_fragmented_workflow(self):
        """Verify fragmented workflow detection."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch(f"task{i}", f"task{i+1}", now, interval_minutes=10.0)
            for i in range(8)
        ]
        result = analyze_task_switching_overhead(switches, 100.0)
        # 8 switches * 5 min = 40 min / 100 min = 40%
        assert result.overhead_tier == TIER_FRAGMENTED

    def test_chaotic_switching(self):
        """Verify chaotic switching pattern."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch(f"task{i}", f"task{i+1}", now, interval_minutes=3.0)
            for i in range(12)
        ]
        result = analyze_task_switching_overhead(switches, 60.0)
        # 12 switches * 5 min = 60 min / 60 min = 100%
        assert result.overhead_tier == TIER_CHAOTIC

    def test_rapid_switches_detected(self):
        """Verify rapid switch detection."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=2.0),  # Rapid
            TaskSwitch("b", "c", now, interval_minutes=3.0),  # Rapid
            TaskSwitch("c", "d", now, interval_minutes=25.0),  # Normal
        ]
        result = analyze_task_switching_overhead(switches, 60.0)
        assert result.metrics.rapid_switches == 2

    def test_focused_periods_detected(self):
        """Verify focused period detection."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=45.0),  # Focused
            TaskSwitch("b", "c", now, interval_minutes=35.0),  # Focused
        ]
        result = analyze_task_switching_overhead(switches, 100.0)
        assert result.metrics.focused_periods == 2

    def test_multitasking_pattern(self):
        """Verify multitasking pattern detection."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=10.0),
            TaskSwitch("b", "a", now, interval_minutes=10.0),
            TaskSwitch("a", "b", now, interval_minutes=10.0),
            TaskSwitch("b", "a", now, interval_minutes=10.0),
        ]
        result = analyze_task_switching_overhead(switches, 60.0)
        assert result.metrics.unique_tasks == 2
        insights_text = " ".join(result.insights).lower()
        assert "alternating" in insights_text or "multitasking" in insights_text

    def test_metrics_preserved(self):
        """Verify metrics are preserved in result."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=20.0),
            TaskSwitch("b", "c", now, interval_minutes=30.0),
        ]
        result = analyze_task_switching_overhead(switches, 100.0)
        assert isinstance(result.metrics, SwitchingMetrics)
        assert result.metrics.total_switches == 2
        assert result.metrics.avg_interval_minutes == 25.0

    def test_insights_generated(self):
        """Verify insights are always generated."""
        now = datetime.now(timezone.utc)
        switches = [TaskSwitch("a", "b", now, interval_minutes=30.0)]
        result = analyze_task_switching_overhead(switches, 60.0)
        assert isinstance(result.insights, list)
        assert len(result.insights) > 0

    def test_result_immutable(self):
        """Verify result is immutable."""
        result = analyze_task_switching_overhead([], 60.0)
        with pytest.raises(AttributeError):
            result.overhead_tier = TIER_CHAOTIC

    def test_metrics_values_rounded(self):
        """Verify metrics are rounded appropriately."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=33.333),
            TaskSwitch("b", "c", now, interval_minutes=66.666),
        ]
        result = analyze_task_switching_overhead(switches, 100.0)
        assert result.metrics.avg_interval_minutes == round(result.metrics.avg_interval_minutes, 2)
        assert result.metrics.overhead_percentage == round(result.metrics.overhead_percentage, 2)


class TestValidation:
    """Test input validation."""

    def test_invalid_switches_type_raises(self):
        """Verify invalid switches type raises ValueError."""
        with pytest.raises(ValueError, match="must be a list"):
            analyze_task_switching_overhead("not a list", 60.0)  # type: ignore

    def test_invalid_switch_instance_raises(self):
        """Verify invalid switch instance raises ValueError."""
        with pytest.raises(ValueError, match="TaskSwitch instances"):
            analyze_task_switching_overhead([{"not": "a switch"}], 60.0)  # type: ignore

    def test_naive_timestamp_raises(self):
        """Verify naive datetime raises ValueError."""
        naive_time = datetime.now()  # No timezone
        switch = TaskSwitch("a", "b", naive_time)
        with pytest.raises(ValueError, match="timezone-aware"):
            analyze_task_switching_overhead([switch], 60.0)

    def test_negative_interval_raises(self):
        """Verify negative interval raises ValueError."""
        now = datetime.now(timezone.utc)
        switch = TaskSwitch("a", "b", now, interval_minutes=-5.0)
        with pytest.raises(ValueError, match="interval_minutes must be non-negative"):
            analyze_task_switching_overhead([switch], 60.0)

    def test_zero_session_duration_raises(self):
        """Verify zero session duration raises ValueError."""
        with pytest.raises(ValueError, match="session_duration_minutes must be positive"):
            analyze_task_switching_overhead([], 0.0)

    def test_negative_session_duration_raises(self):
        """Verify negative session duration raises ValueError."""
        with pytest.raises(ValueError, match="session_duration_minutes must be positive"):
            analyze_task_switching_overhead([], -60.0)

    @pytest.mark.parametrize("duration", ["60", nan, inf, -inf, True])
    def test_invalid_session_duration_type_raises(self, duration):
        """Verify session duration must be a finite number."""
        with pytest.raises(ValueError, match="session_duration_minutes must be a finite number"):
            analyze_task_switching_overhead([], duration)  # type: ignore[arg-type]

    def test_out_of_order_switch_timestamps_raise(self):
        """Verify switch timestamps must be chronological."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now + timedelta(minutes=5)),
            TaskSwitch("b", "c", now),
        ]
        with pytest.raises(ValueError, match="chronological order"):
            analyze_task_switching_overhead(switches, 60.0)

    @pytest.mark.parametrize(
        "switch",
        [
            TaskSwitch("", "b", datetime.now(timezone.utc)),
            TaskSwitch("  ", "b", datetime.now(timezone.utc)),
            TaskSwitch("a", "", datetime.now(timezone.utc)),
            TaskSwitch("a", "  ", datetime.now(timezone.utc)),
        ],
    )
    def test_blank_task_labels_raise(self, switch):
        """Verify task identifiers cannot be blank."""
        with pytest.raises(ValueError, match="task must be a non-empty string"):
            analyze_task_switching_overhead([switch], 60.0)


class TestInsightGeneration:
    """Test insight generation quality."""

    def test_chaotic_tier_insight(self):
        """Verify chaotic tier generates strong warning."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch(f"t{i}", f"t{i+1}", now, interval_minutes=2.0)
            for i in range(15)
        ]
        result = analyze_task_switching_overhead(switches, 100.0)
        insights_text = " ".join(result.insights).lower()
        assert "chaotic" in insights_text or "excessive" in insights_text

    def test_efficient_tier_insight(self):
        """Verify efficient tier generates positive feedback."""
        now = datetime.now(timezone.utc)
        switches = [TaskSwitch("a", "b", now, interval_minutes=40.0)]
        result = analyze_task_switching_overhead(switches, 60.0)
        insights_text = " ".join(result.insights).lower()
        assert "efficient" in insights_text or "minimal" in insights_text

    def test_rapid_switches_insight(self):
        """Verify rapid switches generate warning."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=2.0),
            TaskSwitch("b", "c", now, interval_minutes=3.0),
            TaskSwitch("c", "d", now, interval_minutes=4.0),
        ]
        result = analyze_task_switching_overhead(switches, 60.0)
        insights_text = " ".join(result.insights).lower()
        assert "rapid" in insights_text

    def test_focused_periods_insight(self):
        """Verify focused periods generate positive feedback."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=45.0),
            TaskSwitch("b", "c", now, interval_minutes=50.0),
        ]
        result = analyze_task_switching_overhead(switches, 120.0)
        insights_text = " ".join(result.insights).lower()
        assert "focused" in insights_text

    def test_high_switch_rate_insight(self):
        """Verify high switch rate generates warning."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch(f"t{i}", f"t{i+1}", now, interval_minutes=5.0)
            for i in range(20)
        ]
        result = analyze_task_switching_overhead(switches, 60.0)
        insights_text = " ".join(result.insights).lower()
        assert "high" in insights_text or "rate" in insights_text

    def test_task_diversity_insight(self):
        """Verify high task diversity generates warning."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch(f"task{i}", f"task{i+1}", now, interval_minutes=5.0)
            for i in range(12)
        ]
        result = analyze_task_switching_overhead(switches, 100.0)
        insights_text = " ".join(result.insights).lower()
        assert "tasks" in insights_text or "diversity" in insights_text

    def test_overhead_impact_insight(self):
        """Verify significant overhead generates impact warning."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch(f"t{i}", f"t{i+1}", now, interval_minutes=10.0)
            for i in range(15)
        ]
        result = analyze_task_switching_overhead(switches, 200.0)
        # 15 * 5 = 75 min overhead
        insights_text = " ".join(result.insights).lower()
        assert "minutes" in insights_text or "overhead" in insights_text


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_switch(self):
        """Verify single switch produces valid analysis."""
        now = datetime.now(timezone.utc)
        switches = [TaskSwitch("a", "b", now, interval_minutes=30.0)]
        result = analyze_task_switching_overhead(switches, 60.0)
        assert result.metrics.total_switches == 1
        assert result.overhead_tier == TIER_EFFICIENT

    def test_very_short_session(self):
        """Verify very short session handling."""
        now = datetime.now(timezone.utc)
        switches = [TaskSwitch("a", "b", now, interval_minutes=2.0)]
        result = analyze_task_switching_overhead(switches, 5.0)
        # 1 switch * 5 min / 5 min = 100% overhead
        assert result.overhead_tier == TIER_CHAOTIC

    def test_very_long_session(self):
        """Verify very long session handling."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("a", "b", now, interval_minutes=120.0),
            TaskSwitch("b", "c", now, interval_minutes=180.0),
        ]
        result = analyze_task_switching_overhead(switches, 360.0)
        # 2 * 5 = 10 min / 360 min = 2.78%
        assert result.overhead_tier == TIER_EFFICIENT

    def test_all_same_task(self):
        """Verify switching between same task name."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch("task", "task", now, interval_minutes=10.0),
            TaskSwitch("task", "task", now, interval_minutes=10.0),
        ]
        result = analyze_task_switching_overhead(switches, 60.0)
        assert result.metrics.unique_tasks == 1

    def test_large_number_of_switches(self):
        """Verify handling of many switches."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch(f"t{i}", f"t{i+1}", now, interval_minutes=2.0)
            for i in range(100)
        ]
        result = analyze_task_switching_overhead(switches, 300.0)
        assert result.metrics.total_switches == 100
        assert isinstance(result, TaskSwitchingOverhead)

    def test_overhead_exceeds_session(self):
        """Verify handling when overhead > session duration."""
        now = datetime.now(timezone.utc)
        switches = [
            TaskSwitch(f"t{i}", f"t{i+1}", now, interval_minutes=1.0)
            for i in range(20)
        ]
        result = analyze_task_switching_overhead(switches, 50.0)
        # 20 * 5 = 100 min overhead / 50 min = 200%
        assert result.metrics.overhead_percentage == 200.0
        assert result.overhead_tier == TIER_CHAOTIC


class TestSwitchingMetricsDataclass:
    """Test SwitchingMetrics dataclass properties."""

    def test_metrics_frozen(self):
        """Verify metrics are immutable."""
        metrics = SwitchingMetrics(
            total_switches=5,
            avg_interval_minutes=15.0,
            rapid_switches=2,
            focused_periods=1,
            overhead_minutes=25.0,
            overhead_percentage=20.0,
            unique_tasks=3,
        )
        with pytest.raises(AttributeError):
            metrics.total_switches = 10


class TestTaskSwitchingOverheadDataclass:
    """Test TaskSwitchingOverhead dataclass properties."""

    def test_result_frozen(self):
        """Verify result is immutable."""
        metrics = SwitchingMetrics(
            total_switches=5,
            avg_interval_minutes=15.0,
            rapid_switches=2,
            focused_periods=1,
            overhead_minutes=25.0,
            overhead_percentage=20.0,
            unique_tasks=3,
        )
        result = TaskSwitchingOverhead(
            metrics=metrics,
            overhead_tier=TIER_MODERATE,
            session_duration_minutes=120.0,
            insights=["Test insight"],
        )
        with pytest.raises(AttributeError):
            result.overhead_tier = TIER_EFFICIENT
