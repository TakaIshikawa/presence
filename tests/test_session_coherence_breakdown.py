"""Tests for session coherence breakdown detection."""

import pytest
from datetime import datetime, timedelta, timezone

from synthesis.session_coherence_breakdown import (
    BreakdownType,
    SeverityLevel,
    RecoveryPattern,
    CoherenceBreakdownEvent,
    SessionCoherenceAnalysis,
    detect_topic_shift,
    detect_context_loss,
    detect_fragmentation,
    analyze_session_coherence,
    categorize_severity,
    export_breakdown_events_csv,
    export_breakdown_events_json,
    SEVERITY_MEDIUM_THRESHOLD,
    SEVERITY_HIGH_THRESHOLD,
    MIN_TOPIC_OVERLAP,
    MIN_CONTEXT_SIMILARITY,
    MAX_TURN_GAP_MINUTES,
)


class TestBreakdownTypeEnum:
    """Test BreakdownType enum."""

    def test_topic_shift_value(self):
        assert BreakdownType.TOPIC_SHIFT.value == "topic_shift"

    def test_context_loss_value(self):
        assert BreakdownType.CONTEXT_LOSS.value == "context_loss"

    def test_fragmentation_value(self):
        assert BreakdownType.FRAGMENTATION.value == "fragmentation"

    def test_all_types_defined(self):
        """Verify all expected breakdown types are defined."""
        types = {t.value for t in BreakdownType}
        assert types == {"topic_shift", "context_loss", "fragmentation"}


class TestSeverityLevelEnum:
    """Test SeverityLevel enum."""

    def test_low_value(self):
        assert SeverityLevel.LOW.value == "low"

    def test_medium_value(self):
        assert SeverityLevel.MEDIUM.value == "medium"

    def test_high_value(self):
        assert SeverityLevel.HIGH.value == "high"

    def test_all_levels_defined(self):
        """Verify all expected severity levels are defined."""
        levels = {l.value for l in SeverityLevel}
        assert levels == {"low", "medium", "high"}


class TestRecoveryPatternEnum:
    """Test RecoveryPattern enum."""

    def test_none_value(self):
        assert RecoveryPattern.NONE.value == "none"

    def test_immediate_value(self):
        assert RecoveryPattern.IMMEDIATE.value == "immediate"

    def test_gradual_value(self):
        assert RecoveryPattern.GRADUAL.value == "gradual"

    def test_complete_restart_value(self):
        assert RecoveryPattern.COMPLETE_RESTART.value == "complete_restart"


class TestCategorizeSeverity:
    """Test severity categorization."""

    def test_zero_is_low(self):
        assert categorize_severity(0.0) == SeverityLevel.LOW

    def test_just_below_medium_threshold(self):
        assert categorize_severity(SEVERITY_MEDIUM_THRESHOLD - 0.01) == SeverityLevel.LOW

    def test_at_medium_threshold(self):
        assert categorize_severity(SEVERITY_MEDIUM_THRESHOLD) == SeverityLevel.MEDIUM

    def test_mid_medium_range(self):
        mid = (SEVERITY_MEDIUM_THRESHOLD + SEVERITY_HIGH_THRESHOLD) / 2
        assert categorize_severity(mid) == SeverityLevel.MEDIUM

    def test_just_below_high_threshold(self):
        assert categorize_severity(SEVERITY_HIGH_THRESHOLD - 0.01) == SeverityLevel.MEDIUM

    def test_at_high_threshold(self):
        assert categorize_severity(SEVERITY_HIGH_THRESHOLD) == SeverityLevel.HIGH

    def test_maximum_severity(self):
        assert categorize_severity(1.0) == SeverityLevel.HIGH


class TestDetectTopicShift:
    """Test topic shift detection."""

    def test_no_shift_identical_topics(self):
        """Verify no shift detected when topics are identical."""
        prev = {"topics": ["python", "testing"]}
        curr = {"topics": ["python", "testing"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_topic_shift(prev, curr, timestamp)
        assert result is None

    def test_no_shift_high_overlap(self):
        """Verify no shift when overlap is above threshold."""
        prev = {"topics": ["python", "testing", "pytest"]}
        curr = {"topics": ["python", "testing", "unittest"]}
        timestamp = datetime.now(timezone.utc)

        # Overlap: 2/4 = 0.5 > MIN_TOPIC_OVERLAP (0.3)
        result = detect_topic_shift(prev, curr, timestamp)
        assert result is None

    def test_shift_detected_no_overlap(self):
        """Verify shift detected when topics have no overlap."""
        prev = {"topics": ["python", "testing"]}
        curr = {"topics": ["javascript", "react"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_topic_shift(prev, curr, timestamp)
        assert result is not None
        assert result.breakdown_type == BreakdownType.TOPIC_SHIFT
        assert result.severity_score == 1.0  # No overlap
        assert result.severity_level == SeverityLevel.HIGH

    def test_shift_detected_low_overlap(self):
        """Verify shift detected when overlap is below threshold."""
        prev = {"topics": ["python", "testing", "pytest", "coverage"]}
        curr = {"topics": ["python", "javascript", "react", "vue"]}
        timestamp = datetime.now(timezone.utc)

        # Overlap: 1/7 = 0.14 < MIN_TOPIC_OVERLAP (0.3)
        result = detect_topic_shift(prev, curr, timestamp)
        assert result is not None
        assert result.breakdown_type == BreakdownType.TOPIC_SHIFT
        assert result.severity_score > 0.8  # Very low overlap

    def test_no_shift_empty_previous_topics(self):
        """Verify no shift when previous topics are empty."""
        prev = {"topics": []}
        curr = {"topics": ["python", "testing"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_topic_shift(prev, curr, timestamp)
        assert result is None

    def test_no_shift_empty_current_topics(self):
        """Verify no shift when current topics are empty."""
        prev = {"topics": ["python", "testing"]}
        curr = {"topics": []}
        timestamp = datetime.now(timezone.utc)

        result = detect_topic_shift(prev, curr, timestamp)
        assert result is None

    def test_shift_includes_timestamp(self):
        """Verify shift event includes correct timestamp."""
        prev = {"topics": ["python"]}
        curr = {"topics": ["javascript"]}
        timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

        result = detect_topic_shift(prev, curr, timestamp)
        assert result is not None
        assert result.timestamp == timestamp

    def test_shift_includes_context(self):
        """Verify shift event includes topic context."""
        prev = {"topics": ["python", "testing"]}
        curr = {"topics": ["javascript", "react"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_topic_shift(prev, curr, timestamp)
        assert result is not None
        assert "previous_topics" in result.context
        assert "current_topics" in result.context
        assert "overlap_ratio" in result.context
        assert set(result.context["previous_topics"]) == {"python", "testing"}
        assert set(result.context["current_topics"]) == {"javascript", "react"}

    def test_shift_event_id_format(self):
        """Verify shift event ID follows expected format."""
        prev = {"topics": ["python"]}
        curr = {"topics": ["javascript"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_topic_shift(prev, curr, timestamp)
        assert result is not None
        assert result.event_id.startswith("topic_shift_")

    def test_shift_description_includes_topics(self):
        """Verify shift description includes topic information."""
        prev = {"topics": ["python"]}
        curr = {"topics": ["javascript"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_topic_shift(prev, curr, timestamp)
        assert result is not None
        assert "python" in result.description
        assert "javascript" in result.description


class TestDetectContextLoss:
    """Test context loss detection."""

    def test_no_loss_all_references_retained(self):
        """Verify no loss when all references are retained."""
        expected = {"references": ["var1", "func1", "class1"]}
        actual = {"references": ["var1", "func1", "class1"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_context_loss(expected, actual, timestamp)
        assert result is None

    def test_no_loss_high_retention(self):
        """Verify no loss when retention is above threshold."""
        expected = {"references": ["var1", "func1", "class1"]}
        actual = {"references": ["var1", "func1"]}
        timestamp = datetime.now(timezone.utc)

        # Retention: 2/3 = 0.67 > MIN_CONTEXT_SIMILARITY (0.4)
        result = detect_context_loss(expected, actual, timestamp)
        assert result is None

    def test_loss_detected_no_retention(self):
        """Verify loss detected when no references are retained."""
        expected = {"references": ["var1", "func1"]}
        actual = {"references": ["var2", "func2"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_context_loss(expected, actual, timestamp)
        assert result is not None
        assert result.breakdown_type == BreakdownType.CONTEXT_LOSS
        assert result.severity_score == 1.0
        assert result.severity_level == SeverityLevel.HIGH

    def test_loss_detected_low_retention(self):
        """Verify loss detected when retention is below threshold."""
        expected = {"references": ["var1", "func1", "class1", "module1"]}
        actual = {"references": ["var1"]}
        timestamp = datetime.now(timezone.utc)

        # Retention: 1/4 = 0.25 < MIN_CONTEXT_SIMILARITY (0.4)
        result = detect_context_loss(expected, actual, timestamp)
        assert result is not None
        assert result.breakdown_type == BreakdownType.CONTEXT_LOSS
        assert result.severity_score == 0.75

    def test_no_loss_no_expected_context(self):
        """Verify no loss when no context is expected."""
        expected = {"references": []}
        actual = {"references": ["var1", "func1"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_context_loss(expected, actual, timestamp)
        assert result is None

    def test_loss_includes_timestamp(self):
        """Verify loss event includes correct timestamp."""
        expected = {"references": ["var1"]}
        actual = {"references": ["var2"]}
        timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

        result = detect_context_loss(expected, actual, timestamp)
        assert result is not None
        assert result.timestamp == timestamp

    def test_loss_includes_context(self):
        """Verify loss event includes reference context."""
        expected = {"references": ["var1", "func1"]}
        actual = {"references": ["var2"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_context_loss(expected, actual, timestamp)
        assert result is not None
        assert "expected_references" in result.context
        assert "actual_references" in result.context
        assert "retention_ratio" in result.context

    def test_loss_description_includes_counts(self):
        """Verify loss description includes retention information."""
        expected = {"references": ["var1", "func1", "class1"]}
        actual = {"references": ["var1"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_context_loss(expected, actual, timestamp)
        assert result is not None
        assert "1/3" in result.description


class TestDetectFragmentation:
    """Test conversation fragmentation detection."""

    def test_no_fragmentation_small_gaps(self):
        """Verify no fragmentation when gaps are small."""
        base_time = datetime.now(timezone.utc)
        turns = [
            {"timestamp": base_time},
            {"timestamp": base_time + timedelta(minutes=5)},
            {"timestamp": base_time + timedelta(minutes=10)},
        ]
        timestamp = base_time + timedelta(minutes=10)

        result = detect_fragmentation(turns, timestamp)
        assert result is None

    def test_no_fragmentation_insufficient_turns(self):
        """Verify no fragmentation detected with too few turns."""
        base_time = datetime.now(timezone.utc)
        turns = [
            {"timestamp": base_time},
            {"timestamp": base_time + timedelta(minutes=60)},
        ]
        timestamp = base_time + timedelta(minutes=60)

        result = detect_fragmentation(turns, timestamp)
        assert result is None

    def test_fragmentation_detected_large_gap(self):
        """Verify fragmentation detected with large time gap."""
        base_time = datetime.now(timezone.utc)
        turns = [
            {"timestamp": base_time},
            {"timestamp": base_time + timedelta(minutes=5)},
            {"timestamp": base_time + timedelta(minutes=50)},  # Large gap
        ]
        timestamp = base_time + timedelta(minutes=50)

        result = detect_fragmentation(turns, timestamp)
        assert result is not None
        assert result.breakdown_type == BreakdownType.FRAGMENTATION

    def test_fragmentation_severity_increases_with_gap(self):
        """Verify severity increases with gap size."""
        base_time = datetime.now(timezone.utc)
        turns_small = [
            {"timestamp": base_time},
            {"timestamp": base_time + timedelta(minutes=5)},
            {"timestamp": base_time + timedelta(minutes=40)},
        ]
        turns_large = [
            {"timestamp": base_time},
            {"timestamp": base_time + timedelta(minutes=5)},
            {"timestamp": base_time + timedelta(minutes=120)},
        ]

        result_small = detect_fragmentation(turns_small, base_time + timedelta(minutes=40))
        result_large = detect_fragmentation(turns_large, base_time + timedelta(minutes=120))

        assert result_small is not None
        assert result_large is not None
        assert result_large.severity_score > result_small.severity_score

    def test_fragmentation_with_iso_string_timestamps(self):
        """Verify fragmentation detection works with ISO string timestamps."""
        base_time = datetime.now(timezone.utc)
        turns = [
            {"timestamp": base_time.isoformat()},
            {"timestamp": (base_time + timedelta(minutes=5)).isoformat()},
            {"timestamp": (base_time + timedelta(minutes=50)).isoformat()},
        ]
        timestamp = base_time + timedelta(minutes=50)

        result = detect_fragmentation(turns, timestamp)
        assert result is not None
        assert result.breakdown_type == BreakdownType.FRAGMENTATION

    def test_fragmentation_includes_context(self):
        """Verify fragmentation event includes gap context."""
        base_time = datetime.now(timezone.utc)
        turns = [
            {"timestamp": base_time},
            {"timestamp": base_time + timedelta(minutes=5)},
            {"timestamp": base_time + timedelta(minutes=50)},
        ]
        timestamp = base_time + timedelta(minutes=50)

        result = detect_fragmentation(turns, timestamp)
        assert result is not None
        assert "max_gap_minutes" in result.context
        assert "avg_gap_minutes" in result.context
        assert "turn_count" in result.context
        assert result.context["max_gap_minutes"] > 40

    def test_fragmentation_no_timestamps(self):
        """Verify no fragmentation when turns lack timestamps."""
        turns = [
            {"content": "turn1"},
            {"content": "turn2"},
            {"content": "turn3"},
        ]
        timestamp = datetime.now(timezone.utc)

        result = detect_fragmentation(turns, timestamp)
        assert result is None


class TestAnalyzeSessionCoherence:
    """Test overall session coherence analysis."""

    def test_perfect_coherence_no_breakdowns(self):
        """Verify perfect coherence with no breakdowns."""
        turns = [{"id": i} for i in range(10)]
        breakdown_events = []

        result = analyze_session_coherence("session1", turns, breakdown_events)

        assert result.session_id == "session1"
        assert result.total_turns == 10
        assert result.overall_coherence_score == 1.0
        assert result.fragmentation_count == 0
        assert result.topic_shift_count == 0
        assert result.context_loss_count == 0
        assert len(result.insights) > 0

    def test_coherence_decreases_with_breakdowns(self):
        """Verify coherence score decreases with more breakdowns."""
        turns = [{"id": i} for i in range(10)]
        timestamp = datetime.now(timezone.utc)

        # Create multiple breakdown events
        breakdowns = [
            CoherenceBreakdownEvent(
                event_id=f"event_{i}",
                timestamp=timestamp,
                breakdown_type=BreakdownType.TOPIC_SHIFT,
                severity_score=0.5,
                severity_level=SeverityLevel.MEDIUM,
                description="Test shift",
                context={},
                recovery_pattern=RecoveryPattern.NONE,
                recovery_time_minutes=None,
            )
            for i in range(5)
        ]

        result = analyze_session_coherence("session1", turns, breakdowns)

        assert result.overall_coherence_score < 1.0
        assert result.overall_coherence_score >= 0.0

    def test_breakdown_counts_by_type(self):
        """Verify breakdown counts are tracked by type."""
        turns = [{"id": i} for i in range(10)]
        timestamp = datetime.now(timezone.utc)

        breakdowns = [
            CoherenceBreakdownEvent(
                event_id="event_1",
                timestamp=timestamp,
                breakdown_type=BreakdownType.TOPIC_SHIFT,
                severity_score=0.5,
                severity_level=SeverityLevel.MEDIUM,
                description="Shift",
                context={},
                recovery_pattern=RecoveryPattern.NONE,
                recovery_time_minutes=None,
            ),
            CoherenceBreakdownEvent(
                event_id="event_2",
                timestamp=timestamp,
                breakdown_type=BreakdownType.CONTEXT_LOSS,
                severity_score=0.5,
                severity_level=SeverityLevel.MEDIUM,
                description="Loss",
                context={},
                recovery_pattern=RecoveryPattern.NONE,
                recovery_time_minutes=None,
            ),
            CoherenceBreakdownEvent(
                event_id="event_3",
                timestamp=timestamp,
                breakdown_type=BreakdownType.FRAGMENTATION,
                severity_score=0.5,
                severity_level=SeverityLevel.MEDIUM,
                description="Frag",
                context={},
                recovery_pattern=RecoveryPattern.NONE,
                recovery_time_minutes=None,
            ),
        ]

        result = analyze_session_coherence("session1", turns, breakdowns)

        assert result.topic_shift_count == 1
        assert result.context_loss_count == 1
        assert result.fragmentation_count == 1

    def test_average_recovery_time_calculated(self):
        """Verify average recovery time is calculated correctly."""
        turns = [{"id": i} for i in range(10)]
        timestamp = datetime.now(timezone.utc)

        breakdowns = [
            CoherenceBreakdownEvent(
                event_id="event_1",
                timestamp=timestamp,
                breakdown_type=BreakdownType.TOPIC_SHIFT,
                severity_score=0.5,
                severity_level=SeverityLevel.MEDIUM,
                description="Shift",
                context={},
                recovery_pattern=RecoveryPattern.IMMEDIATE,
                recovery_time_minutes=2.0,
            ),
            CoherenceBreakdownEvent(
                event_id="event_2",
                timestamp=timestamp,
                breakdown_type=BreakdownType.CONTEXT_LOSS,
                severity_score=0.5,
                severity_level=SeverityLevel.MEDIUM,
                description="Loss",
                context={},
                recovery_pattern=RecoveryPattern.GRADUAL,
                recovery_time_minutes=8.0,
            ),
        ]

        result = analyze_session_coherence("session1", turns, breakdowns)

        assert result.average_recovery_time_minutes == 5.0  # (2.0 + 8.0) / 2

    def test_average_recovery_time_none_when_no_recoveries(self):
        """Verify average recovery time is None when no events recovered."""
        turns = [{"id": i} for i in range(10)]
        timestamp = datetime.now(timezone.utc)

        breakdowns = [
            CoherenceBreakdownEvent(
                event_id="event_1",
                timestamp=timestamp,
                breakdown_type=BreakdownType.TOPIC_SHIFT,
                severity_score=0.5,
                severity_level=SeverityLevel.MEDIUM,
                description="Shift",
                context={},
                recovery_pattern=RecoveryPattern.NONE,
                recovery_time_minutes=None,
            ),
        ]

        result = analyze_session_coherence("session1", turns, breakdowns)

        assert result.average_recovery_time_minutes is None

    def test_insights_generated(self):
        """Verify insights are generated for session analysis."""
        turns = [{"id": i} for i in range(10)]
        timestamp = datetime.now(timezone.utc)

        breakdowns = [
            CoherenceBreakdownEvent(
                event_id="event_1",
                timestamp=timestamp,
                breakdown_type=BreakdownType.TOPIC_SHIFT,
                severity_score=0.5,
                severity_level=SeverityLevel.MEDIUM,
                description="Shift",
                context={},
                recovery_pattern=RecoveryPattern.NONE,
                recovery_time_minutes=None,
            ),
        ]

        result = analyze_session_coherence("session1", turns, breakdowns)

        assert len(result.insights) > 0
        assert any("topic shift" in insight.lower() for insight in result.insights)

    def test_zero_turns_edge_case(self):
        """Verify handling of session with zero turns."""
        turns = []
        breakdowns = []

        result = analyze_session_coherence("session1", turns, breakdowns)

        assert result.total_turns == 0
        assert result.overall_coherence_score == 1.0  # No turns = no breakdowns


class TestExportBreakdownEventsCSV:
    """Test CSV export of breakdown events."""

    def test_csv_header(self):
        """Verify CSV includes proper header."""
        events = []
        csv_output = export_breakdown_events_csv(events)

        assert "event_id" in csv_output
        assert "timestamp" in csv_output
        assert "breakdown_type" in csv_output
        assert "severity_score" in csv_output
        assert "severity_level" in csv_output

    def test_csv_single_event(self):
        """Verify CSV export of single event."""
        timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        event = CoherenceBreakdownEvent(
            event_id="test_event_1",
            timestamp=timestamp,
            breakdown_type=BreakdownType.TOPIC_SHIFT,
            severity_score=0.75,
            severity_level=SeverityLevel.HIGH,
            description="Test shift",
            context={},
            recovery_pattern=RecoveryPattern.IMMEDIATE,
            recovery_time_minutes=2.5,
        )

        csv_output = export_breakdown_events_csv([event])
        lines = csv_output.split("\n")

        assert len(lines) == 2  # Header + 1 event
        assert "test_event_1" in lines[1]
        assert "topic_shift" in lines[1]
        assert "0.750" in lines[1]

    def test_csv_multiple_events(self):
        """Verify CSV export of multiple events."""
        timestamp = datetime.now(timezone.utc)
        events = [
            CoherenceBreakdownEvent(
                event_id=f"event_{i}",
                timestamp=timestamp,
                breakdown_type=BreakdownType.TOPIC_SHIFT,
                severity_score=0.5,
                severity_level=SeverityLevel.MEDIUM,
                description=f"Event {i}",
                context={},
                recovery_pattern=RecoveryPattern.NONE,
                recovery_time_minutes=None,
            )
            for i in range(3)
        ]

        csv_output = export_breakdown_events_csv(events)
        lines = csv_output.split("\n")

        assert len(lines) == 4  # Header + 3 events

    def test_csv_empty_recovery_time(self):
        """Verify CSV handles None recovery time."""
        timestamp = datetime.now(timezone.utc)
        event = CoherenceBreakdownEvent(
            event_id="test_event",
            timestamp=timestamp,
            breakdown_type=BreakdownType.TOPIC_SHIFT,
            severity_score=0.5,
            severity_level=SeverityLevel.MEDIUM,
            description="Test",
            context={},
            recovery_pattern=RecoveryPattern.NONE,
            recovery_time_minutes=None,
        )

        csv_output = export_breakdown_events_csv([event])
        lines = csv_output.split("\n")

        # Should have empty field for recovery_time_minutes
        assert len(lines) == 2


class TestExportBreakdownEventsJSON:
    """Test JSON export of breakdown events."""

    def test_json_empty_list(self):
        """Verify JSON export of empty event list."""
        import json

        events = []
        json_output = export_breakdown_events_json(events)
        parsed = json.loads(json_output)

        assert parsed == []

    def test_json_single_event(self):
        """Verify JSON export of single event."""
        import json

        timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        event = CoherenceBreakdownEvent(
            event_id="test_event_1",
            timestamp=timestamp,
            breakdown_type=BreakdownType.TOPIC_SHIFT,
            severity_score=0.75,
            severity_level=SeverityLevel.HIGH,
            description="Test shift",
            context={"previous_topics": ["python"], "current_topics": ["javascript"]},
            recovery_pattern=RecoveryPattern.IMMEDIATE,
            recovery_time_minutes=2.5,
        )

        json_output = export_breakdown_events_json([event])
        parsed = json.loads(json_output)

        assert len(parsed) == 1
        assert parsed[0]["event_id"] == "test_event_1"
        assert parsed[0]["breakdown_type"] == "topic_shift"
        assert parsed[0]["severity_score"] == 0.75
        assert parsed[0]["severity_level"] == "high"

    def test_json_includes_context(self):
        """Verify JSON export includes event context."""
        import json

        timestamp = datetime.now(timezone.utc)
        event = CoherenceBreakdownEvent(
            event_id="test_event",
            timestamp=timestamp,
            breakdown_type=BreakdownType.CONTEXT_LOSS,
            severity_score=0.5,
            severity_level=SeverityLevel.MEDIUM,
            description="Test",
            context={"expected_references": ["ref1", "ref2"]},
            recovery_pattern=RecoveryPattern.NONE,
            recovery_time_minutes=None,
        )

        json_output = export_breakdown_events_json([event])
        parsed = json.loads(json_output)

        assert "context" in parsed[0]
        assert parsed[0]["context"]["expected_references"] == ["ref1", "ref2"]

    def test_json_sorted_keys(self):
        """Verify JSON output has sorted keys for determinism."""
        timestamp = datetime.now(timezone.utc)
        event = CoherenceBreakdownEvent(
            event_id="test_event",
            timestamp=timestamp,
            breakdown_type=BreakdownType.TOPIC_SHIFT,
            severity_score=0.5,
            severity_level=SeverityLevel.MEDIUM,
            description="Test",
            context={},
            recovery_pattern=RecoveryPattern.NONE,
            recovery_time_minutes=None,
        )

        json_output = export_breakdown_events_json([event])

        # Keys should be in alphabetical order
        lines = json_output.split("\n")
        # First event should have keys in order
        assert '"breakdown_type"' in json_output
        assert '"context"' in json_output
        # Verify sorted order by checking positions
        breakdown_pos = json_output.index('"breakdown_type"')
        context_pos = json_output.index('"context"')
        assert breakdown_pos < context_pos


class TestCoherenceBreakdownEventDataclass:
    """Test CoherenceBreakdownEvent dataclass."""

    def test_event_frozen(self):
        """Verify event is immutable."""
        timestamp = datetime.now(timezone.utc)
        event = CoherenceBreakdownEvent(
            event_id="test",
            timestamp=timestamp,
            breakdown_type=BreakdownType.TOPIC_SHIFT,
            severity_score=0.5,
            severity_level=SeverityLevel.MEDIUM,
            description="Test",
            context={},
            recovery_pattern=RecoveryPattern.NONE,
            recovery_time_minutes=None,
        )

        with pytest.raises(AttributeError):
            event.severity_score = 0.9

    def test_event_all_fields_present(self):
        """Verify event has all required fields."""
        timestamp = datetime.now(timezone.utc)
        event = CoherenceBreakdownEvent(
            event_id="test",
            timestamp=timestamp,
            breakdown_type=BreakdownType.TOPIC_SHIFT,
            severity_score=0.5,
            severity_level=SeverityLevel.MEDIUM,
            description="Test",
            context={"key": "value"},
            recovery_pattern=RecoveryPattern.IMMEDIATE,
            recovery_time_minutes=2.0,
        )

        assert event.event_id == "test"
        assert event.timestamp == timestamp
        assert event.breakdown_type == BreakdownType.TOPIC_SHIFT
        assert event.severity_score == 0.5
        assert event.severity_level == SeverityLevel.MEDIUM
        assert event.description == "Test"
        assert event.context == {"key": "value"}
        assert event.recovery_pattern == RecoveryPattern.IMMEDIATE
        assert event.recovery_time_minutes == 2.0


class TestSessionCoherenceAnalysisDataclass:
    """Test SessionCoherenceAnalysis dataclass."""

    def test_analysis_frozen(self):
        """Verify analysis is immutable."""
        timestamp = datetime.now(timezone.utc)
        analysis = SessionCoherenceAnalysis(
            session_id="test",
            analyzed_at=timestamp,
            total_turns=10,
            breakdown_events=[],
            overall_coherence_score=0.9,
            fragmentation_count=0,
            topic_shift_count=0,
            context_loss_count=0,
            average_recovery_time_minutes=None,
            insights=[],
        )

        with pytest.raises(AttributeError):
            analysis.overall_coherence_score = 0.5

    def test_analysis_all_fields_present(self):
        """Verify analysis has all required fields."""
        timestamp = datetime.now(timezone.utc)
        analysis = SessionCoherenceAnalysis(
            session_id="test_session",
            analyzed_at=timestamp,
            total_turns=10,
            breakdown_events=[],
            overall_coherence_score=0.95,
            fragmentation_count=1,
            topic_shift_count=2,
            context_loss_count=1,
            average_recovery_time_minutes=5.0,
            insights=["Insight 1", "Insight 2"],
        )

        assert analysis.session_id == "test_session"
        assert analysis.analyzed_at == timestamp
        assert analysis.total_turns == 10
        assert analysis.overall_coherence_score == 0.95
        assert analysis.fragmentation_count == 1
        assert analysis.topic_shift_count == 2
        assert analysis.context_loss_count == 1
        assert analysis.average_recovery_time_minutes == 5.0
        assert len(analysis.insights) == 2


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_topic_shift_missing_topics_key(self):
        """Verify handling of missing topics key."""
        prev = {}
        curr = {"topics": ["python"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_topic_shift(prev, curr, timestamp)
        assert result is None

    def test_context_loss_missing_references_key(self):
        """Verify handling of missing references key."""
        expected = {}
        actual = {"references": ["ref1"]}
        timestamp = datetime.now(timezone.utc)

        result = detect_context_loss(expected, actual, timestamp)
        assert result is None

    def test_fragmentation_empty_turns(self):
        """Verify handling of empty turns list."""
        turns = []
        timestamp = datetime.now(timezone.utc)

        result = detect_fragmentation(turns, timestamp)
        assert result is None

    def test_analyze_coherence_empty_session(self):
        """Verify analysis of completely empty session."""
        result = analyze_session_coherence("empty_session", [], [])

        assert result.session_id == "empty_session"
        assert result.total_turns == 0
        assert result.overall_coherence_score == 1.0
        assert len(result.breakdown_events) == 0
