"""Tests for tool context handoff efficiency analysis."""

import pytest
from datetime import datetime, timezone

from engagement.tool_context_handoff import (
    HandoffEfficiency,
    BottleneckType,
    ToolContextHandoffEvent,
    ToolContextHandoffAnalysis,
    measure_information_loss,
    identify_bottlenecks,
    calculate_handoff_efficiency,
    categorize_efficiency,
    analyze_tool_handoff,
    analyze_handoff_batch,
    export_handoff_events_csv,
    export_handoff_events_json,
    EFFICIENCY_HIGH_THRESHOLD,
    EFFICIENCY_MEDIUM_THRESHOLD,
    MIN_CONTEXT_RETENTION,
    MIN_DATA_COMPLETENESS,
)


class TestHandoffEfficiencyEnum:
    """Test HandoffEfficiency enum."""

    def test_high_value(self):
        assert HandoffEfficiency.HIGH.value == "high"

    def test_medium_value(self):
        assert HandoffEfficiency.MEDIUM.value == "medium"

    def test_low_value(self):
        assert HandoffEfficiency.LOW.value == "low"

    def test_all_levels_defined(self):
        """Verify all expected efficiency levels are defined."""
        levels = {l.value for l in HandoffEfficiency}
        assert levels == {"high", "medium", "low"}


class TestBottleneckTypeEnum:
    """Test BottleneckType enum."""

    def test_data_format_mismatch_value(self):
        assert BottleneckType.DATA_FORMAT_MISMATCH.value == "data_format_mismatch"

    def test_missing_context_value(self):
        assert BottleneckType.MISSING_CONTEXT.value == "missing_context"

    def test_incomplete_transfer_value(self):
        assert BottleneckType.INCOMPLETE_TRANSFER.value == "incomplete_transfer"

    def test_schema_incompatibility_value(self):
        assert BottleneckType.SCHEMA_INCOMPATIBILITY.value == "schema_incompatibility"


class TestMeasureInformationLoss:
    """Test information loss measurement."""

    def test_no_loss_identical_contexts(self):
        """Verify no loss when contexts are identical."""
        source = {"key1": "value1", "key2": 42}
        transferred = {"key1": "value1", "key2": 42}

        loss = measure_information_loss(source, transferred)
        assert loss == 0.0

    def test_no_loss_empty_source(self):
        """Verify no loss when source is empty."""
        source = {}
        transferred = {"key1": "value1"}

        loss = measure_information_loss(source, transferred)
        assert loss == 0.0

    def test_full_loss_no_keys_retained(self):
        """Verify full loss when no keys are retained."""
        source = {"key1": "value1", "key2": "value2"}
        transferred = {}

        loss = measure_information_loss(source, transferred)
        assert loss == 1.0

    def test_partial_loss_some_keys_missing(self):
        """Verify partial loss when some keys are missing."""
        source = {"key1": "value1", "key2": "value2", "key3": "value3"}
        transferred = {"key1": "value1"}

        loss = measure_information_loss(source, transferred)
        assert 0.0 < loss < 1.0

    def test_partial_loss_value_changes(self):
        """Verify partial loss when values are modified."""
        source = {"key1": "value1", "key2": "value2"}
        transferred = {"key1": "modified", "key2": "value2"}

        loss = measure_information_loss(source, transferred)
        assert 0.0 < loss < 1.0

    def test_numeric_approximation_no_loss(self):
        """Verify numeric values are considered equal if approximately same."""
        source = {"value": 1.0}
        transferred = {"value": 1.0001}

        loss = measure_information_loss(source, transferred)
        # Should be very small loss or no loss
        assert loss < 0.1

    def test_numeric_approximation_with_loss(self):
        """Verify numeric values show loss if significantly different."""
        source = {"value": 1.0}
        transferred = {"value": 5.0}

        loss = measure_information_loss(source, transferred)
        assert loss > 0.0


class TestIdentifyBottlenecks:
    """Test bottleneck identification."""

    def test_no_bottlenecks_perfect_transfer(self):
        """Verify no bottlenecks with perfect transfer."""
        source = {"key1": "value1", "key2": 42}
        transferred = {"key1": "value1", "key2": 42}

        bottlenecks = identify_bottlenecks(source, transferred, "tool1", "tool2")
        assert bottlenecks == []

    def test_missing_context_bottleneck(self):
        """Verify missing context bottleneck is detected."""
        source = {"key1": "v1", "key2": "v2", "key3": "v3", "key4": "v4"}
        transferred = {"key1": "v1"}  # 75% missing

        bottlenecks = identify_bottlenecks(source, transferred, "tool1", "tool2")
        assert BottleneckType.MISSING_CONTEXT in bottlenecks

    def test_data_format_mismatch_bottleneck(self):
        """Verify data format mismatch bottleneck is detected."""
        source = {"key1": 42}
        transferred = {"key1": "42"}  # Type changed

        bottlenecks = identify_bottlenecks(source, transferred, "tool1", "tool2")
        assert BottleneckType.DATA_FORMAT_MISMATCH in bottlenecks

    def test_incomplete_transfer_bottleneck(self):
        """Verify incomplete transfer bottleneck is detected."""
        source = {"data": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}
        transferred = {"data": [1, 2]}  # Truncated

        bottlenecks = identify_bottlenecks(source, transferred, "tool1", "tool2")
        assert BottleneckType.INCOMPLETE_TRANSFER in bottlenecks

    def test_schema_incompatibility_bottleneck(self):
        """Verify schema incompatibility bottleneck is detected."""
        source = {"schema": "v1", "data": "test"}
        transferred = {"schema": "v2", "data": "test"}

        bottlenecks = identify_bottlenecks(source, transferred, "tool1", "tool2")
        assert BottleneckType.SCHEMA_INCOMPATIBILITY in bottlenecks

    def test_multiple_bottlenecks(self):
        """Verify multiple bottlenecks can be detected."""
        source = {"key1": 42, "key2": "v2", "key3": "v3"}
        transferred = {"key1": "42"}  # Type mismatch + missing keys

        bottlenecks = identify_bottlenecks(source, transferred, "tool1", "tool2")
        assert len(bottlenecks) >= 1


class TestCalculateHandoffEfficiency:
    """Test handoff efficiency calculation."""

    def test_perfect_efficiency(self):
        """Verify perfect efficiency with no loss."""
        efficiency = calculate_handoff_efficiency(
            information_loss=0.0, context_retention_ratio=1.0, data_completeness=1.0
        )
        assert efficiency == 1.0

    def test_zero_efficiency(self):
        """Verify low efficiency with maximum loss."""
        efficiency = calculate_handoff_efficiency(
            information_loss=1.0, context_retention_ratio=0.0, data_completeness=0.0
        )
        assert efficiency == 0.0

    def test_medium_efficiency(self):
        """Verify medium efficiency calculation."""
        efficiency = calculate_handoff_efficiency(
            information_loss=0.5, context_retention_ratio=0.5, data_completeness=0.5
        )
        assert 0.0 < efficiency < 1.0

    def test_efficiency_bounded(self):
        """Verify efficiency is bounded to [0.0, 1.0]."""
        # Test upper bound
        efficiency_high = calculate_handoff_efficiency(
            information_loss=-1.0,  # Invalid but testing bounds
            context_retention_ratio=2.0,
            data_completeness=2.0,
        )
        assert efficiency_high <= 1.0

        # Test lower bound
        efficiency_low = calculate_handoff_efficiency(
            information_loss=2.0,  # Invalid but testing bounds
            context_retention_ratio=-1.0,
            data_completeness=-1.0,
        )
        assert efficiency_low >= 0.0


class TestCategorizeEfficiency:
    """Test efficiency categorization."""

    def test_high_efficiency_at_threshold(self):
        assert categorize_efficiency(EFFICIENCY_HIGH_THRESHOLD) == HandoffEfficiency.HIGH

    def test_high_efficiency_above_threshold(self):
        assert categorize_efficiency(0.9) == HandoffEfficiency.HIGH

    def test_high_efficiency_perfect(self):
        assert categorize_efficiency(1.0) == HandoffEfficiency.HIGH

    def test_medium_efficiency_at_threshold(self):
        assert (
            categorize_efficiency(EFFICIENCY_MEDIUM_THRESHOLD)
            == HandoffEfficiency.MEDIUM
        )

    def test_medium_efficiency_mid_range(self):
        mid = (EFFICIENCY_MEDIUM_THRESHOLD + EFFICIENCY_HIGH_THRESHOLD) / 2
        assert categorize_efficiency(mid) == HandoffEfficiency.MEDIUM

    def test_low_efficiency_below_medium(self):
        assert categorize_efficiency(0.3) == HandoffEfficiency.LOW

    def test_low_efficiency_zero(self):
        assert categorize_efficiency(0.0) == HandoffEfficiency.LOW


class TestAnalyzeToolHandoff:
    """Test single tool handoff analysis."""

    def test_perfect_handoff(self):
        """Verify analysis of perfect handoff."""
        timestamp = datetime.now(timezone.utc)
        source_context = {"key1": "value1", "key2": 42}
        transferred_context = {"key1": "value1", "key2": 42}

        event = analyze_tool_handoff(
            handoff_id="test_handoff",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            source_context=source_context,
            transferred_context=transferred_context,
        )

        assert event.handoff_id == "test_handoff"
        assert event.source_tool == "tool_a"
        assert event.target_tool == "tool_b"
        assert event.information_loss_score == 0.0
        assert event.efficiency_score > EFFICIENCY_HIGH_THRESHOLD
        assert event.efficiency_level == HandoffEfficiency.HIGH
        assert event.success is True
        assert event.bottlenecks == []

    def test_failed_handoff_missing_context(self):
        """Verify analysis of failed handoff due to missing context."""
        timestamp = datetime.now(timezone.utc)
        source_context = {"key1": "v1", "key2": "v2", "key3": "v3"}
        transferred_context = {}  # Nothing transferred

        event = analyze_tool_handoff(
            handoff_id="test_handoff",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            source_context=source_context,
            transferred_context=transferred_context,
        )

        assert event.information_loss_score > 0.5
        assert event.efficiency_score < EFFICIENCY_MEDIUM_THRESHOLD
        assert event.success is False

    def test_handoff_includes_metadata(self):
        """Verify handoff event includes metadata."""
        timestamp = datetime.now(timezone.utc)
        source_context = {"key1": "value1"}
        transferred_context = {"key1": "value1"}

        event = analyze_tool_handoff(
            handoff_id="test_handoff",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            source_context=source_context,
            transferred_context=transferred_context,
        )

        assert "context_retention_ratio" in event.metadata
        assert "data_completeness" in event.metadata

    def test_handoff_context_size_calculation(self):
        """Verify context size is calculated correctly."""
        timestamp = datetime.now(timezone.utc)
        source_context = {"key1": "value1", "key2": "value2"}
        transferred_context = {"key1": "value1"}

        event = analyze_tool_handoff(
            handoff_id="test_handoff",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            source_context=source_context,
            transferred_context=transferred_context,
        )

        assert event.context_size_bytes > 0
        assert event.transferred_size_bytes > 0
        assert event.transferred_size_bytes < event.context_size_bytes


class TestAnalyzeHandoffBatch:
    """Test batch handoff analysis."""

    def test_empty_batch(self):
        """Verify analysis of empty batch."""
        analysis = analyze_handoff_batch([])

        assert analysis.total_handoffs == 0
        assert analysis.successful_handoffs == 0
        assert analysis.failed_handoffs == 0
        assert analysis.success_rate == 0.0
        assert analysis.average_efficiency_score == 0.0
        assert analysis.bottleneck_counts == {}

    def test_batch_with_all_successful(self):
        """Verify analysis of batch with all successful handoffs."""
        timestamp = datetime.now(timezone.utc)
        events = [
            ToolContextHandoffEvent(
                handoff_id=f"handoff_{i}",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=100,
                information_loss_score=0.0,
                efficiency_score=0.95,
                efficiency_level=HandoffEfficiency.HIGH,
                success=True,
                bottlenecks=[],
                metadata={},
            )
            for i in range(5)
        ]

        analysis = analyze_handoff_batch(events)

        assert analysis.total_handoffs == 5
        assert analysis.successful_handoffs == 5
        assert analysis.failed_handoffs == 0
        assert analysis.success_rate == 1.0

    def test_batch_with_mixed_results(self):
        """Verify analysis of batch with mixed success/failure."""
        timestamp = datetime.now(timezone.utc)
        events = [
            ToolContextHandoffEvent(
                handoff_id="success_1",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=100,
                information_loss_score=0.0,
                efficiency_score=0.9,
                efficiency_level=HandoffEfficiency.HIGH,
                success=True,
                bottlenecks=[],
                metadata={},
            ),
            ToolContextHandoffEvent(
                handoff_id="failure_1",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=20,
                information_loss_score=0.8,
                efficiency_score=0.3,
                efficiency_level=HandoffEfficiency.LOW,
                success=False,
                bottlenecks=[BottleneckType.MISSING_CONTEXT],
                metadata={},
            ),
        ]

        analysis = analyze_handoff_batch(events)

        assert analysis.total_handoffs == 2
        assert analysis.successful_handoffs == 1
        assert analysis.failed_handoffs == 1
        assert analysis.success_rate == 0.5

    def test_batch_bottleneck_counting(self):
        """Verify bottleneck counts are calculated correctly."""
        timestamp = datetime.now(timezone.utc)
        events = [
            ToolContextHandoffEvent(
                handoff_id="event_1",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=50,
                information_loss_score=0.5,
                efficiency_score=0.5,
                efficiency_level=HandoffEfficiency.MEDIUM,
                success=True,
                bottlenecks=[BottleneckType.MISSING_CONTEXT],
                metadata={},
            ),
            ToolContextHandoffEvent(
                handoff_id="event_2",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=50,
                information_loss_score=0.5,
                efficiency_score=0.5,
                efficiency_level=HandoffEfficiency.MEDIUM,
                success=True,
                bottlenecks=[BottleneckType.MISSING_CONTEXT, BottleneckType.DATA_FORMAT_MISMATCH],
                metadata={},
            ),
        ]

        analysis = analyze_handoff_batch(events)

        assert analysis.bottleneck_counts["missing_context"] == 2
        assert analysis.bottleneck_counts["data_format_mismatch"] == 1

    def test_batch_average_calculations(self):
        """Verify average metrics are calculated correctly."""
        timestamp = datetime.now(timezone.utc)
        events = [
            ToolContextHandoffEvent(
                handoff_id="event_1",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=100,
                information_loss_score=0.2,
                efficiency_score=0.8,
                efficiency_level=HandoffEfficiency.HIGH,
                success=True,
                bottlenecks=[],
                metadata={},
            ),
            ToolContextHandoffEvent(
                handoff_id="event_2",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=60,
                information_loss_score=0.4,
                efficiency_score=0.6,
                efficiency_level=HandoffEfficiency.MEDIUM,
                success=True,
                bottlenecks=[],
                metadata={},
            ),
        ]

        analysis = analyze_handoff_batch(events)

        assert analysis.average_efficiency_score == pytest.approx(0.7)  # (0.8 + 0.6) / 2
        assert analysis.average_information_loss == pytest.approx(0.3)  # (0.2 + 0.4) / 2

    def test_batch_recommendations_generated(self):
        """Verify recommendations are generated."""
        timestamp = datetime.now(timezone.utc)
        events = [
            ToolContextHandoffEvent(
                handoff_id="event_1",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=50,
                information_loss_score=0.5,
                efficiency_score=0.4,
                efficiency_level=HandoffEfficiency.LOW,
                success=False,
                bottlenecks=[BottleneckType.MISSING_CONTEXT],
                metadata={},
            ),
        ]

        analysis = analyze_handoff_batch(events)

        assert len(analysis.recommendations) > 0


class TestExportHandoffEventsCSV:
    """Test CSV export of handoff events."""

    def test_csv_header(self):
        """Verify CSV includes proper header."""
        events = []
        csv_output = export_handoff_events_csv(events)

        assert "handoff_id" in csv_output
        assert "timestamp" in csv_output
        assert "source_tool" in csv_output
        assert "target_tool" in csv_output
        assert "efficiency_score" in csv_output

    def test_csv_single_event(self):
        """Verify CSV export of single event."""
        timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        event = ToolContextHandoffEvent(
            handoff_id="test_handoff",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            context_size_bytes=100,
            transferred_size_bytes=90,
            information_loss_score=0.1,
            efficiency_score=0.9,
            efficiency_level=HandoffEfficiency.HIGH,
            success=True,
            bottlenecks=[],
            metadata={},
        )

        csv_output = export_handoff_events_csv([event])
        lines = csv_output.split("\n")

        assert len(lines) == 2  # Header + 1 event
        assert "test_handoff" in lines[1]
        assert "tool_a" in lines[1]
        assert "tool_b" in lines[1]

    def test_csv_multiple_events(self):
        """Verify CSV export of multiple events."""
        timestamp = datetime.now(timezone.utc)
        events = [
            ToolContextHandoffEvent(
                handoff_id=f"handoff_{i}",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=90,
                information_loss_score=0.1,
                efficiency_score=0.9,
                efficiency_level=HandoffEfficiency.HIGH,
                success=True,
                bottlenecks=[],
                metadata={},
            )
            for i in range(3)
        ]

        csv_output = export_handoff_events_csv(events)
        lines = csv_output.split("\n")

        assert len(lines) == 4  # Header + 3 events

    def test_csv_bottlenecks_formatting(self):
        """Verify CSV formats bottlenecks correctly."""
        timestamp = datetime.now(timezone.utc)
        event = ToolContextHandoffEvent(
            handoff_id="test_handoff",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            context_size_bytes=100,
            transferred_size_bytes=50,
            information_loss_score=0.5,
            efficiency_score=0.5,
            efficiency_level=HandoffEfficiency.MEDIUM,
            success=True,
            bottlenecks=[BottleneckType.MISSING_CONTEXT, BottleneckType.DATA_FORMAT_MISMATCH],
            metadata={},
        )

        csv_output = export_handoff_events_csv([event])
        lines = csv_output.split("\n")

        # Bottlenecks should be comma-separated
        assert "missing_context" in lines[1]
        assert "data_format_mismatch" in lines[1]


class TestExportHandoffEventsJSON:
    """Test JSON export of handoff events."""

    def test_json_empty_list(self):
        """Verify JSON export of empty event list."""
        import json

        events = []
        json_output = export_handoff_events_json(events)
        parsed = json.loads(json_output)

        assert parsed == []

    def test_json_single_event(self):
        """Verify JSON export of single event."""
        import json

        timestamp = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        event = ToolContextHandoffEvent(
            handoff_id="test_handoff",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            context_size_bytes=100,
            transferred_size_bytes=90,
            information_loss_score=0.1,
            efficiency_score=0.9,
            efficiency_level=HandoffEfficiency.HIGH,
            success=True,
            bottlenecks=[],
            metadata={"key": "value"},
        )

        json_output = export_handoff_events_json([event])
        parsed = json.loads(json_output)

        assert len(parsed) == 1
        assert parsed[0]["handoff_id"] == "test_handoff"
        assert parsed[0]["source_tool"] == "tool_a"
        assert parsed[0]["target_tool"] == "tool_b"
        assert parsed[0]["efficiency_score"] == 0.9

    def test_json_includes_metadata(self):
        """Verify JSON export includes event metadata."""
        import json

        timestamp = datetime.now(timezone.utc)
        event = ToolContextHandoffEvent(
            handoff_id="test_handoff",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            context_size_bytes=100,
            transferred_size_bytes=90,
            information_loss_score=0.1,
            efficiency_score=0.9,
            efficiency_level=HandoffEfficiency.HIGH,
            success=True,
            bottlenecks=[],
            metadata={"context_retention_ratio": 0.95},
        )

        json_output = export_handoff_events_json([event])
        parsed = json.loads(json_output)

        assert "metadata" in parsed[0]
        assert parsed[0]["metadata"]["context_retention_ratio"] == 0.95

    def test_json_sorted_keys(self):
        """Verify JSON output has sorted keys for determinism."""
        timestamp = datetime.now(timezone.utc)
        event = ToolContextHandoffEvent(
            handoff_id="test_handoff",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            context_size_bytes=100,
            transferred_size_bytes=90,
            information_loss_score=0.1,
            efficiency_score=0.9,
            efficiency_level=HandoffEfficiency.HIGH,
            success=True,
            bottlenecks=[],
            metadata={},
        )

        json_output = export_handoff_events_json([event])

        # Verify keys are in sorted order
        assert '"bottlenecks"' in json_output
        assert '"efficiency_score"' in json_output
        bottlenecks_pos = json_output.index('"bottlenecks"')
        efficiency_pos = json_output.index('"efficiency_score"')
        assert bottlenecks_pos < efficiency_pos


class TestToolContextHandoffEventDataclass:
    """Test ToolContextHandoffEvent dataclass."""

    def test_event_frozen(self):
        """Verify event is immutable."""
        timestamp = datetime.now(timezone.utc)
        event = ToolContextHandoffEvent(
            handoff_id="test",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            context_size_bytes=100,
            transferred_size_bytes=90,
            information_loss_score=0.1,
            efficiency_score=0.9,
            efficiency_level=HandoffEfficiency.HIGH,
            success=True,
            bottlenecks=[],
            metadata={},
        )

        with pytest.raises(AttributeError):
            event.efficiency_score = 0.5

    def test_event_all_fields_present(self):
        """Verify event has all required fields."""
        timestamp = datetime.now(timezone.utc)
        event = ToolContextHandoffEvent(
            handoff_id="test_handoff",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            context_size_bytes=100,
            transferred_size_bytes=90,
            information_loss_score=0.1,
            efficiency_score=0.9,
            efficiency_level=HandoffEfficiency.HIGH,
            success=True,
            bottlenecks=[BottleneckType.MISSING_CONTEXT],
            metadata={"key": "value"},
        )

        assert event.handoff_id == "test_handoff"
        assert event.timestamp == timestamp
        assert event.source_tool == "tool_a"
        assert event.target_tool == "tool_b"
        assert event.context_size_bytes == 100
        assert event.transferred_size_bytes == 90
        assert event.information_loss_score == 0.1
        assert event.efficiency_score == 0.9
        assert event.efficiency_level == HandoffEfficiency.HIGH
        assert event.success is True
        assert len(event.bottlenecks) == 1
        assert event.metadata == {"key": "value"}


class TestToolContextHandoffAnalysisDataclass:
    """Test ToolContextHandoffAnalysis dataclass."""

    def test_analysis_frozen(self):
        """Verify analysis is immutable."""
        timestamp = datetime.now(timezone.utc)
        analysis = ToolContextHandoffAnalysis(
            analyzed_at=timestamp,
            total_handoffs=10,
            successful_handoffs=8,
            failed_handoffs=2,
            success_rate=0.8,
            average_efficiency_score=0.75,
            average_information_loss=0.25,
            handoff_events=[],
            bottleneck_counts={},
            recommendations=[],
        )

        with pytest.raises(AttributeError):
            analysis.success_rate = 0.9

    def test_analysis_all_fields_present(self):
        """Verify analysis has all required fields."""
        timestamp = datetime.now(timezone.utc)
        analysis = ToolContextHandoffAnalysis(
            analyzed_at=timestamp,
            total_handoffs=10,
            successful_handoffs=8,
            failed_handoffs=2,
            success_rate=0.8,
            average_efficiency_score=0.75,
            average_information_loss=0.25,
            handoff_events=[],
            bottleneck_counts={"missing_context": 3},
            recommendations=["Recommendation 1", "Recommendation 2"],
        )

        assert analysis.analyzed_at == timestamp
        assert analysis.total_handoffs == 10
        assert analysis.successful_handoffs == 8
        assert analysis.failed_handoffs == 2
        assert analysis.success_rate == 0.8
        assert analysis.average_efficiency_score == 0.75
        assert analysis.average_information_loss == 0.25
        assert len(analysis.bottleneck_counts) == 1
        assert len(analysis.recommendations) == 2


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_measure_loss_empty_source_and_transferred(self):
        """Verify handling of both contexts being empty."""
        loss = measure_information_loss({}, {})
        assert loss == 0.0

    def test_identify_bottlenecks_empty_contexts(self):
        """Verify handling of empty contexts."""
        bottlenecks = identify_bottlenecks({}, {}, "tool1", "tool2")
        assert bottlenecks == []

    def test_analyze_handoff_empty_contexts(self):
        """Verify analysis handles empty contexts."""
        timestamp = datetime.now(timezone.utc)
        event = analyze_tool_handoff(
            handoff_id="test",
            timestamp=timestamp,
            source_tool="tool_a",
            target_tool="tool_b",
            source_context={},
            transferred_context={},
        )

        assert event.information_loss_score == 0.0
        assert event.success is True

    def test_categorize_efficiency_edge_values(self):
        """Verify efficiency categorization at exact boundary values."""
        assert categorize_efficiency(0.0) == HandoffEfficiency.LOW
        assert categorize_efficiency(0.5) == HandoffEfficiency.MEDIUM
        assert categorize_efficiency(0.8) == HandoffEfficiency.HIGH
        assert categorize_efficiency(1.0) == HandoffEfficiency.HIGH


class TestRecommendations:
    """Test recommendation generation."""

    def test_low_success_rate_recommendation(self):
        """Verify recommendation generated for low success rate."""
        timestamp = datetime.now(timezone.utc)
        events = [
            ToolContextHandoffEvent(
                handoff_id=f"handoff_{i}",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=50,
                information_loss_score=0.5,
                efficiency_score=0.4,
                efficiency_level=HandoffEfficiency.LOW,
                success=False,
                bottlenecks=[],
                metadata={},
            )
            for i in range(10)
        ]

        analysis = analyze_handoff_batch(events)

        recommendations_text = " ".join(analysis.recommendations).lower()
        assert "success rate" in recommendations_text or "low" in recommendations_text

    def test_high_information_loss_recommendation(self):
        """Verify recommendation generated for high information loss."""
        timestamp = datetime.now(timezone.utc)
        events = [
            ToolContextHandoffEvent(
                handoff_id="handoff_1",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=80,
                information_loss_score=0.6,  # High loss
                efficiency_score=0.5,
                efficiency_level=HandoffEfficiency.MEDIUM,
                success=True,
                bottlenecks=[],
                metadata={},
            )
        ]

        analysis = analyze_handoff_batch(events)

        recommendations_text = " ".join(analysis.recommendations).lower()
        assert "information loss" in recommendations_text or "preservation" in recommendations_text

    def test_bottleneck_specific_recommendations(self):
        """Verify bottleneck-specific recommendations are generated."""
        timestamp = datetime.now(timezone.utc)
        events = [
            ToolContextHandoffEvent(
                handoff_id=f"handoff_{i}",
                timestamp=timestamp,
                source_tool="tool_a",
                target_tool="tool_b",
                context_size_bytes=100,
                transferred_size_bytes=50,
                information_loss_score=0.5,
                efficiency_score=0.5,
                efficiency_level=HandoffEfficiency.MEDIUM,
                success=True,
                bottlenecks=[BottleneckType.MISSING_CONTEXT],
                metadata={},
            )
            for i in range(10)
        ]

        analysis = analyze_handoff_batch(events)

        recommendations_text = " ".join(analysis.recommendations).lower()
        assert "missing context" in recommendations_text or "context" in recommendations_text
