"""Tool context handoff efficiency analyzer for workflow optimization.

Analyzes tool context handoff efficiency by measuring:
- Information loss during tool transitions
- Context propagation bottlenecks
- Handoff success rates

Exports handoff events with efficiency scores and optimization recommendations
for workflow improvement.

Efficiency scoring:
- high (0.8-1.0): Minimal information loss, smooth handoffs
- medium (0.5-0.79): Moderate loss, some inefficiencies
- low (0.0-0.49): Significant loss, poor context propagation
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class HandoffEfficiency(str, Enum):
    """Efficiency levels for tool context handoffs."""

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class BottleneckType(str, Enum):
    """Types of context propagation bottlenecks."""

    DATA_FORMAT_MISMATCH = "data_format_mismatch"
    MISSING_CONTEXT = "missing_context"
    INCOMPLETE_TRANSFER = "incomplete_transfer"
    SCHEMA_INCOMPATIBILITY = "schema_incompatibility"


# Efficiency thresholds (0.0-1.0 scale)
EFFICIENCY_HIGH_THRESHOLD = 0.8
EFFICIENCY_MEDIUM_THRESHOLD = 0.5

# Handoff success criteria
MIN_CONTEXT_RETENTION = 0.7  # Minimum context retention for successful handoff
MIN_DATA_COMPLETENESS = 0.8  # Minimum data completeness


@dataclass(frozen=True)
class ToolContextHandoffEvent:
    """A tool context handoff event."""

    handoff_id: str
    timestamp: datetime
    source_tool: str
    target_tool: str
    context_size_bytes: int
    transferred_size_bytes: int
    information_loss_score: float  # 0.0-1.0 (0.0 = no loss)
    efficiency_score: float  # 0.0-1.0 (1.0 = perfect efficiency)
    efficiency_level: HandoffEfficiency
    success: bool
    bottlenecks: list[BottleneckType]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class ToolContextHandoffAnalysis:
    """Analysis of tool context handoff efficiency."""

    analyzed_at: datetime
    total_handoffs: int
    successful_handoffs: int
    failed_handoffs: int
    success_rate: float  # 0.0-1.0
    average_efficiency_score: float
    average_information_loss: float
    handoff_events: list[ToolContextHandoffEvent]
    bottleneck_counts: dict[str, int]
    recommendations: list[str]


def measure_information_loss(
    source_context: dict[str, Any],
    transferred_context: dict[str, Any],
) -> float:
    """Measure information loss during tool transition.

    Args:
        source_context: Original context from source tool
        transferred_context: Context received by target tool

    Returns:
        Information loss score (0.0-1.0, where 0.0 = no loss)
    """
    if not source_context:
        return 0.0  # No context to lose

    # Calculate key retention
    source_keys = set(source_context.keys())
    transferred_keys = set(transferred_context.keys())

    if not source_keys:
        return 0.0

    retained_keys = source_keys & transferred_keys
    retention_ratio = len(retained_keys) / len(source_keys)

    # Calculate value preservation for retained keys
    value_preservation = 0.0
    for key in retained_keys:
        source_val = source_context[key]
        transferred_val = transferred_context[key]

        if source_val == transferred_val:
            value_preservation += 1.0
        elif isinstance(source_val, (int, float)) and isinstance(
            transferred_val, (int, float)
        ):
            # Numeric values: check if approximately equal
            if abs(source_val - transferred_val) < 0.01:
                value_preservation += 1.0
            else:
                value_preservation += 0.5
        else:
            # Partial match
            value_preservation += 0.5

    avg_value_preservation = (
        value_preservation / len(retained_keys) if retained_keys else 0.0
    )

    # Combined score: weighted average of key retention and value preservation
    combined_retention = (retention_ratio * 0.6) + (avg_value_preservation * 0.4)

    # Information loss is inverse of retention
    information_loss = 1.0 - combined_retention

    return information_loss


def identify_bottlenecks(
    source_context: dict[str, Any],
    transferred_context: dict[str, Any],
    source_tool: str,
    target_tool: str,
) -> list[BottleneckType]:
    """Identify context propagation bottlenecks.

    Args:
        source_context: Original context from source tool
        transferred_context: Context received by target tool
        source_tool: Name of source tool
        target_tool: Name of target tool

    Returns:
        List of detected bottleneck types
    """
    bottlenecks = []

    # Check for missing context
    source_keys = set(source_context.keys())
    transferred_keys = set(transferred_context.keys())
    missing_keys = source_keys - transferred_keys

    if missing_keys:
        if len(missing_keys) > len(source_keys) * 0.3:
            bottlenecks.append(BottleneckType.MISSING_CONTEXT)

    # Check for incomplete transfer
    for key in transferred_keys:
        source_val = source_context.get(key)
        transferred_val = transferred_context.get(key)

        if source_val and transferred_val:
            if isinstance(source_val, (list, dict)) and isinstance(
                transferred_val, (list, dict)
            ):
                # Check if collection was truncated
                if len(str(transferred_val)) < len(str(source_val)) * 0.5:
                    bottlenecks.append(BottleneckType.INCOMPLETE_TRANSFER)
                    break

    # Check for data format mismatches
    for key in transferred_keys:
        source_val = source_context.get(key)
        transferred_val = transferred_context.get(key)

        if source_val is not None and transferred_val is not None:
            if type(source_val) != type(transferred_val):
                bottlenecks.append(BottleneckType.DATA_FORMAT_MISMATCH)
                break

    # Check for schema incompatibility (simplified)
    if "schema" in source_context and "schema" in transferred_context:
        if source_context["schema"] != transferred_context["schema"]:
            bottlenecks.append(BottleneckType.SCHEMA_INCOMPATIBILITY)

    return bottlenecks


def calculate_handoff_efficiency(
    information_loss: float,
    context_retention_ratio: float,
    data_completeness: float,
) -> float:
    """Calculate overall handoff efficiency score.

    Args:
        information_loss: Information loss score (0.0-1.0)
        context_retention_ratio: Ratio of context retained (0.0-1.0)
        data_completeness: Data completeness score (0.0-1.0)

    Returns:
        Efficiency score (0.0-1.0, where 1.0 = perfect efficiency)
    """
    # Efficiency is inverse of information loss, adjusted by retention and completeness
    base_efficiency = 1.0 - information_loss

    # Weight factors
    efficiency_score = (
        base_efficiency * 0.5 + context_retention_ratio * 0.3 + data_completeness * 0.2
    )

    return min(1.0, max(0.0, efficiency_score))


def categorize_efficiency(efficiency_score: float) -> HandoffEfficiency:
    """Categorize efficiency score into level.

    Args:
        efficiency_score: Normalized efficiency score (0.0-1.0)

    Returns:
        HandoffEfficiency enum value
    """
    if efficiency_score >= EFFICIENCY_HIGH_THRESHOLD:
        return HandoffEfficiency.HIGH
    elif efficiency_score >= EFFICIENCY_MEDIUM_THRESHOLD:
        return HandoffEfficiency.MEDIUM
    else:
        return HandoffEfficiency.LOW


def analyze_tool_handoff(
    handoff_id: str,
    timestamp: datetime,
    source_tool: str,
    target_tool: str,
    source_context: dict[str, Any],
    transferred_context: dict[str, Any],
) -> ToolContextHandoffEvent:
    """Analyze a single tool context handoff.

    Args:
        handoff_id: Unique handoff identifier
        timestamp: When the handoff occurred
        source_tool: Name of source tool
        target_tool: Name of target tool
        source_context: Original context from source tool
        transferred_context: Context received by target tool

    Returns:
        ToolContextHandoffEvent with analysis results
    """
    # Calculate context sizes
    import json

    context_size = len(json.dumps(source_context))
    transferred_size = len(json.dumps(transferred_context))

    # Measure information loss
    info_loss = measure_information_loss(source_context, transferred_context)

    # Calculate context retention
    source_keys = set(source_context.keys())
    transferred_keys = set(transferred_context.keys())
    context_retention = (
        len(transferred_keys & source_keys) / len(source_keys)
        if source_keys
        else 1.0
    )

    # Calculate data completeness
    data_completeness = (
        transferred_size / context_size if context_size > 0 else 1.0
    )

    # Calculate efficiency
    efficiency = calculate_handoff_efficiency(
        info_loss, context_retention, data_completeness
    )

    # Categorize efficiency
    efficiency_level = categorize_efficiency(efficiency)

    # Determine success
    success = (
        context_retention >= MIN_CONTEXT_RETENTION
        and data_completeness >= MIN_DATA_COMPLETENESS
    )

    # Identify bottlenecks
    bottlenecks = identify_bottlenecks(
        source_context, transferred_context, source_tool, target_tool
    )

    return ToolContextHandoffEvent(
        handoff_id=handoff_id,
        timestamp=timestamp,
        source_tool=source_tool,
        target_tool=target_tool,
        context_size_bytes=context_size,
        transferred_size_bytes=transferred_size,
        information_loss_score=info_loss,
        efficiency_score=efficiency,
        efficiency_level=efficiency_level,
        success=success,
        bottlenecks=bottlenecks,
        metadata={
            "context_retention_ratio": context_retention,
            "data_completeness": data_completeness,
        },
    )


def analyze_handoff_batch(
    handoff_events: list[ToolContextHandoffEvent],
) -> ToolContextHandoffAnalysis:
    """Analyze a batch of tool context handoffs.

    Args:
        handoff_events: List of handoff events to analyze

    Returns:
        ToolContextHandoffAnalysis with aggregate metrics and recommendations
    """
    analyzed_at = datetime.now(timezone.utc)
    total_handoffs = len(handoff_events)

    if total_handoffs == 0:
        return ToolContextHandoffAnalysis(
            analyzed_at=analyzed_at,
            total_handoffs=0,
            successful_handoffs=0,
            failed_handoffs=0,
            success_rate=0.0,
            average_efficiency_score=0.0,
            average_information_loss=0.0,
            handoff_events=[],
            bottleneck_counts={},
            recommendations=[],
        )

    # Count successes and failures
    successful = sum(1 for e in handoff_events if e.success)
    failed = total_handoffs - successful
    success_rate = successful / total_handoffs

    # Calculate averages
    avg_efficiency = sum(e.efficiency_score for e in handoff_events) / total_handoffs
    avg_info_loss = (
        sum(e.information_loss_score for e in handoff_events) / total_handoffs
    )

    # Count bottlenecks
    bottleneck_counts: dict[str, int] = {}
    for event in handoff_events:
        for bottleneck in event.bottlenecks:
            bottleneck_counts[bottleneck.value] = (
                bottleneck_counts.get(bottleneck.value, 0) + 1
            )

    # Generate recommendations
    recommendations = _generate_recommendations(
        success_rate=success_rate,
        avg_efficiency=avg_efficiency,
        avg_info_loss=avg_info_loss,
        bottleneck_counts=bottleneck_counts,
        total_handoffs=total_handoffs,
    )

    return ToolContextHandoffAnalysis(
        analyzed_at=analyzed_at,
        total_handoffs=total_handoffs,
        successful_handoffs=successful,
        failed_handoffs=failed,
        success_rate=success_rate,
        average_efficiency_score=avg_efficiency,
        average_information_loss=avg_info_loss,
        handoff_events=handoff_events,
        bottleneck_counts=bottleneck_counts,
        recommendations=recommendations,
    )


def export_handoff_events_csv(
    handoff_events: list[ToolContextHandoffEvent],
) -> str:
    """Export handoff events to CSV format.

    Args:
        handoff_events: List of handoff events to export

    Returns:
        CSV-formatted string with header and event rows
    """
    import csv
    import io

    buffer = io.StringIO()
    fieldnames = [
        "handoff_id",
        "timestamp",
        "source_tool",
        "target_tool",
        "context_size_bytes",
        "transferred_size_bytes",
        "information_loss_score",
        "efficiency_score",
        "efficiency_level",
        "success",
        "bottlenecks",
    ]

    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()

    for event in handoff_events:
        writer.writerow(
            {
                "handoff_id": event.handoff_id,
                "timestamp": event.timestamp.isoformat(),
                "source_tool": event.source_tool,
                "target_tool": event.target_tool,
                "context_size_bytes": event.context_size_bytes,
                "transferred_size_bytes": event.transferred_size_bytes,
                "information_loss_score": f"{event.information_loss_score:.3f}",
                "efficiency_score": f"{event.efficiency_score:.3f}",
                "efficiency_level": event.efficiency_level.value,
                "success": str(event.success),
                "bottlenecks": ",".join(b.value for b in event.bottlenecks),
            }
        )

    return buffer.getvalue().rstrip("\r\n")


def export_handoff_events_json(
    handoff_events: list[ToolContextHandoffEvent],
) -> str:
    """Export handoff events to JSON format.

    Args:
        handoff_events: List of handoff events to export

    Returns:
        JSON-formatted string
    """
    import json

    events_data = []
    for event in handoff_events:
        events_data.append(
            {
                "handoff_id": event.handoff_id,
                "timestamp": event.timestamp.isoformat(),
                "source_tool": event.source_tool,
                "target_tool": event.target_tool,
                "context_size_bytes": event.context_size_bytes,
                "transferred_size_bytes": event.transferred_size_bytes,
                "information_loss_score": event.information_loss_score,
                "efficiency_score": event.efficiency_score,
                "efficiency_level": event.efficiency_level.value,
                "success": event.success,
                "bottlenecks": [b.value for b in event.bottlenecks],
                "metadata": event.metadata,
            }
        )

    return json.dumps(events_data, indent=2, sort_keys=True)


def _generate_recommendations(
    success_rate: float,
    avg_efficiency: float,
    avg_info_loss: float,
    bottleneck_counts: dict[str, int],
    total_handoffs: int,
) -> list[str]:
    """Generate optimization recommendations.

    Args:
        success_rate: Overall success rate
        avg_efficiency: Average efficiency score
        avg_info_loss: Average information loss
        bottleneck_counts: Counts of each bottleneck type
        total_handoffs: Total number of handoffs

    Returns:
        List of recommendation strings
    """
    recommendations = []

    # Success rate recommendations
    if success_rate < 0.7:
        recommendations.append(
            f"Low success rate ({success_rate:.1%}) - review handoff protocols and context requirements"
        )
    elif success_rate < 0.9:
        recommendations.append(
            f"Moderate success rate ({success_rate:.1%}) - investigate failed handoffs for patterns"
        )

    # Efficiency recommendations
    if avg_efficiency < EFFICIENCY_MEDIUM_THRESHOLD:
        recommendations.append(
            f"Low average efficiency ({avg_efficiency:.2f}) - significant optimization potential"
        )
    elif avg_efficiency < EFFICIENCY_HIGH_THRESHOLD:
        recommendations.append(
            f"Medium efficiency ({avg_efficiency:.2f}) - consider context optimization strategies"
        )

    # Information loss recommendations
    if avg_info_loss > 0.3:
        recommendations.append(
            f"High information loss ({avg_info_loss:.2f}) - implement better context preservation"
        )

    # Bottleneck-specific recommendations
    for bottleneck_type, count in sorted(
        bottleneck_counts.items(), key=lambda x: x[1], reverse=True
    ):
        if count > total_handoffs * 0.2:  # Affects >20% of handoffs
            if bottleneck_type == BottleneckType.MISSING_CONTEXT.value:
                recommendations.append(
                    f"Missing context detected in {count} handoffs - ensure complete context capture"
                )
            elif bottleneck_type == BottleneckType.DATA_FORMAT_MISMATCH.value:
                recommendations.append(
                    f"Data format mismatches in {count} handoffs - standardize data formats"
                )
            elif bottleneck_type == BottleneckType.INCOMPLETE_TRANSFER.value:
                recommendations.append(
                    f"Incomplete transfers in {count} handoffs - check transfer mechanisms"
                )
            elif bottleneck_type == BottleneckType.SCHEMA_INCOMPATIBILITY.value:
                recommendations.append(
                    f"Schema incompatibilities in {count} handoffs - align schemas across tools"
                )

    # General optimization recommendations
    if avg_efficiency >= EFFICIENCY_HIGH_THRESHOLD:
        recommendations.append(
            f"Excellent efficiency ({avg_efficiency:.2f}) - handoff workflow is well-optimized"
        )

    return recommendations
