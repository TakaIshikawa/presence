"""Session coherence breakdown detection for conversation quality tracking.

Detects coherence breakdowns in Claude sessions by identifying:
- Topic shifts: Changes in conversation focus or domain
- Context loss events: Indicators of lost conversation context
- Conversation fragmentation: Discontinuities in conversation flow

Exports breakdown events with timestamps, severity scores, and recovery patterns
for session quality analysis.

Severity scoring:
- low (0.0-0.33): Minor shifts, natural topic evolution
- medium (0.34-0.66): Noticeable breaks, partial context loss
- high (0.67-1.0): Major disruptions, complete context loss
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class BreakdownType(str, Enum):
    """Types of coherence breakdowns."""

    TOPIC_SHIFT = "topic_shift"
    CONTEXT_LOSS = "context_loss"
    FRAGMENTATION = "fragmentation"


class SeverityLevel(str, Enum):
    """Severity levels for coherence breakdowns."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class RecoveryPattern(str, Enum):
    """Patterns of recovery from coherence breakdowns."""

    NONE = "none"  # No recovery observed
    IMMEDIATE = "immediate"  # Recovered within 1-2 turns
    GRADUAL = "gradual"  # Recovered over 3-5 turns
    COMPLETE_RESTART = "complete_restart"  # Session restarted from scratch


# Severity thresholds (0.0-1.0 scale)
SEVERITY_MEDIUM_THRESHOLD = 0.34
SEVERITY_HIGH_THRESHOLD = 0.67

# Topic shift detection thresholds
MIN_TOPIC_OVERLAP = 0.3  # Minimum overlap to avoid topic shift detection
MIN_CONTEXT_SIMILARITY = 0.4  # Minimum similarity to avoid context loss

# Fragmentation detection
MAX_TURN_GAP_MINUTES = 30.0  # Gap indicating potential fragmentation
MIN_TURNS_FOR_FRAGMENTATION = 3  # Minimum turns to detect fragmentation


@dataclass(frozen=True)
class CoherenceBreakdownEvent:
    """A detected coherence breakdown event."""

    event_id: str
    timestamp: datetime
    breakdown_type: BreakdownType
    severity_score: float  # 0.0-1.0
    severity_level: SeverityLevel
    description: str
    context: dict[str, Any]  # Additional context (turn numbers, topics, etc.)
    recovery_pattern: RecoveryPattern
    recovery_time_minutes: float | None  # Time to recover, if recovered


@dataclass(frozen=True)
class SessionCoherenceAnalysis:
    """Analysis of session coherence breakdowns."""

    session_id: str
    analyzed_at: datetime
    total_turns: int
    breakdown_events: list[CoherenceBreakdownEvent]
    overall_coherence_score: float  # 0.0-1.0 (1.0 = perfect coherence)
    fragmentation_count: int
    topic_shift_count: int
    context_loss_count: int
    average_recovery_time_minutes: float | None
    insights: list[str]


def detect_topic_shift(
    previous_context: dict[str, Any],
    current_context: dict[str, Any],
    timestamp: datetime,
) -> CoherenceBreakdownEvent | None:
    """Detect topic shifts between conversation turns.

    Args:
        previous_context: Context from previous turn (topics, entities, intent)
        current_context: Context from current turn
        timestamp: Timestamp of the current turn

    Returns:
        CoherenceBreakdownEvent if topic shift detected, None otherwise
    """
    prev_topics = set(previous_context.get("topics", []))
    curr_topics = set(current_context.get("topics", []))

    if not prev_topics or not curr_topics:
        return None

    # Calculate topic overlap
    overlap = len(prev_topics & curr_topics)
    total = len(prev_topics | curr_topics)
    overlap_ratio = overlap / total if total > 0 else 1.0

    if overlap_ratio >= MIN_TOPIC_OVERLAP:
        return None  # Sufficient overlap, no shift

    # Calculate severity based on overlap
    severity_score = 1.0 - overlap_ratio
    severity_level = categorize_severity(severity_score)

    # Detect recovery pattern (simplified - would need more turns in practice)
    recovery_pattern = RecoveryPattern.NONE
    recovery_time = None

    event_id = f"topic_shift_{timestamp.timestamp()}"

    return CoherenceBreakdownEvent(
        event_id=event_id,
        timestamp=timestamp,
        breakdown_type=BreakdownType.TOPIC_SHIFT,
        severity_score=severity_score,
        severity_level=severity_level,
        description=f"Topic shift detected: {', '.join(prev_topics)} → {', '.join(curr_topics)}",
        context={
            "previous_topics": list(prev_topics),
            "current_topics": list(curr_topics),
            "overlap_ratio": overlap_ratio,
        },
        recovery_pattern=recovery_pattern,
        recovery_time_minutes=recovery_time,
    )


def detect_context_loss(
    expected_context: dict[str, Any],
    actual_context: dict[str, Any],
    timestamp: datetime,
) -> CoherenceBreakdownEvent | None:
    """Detect context loss events.

    Args:
        expected_context: Expected context based on conversation history
        actual_context: Actual context in the current turn
        timestamp: Timestamp of the current turn

    Returns:
        CoherenceBreakdownEvent if context loss detected, None otherwise
    """
    expected_refs = set(expected_context.get("references", []))
    actual_refs = set(actual_context.get("references", []))

    if not expected_refs:
        return None  # No context expected

    # Calculate context retention
    retained = len(expected_refs & actual_refs)
    expected_count = len(expected_refs)
    retention_ratio = retained / expected_count if expected_count > 0 else 1.0

    if retention_ratio >= MIN_CONTEXT_SIMILARITY:
        return None  # Sufficient context retained

    # Calculate severity
    severity_score = 1.0 - retention_ratio
    severity_level = categorize_severity(severity_score)

    event_id = f"context_loss_{timestamp.timestamp()}"

    return CoherenceBreakdownEvent(
        event_id=event_id,
        timestamp=timestamp,
        breakdown_type=BreakdownType.CONTEXT_LOSS,
        severity_score=severity_score,
        severity_level=severity_level,
        description=f"Context loss: {retained}/{expected_count} references retained",
        context={
            "expected_references": list(expected_refs),
            "actual_references": list(actual_refs),
            "retention_ratio": retention_ratio,
        },
        recovery_pattern=RecoveryPattern.NONE,
        recovery_time_minutes=None,
    )


def detect_fragmentation(
    turns: list[dict[str, Any]],
    timestamp: datetime,
) -> CoherenceBreakdownEvent | None:
    """Detect conversation fragmentation from turn patterns.

    Args:
        turns: List of recent turns with timestamps
        timestamp: Current timestamp

    Returns:
        CoherenceBreakdownEvent if fragmentation detected, None otherwise
    """
    if len(turns) < MIN_TURNS_FOR_FRAGMENTATION:
        return None

    # Calculate time gaps between turns
    gaps_minutes = []
    for i in range(1, len(turns)):
        prev_time = turns[i - 1].get("timestamp")
        curr_time = turns[i].get("timestamp")

        if prev_time and curr_time:
            if isinstance(prev_time, str):
                prev_time = datetime.fromisoformat(prev_time.replace("Z", "+00:00"))
            if isinstance(curr_time, str):
                curr_time = datetime.fromisoformat(curr_time.replace("Z", "+00:00"))

            gap = (curr_time - prev_time).total_seconds() / 60.0
            gaps_minutes.append(gap)

    if not gaps_minutes:
        return None

    # Detect significant gaps
    max_gap = max(gaps_minutes)
    avg_gap = sum(gaps_minutes) / len(gaps_minutes)

    if max_gap < MAX_TURN_GAP_MINUTES:
        return None  # No significant gaps

    # Calculate severity based on gap magnitude
    gap_ratio = max_gap / MAX_TURN_GAP_MINUTES
    severity_score = min(1.0, gap_ratio / 3.0)  # Cap at high severity
    severity_level = categorize_severity(severity_score)

    event_id = f"fragmentation_{timestamp.timestamp()}"

    return CoherenceBreakdownEvent(
        event_id=event_id,
        timestamp=timestamp,
        breakdown_type=BreakdownType.FRAGMENTATION,
        severity_score=severity_score,
        severity_level=severity_level,
        description=f"Conversation fragmentation: {max_gap:.1f}min gap detected",
        context={
            "max_gap_minutes": max_gap,
            "avg_gap_minutes": avg_gap,
            "turn_count": len(turns),
        },
        recovery_pattern=RecoveryPattern.NONE,
        recovery_time_minutes=None,
    )


def analyze_session_coherence(
    session_id: str,
    turns: list[dict[str, Any]],
    breakdown_events: list[CoherenceBreakdownEvent],
) -> SessionCoherenceAnalysis:
    """Analyze overall session coherence.

    Args:
        session_id: Unique session identifier
        turns: All turns in the session
        breakdown_events: Detected breakdown events

    Returns:
        SessionCoherenceAnalysis with overall metrics and insights
    """
    analyzed_at = datetime.now(timezone.utc)

    # Count breakdowns by type
    fragmentation_count = sum(
        1 for e in breakdown_events if e.breakdown_type == BreakdownType.FRAGMENTATION
    )
    topic_shift_count = sum(
        1 for e in breakdown_events if e.breakdown_type == BreakdownType.TOPIC_SHIFT
    )
    context_loss_count = sum(
        1 for e in breakdown_events if e.breakdown_type == BreakdownType.CONTEXT_LOSS
    )

    # Calculate overall coherence score
    total_turns = len(turns)
    if total_turns == 0:
        overall_coherence_score = 1.0
    else:
        # More breakdowns = lower coherence
        breakdown_penalty = len(breakdown_events) / total_turns
        overall_coherence_score = max(0.0, 1.0 - breakdown_penalty)

    # Calculate average recovery time
    recovery_times = [
        e.recovery_time_minutes
        for e in breakdown_events
        if e.recovery_time_minutes is not None
    ]
    avg_recovery_time = (
        sum(recovery_times) / len(recovery_times) if recovery_times else None
    )

    # Generate insights
    insights = _generate_coherence_insights(
        total_turns=total_turns,
        breakdown_events=breakdown_events,
        overall_coherence_score=overall_coherence_score,
        fragmentation_count=fragmentation_count,
        topic_shift_count=topic_shift_count,
        context_loss_count=context_loss_count,
    )

    return SessionCoherenceAnalysis(
        session_id=session_id,
        analyzed_at=analyzed_at,
        total_turns=total_turns,
        breakdown_events=breakdown_events,
        overall_coherence_score=overall_coherence_score,
        fragmentation_count=fragmentation_count,
        topic_shift_count=topic_shift_count,
        context_loss_count=context_loss_count,
        average_recovery_time_minutes=avg_recovery_time,
        insights=insights,
    )


def categorize_severity(severity_score: float) -> SeverityLevel:
    """Categorize severity score into level.

    Args:
        severity_score: Normalized severity score (0.0-1.0)

    Returns:
        SeverityLevel enum value
    """
    if severity_score < SEVERITY_MEDIUM_THRESHOLD:
        return SeverityLevel.LOW
    elif severity_score < SEVERITY_HIGH_THRESHOLD:
        return SeverityLevel.MEDIUM
    else:
        return SeverityLevel.HIGH


def export_breakdown_events_csv(
    breakdown_events: list[CoherenceBreakdownEvent],
) -> str:
    """Export breakdown events to CSV format.

    Args:
        breakdown_events: List of breakdown events to export

    Returns:
        CSV-formatted string with header and event rows
    """
    import csv
    import io

    buffer = io.StringIO()
    fieldnames = [
        "event_id",
        "timestamp",
        "breakdown_type",
        "severity_score",
        "severity_level",
        "description",
        "recovery_pattern",
        "recovery_time_minutes",
    ]

    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()

    for event in breakdown_events:
        writer.writerow(
            {
                "event_id": event.event_id,
                "timestamp": event.timestamp.isoformat(),
                "breakdown_type": event.breakdown_type.value,
                "severity_score": f"{event.severity_score:.3f}",
                "severity_level": event.severity_level.value,
                "description": event.description,
                "recovery_pattern": event.recovery_pattern.value,
                "recovery_time_minutes": (
                    f"{event.recovery_time_minutes:.1f}"
                    if event.recovery_time_minutes is not None
                    else ""
                ),
            }
        )

    return buffer.getvalue().rstrip("\r\n")


def export_breakdown_events_json(
    breakdown_events: list[CoherenceBreakdownEvent],
) -> str:
    """Export breakdown events to JSON format.

    Args:
        breakdown_events: List of breakdown events to export

    Returns:
        JSON-formatted string
    """
    import json

    events_data = []
    for event in breakdown_events:
        events_data.append(
            {
                "event_id": event.event_id,
                "timestamp": event.timestamp.isoformat(),
                "breakdown_type": event.breakdown_type.value,
                "severity_score": event.severity_score,
                "severity_level": event.severity_level.value,
                "description": event.description,
                "context": event.context,
                "recovery_pattern": event.recovery_pattern.value,
                "recovery_time_minutes": event.recovery_time_minutes,
            }
        )

    return json.dumps(events_data, indent=2, sort_keys=True)


def _generate_coherence_insights(
    total_turns: int,
    breakdown_events: list[CoherenceBreakdownEvent],
    overall_coherence_score: float,
    fragmentation_count: int,
    topic_shift_count: int,
    context_loss_count: int,
) -> list[str]:
    """Generate actionable insights about session coherence.

    Args:
        total_turns: Total number of turns in session
        breakdown_events: All detected breakdown events
        overall_coherence_score: Overall coherence score
        fragmentation_count: Number of fragmentation events
        topic_shift_count: Number of topic shifts
        context_loss_count: Number of context loss events

    Returns:
        List of insight strings
    """
    insights = []

    # Overall coherence assessment
    if overall_coherence_score >= 0.9:
        insights.append(
            f"Excellent session coherence ({overall_coherence_score:.2f}) - conversation maintained clear focus"
        )
    elif overall_coherence_score >= 0.7:
        insights.append(
            f"Good session coherence ({overall_coherence_score:.2f}) - minor interruptions detected"
        )
    elif overall_coherence_score >= 0.5:
        insights.append(
            f"Moderate coherence ({overall_coherence_score:.2f}) - frequent breakdowns may impact quality"
        )
    else:
        insights.append(
            f"Low coherence ({overall_coherence_score:.2f}) - significant conversation fragmentation"
        )

    # Breakdown type insights
    if topic_shift_count > 0:
        avg_shifts_per_10_turns = (topic_shift_count / total_turns * 10) if total_turns > 0 else 0
        insights.append(
            f"{topic_shift_count} topic shift(s) detected "
            f"({avg_shifts_per_10_turns:.1f} per 10 turns)"
        )

    if context_loss_count > 0:
        insights.append(
            f"{context_loss_count} context loss event(s) - may need better context management"
        )

    if fragmentation_count > 0:
        insights.append(
            f"{fragmentation_count} fragmentation event(s) - consider session continuity improvements"
        )

    # Severity insights
    high_severity = [e for e in breakdown_events if e.severity_level == SeverityLevel.HIGH]
    if high_severity:
        insights.append(
            f"{len(high_severity)} high-severity breakdown(s) - requires immediate attention"
        )

    # Recovery insights
    unrecovered = [
        e for e in breakdown_events if e.recovery_pattern == RecoveryPattern.NONE
    ]
    if unrecovered:
        insights.append(
            f"{len(unrecovered)} breakdown(s) without recovery - session quality may be degraded"
        )

    return insights
