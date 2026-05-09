"""Session final answer timing analyzer.

Analyzes final answer generation timing and latency patterns across sessions.
Extracts final answer events, calculates time-to-final-answer metrics, identifies
delays and bottlenecks, and reports statistics on final answer latency distribution.

Timing metrics:
- Time to final answer: Duration from session start to final answer
- Final answer latency: Delay before final answer generation
- Latency distribution: Statistics on final answer timing patterns
- Correlation with complexity: How session complexity affects latency
- Bottleneck detection: Identifies delays before final answer

Quality indicators:
- Low average latency (<100 turns): Quick final answer generation
- Low latency variance: Consistent timing across sessions
- Weak complexity correlation: Latency not strongly tied to complexity
- Few bottlenecks: Minimal delays identified before final answer
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, TypedDict


EVENT_SESSION_START = "session_start"
EVENT_FINAL_ANSWER = "final_answer"
EVENT_BOTTLENECK = "bottleneck"


class FinalAnswerTimingDetail(TypedDict):
    """Details of a final answer timing event."""

    session_id: str
    time_to_final_answer_turns: int
    time_to_final_answer_seconds: int
    final_answer_turn: int
    session_complexity_score: float
    had_bottlenecks: bool


class BottleneckDetail(TypedDict):
    """Details of a bottleneck before final answer."""

    session_id: str
    bottleneck_turn: int
    bottleneck_duration_turns: int
    bottleneck_type: str


@dataclass(frozen=True)
class FinalAnswerTimingEvent:
    """Event in final answer timing tracking."""

    event_type: str
    turn_index: int
    session_id: str
    timestamp: int = 0  # Unix timestamp or relative time in seconds
    complexity_score: float = 0.0
    bottleneck_type: str = ""
    bottleneck_duration_turns: int = 0


@dataclass(frozen=True)
class FinalAnswerTimingMetrics:
    """Metrics for final answer timing."""

    total_sessions: int
    sessions_with_final_answer: int
    avg_time_to_final_answer_turns: float
    avg_time_to_final_answer_seconds: float
    median_time_to_final_answer_turns: float
    max_time_to_final_answer_turns: int
    min_time_to_final_answer_turns: int
    latency_variance_turns: float
    sessions_with_bottlenecks: int
    avg_complexity_score: float
    complexity_latency_correlation: float


@dataclass(frozen=True)
class SessionFinalAnswerTimingAnalysis:
    """Complete analysis of final answer timing."""

    metrics: FinalAnswerTimingMetrics
    timing_details: tuple[FinalAnswerTimingDetail, ...]
    bottleneck_details: tuple[BottleneckDetail, ...]
    insights: tuple[str, ...]


def analyze_session_final_answer_timing(
    events: Sequence[FinalAnswerTimingEvent],
) -> SessionFinalAnswerTimingAnalysis:
    """Measure final answer timing and latency patterns.

    Args:
        events: Sequence of final answer timing events

    Returns:
        Analysis with timing metrics, details, and insights
    """
    _validate_events(events)

    if not events:
        return SessionFinalAnswerTimingAnalysis(
            metrics=FinalAnswerTimingMetrics(0, 0, 0.0, 0.0, 0.0, 0, 0, 0.0, 0, 0.0, 0.0),
            timing_details=(),
            bottleneck_details=(),
            insights=("No events provided.",),
        )

    # Track sessions and final answers
    sessions: dict[str, dict] = {}  # session_id -> session data
    bottlenecks: list[BottleneckDetail] = []

    for event in events:
        sid = event.session_id

        if event.event_type == EVENT_SESSION_START:
            sessions[sid] = {
                "start_turn": event.turn_index,
                "start_timestamp": event.timestamp,
                "complexity_score": event.complexity_score,
                "bottlenecks": [],
                "final_answer_turn": None,
                "final_answer_timestamp": None,
            }

        elif event.event_type == EVENT_FINAL_ANSWER:
            if sid in sessions:
                sessions[sid]["final_answer_turn"] = event.turn_index
                sessions[sid]["final_answer_timestamp"] = event.timestamp

        elif event.event_type == EVENT_BOTTLENECK:
            if sid in sessions:
                bottleneck = {
                    "session_id": sid,
                    "bottleneck_turn": event.turn_index,
                    "bottleneck_duration_turns": event.bottleneck_duration_turns,
                    "bottleneck_type": event.bottleneck_type,
                }
                sessions[sid]["bottlenecks"].append(bottleneck)
                bottlenecks.append(bottleneck)

    # Calculate metrics
    timing_details: list[FinalAnswerTimingDetail] = []
    times_to_answer_turns: list[int] = []
    times_to_answer_seconds: list[int] = []
    complexity_scores: list[float] = []
    sessions_with_bottlenecks = 0

    for sid, session_data in sessions.items():
        if session_data["final_answer_turn"] is not None:
            time_to_answer_turns = session_data["final_answer_turn"] - session_data["start_turn"]
            time_to_answer_seconds = session_data["final_answer_timestamp"] - session_data["start_timestamp"]
            had_bottlenecks = len(session_data["bottlenecks"]) > 0

            timing_details.append({
                "session_id": sid,
                "time_to_final_answer_turns": time_to_answer_turns,
                "time_to_final_answer_seconds": time_to_answer_seconds,
                "final_answer_turn": session_data["final_answer_turn"],
                "session_complexity_score": session_data["complexity_score"],
                "had_bottlenecks": had_bottlenecks,
            })

            times_to_answer_turns.append(time_to_answer_turns)
            times_to_answer_seconds.append(time_to_answer_seconds)
            complexity_scores.append(session_data["complexity_score"])

            if had_bottlenecks:
                sessions_with_bottlenecks += 1

    total_sessions = len(sessions)
    sessions_with_final_answer = len(timing_details)

    avg_turns = sum(times_to_answer_turns) / len(times_to_answer_turns) if times_to_answer_turns else 0.0
    avg_seconds = sum(times_to_answer_seconds) / len(times_to_answer_seconds) if times_to_answer_seconds else 0.0
    median_turns = _calculate_median(times_to_answer_turns)
    max_turns = max(times_to_answer_turns) if times_to_answer_turns else 0
    min_turns = min(times_to_answer_turns) if times_to_answer_turns else 0
    variance_turns = _calculate_variance(times_to_answer_turns, avg_turns)

    avg_complexity = sum(complexity_scores) / len(complexity_scores) if complexity_scores else 0.0

    # Calculate correlation between complexity and latency
    correlation = _calculate_correlation(complexity_scores, [float(t) for t in times_to_answer_turns])

    metrics = FinalAnswerTimingMetrics(
        total_sessions=total_sessions,
        sessions_with_final_answer=sessions_with_final_answer,
        avg_time_to_final_answer_turns=round(avg_turns, 1),
        avg_time_to_final_answer_seconds=round(avg_seconds, 1),
        median_time_to_final_answer_turns=round(median_turns, 1),
        max_time_to_final_answer_turns=max_turns,
        min_time_to_final_answer_turns=min_turns,
        latency_variance_turns=round(variance_turns, 1),
        sessions_with_bottlenecks=sessions_with_bottlenecks,
        avg_complexity_score=round(avg_complexity, 2),
        complexity_latency_correlation=round(correlation, 3),
    )

    return SessionFinalAnswerTimingAnalysis(
        metrics=metrics,
        timing_details=tuple(timing_details),
        bottleneck_details=tuple(bottlenecks),
        insights=_generate_insights(metrics),
    )


def _validate_events(events: Sequence[FinalAnswerTimingEvent]) -> None:
    """Validate event sequence structure and content."""
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")

    for index, event in enumerate(events):
        if not isinstance(event, FinalAnswerTimingEvent):
            raise ValueError("events must contain FinalAnswerTimingEvent instances")

        if event.event_type not in {EVENT_SESSION_START, EVENT_FINAL_ANSWER, EVENT_BOTTLENECK}:
            raise ValueError(
                f"event at index {index} has invalid event_type: {event.event_type}"
            )

        if not isinstance(event.turn_index, int) or isinstance(event.turn_index, bool):
            raise ValueError(f"turn_index at index {index} must be an integer")

        if event.turn_index < 0:
            raise ValueError(f"turn_index at index {index} must be non-negative")

        if not isinstance(event.session_id, str) or not event.session_id.strip():
            raise ValueError(
                f"event at index {index} must have a non-empty session_id"
            )


def _calculate_median(values: list[int]) -> float:
    """Calculate median of numeric values."""
    if not values:
        return 0.0

    sorted_values = sorted(values)
    n = len(sorted_values)
    mid = n // 2

    if n % 2 == 0:
        return (sorted_values[mid - 1] + sorted_values[mid]) / 2.0
    else:
        return float(sorted_values[mid])


def _calculate_variance(values: list[int], mean: float) -> float:
    """Calculate variance of numeric values."""
    if not values:
        return 0.0

    squared_diffs = [(v - mean) ** 2 for v in values]
    return sum(squared_diffs) / len(squared_diffs)


def _calculate_correlation(x_values: list[float], y_values: list[float]) -> float:
    """Calculate Pearson correlation coefficient between two lists."""
    if not x_values or not y_values or len(x_values) != len(y_values):
        return 0.0

    n = len(x_values)
    if n < 2:
        return 0.0

    mean_x = sum(x_values) / n
    mean_y = sum(y_values) / n

    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, y_values))

    sum_sq_x = sum((x - mean_x) ** 2 for x in x_values)
    sum_sq_y = sum((y - mean_y) ** 2 for y in y_values)

    denominator = (sum_sq_x * sum_sq_y) ** 0.5

    if denominator == 0:
        return 0.0

    return numerator / denominator


def _generate_insights(metrics: FinalAnswerTimingMetrics) -> tuple[str, ...]:
    """Generate human-readable insights about final answer timing."""
    if metrics.total_sessions == 0:
        return ("No sessions tracked.",)

    insights = [
        f"Analyzed {metrics.total_sessions} session(s), "
        f"{metrics.sessions_with_final_answer} with final answer."
    ]

    if metrics.avg_time_to_final_answer_turns > 0:
        insights.append(
            f"Average time to final answer: {metrics.avg_time_to_final_answer_turns:.1f} turns "
            f"({metrics.avg_time_to_final_answer_seconds:.1f}s)."
        )

    if metrics.avg_time_to_final_answer_turns > 100:
        insights.append(
            "High average latency detected. Consider optimizing final answer generation."
        )

    if metrics.latency_variance_turns > 1000:
        insights.append(
            f"High latency variance ({metrics.latency_variance_turns:.1f}). "
            "Timing is inconsistent across sessions."
        )

    if metrics.sessions_with_bottlenecks > 0:
        bottleneck_rate = metrics.sessions_with_bottlenecks / metrics.sessions_with_final_answer
        insights.append(
            f"{metrics.sessions_with_bottlenecks} session(s) with bottlenecks "
            f"({bottleneck_rate:.1%} of sessions with final answers)."
        )

    if abs(metrics.complexity_latency_correlation) > 0.7:
        direction = "positively" if metrics.complexity_latency_correlation > 0 else "negatively"
        insights.append(
            f"Strong {direction} correlated latency with complexity "
            f"(r={metrics.complexity_latency_correlation:.2f})."
        )

    return tuple(insights)
