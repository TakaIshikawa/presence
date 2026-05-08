"""Session stalled-turn recovery analyzer.

Measures whether Claude sessions recover after blocked or stalled turns.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


STATUS_PROGRESS = "progress"
STATUS_BLOCKED = "blocked"
STATUS_STALLED = "stalled"
STATUS_ABANDONED = "abandoned"
STATUS_RECOVERED = "recovered"

QUALITY_NO_STALLS = "no_stalls"
QUALITY_STRONG = "strong"
QUALITY_PARTIAL = "partial"
QUALITY_POOR = "poor"

RECOVERY_WINDOW_TURNS = 2


@dataclass(frozen=True)
class SessionTurn:
    """Single session turn with workflow status."""

    turn_index: int
    status: str
    topic: str = ""
    note: str = ""


@dataclass(frozen=True)
class StallOutcome:
    """Recovery outcome for one blocked or stalled turn."""

    turn_index: int
    status: str
    classification: str
    recovery_turn_index: int | None
    recovery_latency_turns: int | None


@dataclass(frozen=True)
class StalledTurnRecoveryMetrics:
    """Aggregate stalled-turn recovery metrics."""

    total_turns: int
    blocked_turns: int
    stalled_turns: int
    recovered_turns: int
    abandoned_turns: int
    unresolved_turns: int
    recovery_rate: float
    immediate_recoveries: int
    delayed_recoveries: int
    average_recovery_latency: float


@dataclass(frozen=True)
class SessionStalledTurnRecovery:
    """Complete stalled-turn recovery analysis."""

    metrics: StalledTurnRecoveryMetrics
    stall_outcomes: tuple[StallOutcome, ...]
    recovery_quality: str
    insights: tuple[str, ...]


def analyze_session_stalled_turn_recovery(
    turns: Sequence[SessionTurn],
) -> SessionStalledTurnRecovery:
    """Analyze recovery after blocked or stalled session turns.

    A blocked or stalled turn is recovered by the next later turn whose status is
    ``progress`` or ``recovered``. If an ``abandoned`` turn appears first, the
    stall is classified as abandoned. Recoveries within two turns are immediate;
    later recoveries are delayed.
    """

    _validate_turns(turns)

    if not turns:
        metrics = StalledTurnRecoveryMetrics(0, 0, 0, 0, 0, 0, 0.0, 0, 0, 0.0)
        return SessionStalledTurnRecovery(
            metrics=metrics,
            stall_outcomes=(),
            recovery_quality=QUALITY_NO_STALLS,
            insights=("No turns supplied - no stalled-turn recovery to analyze.",),
        )

    outcomes = tuple(_classify_stalls(turns))
    blocked_count = sum(1 for turn in turns if turn.status == STATUS_BLOCKED)
    stalled_count = sum(1 for turn in turns if turn.status == STATUS_STALLED)
    recovered_count = sum(
        1 for outcome in outcomes if outcome.classification == STATUS_RECOVERED
    )
    abandoned_count = sum(
        1 for outcome in outcomes if outcome.classification == STATUS_ABANDONED
    )
    unresolved_count = sum(
        1 for outcome in outcomes if outcome.classification == "unresolved"
    )
    latencies = [
        outcome.recovery_latency_turns
        for outcome in outcomes
        if outcome.recovery_latency_turns is not None
    ]
    total_stalls = blocked_count + stalled_count
    recovery_rate = recovered_count / total_stalls if total_stalls else 1.0
    immediate_recoveries = sum(
        1
        for latency in latencies
        if latency is not None and latency <= RECOVERY_WINDOW_TURNS
    )
    delayed_recoveries = recovered_count - immediate_recoveries
    average_latency = sum(latencies) / len(latencies) if latencies else 0.0

    metrics = StalledTurnRecoveryMetrics(
        total_turns=len(turns),
        blocked_turns=blocked_count,
        stalled_turns=stalled_count,
        recovered_turns=recovered_count,
        abandoned_turns=abandoned_count,
        unresolved_turns=unresolved_count,
        recovery_rate=round(recovery_rate, 3),
        immediate_recoveries=immediate_recoveries,
        delayed_recoveries=delayed_recoveries,
        average_recovery_latency=round(average_latency, 2),
    )
    quality = _classify_quality(metrics)

    return SessionStalledTurnRecovery(
        metrics=metrics,
        stall_outcomes=outcomes,
        recovery_quality=quality,
        insights=_generate_insights(metrics, quality),
    )


def _validate_turns(turns: Sequence[SessionTurn]) -> None:
    if not isinstance(turns, (list, tuple)):
        raise ValueError("turns must be a list or tuple")

    last_index = -1
    valid_statuses = {
        STATUS_PROGRESS,
        STATUS_BLOCKED,
        STATUS_STALLED,
        STATUS_ABANDONED,
        STATUS_RECOVERED,
    }
    for position, turn in enumerate(turns):
        if not isinstance(turn, SessionTurn):
            raise ValueError("turns must contain SessionTurn instances")
        if not isinstance(turn.turn_index, int) or turn.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if turn.turn_index <= last_index:
            raise ValueError("turn_index values must be strictly increasing")
        if turn.status not in valid_statuses:
            raise ValueError(
                f"turn at position {position} has unsupported status: {turn.status}"
            )
        if not isinstance(turn.topic, str) or not isinstance(turn.note, str):
            raise ValueError("topic and note must be strings")
        last_index = turn.turn_index


def _classify_stalls(turns: Sequence[SessionTurn]) -> list[StallOutcome]:
    outcomes: list[StallOutcome] = []

    for i, turn in enumerate(turns):
        if turn.status not in {STATUS_BLOCKED, STATUS_STALLED}:
            continue

        classification = "unresolved"
        recovery_turn_index: int | None = None
        latency: int | None = None
        for later in turns[i + 1 :]:
            if later.status == STATUS_ABANDONED:
                classification = STATUS_ABANDONED
                break
            if later.status in {STATUS_PROGRESS, STATUS_RECOVERED}:
                classification = STATUS_RECOVERED
                recovery_turn_index = later.turn_index
                latency = later.turn_index - turn.turn_index
                break

        outcomes.append(
            StallOutcome(
                turn_index=turn.turn_index,
                status=turn.status,
                classification=classification,
                recovery_turn_index=recovery_turn_index,
                recovery_latency_turns=latency,
            )
        )

    return outcomes


def _classify_quality(metrics: StalledTurnRecoveryMetrics) -> str:
    total_stalls = metrics.blocked_turns + metrics.stalled_turns
    if total_stalls == 0:
        return QUALITY_NO_STALLS
    if metrics.recovery_rate >= 0.8 and metrics.delayed_recoveries == 0:
        return QUALITY_STRONG
    if metrics.recovery_rate >= 0.5:
        return QUALITY_PARTIAL
    return QUALITY_POOR


def _generate_insights(
    metrics: StalledTurnRecoveryMetrics,
    quality: str,
) -> tuple[str, ...]:
    if quality == QUALITY_NO_STALLS:
        return ("No blocked or stalled turns detected.",)

    insights = [
        f"Recovered {metrics.recovered_turns} of "
        f"{metrics.blocked_turns + metrics.stalled_turns} blocked or stalled turns "
        f"({metrics.recovery_rate:.1%})."
    ]
    if metrics.abandoned_turns:
        insights.append(
            f"{metrics.abandoned_turns} stalled turns were abandoned before recovery."
        )
    if metrics.unresolved_turns:
        insights.append(
            f"{metrics.unresolved_turns} stalled turns remained unresolved at session end."
        )
    if metrics.delayed_recoveries:
        insights.append(
            "Delayed recoveries suggest adding clearer next-step or retry guidance."
        )
    if quality == QUALITY_POOR:
        insights.append(
            "Low recovery rate indicates blocked turns need explicit escalation paths."
        )
    return tuple(insights)
