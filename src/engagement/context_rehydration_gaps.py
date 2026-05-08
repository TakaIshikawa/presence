"""Context rehydration gaps analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence


@dataclass(frozen=True)
class ContextTurn:
    turn_index: int
    file_reads: Sequence[str] = ()
    clarification_asks: Sequence[str] = ()
    resumed_session: bool = False
    has_summary: bool = True


@dataclass(frozen=True)
class ContextRehydrationMetrics:
    total_turns: int
    repeated_file_reads: int
    repeated_clarification_asks: int
    resumed_session_gaps: int
    unnecessary_rediscovery_ratio: float


@dataclass(frozen=True)
class ContextRehydrationGaps:
    metrics: ContextRehydrationMetrics
    severity: str
    top_repeated_context_keys: tuple[str, ...]
    insights: tuple[str, ...]
    repeated_context_counts: Mapping[str, int]


def analyze_context_rehydration_gaps(
    turns: Sequence[ContextTurn],
) -> ContextRehydrationGaps:
    """Detect context rediscovery that should have been retained."""

    _validate_context_turns(turns)
    if not turns:
        metrics = ContextRehydrationMetrics(0, 0, 0, 0, 0.0)
        return ContextRehydrationGaps(metrics, "low", (), ("No turns supplied.",), {})

    file_counts: dict[str, int] = {}
    ask_counts: dict[str, int] = {}
    context_counts: dict[str, int] = {}
    for turn in turns:
        for path in turn.file_reads:
            file_counts[path] = file_counts.get(path, 0) + 1
            context_counts[path] = context_counts.get(path, 0) + 1
        for ask in turn.clarification_asks:
            key = " ".join(ask.lower().split())
            ask_counts[key] = ask_counts.get(key, 0) + 1
            context_counts[key] = context_counts.get(key, 0) + 1

    repeated_file_reads = sum(count - 1 for count in file_counts.values() if count > 1)
    repeated_asks = sum(count - 1 for count in ask_counts.values() if count > 1)
    resumed_gaps = sum(1 for turn in turns if turn.resumed_session and not turn.has_summary)
    rediscovery = repeated_file_reads + repeated_asks + resumed_gaps
    ratio = rediscovery / len(turns)
    metrics = ContextRehydrationMetrics(
        len(turns),
        repeated_file_reads,
        repeated_asks,
        resumed_gaps,
        round(ratio, 3),
    )
    severity = "high" if ratio >= 0.75 else "moderate" if ratio >= 0.25 else "low"
    repeated_counts = {
        key: count for key, count in sorted(context_counts.items()) if count > 1
    }
    repeated_keys = sorted(repeated_counts)
    return ContextRehydrationGaps(
        metrics,
        severity,
        tuple(repeated_keys[:5]),
        _rehydration_insights(metrics, severity),
        repeated_counts,
    )


def _validate_context_turns(turns: Sequence[ContextTurn]) -> None:
    if not isinstance(turns, (list, tuple)):
        raise ValueError("turns must be a list or tuple")
    last_index = -1
    for turn in turns:
        if not isinstance(turn, ContextTurn):
            raise ValueError("turns must contain ContextTurn instances")
        if not isinstance(turn.turn_index, int) or turn.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if turn.turn_index <= last_index:
            raise ValueError("turn_index values must be strictly increasing")
        for name in ("file_reads", "clarification_asks"):
            value = getattr(turn, name)
            if not isinstance(value, (list, tuple)):
                raise ValueError(f"{name} must be a list or tuple")
            if any(not isinstance(item, str) or not item for item in value):
                raise ValueError(f"{name} must contain non-empty strings")
        if not isinstance(turn.resumed_session, bool) or not isinstance(turn.has_summary, bool):
            raise ValueError("resumed_session and has_summary must be booleans")
        last_index = turn.turn_index


def _rehydration_insights(
    metrics: ContextRehydrationMetrics,
    severity: str,
) -> tuple[str, ...]:
    if severity == "low":
        return ("Context reuse looks efficient with few rehydration gaps.",)
    insights = [f"Context rehydration severity is {severity}."]
    if metrics.repeated_file_reads:
        insights.append(f"{metrics.repeated_file_reads} repeated file reads indicate rediscovery.")
    if metrics.repeated_clarification_asks:
        insights.append(f"{metrics.repeated_clarification_asks} repeated clarification asks indicate retained context gaps.")
    if metrics.resumed_session_gaps:
        insights.append(f"{metrics.resumed_session_gaps} resumed turns lacked a usable summary.")
    return tuple(insights)
