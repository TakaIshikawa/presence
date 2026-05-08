"""Session context refresh cadence analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


MARKER_NONE = "none"
MARKER_CONSTRAINT_RECAP = "constraint_recap"
MARKER_PLAN_RECAP = "plan_recap"
MARKER_FILE_CONTEXT_RECAP = "file_context_recap"

STALE_CONTEXT_GAP_TURNS = 4

QUALITY_NO_SESSION = "no_session"
QUALITY_STRONG = "strong"
QUALITY_UNEVEN = "uneven"
QUALITY_STALE = "stale"


@dataclass(frozen=True)
class ContextRefreshTurn:
    turn_index: int
    token_estimate: int
    refresh_markers: tuple[str, ...] = (MARKER_NONE,)


@dataclass(frozen=True)
class ContextRefreshCadenceMetrics:
    total_turns: int
    refresh_turns: int
    average_turns_between_refreshes: float
    longest_refresh_gap: int
    stale_context_windows: int
    marker_counts: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class ContextRefreshCadenceAnalysis:
    metrics: ContextRefreshCadenceMetrics
    quality: str
    insights: tuple[str, ...]


def analyze_session_context_refresh_cadence(
    turns: Sequence[ContextRefreshTurn],
) -> ContextRefreshCadenceAnalysis:
    """Analyze how often a session refreshes task context before continuing."""

    _validate_turns(turns)
    if not turns:
        metrics = ContextRefreshCadenceMetrics(0, 0, 0.0, 0, 0, ())
        return ContextRefreshCadenceAnalysis(
            metrics, QUALITY_NO_SESSION, ("No session turns supplied.",)
        )

    refresh_indexes = [
        turn.turn_index for turn in turns if _has_refresh_marker(turn.refresh_markers)
    ]
    marker_counts: dict[str, int] = {}
    for turn in turns:
        for marker in turn.refresh_markers:
            if marker != MARKER_NONE:
                marker_counts[marker] = marker_counts.get(marker, 0) + 1

    gaps = [
        later - earlier
        for earlier, later in zip(refresh_indexes, refresh_indexes[1:])
    ]
    leading_gap = refresh_indexes[0] - turns[0].turn_index if refresh_indexes else len(turns)
    trailing_gap = turns[-1].turn_index - refresh_indexes[-1] if refresh_indexes else len(turns)
    all_gaps = gaps + [leading_gap, trailing_gap]
    stale_windows = sum(1 for gap in all_gaps if gap > STALE_CONTEXT_GAP_TURNS)
    metrics = ContextRefreshCadenceMetrics(
        total_turns=len(turns),
        refresh_turns=len(refresh_indexes),
        average_turns_between_refreshes=round(sum(gaps) / len(gaps), 2)
        if gaps
        else 0.0,
        longest_refresh_gap=max(all_gaps) if all_gaps else 0,
        stale_context_windows=stale_windows,
        marker_counts=tuple(sorted(marker_counts.items())),
    )
    return ContextRefreshCadenceAnalysis(
        metrics,
        _classify_quality(metrics),
        _generate_insights(metrics),
    )


def _validate_turns(turns: Sequence[ContextRefreshTurn]) -> None:
    if not isinstance(turns, (list, tuple)):
        raise ValueError("turns must be a list or tuple")
    valid = {
        MARKER_NONE,
        MARKER_CONSTRAINT_RECAP,
        MARKER_PLAN_RECAP,
        MARKER_FILE_CONTEXT_RECAP,
    }
    seen: set[int] = set()
    last_index = -1
    for turn in turns:
        if not isinstance(turn, ContextRefreshTurn):
            raise ValueError("turns must contain ContextRefreshTurn instances")
        if not isinstance(turn.turn_index, int) or turn.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if turn.turn_index in seen:
            raise ValueError("turn_index values must be unique")
        if turn.turn_index <= last_index:
            raise ValueError("turn_index values must be strictly increasing")
        if not isinstance(turn.token_estimate, int) or turn.token_estimate < 0:
            raise ValueError("token_estimate must be a non-negative integer")
        if not isinstance(turn.refresh_markers, tuple):
            raise ValueError("refresh_markers must be a tuple")
        if not turn.refresh_markers:
            raise ValueError("refresh_markers must not be empty")
        for marker in turn.refresh_markers:
            if marker not in valid:
                raise ValueError(f"unsupported refresh marker: {marker}")
        if MARKER_NONE in turn.refresh_markers and len(turn.refresh_markers) > 1:
            raise ValueError("none marker cannot be combined with refresh markers")
        seen.add(turn.turn_index)
        last_index = turn.turn_index


def _has_refresh_marker(markers: tuple[str, ...]) -> bool:
    return any(marker != MARKER_NONE for marker in markers)


def _classify_quality(metrics: ContextRefreshCadenceMetrics) -> str:
    if metrics.total_turns == 0:
        return QUALITY_NO_SESSION
    if metrics.refresh_turns == 0 or metrics.stale_context_windows > 1:
        return QUALITY_STALE
    if metrics.stale_context_windows == 1 or metrics.longest_refresh_gap > STALE_CONTEXT_GAP_TURNS:
        return QUALITY_UNEVEN
    return QUALITY_STRONG


def _generate_insights(metrics: ContextRefreshCadenceMetrics) -> tuple[str, ...]:
    if metrics.total_turns == 0:
        return ("No session turns supplied.",)
    insights = [
        f"{metrics.refresh_turns} of {metrics.total_turns} turns refreshed context."
    ]
    insights.append(f"Longest refresh gap was {metrics.longest_refresh_gap} turns.")
    if metrics.marker_counts:
        marker, count = max(metrics.marker_counts, key=lambda item: item[1])
        insights.append(f"Dominant refresh marker: {marker} ({count}).")
    return tuple(insights)
