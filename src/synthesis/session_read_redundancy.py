"""Session read redundancy analyzer for detecting duplicate file reads."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Sequence


EVENT_READ = "read"
EVENT_MODIFICATION = "modification"


@dataclass(frozen=True)
class ReadRedundancyEvent:
    """Event in a read redundancy sequence."""

    event_type: str
    turn_index: int
    file_path: str = ""
    content: str = ""
    content_hash: str = ""


@dataclass(frozen=True)
class DuplicateReadPair:
    """Details of redundant reads for a specific file."""

    file_path: str
    read_count: int
    token_estimate: int


@dataclass(frozen=True)
class SessionReadRedundancyMetrics:
    """Aggregate metrics for read redundancy."""

    total_reads: int
    duplicate_reads: int
    unique_files_with_duplicates: int
    redundancy_rate: float
    wasted_tokens: int


@dataclass(frozen=True)
class SessionReadRedundancyAnalysis:
    """Complete analysis of read redundancy in a session."""

    metrics: SessionReadRedundancyMetrics
    duplicate_pairs: tuple[DuplicateReadPair, ...]
    redundancy_detected: bool
    insights: tuple[str, ...]


def analyze_session_read_redundancy(
    events: Sequence[ReadRedundancyEvent],
) -> SessionReadRedundancyAnalysis:
    """Identify redundant file reads without intervening edits."""

    _validate_events(events)

    if not events:
        return SessionReadRedundancyAnalysis(
            metrics=SessionReadRedundancyMetrics(0, 0, 0, 0.0, 0),
            duplicate_pairs=(),
            redundancy_detected=False,
            insights=("No events provided.",),
        )

    # Track reads and modifications per file
    file_states: dict[str, list[tuple[int, str, str]]] = {}  # file -> [(turn, hash, content)]
    last_modification_turn: dict[str, int] = {}  # file -> last modification turn

    total_reads = 0
    duplicate_reads = 0
    wasted_tokens = 0
    duplicate_details: dict[str, list[tuple[int, int]]] = {}  # file -> [(turn, tokens)]

    for event in events:
        if event.event_type == EVENT_READ:
            total_reads += 1
            file_path = event.file_path

            # Calculate content hash if not provided
            content_hash = event.content_hash
            if not content_hash and event.content:
                content_hash = _hash_content(event.content)

            # Initialize tracking for new file
            if file_path not in file_states:
                file_states[file_path] = []
                last_modification_turn[file_path] = -1

            # Check if this is a duplicate read
            last_mod_turn = last_modification_turn.get(file_path, -1)
            recent_reads = [
                (turn, hash_val, content)
                for turn, hash_val, content in file_states[file_path]
                if turn > last_mod_turn
            ]

            is_duplicate = any(hash_val == content_hash for _, hash_val, _ in recent_reads)

            if is_duplicate:
                duplicate_reads += 1
                tokens = _estimate_tokens(event.content)
                wasted_tokens += tokens

                if file_path not in duplicate_details:
                    duplicate_details[file_path] = []
                duplicate_details[file_path].append((event.turn_index, tokens))

            # Record this read
            file_states[file_path].append((event.turn_index, content_hash, event.content))

        elif event.event_type == EVENT_MODIFICATION:
            # Mark modification for this file
            if event.file_path:
                last_modification_turn[event.file_path] = event.turn_index

    # Build duplicate pairs
    duplicate_pairs: list[DuplicateReadPair] = []
    for file_path, reads in duplicate_details.items():
        duplicate_pairs.append(
            DuplicateReadPair(
                file_path=file_path,
                read_count=len(reads) + 1,  # +1 for original read
                token_estimate=sum(tokens for _, tokens in reads),
            )
        )

    redundancy_rate = duplicate_reads / total_reads if total_reads > 0 else 0.0
    redundancy_detected = redundancy_rate > 0.3

    metrics = SessionReadRedundancyMetrics(
        total_reads=total_reads,
        duplicate_reads=duplicate_reads,
        unique_files_with_duplicates=len(duplicate_details),
        redundancy_rate=round(redundancy_rate, 3),
        wasted_tokens=wasted_tokens,
    )

    return SessionReadRedundancyAnalysis(
        metrics=metrics,
        duplicate_pairs=tuple(sorted(duplicate_pairs, key=lambda p: p.token_estimate, reverse=True)),
        redundancy_detected=redundancy_detected,
        insights=_generate_insights(metrics, redundancy_detected),
    )


def _validate_events(events: Sequence[ReadRedundancyEvent]) -> None:
    """Validate event sequence structure and content."""
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")

    last_turn = -1
    for index, event in enumerate(events):
        if not isinstance(event, ReadRedundancyEvent):
            raise ValueError("events must contain ReadRedundancyEvent instances")

        if event.event_type not in {EVENT_READ, EVENT_MODIFICATION}:
            raise ValueError(
                f"event at index {index} has invalid event_type: {event.event_type}"
            )

        if not isinstance(event.turn_index, int) or isinstance(event.turn_index, bool):
            raise ValueError(f"turn_index at index {index} must be an integer")

        if event.turn_index < 0:
            raise ValueError(f"turn_index at index {index} must be non-negative")

        if event.turn_index < last_turn:
            raise ValueError("events must be ordered by turn_index")

        last_turn = event.turn_index

        if event.event_type == EVENT_READ:
            if not isinstance(event.file_path, str) or not event.file_path.strip():
                raise ValueError(
                    f"read event at index {index} must have a non-empty file_path"
                )
            if not isinstance(event.content, str):
                raise ValueError(
                    f"read event at index {index} must have string content"
                )

        if event.event_type == EVENT_MODIFICATION:
            if not isinstance(event.file_path, str) or not event.file_path.strip():
                raise ValueError(
                    f"modification event at index {index} must have a non-empty file_path"
                )


def _hash_content(content: str) -> str:
    """Generate a hash of file content."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def _estimate_tokens(content: str) -> int:
    """Estimate token count for content (rough approximation)."""
    # Rough estimate: 4 characters per token on average
    return max(1, len(content) // 4)


def _generate_insights(
    metrics: SessionReadRedundancyMetrics,
    redundancy_detected: bool,
) -> tuple[str, ...]:
    """Generate human-readable insights about read redundancy."""
    if metrics.total_reads == 0:
        return ("No read events found.",)

    if metrics.duplicate_reads == 0:
        return ("No redundant file reads detected.",)

    insights = [
        f"Detected {metrics.duplicate_reads} redundant reads out of "
        f"{metrics.total_reads} total ({metrics.redundancy_rate:.1%})."
    ]

    insights.append(
        f"Redundancy affects {metrics.unique_files_with_duplicates} unique file(s)."
    )

    if metrics.wasted_tokens > 0:
        insights.append(
            f"Estimated {metrics.wasted_tokens:,} tokens wasted on redundant reads."
        )

    if redundancy_detected:
        insights.append(
            "High redundancy rate (>30%) detected. Consider caching file contents "
            "or using targeted reads."
        )

    return tuple(insights)
