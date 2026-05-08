"""Session grep optimization analyzer for detecting suboptimal search patterns."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Sequence


EVENT_GREP = "grep"
EVENT_READ = "read"

BROAD_RESULT_THRESHOLD = 100
TOKEN_PER_RESULT = 50  # Estimated tokens per result


@dataclass(frozen=True)
class GrepOptimizationEvent:
    """Event in a grep optimization sequence."""

    event_type: str
    turn_index: int
    pattern: str = ""
    result_count: int = 0
    file_path: str = ""
    glob_filter: str = ""
    type_filter: str = ""


@dataclass(frozen=True)
class OptimizationOpportunity:
    """Details of a detected optimization opportunity."""

    grep_turn: int
    pattern: str
    issue_type: str
    suggestion: str
    estimated_token_savings: int


@dataclass(frozen=True)
class SessionGrepOptimizationMetrics:
    """Aggregate metrics for grep optimization."""

    total_greps: int
    broad_pattern_count: int
    repeated_pattern_count: int
    grep_read_inefficiency_count: int
    missing_filter_count: int
    total_opportunity_count: int
    estimated_total_savings: int


@dataclass(frozen=True)
class SessionGrepOptimizationAnalysis:
    """Complete analysis of grep optimization opportunities."""

    metrics: SessionGrepOptimizationMetrics
    opportunities: tuple[OptimizationOpportunity, ...]
    insights: tuple[str, ...]


def analyze_session_grep_optimization(
    events: Sequence[GrepOptimizationEvent],
) -> SessionGrepOptimizationAnalysis:
    """Identify suboptimal Grep tool usage patterns."""

    _validate_events(events)

    if not events:
        return SessionGrepOptimizationAnalysis(
            metrics=SessionGrepOptimizationMetrics(0, 0, 0, 0, 0, 0, 0),
            opportunities=(),
            insights=("No events provided.",),
        )

    grep_events = [e for e in events if e.event_type == EVENT_GREP]

    if not grep_events:
        return SessionGrepOptimizationAnalysis(
            metrics=SessionGrepOptimizationMetrics(0, 0, 0, 0, 0, 0, 0),
            opportunities=(),
            insights=("No grep events found.",),
        )

    opportunities: list[OptimizationOpportunity] = []

    # Check for broad patterns
    broad_patterns = _detect_broad_patterns(grep_events)
    opportunities.extend(broad_patterns)

    # Check for repeated similar patterns
    repeated_patterns = _detect_repeated_patterns(grep_events)
    opportunities.extend(repeated_patterns)

    # Check for grep-read inefficiency
    grep_read_issues = _detect_grep_read_inefficiency(events)
    opportunities.extend(grep_read_issues)

    # Check for missing filters
    missing_filters = _detect_missing_filters(grep_events)
    opportunities.extend(missing_filters)

    metrics = SessionGrepOptimizationMetrics(
        total_greps=len(grep_events),
        broad_pattern_count=len(broad_patterns),
        repeated_pattern_count=len(repeated_patterns),
        grep_read_inefficiency_count=len(grep_read_issues),
        missing_filter_count=len(missing_filters),
        total_opportunity_count=len(opportunities),
        estimated_total_savings=sum(opp.estimated_token_savings for opp in opportunities),
    )

    return SessionGrepOptimizationAnalysis(
        metrics=metrics,
        opportunities=tuple(sorted(opportunities, key=lambda o: o.estimated_token_savings, reverse=True)),
        insights=_generate_insights(metrics),
    )


def _validate_events(events: Sequence[GrepOptimizationEvent]) -> None:
    """Validate event sequence structure and content."""
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")

    last_turn = -1
    for index, event in enumerate(events):
        if not isinstance(event, GrepOptimizationEvent):
            raise ValueError("events must contain GrepOptimizationEvent instances")

        if event.event_type not in {EVENT_GREP, EVENT_READ}:
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

        if event.event_type == EVENT_GREP:
            if not isinstance(event.pattern, str) or not event.pattern.strip():
                raise ValueError(
                    f"grep event at index {index} must have a non-empty pattern"
                )
            if not isinstance(event.result_count, int) or isinstance(event.result_count, bool):
                raise ValueError(
                    f"grep event at index {index} must have integer result_count"
                )
            if event.result_count < 0:
                raise ValueError(
                    f"grep event at index {index} result_count must be non-negative"
                )


def _detect_broad_patterns(grep_events: list[GrepOptimizationEvent]) -> list[OptimizationOpportunity]:
    """Detect grep patterns that return too many results."""
    opportunities = []

    for event in grep_events:
        if event.result_count >= BROAD_RESULT_THRESHOLD:
            savings = (event.result_count - BROAD_RESULT_THRESHOLD) * TOKEN_PER_RESULT
            opportunities.append(
                OptimizationOpportunity(
                    grep_turn=event.turn_index,
                    pattern=event.pattern,
                    issue_type="broad_pattern",
                    suggestion=f"Refine pattern to reduce {event.result_count} results. "
                               "Consider more specific search terms or add filters.",
                    estimated_token_savings=savings,
                )
            )

    return opportunities


def _detect_repeated_patterns(grep_events: list[GrepOptimizationEvent]) -> list[OptimizationOpportunity]:
    """Detect repeated or very similar grep patterns."""
    opportunities = []
    seen_patterns: dict[str, int] = {}  # normalized pattern -> first turn

    for event in grep_events:
        normalized = _normalize_pattern(event.pattern)

        if normalized in seen_patterns:
            # Found repeated pattern
            first_turn = seen_patterns[normalized]
            if event.turn_index - first_turn <= 10:  # Within 10 turns
                savings = event.result_count * TOKEN_PER_RESULT
                opportunities.append(
                    OptimizationOpportunity(
                        grep_turn=event.turn_index,
                        pattern=event.pattern,
                        issue_type="repeated_pattern",
                        suggestion=f"Pattern similar to turn {first_turn}. "
                                   "Consider caching results or refining search.",
                        estimated_token_savings=savings,
                    )
                )
        else:
            seen_patterns[normalized] = event.turn_index

    return opportunities


def _detect_grep_read_inefficiency(events: Sequence[GrepOptimizationEvent]) -> list[OptimizationOpportunity]:
    """Detect grep followed by full file read when targeted read would suffice."""
    opportunities = []
    flagged_reads: set[int] = set()  # Track which reads we've already flagged

    for i, event in enumerate(events):
        if event.event_type != EVENT_GREP:
            continue

        # Look for Read event in next few turns
        for j in range(i + 1, min(i + 4, len(events))):
            next_event = events[j]

            # Skip if we've already flagged this read
            if j in flagged_reads:
                break

            # Stop if we hit another grep before finding a read
            if next_event.event_type == EVENT_GREP:
                break

            if next_event.event_type == EVENT_READ and next_event.file_path:
                # Check if grep had low result count (suggesting specific location)
                if event.result_count > 0 and event.result_count <= 10:
                    savings = 200  # Estimated savings from targeted read
                    opportunities.append(
                        OptimizationOpportunity(
                            grep_turn=event.turn_index,
                            pattern=event.pattern,
                            issue_type="grep_read_inefficiency",
                            suggestion=f"Grep found {event.result_count} result(s), "
                                       f"followed by full read of {next_event.file_path}. "
                                       "Consider targeted read with offset/limit.",
                            estimated_token_savings=savings,
                        )
                    )
                    flagged_reads.add(j)
                break

    return opportunities


def _detect_missing_filters(grep_events: list[GrepOptimizationEvent]) -> list[OptimizationOpportunity]:
    """Detect grep patterns that could benefit from glob or type filters."""
    opportunities = []

    for event in grep_events:
        # Skip if already has filters
        if event.glob_filter or event.type_filter:
            continue

        # Check if pattern is file-type specific
        file_extensions = _extract_file_extensions(event.pattern)

        if file_extensions and event.result_count > 20:
            savings = event.result_count * 10  # Smaller savings but still worthwhile
            ext_list = ", ".join(sorted(file_extensions))
            opportunities.append(
                OptimizationOpportunity(
                    grep_turn=event.turn_index,
                    pattern=event.pattern,
                    issue_type="missing_filter",
                    suggestion=f"Pattern appears file-type specific ({ext_list}). "
                               f"Add glob or type filter to reduce {event.result_count} results.",
                    estimated_token_savings=savings,
                )
            )

    return opportunities


def _normalize_pattern(pattern: str) -> str:
    """Normalize pattern for similarity comparison."""
    # Remove common variations
    normalized = pattern.lower().strip()
    # Remove quotes
    normalized = normalized.strip('"\'')
    # Normalize whitespace
    normalized = " ".join(normalized.split())
    return normalized


def _extract_file_extensions(pattern: str) -> set[str]:
    """Extract file extensions mentioned in pattern."""
    extensions = set()

    # Common patterns: .py, *.ts, \\.js
    matches = re.findall(r'\.([a-z]{2,4})\b', pattern.lower())
    for match in matches:
        if match in {"py", "js", "ts", "tsx", "jsx", "java", "go", "rs", "cpp", "c", "h"}:
            extensions.add(match)

    return extensions


def _generate_insights(metrics: SessionGrepOptimizationMetrics) -> tuple[str, ...]:
    """Generate human-readable insights about grep optimization."""
    if metrics.total_greps == 0:
        return ("No grep events found.",)

    if metrics.total_opportunity_count == 0:
        return ("No grep optimization opportunities detected.",)

    insights = [
        f"Detected {metrics.total_opportunity_count} optimization opportunity(ies) "
        f"across {metrics.total_greps} grep operations."
    ]

    if metrics.estimated_total_savings > 0:
        insights.append(
            f"Potential token savings: ~{metrics.estimated_total_savings:,} tokens."
        )

    if metrics.broad_pattern_count > 0:
        insights.append(
            f"{metrics.broad_pattern_count} grep(s) returned 100+ results. "
            "Consider refining patterns."
        )

    if metrics.repeated_pattern_count > 0:
        insights.append(
            f"{metrics.repeated_pattern_count} repeated or similar grep pattern(s) detected."
        )

    if metrics.grep_read_inefficiency_count > 0:
        insights.append(
            f"{metrics.grep_read_inefficiency_count} grep-read sequence(s) could use targeted reads."
        )

    if metrics.missing_filter_count > 0:
        insights.append(
            f"{metrics.missing_filter_count} grep(s) could benefit from glob/type filters."
        )

    return tuple(insights)
