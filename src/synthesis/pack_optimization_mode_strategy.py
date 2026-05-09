"""Pack optimization mode strategy compliance scorer.

Aggregates optimization metrics across pack sessions and scores compliance with
CLAUDE_OPTIMIZATION_MODE=optimized strategy (from Run #1). Evaluates pack-level
adherence to proven optimization patterns: targeted reads, cache usage, strategic
verification, and token reduction.

Optimization strategy metrics (from CLAUDE.md Run #1):
- Pack read offset/limit ratio: Target 87% (from Run #1)
- Pack average lines per read: Target 64 (from Run #1)
- Pack token reduction estimate: vs hypothetical baseline
- Cache adoption rate: Sessions using cache / total sessions
- Verify discipline score: Strategic vs excessive verification
- Optimization mode classification: baseline|optimized|mixed

Quality indicators:
- High offset/limit ratio: >85% of reads use targeted approach
- Low lines per read: <70 lines average (proven efficient)
- High cache adoption: >70% of sessions use cache
- Strategic verify usage: Used but not excessive (<10% of reads)
- Clear mode classification: Pack clearly follows one strategy
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_optimization_mode_strategy(records: object) -> dict[str, Any]:
    """Analyze optimization mode strategy compliance across pack sessions.

    Aggregates session-level optimization metrics and scores pack compliance
    with CLAUDE_OPTIMIZATION_MODE=optimized strategy from Run #1.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - optimization_mode: baseline|optimized|unknown
            - read_offset_limit_ratio: Percentage of reads using offset/limit
            - avg_lines_per_read: Average lines read per Read call
            - cache_commands_used: Boolean if cache commands were used
            - verify_commands_used: Boolean if verify commands were used
            - verify_to_read_ratio: Percentage of verify vs total reads
            - total_read_calls: Total Read tool calls in session
            - estimated_token_savings: Estimated tokens saved vs baseline

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - baseline_sessions: Sessions in baseline mode
            - optimized_sessions: Sessions in optimized mode
            - mixed_sessions: Sessions with mixed/unknown mode
            - pack_level_read_offset_limit_ratio: Aggregate offset/limit usage
            - pack_average_lines_per_read: Average lines across all reads
            - sessions_using_cache: Sessions that used cache commands
            - cache_adoption_rate: Percentage of sessions using cache
            - sessions_using_verify: Sessions that used verify commands
            - verify_adoption_rate: Percentage of sessions using verify
            - avg_verify_to_read_ratio: Average verify discipline across sessions
            - pack_token_reduction_estimate: Estimated % token reduction
            - optimization_mode_classification: baseline|optimized|mixed
            - run1_compliance_score: 0-1 score vs Run #1 targets (87%, 64 lines)
            - pack_strategy_effectiveness_score: 0-1 overall strategy score

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    baseline_sessions = 0
    optimized_sessions = 0
    mixed_sessions = 0

    total_reads_count = 0
    total_reads_with_offset_limit = 0
    total_lines_read = 0

    sessions_using_cache = 0
    sessions_using_verify = 0
    verify_to_read_ratios: list[float] = []

    estimated_token_savings: list[float] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        # Classify optimization mode
        mode = _string(record.get("optimization_mode", "unknown")).lower()
        if mode == "baseline":
            baseline_sessions += 1
        elif mode == "optimized":
            optimized_sessions += 1
        else:
            mixed_sessions += 1

        # Aggregate read metrics
        read_offset_ratio = _float(record.get("read_offset_limit_ratio", 0.0))
        avg_lines = _float(record.get("avg_lines_per_read", 0.0))
        total_reads = _int(record.get("total_read_calls", 0))

        if total_reads > 0:
            # Calculate absolute numbers from percentages
            reads_with_offset = int((read_offset_ratio / 100.0) * total_reads)
            total_reads_with_offset_limit += reads_with_offset
            total_reads_count += total_reads

            # Aggregate lines read
            total_lines_read += int(avg_lines * total_reads)

        # Track cache usage
        cache_used = _bool(record.get("cache_commands_used", False))
        if cache_used:
            sessions_using_cache += 1

        # Track verify usage
        verify_used = _bool(record.get("verify_commands_used", False))
        if verify_used:
            sessions_using_verify += 1

        verify_ratio = _float(record.get("verify_to_read_ratio", 0.0))
        if verify_ratio > 0:
            verify_to_read_ratios.append(verify_ratio)

        # Track token savings
        token_savings = _float(record.get("estimated_token_savings", 0.0))
        if token_savings > 0:
            estimated_token_savings.append(token_savings)

    # Calculate pack-level metrics
    pack_read_offset_ratio = _percentage(total_reads_with_offset_limit, total_reads_count)
    pack_avg_lines_per_read = total_lines_read / total_reads_count if total_reads_count > 0 else 0.0
    pack_avg_lines_per_read = round(pack_avg_lines_per_read, 2)

    cache_adoption_rate = _percentage(sessions_using_cache, total_sessions)
    verify_adoption_rate = _percentage(sessions_using_verify, total_sessions)
    avg_verify_ratio = _average(verify_to_read_ratios)

    avg_token_reduction = _average(estimated_token_savings)

    # Classify pack optimization mode
    classification = _classify_pack_mode(
        baseline_sessions,
        optimized_sessions,
        mixed_sessions,
        pack_read_offset_ratio,
        pack_avg_lines_per_read,
    )

    # Calculate Run #1 compliance score (87% offset, 64 lines targets)
    run1_compliance = _calculate_run1_compliance(
        pack_read_offset_ratio,
        pack_avg_lines_per_read,
    )

    # Calculate overall strategy effectiveness
    effectiveness = _calculate_effectiveness_score(
        pack_read_offset_ratio,
        pack_avg_lines_per_read,
        cache_adoption_rate,
        avg_verify_ratio,
        avg_token_reduction,
    )

    return {
        "total_sessions": total_sessions,
        "baseline_sessions": baseline_sessions,
        "optimized_sessions": optimized_sessions,
        "mixed_sessions": mixed_sessions,
        "pack_level_read_offset_limit_ratio": pack_read_offset_ratio,
        "pack_average_lines_per_read": pack_avg_lines_per_read,
        "sessions_using_cache": sessions_using_cache,
        "cache_adoption_rate": cache_adoption_rate,
        "sessions_using_verify": sessions_using_verify,
        "verify_adoption_rate": verify_adoption_rate,
        "avg_verify_to_read_ratio": avg_verify_ratio,
        "pack_token_reduction_estimate": avg_token_reduction,
        "optimization_mode_classification": classification,
        "run1_compliance_score": run1_compliance,
        "pack_strategy_effectiveness_score": effectiveness,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "baseline_sessions": 0,
        "optimized_sessions": 0,
        "mixed_sessions": 0,
        "pack_level_read_offset_limit_ratio": 0.0,
        "pack_average_lines_per_read": 0.0,
        "sessions_using_cache": 0,
        "cache_adoption_rate": 0.0,
        "sessions_using_verify": 0,
        "verify_adoption_rate": 0.0,
        "avg_verify_to_read_ratio": 0.0,
        "pack_token_reduction_estimate": 0.0,
        "optimization_mode_classification": "unknown",
        "run1_compliance_score": 0.0,
        "pack_strategy_effectiveness_score": 0.0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _bool(value: object) -> bool:
    """Convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1")
    return bool(value)


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _classify_pack_mode(
    baseline: int,
    optimized: int,
    mixed: int,
    offset_ratio: float,
    avg_lines: float,
) -> str:
    """Classify pack optimization mode based on session distribution and metrics.

    Args:
        baseline: Number of baseline sessions
        optimized: Number of optimized sessions
        mixed: Number of mixed/unknown sessions
        offset_ratio: Pack-level offset/limit ratio
        avg_lines: Pack average lines per read

    Returns:
        "baseline", "optimized", "mixed", or "unknown"
    """
    total = baseline + optimized + mixed

    if total == 0:
        return "unknown"

    # If >80% sessions are one mode, classify as that mode
    if baseline / total > 0.8:
        return "baseline"
    if optimized / total > 0.8:
        return "optimized"

    # Use metrics to break ties
    if offset_ratio >= 70 and avg_lines <= 80:
        return "optimized"
    elif offset_ratio < 30 and avg_lines > 150:
        return "baseline"

    return "mixed"


def _calculate_run1_compliance(
    offset_ratio: float,
    avg_lines: float,
) -> float:
    """Calculate compliance with Run #1 proven targets (87%, 64 lines).

    Args:
        offset_ratio: Pack-level offset/limit ratio (target: 87%)
        avg_lines: Pack average lines per read (target: 64)

    Returns:
        Score 0-1 where 1.0 = perfect compliance with Run #1
    """
    # Offset ratio component (0-0.5)
    # Target: 87%
    if offset_ratio >= 87:
        offset_component = 0.5
    else:
        offset_component = (offset_ratio / 87.0) * 0.5

    # Lines per read component (0-0.5)
    # Target: 64 lines (lower is better, penalize higher)
    if avg_lines <= 64:
        lines_component = 0.5
    else:
        # Penalize lines >64
        lines_component = max(0.0, 0.5 - (avg_lines - 64) / 200.0)

    score = offset_component + lines_component
    return round(max(0.0, min(1.0, score)), 3)


def _calculate_effectiveness_score(
    offset_ratio: float,
    avg_lines: float,
    cache_adoption: float,
    verify_ratio: float,
    token_reduction: float,
) -> float:
    """Calculate overall optimization strategy effectiveness score (0-1).

    Score components:
    - 0.35: Offset/limit ratio (>85% is optimal)
    - 0.25: Lines per read (<70 is optimal)
    - 0.2: Cache adoption (>70% is good)
    - 0.1: Strategic verify usage (present but <10% is optimal)
    - 0.1: Token reduction achieved (>50% is excellent)
    """
    # Offset ratio component (0-0.35)
    if offset_ratio >= 85:
        offset_component = 0.35
    else:
        offset_component = (offset_ratio / 85.0) * 0.35

    # Lines per read component (0-0.25)
    if avg_lines <= 70:
        lines_component = 0.25
    else:
        # Penalize higher lines
        lines_component = max(0.0, 0.25 - (avg_lines - 70) / 200.0)

    # Cache adoption component (0-0.2)
    if cache_adoption >= 70:
        cache_component = 0.2
    else:
        cache_component = (cache_adoption / 70.0) * 0.2

    # Verify discipline component (0-0.1)
    # Optimal: present but strategic (<10%)
    if 1 <= verify_ratio <= 10:
        verify_component = 0.1
    elif verify_ratio == 0:
        verify_component = 0.05  # Some credit for no verify
    elif verify_ratio < 1:
        verify_component = verify_ratio * 0.1
    else:
        # Penalize excessive verify
        verify_component = max(0.0, 0.1 - (verify_ratio - 10) / 50.0)

    # Token reduction component (0-0.1)
    if token_reduction >= 50:
        token_component = 0.1
    else:
        token_component = (token_reduction / 50.0) * 0.1

    score = (
        offset_component +
        lines_component +
        cache_component +
        verify_component +
        token_component
    )

    return round(max(0.0, min(1.0, score)), 3)
