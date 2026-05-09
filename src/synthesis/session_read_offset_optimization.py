"""Session Read offset/limit parameter optimization analyzer.

Analyzes Read tool usage patterns to measure offset/limit parameter adoption,
identify redundant full-file reads, and calculate token savings from targeted reads.

Read optimization metrics:
- Offset/limit usage: Percentage of reads using targeted parameters
- Average lines read: Mean lines per Read invocation
- Redundant full reads: Full reads after recent edits
- Token savings: Estimated savings from targeted vs full reads
- Cache opportunities: Re-reads that could use cache instead

Quality indicators:
- High offset/limit usage: >85% of reads are targeted
- Low average lines: <70 lines per read
- Few redundant full reads: <15% full reads after edits
- High token savings: >50% vs baseline full reads
- Low cache misses: <10% missed cache opportunities
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_read_offset_optimization(records: object) -> dict[str, Any]:
    """Analyze Read offset/limit usage and optimization patterns.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number
            - tool_name: Tool used (Read, Edit, etc.)
            - file_path: File being read
            - offset: Optional read offset
            - limit: Optional read limit
            - lines_read: Number of lines read
            - after_edit: Boolean if read follows edit
            - cache_available: Boolean if cache could be used

    Returns:
        Dict with optimization metrics including offset/limit usage,
        average lines read, redundant reads, token savings, and cache opportunities.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    total_turns = 0
    read_invocations = 0
    reads_with_offset_limit = 0
    lines_read_list: list[int] = []
    redundant_full_reads = 0
    cache_opportunities = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1
        tool_name = _string(record.get("tool_name"))

        if tool_name.lower() != "read":
            continue

        read_invocations += 1

        # Check offset/limit usage
        offset = record.get("offset")
        limit = record.get("limit")
        if offset is not None or limit is not None:
            reads_with_offset_limit += 1

        # Track lines read
        lines_read = _int(record.get("lines_read", 0))
        if lines_read > 0:
            lines_read_list.append(lines_read)

        # Check for redundant full reads
        after_edit = _bool(record.get("after_edit", False))
        if after_edit and offset is None and limit is None:
            redundant_full_reads += 1

        # Check cache opportunities
        cache_available = _bool(record.get("cache_available", False))
        if cache_available:
            cache_opportunities += 1

    # Calculate metrics
    offset_limit_percentage = _percentage(reads_with_offset_limit, read_invocations)
    avg_lines_read = _average(lines_read_list)
    redundant_read_ratio = _percentage(redundant_full_reads, read_invocations)
    cache_opportunity_ratio = _percentage(cache_opportunities, read_invocations)

    # Estimate token savings (assume 4 tokens per line, 64 baseline)
    baseline_tokens = read_invocations * 64 * 4
    actual_tokens = sum(lines_read_list) * 4
    token_savings = baseline_tokens - actual_tokens
    token_savings_percentage = _percentage(token_savings, baseline_tokens)

    optimization_score = _calculate_optimization_score(
        offset_limit_percentage,
        avg_lines_read,
        redundant_read_ratio,
        token_savings_percentage,
    )

    return {
        "total_turns": total_turns,
        "read_invocations": read_invocations,
        "reads_with_offset_limit": reads_with_offset_limit,
        "offset_limit_percentage": offset_limit_percentage,
        "lines_read_list": lines_read_list,
        "avg_lines_read": avg_lines_read,
        "redundant_full_reads": redundant_full_reads,
        "redundant_read_ratio": redundant_read_ratio,
        "cache_opportunities": cache_opportunities,
        "cache_opportunity_ratio": cache_opportunity_ratio,
        "baseline_tokens": baseline_tokens,
        "actual_tokens": actual_tokens,
        "token_savings": token_savings,
        "token_savings_percentage": token_savings_percentage,
        "optimization_score": optimization_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "read_invocations": 0,
        "reads_with_offset_limit": 0,
        "offset_limit_percentage": 0.0,
        "lines_read_list": [],
        "avg_lines_read": 0.0,
        "redundant_full_reads": 0,
        "redundant_read_ratio": 0.0,
        "cache_opportunities": 0,
        "cache_opportunity_ratio": 0.0,
        "baseline_tokens": 0,
        "actual_tokens": 0,
        "token_savings": 0,
        "token_savings_percentage": 0.0,
        "optimization_score": 0.0,
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
            return int(float(value))
        except ValueError:
            return 0
    return 0


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


def _calculate_optimization_score(
    offset_limit_percentage: float,
    avg_lines_read: float,
    redundant_read_ratio: float,
    token_savings_percentage: float,
) -> float:
    """Calculate overall optimization score (0-1)."""
    # Offset/limit component (0-0.30)
    if offset_limit_percentage >= 85.0:
        offset_component = 0.30
    else:
        offset_component = (offset_limit_percentage / 85.0) * 0.30

    # Average lines component (0-0.25)
    if avg_lines_read <= 70.0:
        lines_component = 0.25
    else:
        penalty = min(avg_lines_read - 70.0, 170.0) / 170.0
        lines_component = 0.25 * (1.0 - penalty)

    # Redundant reads penalty (0-0.20)
    if redundant_read_ratio <= 15.0:
        redundant_component = 0.20
    else:
        penalty = min(redundant_read_ratio - 15.0, 85.0) / 85.0
        redundant_component = 0.20 * (1.0 - penalty)

    # Token savings component (0-0.25)
    if token_savings_percentage >= 50.0:
        savings_component = 0.25
    else:
        savings_component = (token_savings_percentage / 50.0) * 0.25

    score = (
        offset_component +
        lines_component +
        redundant_component +
        savings_component
    )
    return round(max(0.0, min(1.0, score)), 3)
