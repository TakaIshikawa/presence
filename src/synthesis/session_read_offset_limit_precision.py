"""Session Read tool offset/limit precision analyzer.

Analyzes Read tool offset/limit usage precision in Claude Code sessions to measure
targeted vs exploratory reading patterns. Tracks how effectively the agent uses
offset and limit parameters to read specific sections vs full files.

Read precision metrics:
- Total Read calls: Number of times Read tool was invoked
- Offset/limit usage rate: Percentage of reads using offset or limit parameters
- Average limit value: Mean limit value when limit parameter is used
- Negative offset usage rate: Percentage of reads using negative offsets (tail reads)
- Average lines read: Mean number of lines read per Read call
- Targeted read rate: Percentage of reads with limit <100 (focused reads)
- Full-file read rate: Percentage of reads with no offset/limit (exploratory reads)

Quality indicators:
- High offset/limit usage (>85%): Good targeted reading discipline
- Low average limit (<100): Focused, precise reads
- High negative offset usage (>20%): Good tail-read pattern for verification
- Low full-file rate (<15%): Minimal exploratory reading overhead
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_read_offset_limit_precision(records: object) -> dict[str, Any]:
    """Analyze Read tool offset/limit usage precision in Claude Code sessions.

    Evaluates reading precision through offset/limit parameter usage patterns,
    measuring targeted vs exploratory reading effectiveness.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_read_calls: Number of Read tool invocations
            - reads_with_offset_or_limit: Reads using offset or limit params
            - reads_with_limit: Reads using limit parameter
            - total_limit_value: Sum of all limit values used
            - reads_with_negative_offset: Reads using negative offset (tail)
            - total_lines_read: Total lines read across all calls
            - targeted_reads: Reads with limit <100
            - full_file_reads: Reads with no offset/limit
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_reads: Count of sessions using Read tool
            - avg_read_calls: Average Read invocations per session
            - avg_offset_limit_usage_rate: Average % using offset or limit
            - avg_limit_value: Average limit value when used
            - avg_negative_offset_rate: Average % using negative offset
            - avg_lines_read_per_call: Average lines per Read call
            - avg_targeted_read_rate: Average % of targeted reads (limit <100)
            - avg_full_file_read_rate: Average % of full-file reads
            - high_precision_sessions: Count of sessions with >85% offset/limit usage
            - low_precision_sessions: Count of sessions with <50% offset/limit usage

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_reads = 0

    read_calls: list[int | float] = []
    offset_limit_usage_rates: list[float] = []
    limit_values: list[float] = []
    negative_offset_rates: list[float] = []
    lines_per_call: list[float] = []
    targeted_read_rates: list[float] = []
    full_file_read_rates: list[float] = []

    high_precision_sessions = 0  # >85% offset/limit usage
    low_precision_sessions = 0   # <50% offset/limit usage

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_reads = _extract_int(record.get("total_read_calls"))
        reads_with_offset_limit = _extract_int(record.get("reads_with_offset_or_limit"))
        reads_with_limit = _extract_int(record.get("reads_with_limit"))
        total_limit = _extract_int(record.get("total_limit_value"))
        negative_offset_reads = _extract_int(record.get("reads_with_negative_offset"))
        total_lines = _extract_int(record.get("total_lines_read"))
        targeted = _extract_int(record.get("targeted_reads"))
        full_file = _extract_int(record.get("full_file_reads"))

        # Track sessions using Read
        if total_reads is not None and total_reads > 0:
            sessions_with_reads += 1
            read_calls.append(total_reads)

            # Calculate offset/limit usage rate
            if reads_with_offset_limit is not None:
                usage_rate = _percentage(reads_with_offset_limit, total_reads)
                offset_limit_usage_rates.append(usage_rate)

                # Classify precision quality
                if usage_rate > 85.0:
                    high_precision_sessions += 1
                elif usage_rate < 50.0:
                    low_precision_sessions += 1

            # Calculate average limit value
            if reads_with_limit is not None and reads_with_limit > 0 and total_limit is not None:
                avg_limit = total_limit / reads_with_limit
                limit_values.append(avg_limit)

            # Calculate negative offset rate
            if negative_offset_reads is not None:
                negative_offset_rates.append(
                    _percentage(negative_offset_reads, total_reads)
                )

            # Calculate average lines per call
            if total_lines is not None:
                lines_per_call.append(total_lines / total_reads)

            # Calculate targeted read rate
            if targeted is not None:
                targeted_read_rates.append(_percentage(targeted, total_reads))

            # Calculate full-file read rate
            if full_file is not None:
                full_file_read_rates.append(_percentage(full_file, total_reads))

    # Calculate aggregate metrics
    avg_reads = _average(read_calls)
    avg_offset_limit = _average(offset_limit_usage_rates)
    avg_limit = _average(limit_values)
    avg_negative = _average(negative_offset_rates)
    avg_lines = _average(lines_per_call)
    avg_targeted = _average(targeted_read_rates)
    avg_full_file = _average(full_file_read_rates)

    return {
        "total_sessions": total_sessions,
        "sessions_with_reads": sessions_with_reads,
        "avg_read_calls": avg_reads,
        "avg_offset_limit_usage_rate": avg_offset_limit,
        "avg_limit_value": avg_limit,
        "avg_negative_offset_rate": avg_negative,
        "avg_lines_read_per_call": avg_lines,
        "avg_targeted_read_rate": avg_targeted,
        "avg_full_file_read_rate": avg_full_file,
        "high_precision_sessions": high_precision_sessions,
        "low_precision_sessions": low_precision_sessions,
    }


def _extract_int(value: object) -> int | None:
    """Extract integer from value if available."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
