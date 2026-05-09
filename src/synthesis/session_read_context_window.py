"""Session Read tool context window analyzer for read efficiency.

Analyzes Read tool usage patterns to measure context window efficiency and
targeted reading strategies. Tracks usage of offset/limit parameters versus
full-file reads to identify optimization opportunities.

Context window metrics:
- Average lines read per Read call: Mean context window size
- Targeted read percentage: Ratio of reads using offset/limit vs full reads
- Context window distribution: Breakdown of window sizes
- Full-file read frequency: How often entire files are read
- Targeted read efficiency: Effectiveness of focused reading

Optimization indicators:
- High targeted read percentage: Efficient use of offset/limit parameters
- Small average window size: Focused reading patterns
- Strategic full reads: Full reads used appropriately for exploration
- Large window sizes: Potential over-reading that could be optimized
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_read_context_window(records: object) -> dict[str, Any]:
    """Analyze Read tool context window usage and efficiency.

    Tracks Read tool calls, measures context window sizes (lines read),
    and identifies patterns of targeted vs full-file reads.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Read, etc.)
            - file_path: Path to file being read
            - offset: Optional starting line for read (targeted)
            - limit: Optional line count limit for read (targeted)
            - lines_read: Optional actual number of lines read
            - turn_index: Turn number when tool was invoked

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls analyzed
            - read_call_count: Number of Read tool calls
            - targeted_read_count: Reads using offset/limit parameters
            - full_read_count: Reads without offset/limit (full-file)
            - targeted_read_percentage: Percentage of targeted reads
            - avg_lines_per_read: Average lines read per Read call
            - avg_lines_targeted_read: Average lines for targeted reads
            - avg_lines_full_read: Average lines for full reads
            - window_size_distribution: Breakdown by size category
            - small_window_reads: Reads with < 50 lines
            - medium_window_reads: Reads with 50-200 lines
            - large_window_reads: Reads with > 200 lines

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    read_call_count = 0
    targeted_read_count = 0
    full_read_count = 0

    all_lines_read: list[int | float] = []
    targeted_lines_read: list[int | float] = []
    full_lines_read: list[int | float] = []

    small_window_reads = 0  # < 50 lines
    medium_window_reads = 0  # 50-200 lines
    large_window_reads = 0  # > 200 lines

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        total_tool_calls += 1
        tool_lower = tool_name.lower()

        if tool_lower == "read":
            read_call_count += 1

            # Determine if this is a targeted or full read
            offset = record.get("offset")
            limit = record.get("limit")
            has_offset = offset is not None
            has_limit = limit is not None

            is_targeted = has_offset or has_limit

            if is_targeted:
                targeted_read_count += 1
            else:
                full_read_count += 1

            # Extract lines read count
            lines_read = _extract_lines_read(record)
            if lines_read is not None:
                all_lines_read.append(lines_read)

                if is_targeted:
                    targeted_lines_read.append(lines_read)
                else:
                    full_lines_read.append(lines_read)

                # Categorize window size
                if lines_read < 50:
                    small_window_reads += 1
                elif lines_read <= 200:
                    medium_window_reads += 1
                else:
                    large_window_reads += 1

    # Calculate metrics
    targeted_read_pct = _percentage(targeted_read_count, read_call_count)
    avg_lines_per_read = _average(all_lines_read)
    avg_lines_targeted = _average(targeted_lines_read)
    avg_lines_full = _average(full_lines_read)

    # Window size distribution
    total_categorized = small_window_reads + medium_window_reads + large_window_reads
    window_size_distribution = {
        "small_window_percentage": _percentage(small_window_reads, total_categorized),
        "medium_window_percentage": _percentage(medium_window_reads, total_categorized),
        "large_window_percentage": _percentage(large_window_reads, total_categorized),
    }

    return {
        "total_tool_calls": total_tool_calls,
        "read_call_count": read_call_count,
        "targeted_read_count": targeted_read_count,
        "full_read_count": full_read_count,
        "targeted_read_percentage": targeted_read_pct,
        "avg_lines_per_read": avg_lines_per_read,
        "avg_lines_targeted_read": avg_lines_targeted,
        "avg_lines_full_read": avg_lines_full,
        "window_size_distribution": window_size_distribution,
        "small_window_reads": small_window_reads,
        "medium_window_reads": medium_window_reads,
        "large_window_reads": large_window_reads,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_lines_read(record: Mapping[str, Any]) -> int | None:
    """Extract lines read count from record if available."""
    lines_read = record.get("lines_read")
    if isinstance(lines_read, int) and not isinstance(lines_read, bool):
        return lines_read
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
