"""Session Read tool offset/limit usage analyzer for targeted read patterns.

Analyzes usage patterns of offset and limit parameters in Read tool calls to
measure targeted reading efficiency. This is a core metric for optimization
mode tracking, as Run #1 showed 87% targeted reads achieved 58% token savings.

Targeted read metrics:
- Percentage using offset/limit: Ratio of targeted vs full-file reads
- Average lines read per call: Mean read size across all operations
- Read size distribution: Buckets showing read pattern distribution
- Targeted vs full-file ratio: Balance of focused vs exploratory reads

Optimization indicators:
- High offset/limit usage (85%+): Excellent targeted read adoption
- Low average lines (<70): Efficient focused reading patterns
- Small read sizes (<50 lines): Highly optimized read strategy
- Large reads (200+ lines): Potential optimization opportunities
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_read_offset_limit_usage(records: object) -> dict[str, Any]:
    """Analyze Read tool offset/limit parameter usage and read size patterns.

    Tracks Read tool calls, measures usage of offset/limit parameters for
    targeted reads, and analyzes read size distribution to identify
    optimization patterns.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Read, etc.)
            - file_path: Path to file being read
            - offset: Optional starting line for read (negative for tail)
            - limit: Optional line count limit for read
            - lines_read: Optional actual number of lines read
            - turn_index: Turn number when tool was invoked

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls analyzed
            - read_call_count: Number of Read tool calls
            - targeted_read_count: Reads using offset or limit parameters
            - full_read_count: Reads without offset or limit (full-file)
            - targeted_read_percentage: Percentage of targeted reads
            - avg_lines_per_read: Average lines read per Read call
            - avg_lines_targeted_read: Average lines for targeted reads
            - avg_lines_full_read: Average lines for full reads
            - read_size_distribution: Dict with buckets <50, 50-100, 100-200, 200+
            - reads_with_offset: Count of reads using offset parameter
            - reads_with_limit: Count of reads using limit parameter
            - reads_with_both: Count of reads using both offset and limit
            - reads_with_negative_offset: Count using negative offset (tail reads)

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

    reads_with_offset = 0
    reads_with_limit = 0
    reads_with_both = 0
    reads_with_negative_offset = 0

    all_lines_read: list[int | float] = []
    targeted_lines_read: list[int | float] = []
    full_lines_read: list[int | float] = []

    # Read size distribution buckets
    bucket_under_50 = 0
    bucket_50_to_100 = 0
    bucket_100_to_200 = 0
    bucket_over_200 = 0

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

            # Extract offset and limit parameters
            offset = record.get("offset")
            limit = record.get("limit")

            has_offset = offset is not None
            has_limit = limit is not None
            is_targeted = has_offset or has_limit

            # Track parameter usage patterns
            if has_offset:
                reads_with_offset += 1
                # Check for negative offset (tail reads)
                if isinstance(offset, int) and offset < 0:
                    reads_with_negative_offset += 1

            if has_limit:
                reads_with_limit += 1

            if has_offset and has_limit:
                reads_with_both += 1

            # Count targeted vs full reads
            if is_targeted:
                targeted_read_count += 1
            else:
                full_read_count += 1

            # Extract lines read count and categorize
            lines_read = _extract_lines_read(record)
            if lines_read is not None:
                all_lines_read.append(lines_read)

                if is_targeted:
                    targeted_lines_read.append(lines_read)
                else:
                    full_lines_read.append(lines_read)

                # Categorize into distribution buckets
                if lines_read < 50:
                    bucket_under_50 += 1
                elif lines_read < 100:
                    bucket_50_to_100 += 1
                elif lines_read < 200:
                    bucket_100_to_200 += 1
                else:
                    bucket_over_200 += 1

    # Calculate metrics
    targeted_read_percentage = _percentage(targeted_read_count, read_call_count)
    avg_lines_per_read = _average(all_lines_read)
    avg_lines_targeted = _average(targeted_lines_read)
    avg_lines_full = _average(full_lines_read)

    # Calculate distribution percentages
    total_categorized = bucket_under_50 + bucket_50_to_100 + bucket_100_to_200 + bucket_over_200
    read_size_distribution = {
        "under_50_lines": bucket_under_50,
        "50_to_100_lines": bucket_50_to_100,
        "100_to_200_lines": bucket_100_to_200,
        "over_200_lines": bucket_over_200,
        "under_50_percentage": _percentage(bucket_under_50, total_categorized),
        "50_to_100_percentage": _percentage(bucket_50_to_100, total_categorized),
        "100_to_200_percentage": _percentage(bucket_100_to_200, total_categorized),
        "over_200_percentage": _percentage(bucket_over_200, total_categorized),
    }

    return {
        "total_tool_calls": total_tool_calls,
        "read_call_count": read_call_count,
        "targeted_read_count": targeted_read_count,
        "full_read_count": full_read_count,
        "targeted_read_percentage": targeted_read_percentage,
        "avg_lines_per_read": avg_lines_per_read,
        "avg_lines_targeted_read": avg_lines_targeted,
        "avg_lines_full_read": avg_lines_full,
        "read_size_distribution": read_size_distribution,
        "reads_with_offset": reads_with_offset,
        "reads_with_limit": reads_with_limit,
        "reads_with_both": reads_with_both,
        "reads_with_negative_offset": reads_with_negative_offset,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_lines_read(record: Mapping[str, Any]) -> int | None:
    """Extract lines read count from record if available.

    Handles both explicit lines_read field and inference from limit parameter.
    """
    lines_read = record.get("lines_read")
    if isinstance(lines_read, int) and not isinstance(lines_read, bool):
        return lines_read

    # If lines_read not available, try to infer from limit parameter
    limit = record.get("limit")
    if isinstance(limit, int) and not isinstance(limit, bool) and limit > 0:
        return limit

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
