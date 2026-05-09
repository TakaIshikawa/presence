"""Session tool result size distribution analyzer for result efficiency.

Analyzes the distribution of tool result sizes across a session to identify
patterns in result verbosity and potential optimization opportunities. Large
tool results consume more tokens and may indicate inefficient tool usage.

Result size buckets (in KB):
- <10 KB: Small results (efficient)
- 10-50 KB: Medium results (moderate)
- 50-100 KB: Large results (less efficient)
- 100+ KB: Oversized results (potentially inefficient)

Result size patterns:
- Compact results: >75% results under 10 KB
- Moderate results: 50-75% results under 50 KB
- Mixed results: Balanced distribution
- Verbose results: >50% results over 50 KB
"""

from __future__ import annotations

from typing import Any, Mapping

# Size bucket thresholds (in KB)
BUCKET_SMALL_KB = 10
BUCKET_MEDIUM_KB = 50
BUCKET_LARGE_KB = 100

# Oversized result threshold
OVERSIZED_THRESHOLD_KB = 100


def analyze_session_tool_result_size_distribution(records: object) -> dict[str, Any]:
    """Analyze distribution of tool result sizes across a session.

    Categorizes tool results into size buckets and identifies oversized
    results that may indicate inefficient tool usage.

    Args:
        records: List of tool result dictionaries with keys:
            - tool_name: Name of the tool executed
            - result_size_bytes: Size of tool result in bytes
            - turn_index: Turn number when result was returned
            - tool_call_id: Optional tool call identifier

    Returns:
        Dict with:
            - total_results: Total number of tool results
            - result_size_histogram: Dict mapping KB ranges to counts
            - oversized_results: List of results exceeding 100KB
            - average_result_size: Average result size in KB
            - median_result_size: Median result size in KB
            - p95_result_size: 95th percentile result size in KB
            - tools_by_avg_result_size: Dict of tools ranked by average size

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool result dictionaries")

    histogram = {
        "0_10kb": 0,
        "10_50kb": 0,
        "50_100kb": 0,
        "100kb_plus": 0,
    }

    oversized_results: list[dict[str, Any]] = []
    result_sizes_kb: list[float] = []
    tool_sizes: dict[str, list[float]] = {}

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        result_size_bytes = _number(record.get("result_size_bytes"))
        if result_size_bytes is None or result_size_bytes < 0:
            continue

        result_size_kb = result_size_bytes / 1024.0
        result_sizes_kb.append(result_size_kb)

        # Categorize into histogram bucket
        bucket = _categorize_kb_bucket(result_size_kb)
        histogram[bucket] += 1

        # Track oversized results
        if result_size_kb > OVERSIZED_THRESHOLD_KB:
            tool_name = str(record.get("tool_name", "unknown"))
            turn_index = record.get("turn_index", index)
            oversized_results.append({
                "tool_name": tool_name,
                "result_size_kb": round(result_size_kb, 2),
                "turn_index": turn_index,
            })

        # Track by tool for ranking
        tool_name = str(record.get("tool_name", "unknown"))
        if tool_name not in tool_sizes:
            tool_sizes[tool_name] = []
        tool_sizes[tool_name].append(result_size_kb)

    # Calculate statistics
    total_results = len(result_sizes_kb)
    avg_size = _average(result_sizes_kb)
    median_size = _median(result_sizes_kb)
    p95_size = _percentile(result_sizes_kb, 95)

    # Rank tools by average result size
    tools_by_avg_size = {
        tool: round(sum(sizes) / len(sizes), 2)
        for tool, sizes in tool_sizes.items()
    }
    # Sort by average size descending
    tools_by_avg_size = dict(
        sorted(tools_by_avg_size.items(), key=lambda x: x[1], reverse=True)
    )

    return {
        "total_results": total_results,
        "result_size_histogram": histogram,
        "oversized_results": oversized_results[:20],  # Limit to 20 examples
        "average_result_size": avg_size,
        "median_result_size": median_size,
        "p95_result_size": p95_size,
        "tools_by_avg_result_size": tools_by_avg_size,
    }


def _number(value: object) -> int | None:
    """Extract integer from value, handling various types."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _categorize_kb_bucket(size_kb: float) -> str:
    """Categorize result size into a histogram bucket."""
    if size_kb < BUCKET_SMALL_KB:
        return "0_10kb"
    elif size_kb < BUCKET_MEDIUM_KB:
        return "10_50kb"
    elif size_kb < BUCKET_LARGE_KB:
        return "50_100kb"
    else:
        return "100kb_plus"


def _average(values: list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _median(values: list[float]) -> float:
    """Calculate median of numeric values."""
    if not values:
        return 0.0
    sorted_values = sorted(values)
    n = len(sorted_values)
    if n % 2 == 0:
        return round((sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2, 2)
    else:
        return round(sorted_values[n // 2], 2)


def _percentile(values: list[float], percentile: int) -> float:
    """Calculate percentile of numeric values."""
    if not values:
        return 0.0
    sorted_values = sorted(values)
    # Use linear interpolation for percentile calculation
    rank = (percentile / 100.0) * (len(sorted_values) - 1)
    index = int(rank)
    if index >= len(sorted_values) - 1:
        return round(sorted_values[-1], 2)
    # Interpolate between two adjacent values
    fraction = rank - index
    value = sorted_values[index] + fraction * (sorted_values[index + 1] - sorted_values[index])
    return round(value, 2)
