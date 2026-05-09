"""Pack cache hit rate analyzer for caching effectiveness.

Analyzes cache command usage and effectiveness in execution packs. Measures
cache query frequency, hit rate (queries returning cached data vs misses),
cache snapshot coverage (files cached vs files read), and correlation between
cache usage and token efficiency.

Cache metrics:
- Cache query frequency: How often cache queries are performed
- Cache hit rate: Ratio of queries with cached data vs misses
- Cache snapshot coverage: Percentage of files cached vs total files read
- Cache-to-read ratio: Balance of cache queries to Read tool calls
- Token efficiency correlation: Relationship between caching and token usage

Optimization indicators:
- High hit rate: Effective cache usage reducing redundant reads
- Good coverage: Strategic caching of frequently accessed files
- Balanced query ratio: Not over-querying or under-utilizing cache
- Token efficiency: Lower token usage correlates with higher cache usage
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_cache_hit_rate(records: object) -> dict[str, Any]:
    """Analyze cache command usage and effectiveness in execution packs.

    Tracks cache queries, measures hit rate, calculates coverage, and
    identifies correlation between cache usage and token efficiency.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - cache_query_count: Number of cache queries performed
            - cache_hit_count: Number of queries returning cached data
            - cache_snapshot_count: Number of files cached via snapshots
            - total_files_read: Total number of unique files read
            - read_tool_count: Total number of Read tool calls
            - total_tokens: Optional total token usage for pack
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_cache_query_frequency: Average cache queries per pack
            - avg_cache_hit_rate: Average hit rate across packs
            - avg_cache_coverage: Average snapshot coverage percentage
            - avg_cache_to_read_ratio: Average cache queries to Read ratio
            - high_hit_rate_packs: Count of packs with >80% hit rate
            - low_hit_rate_packs: Count of packs with <20% hit rate
            - no_cache_packs: Count of packs with no cache usage
            - token_efficiency_correlation: Cache usage vs token efficiency

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    cache_query_frequencies: list[int | float] = []
    cache_hit_rates: list[float] = []
    cache_coverages: list[float] = []
    cache_to_read_ratios: list[float] = []

    high_hit_rate_packs = 0  # > 80% hit rate
    low_hit_rate_packs = 0   # < 20% hit rate
    no_cache_packs = 0       # No cache usage

    # For correlation analysis
    cache_usages: list[float] = []
    token_efficiencies: list[float] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        cache_query_count = _extract_int(record.get("cache_query_count"))
        cache_hit_count = _extract_int(record.get("cache_hit_count"))
        cache_snapshot_count = _extract_int(record.get("cache_snapshot_count"))
        total_files_read = _extract_int(record.get("total_files_read"))
        read_tool_count = _extract_int(record.get("read_tool_count"))
        total_tokens = _extract_int(record.get("total_tokens"))

        total_packs += 1

        # Track query frequency
        if cache_query_count is not None:
            cache_query_frequencies.append(cache_query_count)

            if cache_query_count == 0:
                no_cache_packs += 1
            else:
                # Calculate hit rate
                if cache_hit_count is not None and cache_query_count > 0:
                    hit_rate = _percentage(cache_hit_count, cache_query_count)
                    cache_hit_rates.append(hit_rate)

                    if hit_rate > 80.0:
                        high_hit_rate_packs += 1
                    elif hit_rate < 20.0:
                        low_hit_rate_packs += 1

        # Calculate cache coverage
        if cache_snapshot_count is not None and total_files_read is not None and total_files_read > 0:
            coverage = _percentage(cache_snapshot_count, total_files_read)
            cache_coverages.append(coverage)

        # Calculate cache-to-read ratio
        if cache_query_count is not None and read_tool_count is not None and read_tool_count > 0:
            ratio = _percentage(cache_query_count, read_tool_count)
            cache_to_read_ratios.append(ratio)

        # Track for correlation analysis
        if cache_query_count is not None and total_tokens is not None:
            cache_usages.append(float(cache_query_count))
            # Token efficiency: lower tokens = higher efficiency
            # Normalize by some baseline (e.g., 10000 tokens)
            if total_tokens > 0:
                token_efficiencies.append(10000.0 / total_tokens)

    # Calculate metrics
    avg_query_frequency = _average(cache_query_frequencies)
    avg_hit_rate = _average(cache_hit_rates)
    avg_coverage = _average(cache_coverages)
    avg_cache_to_read = _average(cache_to_read_ratios)

    # Calculate correlation
    correlation = _calculate_correlation(cache_usages, token_efficiencies)

    return {
        "total_packs": total_packs,
        "avg_cache_query_frequency": avg_query_frequency,
        "avg_cache_hit_rate": avg_hit_rate,
        "avg_cache_coverage": avg_coverage,
        "avg_cache_to_read_ratio": avg_cache_to_read,
        "high_hit_rate_packs": high_hit_rate_packs,
        "low_hit_rate_packs": low_hit_rate_packs,
        "no_cache_packs": no_cache_packs,
        "token_efficiency_correlation": correlation,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


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


def _calculate_correlation(x_values: list[float], y_values: list[float]) -> float:
    """Calculate Pearson correlation coefficient.

    Returns correlation between -1.0 (negative) and 1.0 (positive).
    Returns 0.0 if insufficient data or no variance.
    """
    if not x_values or not y_values or len(x_values) != len(y_values):
        return 0.0

    n = len(x_values)
    if n < 2:
        return 0.0

    # Calculate means
    mean_x = sum(x_values) / n
    mean_y = sum(y_values) / n

    # Calculate covariance and standard deviations
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, y_values))
    std_x = (sum((x - mean_x) ** 2 for x in x_values)) ** 0.5
    std_y = (sum((y - mean_y) ** 2 for y in y_values)) ** 0.5

    # Avoid division by zero
    if std_x == 0 or std_y == 0:
        return 0.0

    correlation = covariance / (std_x * std_y)
    return round(correlation, 3)
