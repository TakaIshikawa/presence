"""Pack Read-cache integration and query-before-read discipline analyzer.

Analyzes execution pack transcripts for cache tool integration with Read operations,
measuring query-before-read discipline, cache snapshot adoption, offset/limit usage
after cache hits, cache effectiveness, and anti-pattern detection.

Cache-Read integration metrics:
1. Cache query usage before repeated reads:
   - /cache query invocations before Read calls
   - Prevents redundant full-file reads

2. Cache snapshot usage:
   - /cache snapshot after full reads
   - Enables future targeted access

3. Read offset/limit adoption:
   - Targeted reads after cache hits
   - Reduces token consumption

4. Cache effectiveness:
   - Query hit rate
   - Cache-enabled targeted read rate

5. Anti-pattern detection:
   - Re-reading recently cached files without query
   - Full reads when cache data available
   - Never snapshotting after full reads

Quality indicators:
- High cache query rate before reads (>70%)
- High snapshot rate after full reads (>80%)
- Offset/limit adoption after cache hits (>85%)
- Cache hit rate (>60%)
- Low anti-pattern detection (<10%)
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_read_cache_integration(records: object) -> dict[str, Any]:
    """Analyze cache-Read integration patterns across pack transcripts.

    Args:
        records: List of session dictionaries with keys:
            - cache_queries_before_read: Number of cache queries before Read
            - cache_snapshots_after_read: Number of snapshots after full reads
            - reads_with_offset_limit_after_cache: Targeted reads after cache hits
            - cache_hit_count: Number of successful cache hits
            - cache_miss_count: Number of cache misses
            - reads_without_prior_query: Reads without checking cache first
            - full_reads_with_cache_available: Full reads when cache has data
            - full_reads_without_snapshot: Full reads not followed by snapshot

    Returns:
        Dict with:
            - total_sessions: Number of sessions analyzed
            - total_cache_queries: Total cache query invocations
            - total_cache_snapshots: Total cache snapshots created
            - reads_with_offset_limit: Targeted reads after cache hits
            - cache_query_before_read_rate: Percentage of reads with prior query
            - cache_snapshot_after_read_rate: Percentage of full reads with snapshot
            - offset_limit_adoption_rate: Percentage using offset/limit after cache
            - cache_hit_rate: Percentage of cache queries that hit
            - anti_pattern_reads_without_query: Reads skipping cache check
            - anti_pattern_full_reads_with_cache: Full reads when cache available
            - anti_pattern_no_snapshot: Full reads without snapshot
            - anti_pattern_rate: Overall anti-pattern percentage
            - effectiveness_score: Overall cache-Read integration score (0-1)

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
    total_cache_queries = 0
    total_cache_snapshots = 0
    reads_with_offset_limit = 0
    cache_hit_count = 0
    cache_miss_count = 0
    reads_without_prior_query = 0
    full_reads_with_cache_available = 0
    full_reads_without_snapshot = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        queries = _int(record.get("cache_queries_before_read", 0))
        snapshots = _int(record.get("cache_snapshots_after_read", 0))
        offset_limit_reads = _int(record.get("reads_with_offset_limit_after_cache", 0))
        hits = _int(record.get("cache_hit_count", 0))
        misses = _int(record.get("cache_miss_count", 0))
        no_query = _int(record.get("reads_without_prior_query", 0))
        full_with_cache = _int(record.get("full_reads_with_cache_available", 0))
        no_snapshot = _int(record.get("full_reads_without_snapshot", 0))

        total_cache_queries += queries
        total_cache_snapshots += snapshots
        reads_with_offset_limit += offset_limit_reads
        cache_hit_count += hits
        cache_miss_count += misses
        reads_without_prior_query += no_query
        full_reads_with_cache_available += full_with_cache
        full_reads_without_snapshot += no_snapshot

    # Calculate rates
    total_cache_attempts = cache_hit_count + cache_miss_count
    cache_hit_rate = _percentage(cache_hit_count, total_cache_attempts)

    total_reads = total_cache_queries + reads_without_prior_query
    cache_query_before_read_rate = _percentage(total_cache_queries, total_reads)

    total_full_reads = total_cache_snapshots + full_reads_without_snapshot
    cache_snapshot_after_read_rate = _percentage(
        total_cache_snapshots, total_full_reads
    )

    offset_limit_adoption_rate = _percentage(
        reads_with_offset_limit, cache_hit_count
    )

    # Anti-pattern metrics
    total_anti_patterns = (
        reads_without_prior_query
        + full_reads_with_cache_available
        + full_reads_without_snapshot
    )
    total_read_operations = total_reads + total_full_reads
    anti_pattern_rate = _percentage(total_anti_patterns, total_read_operations)

    # Effectiveness score (0-1)
    effectiveness_score = _calculate_effectiveness_score(
        cache_query_before_read_rate,
        cache_snapshot_after_read_rate,
        offset_limit_adoption_rate,
        cache_hit_rate,
        anti_pattern_rate,
    )

    return {
        "total_sessions": total_sessions,
        "total_cache_queries": total_cache_queries,
        "total_cache_snapshots": total_cache_snapshots,
        "reads_with_offset_limit": reads_with_offset_limit,
        "cache_query_before_read_rate": cache_query_before_read_rate,
        "cache_snapshot_after_read_rate": cache_snapshot_after_read_rate,
        "offset_limit_adoption_rate": offset_limit_adoption_rate,
        "cache_hit_rate": cache_hit_rate,
        "anti_pattern_reads_without_query": reads_without_prior_query,
        "anti_pattern_full_reads_with_cache": full_reads_with_cache_available,
        "anti_pattern_no_snapshot": full_reads_without_snapshot,
        "anti_pattern_rate": anti_pattern_rate,
        "effectiveness_score": effectiveness_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "total_cache_queries": 0,
        "total_cache_snapshots": 0,
        "reads_with_offset_limit": 0,
        "cache_query_before_read_rate": 0.0,
        "cache_snapshot_after_read_rate": 0.0,
        "offset_limit_adoption_rate": 0.0,
        "cache_hit_rate": 0.0,
        "anti_pattern_reads_without_query": 0,
        "anti_pattern_full_reads_with_cache": 0,
        "anti_pattern_no_snapshot": 0,
        "anti_pattern_rate": 0.0,
        "effectiveness_score": 0.0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _calculate_effectiveness_score(
    query_rate: float,
    snapshot_rate: float,
    offset_adoption_rate: float,
    hit_rate: float,
    anti_pattern_rate: float,
) -> float:
    """Calculate overall cache-Read integration effectiveness score (0-1).

    Scoring components:
    - Query rate (0-0.25): How often cache is queried before reads
    - Snapshot rate (0-0.25): How often snapshots are created after reads
    - Offset adoption (0-0.20): Targeted reads after cache hits
    - Hit rate (0-0.15): Cache effectiveness
    - Anti-pattern penalty (0-0.15): Deduction for anti-patterns

    Returns:
        Effectiveness score from 0.0 to 1.0
    """
    # Query rate component (0-0.25)
    query_component = (query_rate / 100.0) * 0.25

    # Snapshot rate component (0-0.25)
    snapshot_component = (snapshot_rate / 100.0) * 0.25

    # Offset adoption component (0-0.20)
    offset_component = (offset_adoption_rate / 100.0) * 0.20

    # Hit rate component (0-0.15)
    hit_component = (hit_rate / 100.0) * 0.15

    # Anti-pattern penalty (0-0.15)
    # Lower anti-pattern rate = higher score
    if anti_pattern_rate <= 10.0:
        anti_pattern_component = 0.15
    else:
        penalty = min(anti_pattern_rate - 10.0, 90.0) / 90.0
        anti_pattern_component = 0.15 * (1.0 - penalty)

    score = (
        query_component
        + snapshot_component
        + offset_component
        + hit_component
        + anti_pattern_component
    )
    return round(max(0.0, min(1.0, score)), 3)
