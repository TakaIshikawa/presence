"""Session cache hit ratio analyzer for workflow efficiency reports.

Tracks cache usage patterns in agent sessions to measure caching effectiveness
and identify missed optimization opportunities. Effective caching reduces token
consumption and improves session performance.

Cache operations:
- /cache query <file>: Check cache for file
- /cache snapshot <file>: Store file summary in cache
- /cache clear: Clear cache

Cache effectiveness indicators:
- High cache hit rate (queries that find cached data)
- Reduced re-reads for cached files
- Correlation between cache usage and token efficiency
- Few missed opportunities (repeated full reads without caching)
"""

from __future__ import annotations

from typing import Any, Mapping


# Cache command types
CACHE_QUERY = 'query'
CACHE_SNAPSHOT = 'snapshot'
CACHE_CLEAR = 'clear'


def analyze_session_cache_hit_ratio(records: object) -> dict[str, Any]:
    """Analyze cache usage patterns and effectiveness in a session.

    Evaluates how effectively the agent uses caching to reduce redundant
    file reads and improve token efficiency.

    Args:
        records: List of cache/read operation dictionaries with keys:
            - operation_type: 'cache_query', 'cache_snapshot', 'cache_clear', or 'read'
            - file_path: File path (for cache queries/snapshots and reads)
            - cache_hit: Boolean indicating if cache query hit (for cache_query only)
            - turn_index: Turn number when operation occurred
            - bytes_read: Number of bytes read (for read operations only)

    Returns:
        Dict with:
            - total_cache_queries: Total cache query attempts
            - cache_hits: Number of successful cache hits
            - cache_misses: Number of cache misses
            - cache_snapshots: Number of cache snapshot operations
            - cache_clears: Number of cache clear operations
            - cache_hit_rate: Percentage of queries that hit cache
            - total_file_reads: Total file read operations
            - reads_of_cached_files: Number of reads for files in cache
            - reads_of_uncached_files: Number of reads for files not in cache
            - repeated_full_reads: Files read multiple times without caching
            - missed_caching_opportunities: Count of missed opportunities
            - avg_bytes_per_cached_read: Average bytes for cached file reads
            - avg_bytes_per_uncached_read: Average bytes for uncached reads
            - cache_effectiveness_score: Overall effectiveness (0-100)
            - examples: Sample operations and patterns

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of operation dictionaries")

    total_cache_queries = 0
    cache_hits = 0
    cache_misses = 0
    cache_snapshots = 0
    cache_clears = 0
    total_file_reads = 0
    reads_of_cached_files = 0
    reads_of_uncached_files = 0
    repeated_full_reads = 0
    missed_caching_opportunities = 0

    # Track which files are in cache
    cached_files: set[str] = set()
    # Track file read counts for missed opportunity detection
    file_read_counts: dict[str, int] = {}
    file_read_bytes: dict[str, list[int]] = {}
    # Track bytes for cache effectiveness
    cached_read_bytes: list[int] = []
    uncached_read_bytes: list[int] = []

    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        operation_type = _string(record.get('operation_type'))
        file_path = _string(record.get('file_path'))
        turn_index = record.get('turn_index', index)

        if operation_type == 'cache_query':
            total_cache_queries += 1
            cache_hit = record.get('cache_hit') is True

            if cache_hit:
                cache_hits += 1
                if len(examples) < 5:
                    examples.append({
                        'turn_index': turn_index,
                        'operation': 'cache_hit',
                        'file_path': file_path,
                        'description': 'Successful cache query',
                    })
            else:
                cache_misses += 1

        elif operation_type == 'cache_snapshot':
            cache_snapshots += 1
            if file_path:
                cached_files.add(file_path)
            if len(examples) < 5:
                examples.append({
                    'turn_index': turn_index,
                    'operation': 'cache_snapshot',
                    'file_path': file_path,
                    'description': 'File cached for future use',
                })

        elif operation_type == 'cache_clear':
            cache_clears += 1
            cached_files.clear()

        elif operation_type == 'read':
            total_file_reads += 1
            bytes_read = _number(record.get('bytes_read'))

            # Track read counts
            if file_path:
                file_read_counts[file_path] = file_read_counts.get(file_path, 0) + 1
                if file_path not in file_read_bytes:
                    file_read_bytes[file_path] = []
                if bytes_read is not None:
                    file_read_bytes[file_path].append(bytes_read)

            # Determine if this read is for a cached file
            if file_path in cached_files:
                reads_of_cached_files += 1
                if bytes_read is not None:
                    cached_read_bytes.append(bytes_read)
            else:
                reads_of_uncached_files += 1
                if bytes_read is not None:
                    uncached_read_bytes.append(bytes_read)

                # Check for missed caching opportunity
                # If this is the 2nd+ read of the same file without caching
                if file_path and file_read_counts.get(file_path, 0) > 1:
                    missed_caching_opportunities += 1
                    if len(examples) < 5:
                        examples.append({
                            'turn_index': turn_index,
                            'operation': 'missed_opportunity',
                            'file_path': file_path,
                            'description': f'File read {file_read_counts[file_path]} times without caching',
                        })

    # Calculate repeated full reads (files read 2+ times)
    repeated_full_reads = sum(1 for count in file_read_counts.values() if count > 1)

    # Calculate metrics
    cache_hit_rate = _percentage(cache_hits, total_cache_queries)

    avg_bytes_per_cached_read = (
        round(sum(cached_read_bytes) / len(cached_read_bytes), 2)
        if cached_read_bytes else 0.0
    )
    avg_bytes_per_uncached_read = (
        round(sum(uncached_read_bytes) / len(uncached_read_bytes), 2)
        if uncached_read_bytes else 0.0
    )

    # Calculate overall effectiveness score (0-100)
    cache_effectiveness_score = _calculate_cache_effectiveness_score(
        cache_hit_rate=cache_hit_rate,
        cache_usage_rate=_percentage(cache_snapshots, total_file_reads),
        missed_opportunity_rate=_percentage(missed_caching_opportunities, total_file_reads),
    )

    return {
        'total_cache_queries': total_cache_queries,
        'cache_hits': cache_hits,
        'cache_misses': cache_misses,
        'cache_snapshots': cache_snapshots,
        'cache_clears': cache_clears,
        'cache_hit_rate': cache_hit_rate,
        'total_file_reads': total_file_reads,
        'reads_of_cached_files': reads_of_cached_files,
        'reads_of_uncached_files': reads_of_uncached_files,
        'repeated_full_reads': repeated_full_reads,
        'missed_caching_opportunities': missed_caching_opportunities,
        'avg_bytes_per_cached_read': avg_bytes_per_cached_read,
        'avg_bytes_per_uncached_read': avg_bytes_per_uncached_read,
        'cache_effectiveness_score': cache_effectiveness_score,
        'examples': examples[:5],
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


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


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _calculate_cache_effectiveness_score(
    cache_hit_rate: float,
    cache_usage_rate: float,
    missed_opportunity_rate: float,
) -> float:
    """Calculate overall cache effectiveness score (0-100).

    Args:
        cache_hit_rate: Percentage of cache queries that hit
        cache_usage_rate: Percentage of reads that use cache snapshots
        missed_opportunity_rate: Percentage of reads that are missed opportunities

    Returns:
        Score from 0-100 indicating cache effectiveness
    """
    # Weight different factors
    # High cache hit rate is good
    # High cache usage rate is good
    # High missed opportunity rate is bad
    score = (
        cache_hit_rate * 0.40 +
        cache_usage_rate * 0.35 -
        missed_opportunity_rate * 0.25
    )

    # Ensure score stays in 0-100 range
    return round(max(0.0, min(100.0, score)), 2)
