"""Session cache command usage analyzer for cache skill adoption patterns.

Analyzes usage patterns of the cache skill in Claude Code sessions to measure
cache adoption and effectiveness. Tracks cache commands (snapshot, query, clear),
cache-query-before-read patterns, and cache hit inference.

Cache command metrics:
- Total cache commands: Frequency of cache skill invocations
- Command type breakdown: snapshot, query, clear distribution
- Cache-query-before-read pattern: Proactive cache checking before reads
- Cache hit inference: Estimated cache effectiveness from tool results

Optimization indicators:
- High cache query usage: Proactive cache checking before reads
- Cache-query-before-read pattern: Strategic cache-first workflow
- Snapshot commands after full reads: Building cache for future use
- Low cache usage: Missed optimization opportunities
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_session_cache_command_usage(records: object) -> dict[str, Any]:
    """Analyze cache skill usage patterns and cache-first workflow adoption.

    Tracks Skill tool calls for cache commands, measures command type
    distribution, and detects cache-query-before-read patterns.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Skill, Read, etc.)
            - skill: Skill name for Skill tool calls (e.g., "cache")
            - skill_args: Arguments passed to skill (e.g., "snapshot", "query <file>")
            - file_path: Path for Read tool calls
            - turn_index: Turn number when tool was invoked
            - tool_result: Optional result message from tool execution

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls analyzed
            - cache_command_count: Number of cache skill invocations
            - cache_snapshot_count: Count of cache snapshot commands
            - cache_query_count: Count of cache query commands
            - cache_clear_count: Count of cache clear commands
            - cache_query_before_read_count: Count of cache-query-before-read patterns
            - cache_query_before_read_percentage: Percentage of reads with prior cache query
            - cache_hit_inferred_count: Estimated cache hits from tool results
            - cache_miss_inferred_count: Estimated cache misses
            - command_type_distribution: Dict with breakdown by command type

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    cache_command_count = 0

    cache_snapshot_count = 0
    cache_query_count = 0
    cache_clear_count = 0

    cache_query_before_read_count = 0
    cache_hit_inferred_count = 0
    cache_miss_inferred_count = 0

    # Track recent cache queries for pattern detection
    recent_cache_queries: dict[str, int] = {}  # file_path -> turn_index
    read_tool_calls: list[tuple[str, int]] = []  # (file_path, turn_index)

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        total_tool_calls += 1
        tool_lower = tool_name.lower()
        turn_index = record.get("turn_index", 0)

        if tool_lower == "skill":
            skill_name = _string(record.get("skill"))

            if skill_name.lower() == "cache":
                cache_command_count += 1

                # Extract command type from skill_args
                skill_args = _string(record.get("skill_args", ""))
                command_type = _extract_cache_command_type(skill_args)

                if command_type == "snapshot":
                    cache_snapshot_count += 1
                elif command_type == "query":
                    cache_query_count += 1
                    # Track file path from query command
                    file_path = _extract_file_from_cache_query(skill_args)
                    if file_path:
                        recent_cache_queries[file_path] = turn_index
                elif command_type == "clear":
                    cache_clear_count += 1

                # Infer cache hits/misses from tool result
                tool_result = _string(record.get("tool_result", ""))
                if tool_result:
                    if _infer_cache_hit(tool_result):
                        cache_hit_inferred_count += 1
                    elif _infer_cache_miss(tool_result):
                        cache_miss_inferred_count += 1

        elif tool_lower == "read":
            file_path = _string(record.get("file_path", ""))
            if file_path:
                read_tool_calls.append((file_path, turn_index))

                # Check if there was a cache query within 2 tool calls before this read
                if file_path in recent_cache_queries:
                    query_turn = recent_cache_queries[file_path]
                    # Within 2 tool calls (loose proximity check)
                    if turn_index - query_turn <= 2:
                        cache_query_before_read_count += 1

    # Calculate metrics
    total_reads = len(read_tool_calls)
    cache_query_before_read_percentage = _percentage(
        cache_query_before_read_count,
        total_reads
    )

    command_type_distribution = {
        "snapshot": cache_snapshot_count,
        "query": cache_query_count,
        "clear": cache_clear_count,
        "snapshot_percentage": _percentage(cache_snapshot_count, cache_command_count),
        "query_percentage": _percentage(cache_query_count, cache_command_count),
        "clear_percentage": _percentage(cache_clear_count, cache_command_count),
    }

    return {
        "total_tool_calls": total_tool_calls,
        "cache_command_count": cache_command_count,
        "cache_snapshot_count": cache_snapshot_count,
        "cache_query_count": cache_query_count,
        "cache_clear_count": cache_clear_count,
        "cache_query_before_read_count": cache_query_before_read_count,
        "cache_query_before_read_percentage": cache_query_before_read_percentage,
        "cache_hit_inferred_count": cache_hit_inferred_count,
        "cache_miss_inferred_count": cache_miss_inferred_count,
        "command_type_distribution": command_type_distribution,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_cache_command_type(skill_args: str) -> str:
    """Extract cache command type from skill arguments.

    Examples:
        "snapshot" -> "snapshot"
        "query file.py" -> "query"
        "clear" -> "clear"
    """
    if not skill_args:
        return ""

    args_lower = skill_args.lower().strip()

    if args_lower.startswith("snapshot"):
        return "snapshot"
    elif args_lower.startswith("query"):
        return "query"
    elif args_lower.startswith("clear"):
        return "clear"

    return ""


def _extract_file_from_cache_query(skill_args: str) -> str:
    """Extract file path from cache query command.

    Examples:
        "query file.py" -> "file.py"
        "query src/main.py" -> "src/main.py"
    """
    if not skill_args:
        return ""

    parts = skill_args.split(maxsplit=1)
    if len(parts) >= 2 and parts[0].lower() == "query":
        return parts[1].strip()

    return ""


def _infer_cache_hit(tool_result: str) -> bool:
    """Infer cache hit from tool result message.

    Cache hits typically contain messages about cached data being found.
    Check for miss first to avoid false positives from "not cached" etc.
    """
    if not tool_result:
        return False

    result_lower = tool_result.lower()

    # Check for miss indicators first to avoid false positives
    miss_indicators = ["cache miss", "not cached", "not in cache", "cache empty"]
    if any(indicator in result_lower for indicator in miss_indicators):
        return False

    # Then check for hit indicators
    hit_indicators = ["cached", "cache hit", "found in cache", "using cached"]
    return any(indicator in result_lower for indicator in hit_indicators)


def _infer_cache_miss(tool_result: str) -> bool:
    """Infer cache miss from tool result message.

    Cache misses typically contain messages about cache not found or empty.
    """
    if not tool_result:
        return False

    result_lower = tool_result.lower()
    miss_indicators = ["cache miss", "not cached", "not in cache", "cache empty"]

    return any(indicator in result_lower for indicator in miss_indicators)


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
