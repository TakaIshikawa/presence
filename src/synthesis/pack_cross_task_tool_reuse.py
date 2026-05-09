"""Pack cross-task tool reuse efficiency analyzer.

Analyzes how efficiently agents reuse tool outputs across tasks in an execution
pack. Measures file re-reads, pattern repetition, and opportunities for cross-task
context sharing.

Cross-task reuse metrics:
- File reuse percentage: Files read multiple times vs cached/reused
- Pattern repetition: Grep patterns repeated across tasks
- Identical read invocations: Same file/offset/limit combinations
- Cache opportunity score: Files that should be cached for reuse
- Cross-task sharing efficiency: Overall reuse effectiveness

Quality indicators:
- High reuse percentage (>70%): Effective context sharing across tasks
- Low redundant reads (<15%): Minimal duplicate file accesses
- High cache opportunity score: Strategic caching identified
- Efficient pattern reuse: Shared search patterns consolidated
- Good cross-task coordination: Minimal redundant tool calls
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping


def analyze_pack_cross_task_tool_reuse(records: object) -> dict[str, Any]:
    """Analyze tool output reuse efficiency across tasks in execution packs.

    Evaluates how well agents share context and reuse tool outputs across
    multiple tasks in a pack, identifying redundant calls and caching opportunities.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - tasks: List of task dicts with:
                - task_id: Task identifier
                - tool_calls: List of tool call dicts with:
                    - tool_name: Name of the tool
                    - parameters: Dict of tool parameters
                    - result_hash: Optional hash of result for dedup
            - total_read_calls: Total Read tool invocations across pack
            - unique_files_read: Number of unique files accessed

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_total_tool_calls: Average tool calls across pack
            - avg_unique_tool_calls: Average unique tool calls (no duplicates)
            - avg_tool_reuse_percentage: % of tool calls that are reused
            - avg_file_reuse_percentage: % of files read multiple times
            - avg_redundant_read_ratio: % of Read calls that are duplicates
            - avg_grep_pattern_reuse: % of Grep patterns repeated across tasks
            - avg_cache_opportunity_score: 0-100 score for caching potential
            - high_reuse_packs: Count of packs with >70% reuse
            - low_reuse_packs: Count of packs with <30% reuse
            - common_reuse_patterns: Most frequently reused tool/parameter combinations

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    if not records:
        return _empty_result()

    total_packs = 0
    total_tool_calls_list: list[int] = []
    unique_tool_calls_list: list[int] = []
    tool_reuse_percentages: list[float] = []
    file_reuse_percentages: list[float] = []
    redundant_read_ratios: list[float] = []
    grep_pattern_reuse_list: list[float] = []
    cache_opportunity_scores: list[float] = []

    high_reuse_packs = 0  # >70% reuse
    low_reuse_packs = 0   # <30% reuse

    reuse_patterns: defaultdict[tuple[str, str], int] = defaultdict(int)

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        # Extract pack-level data
        tasks = record.get("tasks")
        if not isinstance(tasks, list):
            continue

        # Track all tool calls across tasks
        all_tool_calls: list[Mapping[str, Any]] = []
        file_access_count: defaultdict[str, int] = defaultdict(int)
        grep_patterns: defaultdict[str, int] = defaultdict(int)
        read_call_signatures: defaultdict[str, int] = defaultdict(int)

        for task in tasks:
            if not isinstance(task, Mapping):
                continue

            tool_calls = task.get("tool_calls")
            if not isinstance(tool_calls, list):
                continue

            for call in tool_calls:
                if not isinstance(call, Mapping):
                    continue

                tool_name = _string(call.get("tool_name"))
                if not tool_name:
                    continue

                all_tool_calls.append(call)

                params = call.get("parameters")
                if not isinstance(params, Mapping):
                    continue

                # Track file accesses
                file_path = _string(params.get("file_path") or params.get("path") or "")
                if file_path and tool_name == "Read":
                    file_access_count[file_path] += 1

                    # Track Read call signature for duplicate detection
                    offset = params.get("offset", "")
                    limit = params.get("limit", "")
                    signature = f"{file_path}|{offset}|{limit}"
                    read_call_signatures[signature] += 1

                # Track Grep patterns
                if tool_name == "Grep":
                    pattern = _string(params.get("pattern") or "")
                    if pattern:
                        grep_patterns[pattern] += 1

        if not all_tool_calls:
            continue

        # Calculate reuse metrics
        total_calls = len(all_tool_calls)
        total_tool_calls_list.append(total_calls)

        # Count unique tool calls (by tool name + params)
        unique_calls = _count_unique_tool_calls(all_tool_calls)
        unique_tool_calls_list.append(unique_calls)

        # Tool reuse percentage
        reused_calls = total_calls - unique_calls
        tool_reuse_pct = _percentage(reused_calls, total_calls)
        tool_reuse_percentages.append(tool_reuse_pct)

        # File reuse percentage
        total_files = len(file_access_count)
        reused_files = sum(1 for count in file_access_count.values() if count > 1)
        file_reuse_pct = _percentage(reused_files, total_files)
        file_reuse_percentages.append(file_reuse_pct)

        # Redundant read ratio
        total_reads = sum(file_access_count.values())
        redundant_reads = sum(max(0, count - 1) for count in file_access_count.values())
        redundant_ratio = _percentage(redundant_reads, total_reads)
        redundant_read_ratios.append(redundant_ratio)

        # Grep pattern reuse
        total_grep_calls = sum(grep_patterns.values())
        unique_patterns = len(grep_patterns)
        reused_patterns = total_grep_calls - unique_patterns
        grep_reuse_pct = _percentage(reused_patterns, total_grep_calls)
        grep_pattern_reuse_list.append(grep_reuse_pct)

        # Cache opportunity score (0-100)
        # High score = many files accessed multiple times = good cache candidates
        cache_score = _calculate_cache_opportunity_score(
            file_access_count, read_call_signatures
        )
        cache_opportunity_scores.append(cache_score)

        # Track common reuse patterns
        for call in all_tool_calls:
            tool_name = _string(call.get("tool_name"))
            params = call.get("parameters")
            if isinstance(params, Mapping):
                param_key = _extract_primary_param(tool_name, params)
                if param_key:
                    reuse_patterns[(tool_name, param_key)] += 1

        # Classify pack
        if tool_reuse_pct > 70:
            high_reuse_packs += 1
        elif tool_reuse_pct < 30:
            low_reuse_packs += 1

    # Calculate aggregate metrics
    avg_total_calls = _average([float(x) for x in total_tool_calls_list])
    avg_unique_calls = _average([float(x) for x in unique_tool_calls_list])
    avg_tool_reuse = _average(tool_reuse_percentages)
    avg_file_reuse = _average(file_reuse_percentages)
    avg_redundant_read = _average(redundant_read_ratios)
    avg_grep_reuse = _average(grep_pattern_reuse_list)
    avg_cache_score = _average(cache_opportunity_scores)

    # Format common reuse patterns
    common_patterns = [
        {"tool": tool, "parameter": param, "reuse_count": count}
        for (tool, param), count in sorted(
            reuse_patterns.items(), key=lambda x: x[1], reverse=True
        )[:10]
    ]

    return {
        "total_packs": total_packs,
        "avg_total_tool_calls": avg_total_calls,
        "avg_unique_tool_calls": avg_unique_calls,
        "avg_tool_reuse_percentage": avg_tool_reuse,
        "avg_file_reuse_percentage": avg_file_reuse,
        "avg_redundant_read_ratio": avg_redundant_read,
        "avg_grep_pattern_reuse": avg_grep_reuse,
        "avg_cache_opportunity_score": avg_cache_score,
        "high_reuse_packs": high_reuse_packs,
        "low_reuse_packs": low_reuse_packs,
        "common_reuse_patterns": common_patterns,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_packs": 0,
        "avg_total_tool_calls": 0.0,
        "avg_unique_tool_calls": 0.0,
        "avg_tool_reuse_percentage": 0.0,
        "avg_file_reuse_percentage": 0.0,
        "avg_redundant_read_ratio": 0.0,
        "avg_grep_pattern_reuse": 0.0,
        "avg_cache_opportunity_score": 0.0,
        "high_reuse_packs": 0,
        "low_reuse_packs": 0,
        "common_reuse_patterns": [],
    }


def _count_unique_tool_calls(tool_calls: list[Mapping[str, Any]]) -> int:
    """Count unique tool calls based on tool name and parameters.

    Args:
        tool_calls: List of tool call dicts

    Returns:
        Count of unique tool calls
    """
    signatures: set[str] = set()

    for call in tool_calls:
        tool_name = _string(call.get("tool_name"))
        params = call.get("parameters")

        if not tool_name or not isinstance(params, Mapping):
            continue

        # Create signature from tool name + key parameters
        sig_parts = [tool_name]

        # Add relevant parameters based on tool type
        if tool_name == "Read":
            sig_parts.append(_string(params.get("file_path") or params.get("path") or ""))
            sig_parts.append(str(params.get("offset", "")))
            sig_parts.append(str(params.get("limit", "")))
        elif tool_name in ["Grep", "Glob"]:
            sig_parts.append(_string(params.get("pattern") or ""))
            sig_parts.append(_string(params.get("path") or ""))
        elif tool_name in ["Edit", "Write"]:
            sig_parts.append(_string(params.get("file_path") or ""))
        else:
            # For other tools, use string representation of all params
            sig_parts.append(str(sorted(params.items())))

        signature = "|".join(sig_parts)
        signatures.add(signature)

    return len(signatures)


def _calculate_cache_opportunity_score(
    file_access_count: dict[str, int],
    read_signatures: dict[str, int]
) -> float:
    """Calculate cache opportunity score based on file reuse patterns.

    Args:
        file_access_count: Map of file paths to access counts
        read_signatures: Map of Read call signatures to counts

    Returns:
        Score from 0-100 (higher = better caching opportunities)
    """
    if not file_access_count:
        return 0.0

    total_files = len(file_access_count)
    total_accesses = sum(file_access_count.values())

    # Score components:
    # 1. Percentage of files accessed multiple times (0-50 points)
    multi_access_files = sum(1 for count in file_access_count.values() if count > 1)
    multi_access_pct = (multi_access_files / total_files) * 100
    multi_access_score = (multi_access_pct / 100.0) * 50.0

    # 2. Average reuse factor (0-30 points)
    # Higher average = more opportunities
    avg_reuse = total_accesses / total_files if total_files > 0 else 1.0
    # Cap at 5x reuse = max score
    reuse_score = min((avg_reuse - 1.0) / 4.0, 1.0) * 30.0

    # 3. Duplicate Read signatures (0-20 points)
    # Identical reads are prime cache candidates
    duplicate_reads = sum(max(0, count - 1) for count in read_signatures.values())
    total_reads = sum(read_signatures.values())
    duplicate_ratio = duplicate_reads / total_reads if total_reads > 0 else 0.0
    duplicate_score = duplicate_ratio * 20.0

    total_score = multi_access_score + reuse_score + duplicate_score
    return round(max(0.0, min(100.0, total_score)), 2)


def _extract_primary_param(tool_name: str, params: Mapping[str, Any]) -> str:
    """Extract primary parameter for reuse pattern tracking.

    Args:
        tool_name: Name of the tool
        params: Tool parameters

    Returns:
        Primary parameter value as string
    """
    if tool_name == "Read":
        return _string(params.get("file_path") or params.get("path") or "")
    elif tool_name in ["Grep", "Glob"]:
        return _string(params.get("pattern") or "")
    elif tool_name in ["Edit", "Write"]:
        return _string(params.get("file_path") or "")
    elif tool_name == "Bash":
        cmd = _string(params.get("command") or "")
        # Return first 50 chars of command
        return cmd[:50] if cmd else ""
    else:
        return ""


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace.

    Args:
        value: Value to convert

    Returns:
        String value
    """
    return value.strip() if isinstance(value, str) else ""


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator.

    Args:
        numerator: Numerator value
        denominator: Denominator value

    Returns:
        Percentage value (0.0-100.0)
    """
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[float]) -> float:
    """Calculate average of numeric values.

    Args:
        values: List of numeric values

    Returns:
        Average value
    """
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
