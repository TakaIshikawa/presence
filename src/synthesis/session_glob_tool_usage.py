"""Session glob tool usage pattern analyzer for file discovery efficiency.

Analyzes how effectively agents use the Glob tool for file discovery versus
Read tool for directory scanning. Tracks glob pattern specificity, usage rates,
and identifies anti-patterns where Read is used when Glob would be more efficient.

Usage metrics:
- Glob call frequency: How often Glob is used vs Read for discovery
- Pattern specificity: Average number of wildcards and path depth
- Glob-to-read ratio: Proportion of discovery tasks using Glob
- False negatives: Subsequent Read calls on same paths after failed Glob
- Pattern reuse: Efficiency of repeated glob patterns

Opportunity detection:
- Excessive Read usage for directory scanning
- Overly broad glob patterns returning too many results
- Missing glob patterns for common file types
- Sequential Reads that could be replaced by single Glob
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_session_glob_tool_usage(records: object) -> dict[str, Any]:
    """Analyze glob tool usage patterns versus read-based discovery.

    Tracks when agents use Glob for file discovery, measures pattern
    specificity, and identifies missed opportunities where Glob would
    be more efficient than Read.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Glob, Read, etc.)
            - pattern: For Glob, the glob pattern used
            - file_path: For Read, the path being read
            - result_count: Optional number of results returned
            - turn_index: Turn number when tool was invoked

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls analyzed
            - glob_call_count: Number of Glob tool calls
            - read_call_count: Number of Read tool calls (for comparison)
            - directory_read_count: Number of Read calls on directories
            - glob_usage_rate: Percentage of discovery tasks using Glob
            - avg_pattern_specificity: Average specificity score (0-100)
            - avg_wildcards_per_pattern: Mean number of wildcards in patterns
            - avg_path_depth: Average directory depth in glob patterns
            - common_patterns: Most frequently used glob patterns
            - pattern_reuse_rate: Percentage of patterns used multiple times
            - false_negative_count: Read calls on paths after failed Glob
            - missed_glob_opportunities: Sequential Reads that could be Glob
            - overly_broad_patterns: Patterns with very low specificity

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    glob_call_count = 0
    read_call_count = 0
    directory_read_count = 0

    glob_patterns: list[str] = []
    pattern_counter: Counter[str] = Counter()
    specificity_scores: list[float] = []
    wildcard_counts: list[int | float] = []
    path_depths: list[int | float] = []

    read_paths: set[str] = set()
    glob_searched_paths: set[str] = set()
    false_negative_count = 0

    # Track sequential reads for missed glob opportunity detection
    sequential_reads: list[str] = []
    missed_glob_opportunities = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        total_tool_calls += 1
        tool_lower = tool_name.lower()

        if tool_lower == "glob":
            glob_call_count += 1
            pattern = _string(record.get("pattern", ""))

            if pattern:
                glob_patterns.append(pattern)
                pattern_counter[pattern] += 1

                # Calculate pattern metrics
                specificity = _calculate_specificity(pattern)
                specificity_scores.append(specificity)

                wildcards = pattern.count("*") + pattern.count("?")
                wildcard_counts.append(wildcards)

                depth = pattern.count("/")
                path_depths.append(depth)

                # Track searched paths for false negative detection
                glob_searched_paths.add(_extract_base_path(pattern))

            # Reset sequential reads when glob is used
            sequential_reads = []

        elif tool_lower == "read":
            read_call_count += 1
            file_path = _string(record.get("file_path", ""))

            if file_path:
                read_paths.add(file_path)

                # Check if reading a directory
                if _is_directory_read(record):
                    directory_read_count += 1

                # Check for false negative (read after glob should have found it)
                base_path = _get_directory_part(file_path)
                if base_path in glob_searched_paths:
                    false_negative_count += 1

                # Track sequential reads for pattern detection
                sequential_reads.append(file_path)

                # Detect missed glob opportunity: 3+ sequential reads in same dir
                if len(sequential_reads) >= 3:
                    if _same_directory_pattern(sequential_reads[-3:]):
                        missed_glob_opportunities += 1
                        sequential_reads = []  # Reset after detection

    # Calculate discovery task metrics
    discovery_tasks = glob_call_count + directory_read_count
    glob_usage_rate = _percentage(glob_call_count, discovery_tasks)

    # Calculate average metrics
    avg_pattern_specificity = _average(specificity_scores)
    avg_wildcards_per_pattern = _average(wildcard_counts)
    avg_path_depth = _average(path_depths)

    # Calculate pattern reuse
    reused_patterns = sum(1 for count in pattern_counter.values() if count > 1)
    pattern_reuse_rate = _percentage(reused_patterns, len(pattern_counter))

    # Identify overly broad patterns (specificity < 30)
    overly_broad_patterns = [
        pattern for pattern in glob_patterns
        if _calculate_specificity(pattern) < 30.0
    ]

    # Format common patterns
    common_patterns = [
        {"pattern": pattern, "count": count}
        for pattern, count in pattern_counter.most_common(5)
    ]

    return {
        "total_tool_calls": total_tool_calls,
        "glob_call_count": glob_call_count,
        "read_call_count": read_call_count,
        "directory_read_count": directory_read_count,
        "glob_usage_rate": glob_usage_rate,
        "avg_pattern_specificity": avg_pattern_specificity,
        "avg_wildcards_per_pattern": avg_wildcards_per_pattern,
        "avg_path_depth": avg_path_depth,
        "common_patterns": common_patterns,
        "pattern_reuse_rate": pattern_reuse_rate,
        "false_negative_count": false_negative_count,
        "missed_glob_opportunities": missed_glob_opportunities,
        "overly_broad_pattern_count": len(overly_broad_patterns),
        "overly_broad_examples": overly_broad_patterns[:3],  # Limit to 3 examples
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _calculate_specificity(pattern: str) -> float:
    """Calculate specificity score for a glob pattern.

    Higher specificity = more specific pattern (better targeting)
    Lower specificity = broader pattern (less efficient)

    Factors:
    - File extension: +30 points
    - Concrete directory names: +10 points each
    - Double wildcard (**): -20 points
    - Single wildcard (*): -5 points
    - Question mark (?): -2 points

    Score normalized to 0-100 range.
    """
    if not pattern:
        return 0.0

    score = 50.0  # Base score

    # File extension specificity
    if "." in pattern.split("/")[-1]:
        score += 30.0

    # Concrete directory names (non-wildcard path components)
    parts = pattern.split("/")
    for part in parts:
        if part and "*" not in part and "?" not in part:
            score += 10.0

    # Wildcard penalties
    score -= pattern.count("**") * 20.0
    score -= pattern.count("*") * 5.0
    score -= pattern.count("?") * 2.0

    # Normalize to 0-100
    return max(0.0, min(100.0, score))


def _extract_base_path(pattern: str) -> str:
    """Extract base directory path from glob pattern.

    Example: 'src/**/*.py' -> 'src'
    """
    parts = pattern.split("/")
    # Find first part with wildcard
    for i, part in enumerate(parts):
        if "*" in part or "?" in part:
            return "/".join(parts[:i]) if i > 0 else ""
    return pattern


def _get_directory_part(file_path: str) -> str:
    """Extract directory portion from file path."""
    parts = file_path.split("/")
    return "/".join(parts[:-1]) if len(parts) > 1 else ""


def _is_directory_read(record: Mapping[str, Any]) -> bool:
    """Heuristic to detect if Read call is for directory scanning.

    Indicators:
    - File path ends with '/'
    - Explicit flag in record
    - Path is known directory (tests/, src/, etc.)
    """
    file_path = _string(record.get("file_path", ""))

    if file_path.endswith("/"):
        return True

    if record.get("is_directory") is True:
        return True

    # Common directory names without extensions
    common_dirs = {"src", "tests", "test", "lib", "bin", "docs", "examples"}
    basename = file_path.split("/")[-1]
    if basename in common_dirs:
        return True

    return False


def _same_directory_pattern(file_paths: list[str]) -> bool:
    """Check if multiple file paths are in the same directory.

    Used to detect missed glob opportunities where multiple sequential
    reads in the same directory could be replaced by a single glob.
    """
    if len(file_paths) < 2:
        return False

    directories = [_get_directory_part(path) for path in file_paths]

    # All in same directory
    return len(set(directories)) == 1


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
