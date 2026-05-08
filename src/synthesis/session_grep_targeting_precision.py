"""Session grep targeting precision analyzer for search efficiency.

Analyzes how effectively agents use the Grep tool for targeted searches versus
broad unfocused patterns. Tracks result precision, pattern refinement chains,
and context flag usage to identify search efficiency patterns.

Precision metrics:
- Grep call frequency: Total number of grep searches
- Results per grep: Average number of results returned
- Precision score: Ratio of relevant results to total results
- Pattern refinement: Sequential grep calls refining searches
- Context flag usage: Adoption of -A/-B/-C flags for context

Efficiency indicators:
- High precision: Targeted patterns returning focused results
- Pattern refinement: Iterative improvement of search patterns
- Context usage: Appropriate use of context flags for comprehension
- Low precision: Overly broad patterns returning excessive results
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_session_grep_targeting_precision(records: object) -> dict[str, Any]:
    """Analyze grep tool usage precision and effectiveness.

    Tracks grep calls, measures result precision, and identifies pattern
    refinement chains where searches are iteratively improved.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Grep, etc.)
            - pattern: The regex pattern used
            - result_count: Optional number of results returned
            - context_flags: Optional dict with -A/-B/-C flag usage
            - turn_index: Turn number when grep was invoked

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls analyzed
            - grep_call_count: Number of Grep tool calls
            - avg_results_per_grep: Average number of results per grep
            - precision_score: Estimated precision (0-100)
            - pattern_refinement_chains: Number of iterative refinements
            - avg_chain_length: Average length of refinement chains
            - context_flag_usage_rate: Percentage using -A/-B/-C flags
            - context_a_usage: Count of -A flag usage
            - context_b_usage: Count of -B flag usage
            - context_c_usage: Count of -C flag usage
            - high_precision_searches: Count of focused searches (< 20 results)
            - low_precision_searches: Count of broad searches (> 100 results)
            - common_patterns: Most frequently used grep patterns

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    grep_call_count = 0

    result_counts: list[int | float] = []
    precision_scores: list[float] = []

    context_a_usage = 0
    context_b_usage = 0
    context_c_usage = 0
    context_flag_count = 0

    high_precision_searches = 0  # < 20 results
    low_precision_searches = 0  # > 100 results

    pattern_counter: Counter[str] = Counter()

    # Track pattern refinement chains
    previous_pattern: str | None = None
    current_chain_length = 0
    refinement_chains: list[int | float] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        total_tool_calls += 1
        tool_lower = tool_name.lower()

        if tool_lower == "grep":
            grep_call_count += 1
            pattern = _string(record.get("pattern", ""))

            if pattern:
                pattern_counter[pattern] += 1

                # Track pattern refinement chains
                if previous_pattern and _is_refinement(previous_pattern, pattern):
                    current_chain_length += 1
                else:
                    # End of chain, record it if length > 1
                    if current_chain_length > 1:
                        refinement_chains.append(current_chain_length)
                    current_chain_length = 1

                previous_pattern = pattern

            # Track result counts
            result_count = _extract_result_count(record)
            if result_count is not None:
                result_counts.append(result_count)

                # Calculate precision score for this search
                precision = _calculate_precision(result_count)
                precision_scores.append(precision)

                # Classify search precision
                if result_count < 20:
                    high_precision_searches += 1
                elif result_count > 100:
                    low_precision_searches += 1

            # Track context flag usage
            context_flags = record.get("context_flags")
            if isinstance(context_flags, Mapping):
                if context_flags.get("A") or context_flags.get("-A"):
                    context_a_usage += 1
                    context_flag_count += 1
                if context_flags.get("B") or context_flags.get("-B"):
                    context_b_usage += 1
                    context_flag_count += 1
                if context_flags.get("C") or context_flags.get("-C"):
                    context_c_usage += 1
                    context_flag_count += 1

    # Record final chain if active
    if current_chain_length > 1:
        refinement_chains.append(current_chain_length)

    # Calculate metrics
    avg_results_per_grep = _average(result_counts)
    avg_precision = _average(precision_scores)
    pattern_refinement_count = len(refinement_chains)
    avg_chain_length = _average(refinement_chains)
    context_flag_usage_rate = _percentage(context_flag_count, grep_call_count)

    # Format common patterns
    common_patterns = [
        {"pattern": pattern, "count": count}
        for pattern, count in pattern_counter.most_common(5)
    ]

    return {
        "total_tool_calls": total_tool_calls,
        "grep_call_count": grep_call_count,
        "avg_results_per_grep": avg_results_per_grep,
        "precision_score": avg_precision,
        "pattern_refinement_chains": pattern_refinement_count,
        "avg_chain_length": avg_chain_length,
        "context_flag_usage_rate": context_flag_usage_rate,
        "context_a_usage": context_a_usage,
        "context_b_usage": context_b_usage,
        "context_c_usage": context_c_usage,
        "high_precision_searches": high_precision_searches,
        "low_precision_searches": low_precision_searches,
        "common_patterns": common_patterns,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_result_count(record: Mapping[str, Any]) -> int | None:
    """Extract result count from record if available."""
    result_count = record.get("result_count")
    if isinstance(result_count, int) and not isinstance(result_count, bool):
        return result_count
    return None


def _calculate_precision(result_count: int | float) -> float:
    """Calculate precision score based on result count.

    Precision scoring heuristic:
    - 1-10 results: 95-100 precision (very targeted)
    - 11-20 results: 85-95 precision (well-targeted)
    - 21-50 results: 70-85 precision (moderately targeted)
    - 51-100 results: 50-70 precision (somewhat broad)
    - 101-500 results: 20-50 precision (broad)
    - 500+ results: 0-20 precision (very broad)

    Returns:
        Precision score from 0.0 to 100.0
    """
    if result_count <= 0:
        return 100.0  # Zero results could be perfect targeting
    elif result_count <= 10:
        return 100.0 - (result_count - 1) * 0.5
    elif result_count <= 20:
        return 95.0 - (result_count - 10) * 1.0
    elif result_count <= 50:
        return 85.0 - (result_count - 20) * 0.5
    elif result_count <= 100:
        return 70.0 - (result_count - 50) * 0.4
    elif result_count <= 500:
        return 50.0 - (result_count - 100) * 0.075
    else:
        return max(0.0, 20.0 - (result_count - 500) * 0.02)


def _is_refinement(previous: str, current: str) -> bool:
    """Heuristic to detect if current pattern refines previous pattern.

    Refinement indicators:
    - Current contains previous (adds specificity)
    - Both patterns share substantial common substring
    - Current has more specific regex operators

    Examples:
    - "error" -> "error.*authentication" (refinement)
    - "function" -> "function\\s+\\w+" (refinement)
    - "test" -> "error" (not refinement)
    """
    if not previous or not current:
        return False

    # Current contains previous
    if previous in current and len(current) > len(previous):
        return True

    # Previous contains current (narrowing)
    if current in previous and len(previous) > len(current):
        return True

    # Check for common substantial substring (> 50% overlap)
    min_len = min(len(previous), len(current))
    if min_len > 0:
        # Find longest common substring
        lcs_len = _longest_common_substring_length(previous, current)
        if lcs_len / min_len > 0.5:
            return True

    return False


def _longest_common_substring_length(s1: str, s2: str) -> int:
    """Calculate length of longest common substring."""
    if not s1 or not s2:
        return 0

    m, n = len(s1), len(s2)
    max_len = 0

    # Dynamic programming approach (simplified for short strings)
    for i in range(m):
        for j in range(n):
            length = 0
            while (i + length < m and j + length < n and
                   s1[i + length] == s2[j + length]):
                length += 1
            max_len = max(max_len, length)

    return max_len


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
