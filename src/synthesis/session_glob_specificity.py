"""Session Glob pattern specificity analyzer.

Analyzes Glob tool usage patterns in Claude Code session transcripts to measure
pattern specificity and search efficiency. Categorizes patterns by specificity level,
identifies overly broad searches, and detects cases where Grep would be more appropriate.

Glob pattern specificity metrics:
- Pattern categories: Exact file, *.ext, **/*.ext, broad wildcards
- Overly broad patterns: Patterns returning >50 results
- Average result count per pattern: Efficiency measure
- Targeted vs exploratory ratio: Specific vs broad searches
- Grep-instead opportunities: Content searches misusing Glob

Quality indicators:
- High targeted usage: >60% exact file or *.ext patterns
- Low broad pattern count: <20% patterns with >50 results
- Low average result count: <20 results per pattern
- High targeted ratio: >70% targeted vs exploratory
- Few Grep opportunities: <10% patterns should use Grep
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_glob_specificity(records: object) -> dict[str, Any]:
    """Analyze Glob pattern specificity and search efficiency in agent sessions.

    Evaluates pattern types, measures result counts, and identifies overly broad
    or misused Glob patterns.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_name: Name of the tool (Glob, etc.)
            - pattern: Glob pattern used
            - result_count: Optional number of files returned
            - is_content_search: Optional boolean if searching for content

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - glob_invocations: Total Glob tool calls
            - exact_file_patterns: Count of exact filename patterns (no wildcards)
            - single_extension_patterns: Count of *.ext patterns
            - recursive_extension_patterns: Count of **/*.ext patterns
            - broad_wildcard_patterns: Count of patterns with multiple wildcards
            - exact_file_ratio: Percentage of exact file patterns
            - single_extension_ratio: Percentage of single extension patterns
            - recursive_extension_ratio: Percentage of recursive patterns
            - broad_wildcard_ratio: Percentage of broad patterns
            - patterns_with_many_results: Patterns returning >50 results
            - overly_broad_ratio: Percentage of patterns with >50 results
            - result_counts: List of result counts per pattern
            - avg_result_count: Average results per pattern
            - median_result_count: Median results per pattern
            - targeted_patterns: Exact + single extension patterns
            - exploratory_patterns: Recursive + broad patterns
            - targeted_vs_exploratory_ratio: Percentage targeted
            - grep_instead_opportunities: Patterns that should use Grep
            - grep_opportunity_ratio: Percentage that should use Grep
            - specificity_score: 0-1 overall specificity score

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    total_turns = 0
    glob_invocations = 0

    # Pattern categories
    exact_file_patterns = 0
    single_extension_patterns = 0
    recursive_extension_patterns = 0
    broad_wildcard_patterns = 0

    # Result tracking
    patterns_with_many_results = 0
    result_counts: list[int] = []

    # Grep opportunity detection
    grep_instead_opportunities = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1
        tool_name = _string(record.get("tool_name"))

        if tool_name.lower() != "glob":
            continue

        glob_invocations += 1

        # Analyze pattern
        pattern = _string(record.get("pattern", ""))
        if pattern:
            category = _categorize_pattern(pattern)
            if category == "exact":
                exact_file_patterns += 1
            elif category == "single_extension":
                single_extension_patterns += 1
            elif category == "recursive_extension":
                recursive_extension_patterns += 1
            elif category == "broad":
                broad_wildcard_patterns += 1

        # Track result count
        result_count = _int(record.get("result_count"))
        if result_count >= 0:
            result_counts.append(result_count)
            if result_count > 50:
                patterns_with_many_results += 1

        # Detect Grep opportunities (content searches)
        is_content_search = _bool(record.get("is_content_search", False))
        if is_content_search:
            grep_instead_opportunities += 1

    # Calculate aggregate metrics
    total_patterns = (
        exact_file_patterns +
        single_extension_patterns +
        recursive_extension_patterns +
        broad_wildcard_patterns
    )

    exact_file_ratio = _percentage(exact_file_patterns, total_patterns)
    single_extension_ratio = _percentage(single_extension_patterns, total_patterns)
    recursive_extension_ratio = _percentage(recursive_extension_patterns, total_patterns)
    broad_wildcard_ratio = _percentage(broad_wildcard_patterns, total_patterns)

    overly_broad_ratio = _percentage(patterns_with_many_results, glob_invocations)

    avg_result_count = _average(result_counts)
    median_result_count = _median(result_counts)

    targeted_patterns = exact_file_patterns + single_extension_patterns
    exploratory_patterns = recursive_extension_patterns + broad_wildcard_patterns
    targeted_vs_exploratory = _percentage(
        targeted_patterns,
        targeted_patterns + exploratory_patterns
    )

    grep_opportunity_ratio = _percentage(grep_instead_opportunities, glob_invocations)

    # Calculate specificity score
    specificity_score = _calculate_specificity_score(
        targeted_vs_exploratory,
        overly_broad_ratio,
        avg_result_count,
        grep_opportunity_ratio,
    )

    return {
        "total_turns": total_turns,
        "glob_invocations": glob_invocations,
        "exact_file_patterns": exact_file_patterns,
        "single_extension_patterns": single_extension_patterns,
        "recursive_extension_patterns": recursive_extension_patterns,
        "broad_wildcard_patterns": broad_wildcard_patterns,
        "exact_file_ratio": exact_file_ratio,
        "single_extension_ratio": single_extension_ratio,
        "recursive_extension_ratio": recursive_extension_ratio,
        "broad_wildcard_ratio": broad_wildcard_ratio,
        "patterns_with_many_results": patterns_with_many_results,
        "overly_broad_ratio": overly_broad_ratio,
        "result_counts": result_counts,
        "avg_result_count": avg_result_count,
        "median_result_count": median_result_count,
        "targeted_patterns": targeted_patterns,
        "exploratory_patterns": exploratory_patterns,
        "targeted_vs_exploratory_ratio": targeted_vs_exploratory,
        "grep_instead_opportunities": grep_instead_opportunities,
        "grep_opportunity_ratio": grep_opportunity_ratio,
        "specificity_score": specificity_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "glob_invocations": 0,
        "exact_file_patterns": 0,
        "single_extension_patterns": 0,
        "recursive_extension_patterns": 0,
        "broad_wildcard_patterns": 0,
        "exact_file_ratio": 0.0,
        "single_extension_ratio": 0.0,
        "recursive_extension_ratio": 0.0,
        "broad_wildcard_ratio": 0.0,
        "patterns_with_many_results": 0,
        "overly_broad_ratio": 0.0,
        "result_counts": [],
        "avg_result_count": 0.0,
        "median_result_count": 0.0,
        "targeted_patterns": 0,
        "exploratory_patterns": 0,
        "targeted_vs_exploratory_ratio": 0.0,
        "grep_instead_opportunities": 0,
        "grep_opportunity_ratio": 0.0,
        "specificity_score": 0.0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _bool(value: object) -> bool:
    """Convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1")
    return bool(value)


def _int(value: object) -> int:
    """Convert value to int, returning -1 for invalid values."""
    if value is None:
        return -1
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return -1
    return -1


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _median(values: list[int] | list[float]) -> float:
    """Calculate median of numeric values."""
    if not values:
        return 0.0
    sorted_values = sorted(values)
    n = len(sorted_values)
    if n % 2 == 0:
        return round((sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2, 2)
    else:
        return float(sorted_values[n // 2])


def _categorize_pattern(pattern: str) -> str:
    """Categorize glob pattern by specificity.

    Categories:
    - exact: No wildcards (exact filename)
    - single_extension: *.ext pattern (single directory)
    - recursive_extension: **/*.ext pattern (recursive search)
    - broad: Multiple wildcards or complex patterns

    Args:
        pattern: Glob pattern string

    Returns:
        Category string
    """
    if not pattern:
        return "broad"

    # Count wildcards
    star_count = pattern.count("*")
    question_count = pattern.count("?")

    # Exact file (no wildcards)
    if star_count == 0 and question_count == 0:
        return "exact"

    # Single extension pattern: *.ext
    if pattern.startswith("*.") and star_count == 1 and question_count == 0:
        # Check if there's only one extension
        if pattern.count(".") == 1:
            return "single_extension"

    # Recursive extension pattern: **/*.ext
    if "**/*." in pattern and star_count == 3 and question_count == 0:
        return "recursive_extension"

    # Everything else is broad
    return "broad"


def _calculate_specificity_score(
    targeted_ratio: float,
    overly_broad_ratio: float,
    avg_result_count: float,
    grep_opportunity_ratio: float,
) -> float:
    """Calculate overall specificity score (0-1).

    Score components:
    - 0.35: Targeted vs exploratory ratio (higher is better)
    - 0.25: Overly broad penalty (lower is better)
    - 0.25: Average result count (lower is better, <20 is optimal)
    - 0.15: Grep opportunity penalty (lower is better)
    """
    # Targeted ratio component (0-0.35)
    # Target: >70% targeted
    if targeted_ratio >= 70.0:
        targeted_component = 0.35
    else:
        targeted_component = (targeted_ratio / 70.0) * 0.35

    # Overly broad penalty (0-0.25)
    # Target: <20% overly broad
    if overly_broad_ratio <= 20.0:
        broad_component = 0.25
    else:
        penalty = min(overly_broad_ratio - 20.0, 80.0) / 80.0
        broad_component = 0.25 * (1.0 - penalty)

    # Result count component (0-0.25)
    # Target: <20 results average
    if avg_result_count <= 20.0:
        result_component = 0.25
    else:
        penalty = min(avg_result_count - 20.0, 80.0) / 80.0
        result_component = 0.25 * (1.0 - penalty)

    # Grep opportunity penalty (0-0.15)
    # Target: <10% grep opportunities
    if grep_opportunity_ratio <= 10.0:
        grep_component = 0.15
    else:
        penalty = min(grep_opportunity_ratio - 10.0, 90.0) / 90.0
        grep_component = 0.15 * (1.0 - penalty)

    score = (
        targeted_component +
        broad_component +
        result_component +
        grep_component
    )
    return round(max(0.0, min(1.0, score)), 3)
