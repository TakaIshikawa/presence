"""Pack Glob vs Grep search strategy efficiency analyzer.

Analyzes Glob and Grep tool usage patterns across all sessions in an execution pack
to measure search strategy efficiency and workflow optimization. Identifies optimal
vs suboptimal search patterns and measures search-to-action latency.

Search strategy metrics:
- Glob vs Grep usage: Total counts and ratios across pack sessions
- Glob-first ratio: Fraction of search workflows starting with Glob
- Grep-to-Read latency: Average turns between Grep and subsequent Read
- Unnecessary Grep count: Grep used when Glob would suffice (filename searches)
- Search-to-action efficiency: Fraction of searches leading to edits within 3 turns

Quality indicators:
- High Glob-first ratio: >70% of searches start with Glob (proper workflow)
- Low Grep-to-Read latency: <2 turns average (efficient workflow)
- Low unnecessary Grep: Minimal filename searches with Grep
- High search-to-action: >60% of searches lead to concrete actions
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_glob_grep_efficiency(records: object) -> dict[str, Any]:
    """Analyze Glob and Grep search strategy efficiency across pack sessions.

    Evaluates search workflow patterns, identifies optimal strategies, and measures
    search-to-action latency across all sessions in an execution pack.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - glob_count: Number of Glob tool calls
            - grep_count: Number of Grep tool calls
            - glob_first_searches: Number of search workflows starting with Glob
            - total_search_workflows: Total number of search workflows
            - grep_to_read_turns: List of turn counts between Grep and Read
            - unnecessary_grep_count: Grep calls that should have been Glob
            - searches_leading_to_edits: Searches followed by Edit/Write within 3 turns
            - total_searches: Total search operations (Glob + Grep)

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - total_glob_count: Sum of Glob calls across all sessions
            - total_grep_count: Sum of Grep calls across all sessions
            - total_searches: Total search operations
            - glob_grep_ratio: Percentage of Glob vs total searches
            - glob_first_ratio: Percentage of workflows starting with Glob
            - avg_grep_to_read_latency: Average turns between Grep and Read
            - max_grep_to_read_latency: Maximum latency observed
            - total_unnecessary_grep_count: Total Grep calls that should be Glob
            - unnecessary_grep_ratio: Percentage of Grep calls that are unnecessary
            - searches_leading_to_action: Total searches leading to edits
            - search_to_action_efficiency: Percentage of searches leading to action
            - optimal_pattern_sessions: Sessions with >70% Glob-first ratio
            - search_strategy_score: 0-1 overall efficiency score

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
    total_glob_count = 0
    total_grep_count = 0

    glob_first_searches = 0
    total_search_workflows = 0

    grep_to_read_turns: list[int] = []
    total_unnecessary_grep_count = 0

    searches_leading_to_action = 0
    total_searches = 0

    optimal_pattern_sessions = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        # Count Glob and Grep calls
        glob_count = _int(record.get("glob_count", 0))
        grep_count = _int(record.get("grep_count", 0))
        total_glob_count += glob_count
        total_grep_count += grep_count

        # Track search workflows
        session_glob_first = _int(record.get("glob_first_searches", 0))
        session_workflows = _int(record.get("total_search_workflows", 0))
        glob_first_searches += session_glob_first
        total_search_workflows += session_workflows

        # Track Grep-to-Read latency
        session_latencies = record.get("grep_to_read_turns")
        if isinstance(session_latencies, list):
            for latency in session_latencies:
                turns = _int(latency)
                if turns > 0:
                    grep_to_read_turns.append(turns)

        # Track unnecessary Grep usage
        unnecessary = _int(record.get("unnecessary_grep_count", 0))
        total_unnecessary_grep_count += unnecessary

        # Track search-to-action efficiency
        session_actions = _int(record.get("searches_leading_to_edits", 0))
        session_total = _int(record.get("total_searches", 0))
        searches_leading_to_action += session_actions
        total_searches += session_total

        # Check if session follows optimal pattern
        if session_workflows > 0:
            session_glob_ratio = (session_glob_first / session_workflows) * 100.0
            if session_glob_ratio > 70.0:
                optimal_pattern_sessions += 1

    # Calculate aggregate metrics
    total_search_ops = total_glob_count + total_grep_count
    glob_grep_ratio = _percentage(total_glob_count, total_search_ops)
    glob_first_ratio = _percentage(glob_first_searches, total_search_workflows)

    avg_grep_to_read_latency = _average(grep_to_read_turns)
    max_grep_to_read_latency = max(grep_to_read_turns) if grep_to_read_turns else 0

    unnecessary_grep_ratio = _percentage(total_unnecessary_grep_count, total_grep_count)
    search_to_action_efficiency = _percentage(searches_leading_to_action, total_searches)

    # Calculate strategy score
    strategy_score = _calculate_strategy_score(
        glob_first_ratio,
        avg_grep_to_read_latency,
        unnecessary_grep_ratio,
        search_to_action_efficiency,
    )

    return {
        "total_sessions": total_sessions,
        "total_glob_count": total_glob_count,
        "total_grep_count": total_grep_count,
        "total_searches": total_search_ops,
        "glob_grep_ratio": glob_grep_ratio,
        "glob_first_ratio": glob_first_ratio,
        "avg_grep_to_read_latency": avg_grep_to_read_latency,
        "max_grep_to_read_latency": max_grep_to_read_latency,
        "total_unnecessary_grep_count": total_unnecessary_grep_count,
        "unnecessary_grep_ratio": unnecessary_grep_ratio,
        "searches_leading_to_action": searches_leading_to_action,
        "search_to_action_efficiency": search_to_action_efficiency,
        "optimal_pattern_sessions": optimal_pattern_sessions,
        "search_strategy_score": strategy_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "total_glob_count": 0,
        "total_grep_count": 0,
        "total_searches": 0,
        "glob_grep_ratio": 0.0,
        "glob_first_ratio": 0.0,
        "avg_grep_to_read_latency": 0.0,
        "max_grep_to_read_latency": 0,
        "total_unnecessary_grep_count": 0,
        "unnecessary_grep_ratio": 0.0,
        "searches_leading_to_action": 0,
        "search_to_action_efficiency": 0.0,
        "optimal_pattern_sessions": 0,
        "search_strategy_score": 0.0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


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


def _calculate_strategy_score(
    glob_first_ratio: float,
    grep_to_read_latency: float,
    unnecessary_grep_ratio: float,
    search_to_action: float,
) -> float:
    """Calculate search strategy efficiency score (0-1).

    Score components:
    - 0.4: Glob-first workflow (>70% is optimal)
    - 0.3: Search-to-action efficiency (>60% is good)
    - 0.2: Low Grep-to-Read latency (<2 turns is optimal)
    - 0.1: Low unnecessary Grep usage (<10% is good)
    """
    # Glob-first component (0-0.4)
    # Target: >70% Glob-first
    if glob_first_ratio >= 70:
        glob_component = 0.4
    else:
        glob_component = (glob_first_ratio / 70.0) * 0.4

    # Search-to-action component (0-0.3)
    # Target: >60% efficiency
    if search_to_action >= 60:
        action_component = 0.3
    else:
        action_component = (search_to_action / 60.0) * 0.3

    # Latency component (0-0.2)
    # Optimal: <2 turns, penalize higher latency
    if grep_to_read_latency == 0:
        latency_component = 0.2
    elif grep_to_read_latency <= 2:
        latency_component = 0.2
    else:
        # Penalize latency >2 turns
        latency_component = max(0.0, 0.2 - (grep_to_read_latency - 2) * 0.05)

    # Unnecessary Grep component (0-0.1)
    # Target: <10% unnecessary
    if unnecessary_grep_ratio <= 10:
        unnecessary_component = 0.1
    else:
        # Penalize higher unnecessary usage
        unnecessary_component = max(0.0, 0.1 - (unnecessary_grep_ratio - 10) / 200.0)

    score = (
        glob_component +
        action_component +
        latency_component +
        unnecessary_component
    )

    return round(max(0.0, min(1.0, score)), 3)
