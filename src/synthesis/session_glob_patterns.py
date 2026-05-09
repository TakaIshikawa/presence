"""Session Glob pattern specificity and search efficiency analyzer.

Analyzes Glob tool usage patterns in Claude Code sessions to measure pattern
specificity, search efficiency, and file discovery effectiveness. Tracks broad
vs targeted patterns, files returned vs used, and pattern optimization.

Glob pattern metrics:
- Total Glob calls: Number of Glob tool invocations
- Pattern specificity score: 0.0 (broad) to 1.0 (highly specific)
- Inefficient searches: Patterns returning >50 files
- Pattern-to-action ratio: Files returned vs files actually used
- Duplicate patterns: Similar patterns that could be combined
- Search efficiency score: 0-100 score (specific patterns and high usage = higher)

Quality indicators:
- High specificity score (>0.7): Good targeted pattern usage
- Low inefficient searches (<20%): Minimal overly broad patterns
- High pattern-to-action ratio (>40%): Files found are actually used
- Few duplicate patterns (<15%): Efficient upfront pattern design
- High efficiency score (>75): Optimal Glob usage strategy
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_glob_patterns(records: object) -> dict[str, Any]:
    """Analyze Glob tool usage patterns and efficiency in Claude Code sessions.

    Evaluates search efficiency through pattern specificity, files returned
    vs used, and duplicate pattern detection.

    Args:
        records: List of Glob call dictionaries with keys:
            - glob_id: Glob call identifier
            - pattern: The glob pattern used
            - files_returned: Number of files matching pattern
            - files_used: Number of matched files actually read/edited
            - specificity_score: Pattern specificity (0.0-1.0)
            - is_inefficient: Boolean indicating >50 files returned
            - is_duplicate: Boolean indicating similar pattern exists
            - pattern_type: Classification (exact, medium, broad)
            - session_id: Session identifier

    Returns:
        Dict with:
            - total_glob_calls: Total number of Glob invocations
            - avg_specificity_score: Average pattern specificity (0.0-1.0)
            - inefficient_searches: Count of patterns returning >50 files
            - inefficient_search_ratio: Percentage of inefficient searches
            - avg_files_returned: Average files per Glob call
            - avg_files_used: Average files actually used
            - pattern_to_action_ratio: Percentage of returned files used
            - duplicate_patterns: Count of duplicate/similar patterns
            - duplicate_pattern_ratio: Percentage of duplicate patterns
            - search_efficiency_score: Score 0-100 (higher = better efficiency)
            - high_efficiency_searches: Count with score >75
            - low_efficiency_searches: Count with score <40
            - patterns_by_type: Dict of pattern counts by type
            - inefficient_patterns: List of patterns with >50 files (limited)
            - recommendations: List of actionable suggestions

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of Glob call dictionaries")

    if not records:
        return _empty_result()

    total_glob_calls = 0
    specificity_scores: list[float] = []
    inefficient_searches = 0
    files_returned_list: list[int | float] = []
    files_used_list: list[int | float] = []
    pattern_to_action_ratios: list[float] = []
    duplicate_patterns = 0
    efficiency_scores: list[float] = []
    high_efficiency_searches = 0
    low_efficiency_searches = 0
    patterns_by_type: dict[str, int] = {}
    inefficient_patterns_list: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_glob_calls += 1

        pattern = record.get("pattern", "")
        specificity = _extract_number(record.get("specificity_score"))
        files_returned = _extract_number(record.get("files_returned"))
        files_used = _extract_number(record.get("files_used"))
        is_inefficient = record.get("is_inefficient", False)
        is_duplicate = record.get("is_duplicate", False)
        pattern_type = record.get("pattern_type", "unknown")

        # Track specificity scores
        if specificity is not None:
            specificity_scores.append(specificity)

        # Track inefficient searches
        if is_inefficient:
            inefficient_searches += 1
            if len(inefficient_patterns_list) < 10:  # Limit to 10 examples
                inefficient_patterns_list.append({
                    "pattern": pattern,
                    "files_returned": files_returned or 0,
                })

        # Track files returned and used
        if files_returned is not None:
            files_returned_list.append(files_returned)
        if files_used is not None:
            files_used_list.append(files_used)

        # Calculate pattern-to-action ratio for this call
        if files_returned is not None and files_used is not None and files_returned > 0:
            ratio = (files_used / files_returned) * 100.0
            pattern_to_action_ratios.append(min(ratio, 100.0))

        # Track duplicate patterns
        if is_duplicate:
            duplicate_patterns += 1

        # Track pattern types
        if pattern_type in patterns_by_type:
            patterns_by_type[pattern_type] += 1
        else:
            patterns_by_type[pattern_type] = 1

        # Calculate efficiency score for this search
        efficiency_score = _calculate_search_efficiency_score(
            specificity=specificity,
            files_returned=files_returned,
            files_used=files_used,
            is_duplicate=is_duplicate,
        )
        efficiency_scores.append(efficiency_score)

        # Classify efficiency quality
        if efficiency_score > 75.0:
            high_efficiency_searches += 1
        elif efficiency_score < 40.0:
            low_efficiency_searches += 1

    # Calculate aggregate metrics
    avg_specificity = _average(specificity_scores)
    inefficient_ratio = _percentage(inefficient_searches, total_glob_calls)
    avg_files_returned = _average(files_returned_list)
    avg_files_used = _average(files_used_list)
    avg_pattern_to_action = _average(pattern_to_action_ratios)
    duplicate_ratio = _percentage(duplicate_patterns, total_glob_calls)
    avg_efficiency = _average(efficiency_scores)

    # Generate recommendations
    recommendations = _generate_recommendations(
        avg_specificity=avg_specificity,
        inefficient_ratio=inefficient_ratio,
        avg_pattern_to_action=avg_pattern_to_action,
        duplicate_ratio=duplicate_ratio,
        avg_efficiency=avg_efficiency,
    )

    return {
        "total_glob_calls": total_glob_calls,
        "avg_specificity_score": avg_specificity,
        "inefficient_searches": inefficient_searches,
        "inefficient_search_ratio": inefficient_ratio,
        "avg_files_returned": avg_files_returned,
        "avg_files_used": avg_files_used,
        "pattern_to_action_ratio": avg_pattern_to_action,
        "duplicate_patterns": duplicate_patterns,
        "duplicate_pattern_ratio": duplicate_ratio,
        "search_efficiency_score": avg_efficiency,
        "high_efficiency_searches": high_efficiency_searches,
        "low_efficiency_searches": low_efficiency_searches,
        "patterns_by_type": patterns_by_type,
        "inefficient_patterns": inefficient_patterns_list,
        "recommendations": recommendations,
    }


def _calculate_search_efficiency_score(
    specificity: float | None,
    files_returned: int | float | None,
    files_used: int | float | None,
    is_duplicate: bool,
) -> float:
    """Calculate search efficiency score (0-100).

    Higher scores indicate better efficiency:
    - High pattern specificity (>0.7)
    - Moderate files returned (<50)
    - High pattern-to-action ratio (>40%)
    - Not a duplicate pattern

    Scoring breakdown:
    - Specificity: 40 points (0.7 threshold)
    - Files returned control: 25 points (50 files threshold)
    - Pattern-to-action ratio: 25 points (40% threshold)
    - No duplication: 10 points
    """
    score = 0.0

    # Specificity component (40 points)
    if specificity is not None:
        if specificity >= 0.9:  # >=0.9 = excellent
            score += 40.0
        elif specificity >= 0.7:  # >=0.7 = good
            score += 30.0
        elif specificity >= 0.5:  # >=0.5 = acceptable
            score += 20.0
        elif specificity >= 0.3:  # >=0.3 = poor
            score += 10.0
        # <0.3 = 0 points (very broad)

    # Files returned control component (25 points)
    if files_returned is not None:
        if files_returned <= 10:  # <=10 files = excellent
            score += 25.0
        elif files_returned <= 30:  # <=30 files = good
            score += 20.0
        elif files_returned <= 50:  # <=50 files = acceptable
            score += 15.0
        elif files_returned <= 100:  # <=100 files = poor
            score += 10.0
        # >100 files = 0 points

    # Pattern-to-action ratio component (25 points)
    if files_returned is not None and files_used is not None and files_returned > 0:
        ratio = (files_used / files_returned) * 100.0
        if ratio >= 60:  # >=60% = excellent
            score += 25.0
        elif ratio >= 40:  # >=40% = good
            score += 20.0
        elif ratio >= 20:  # >=20% = acceptable
            score += 15.0
        elif ratio >= 10:  # >=10% = poor
            score += 10.0
        # <10% = 0 points

    # No duplication component (10 points)
    if not is_duplicate:
        score += 10.0

    return round(score, 2)


def _generate_recommendations(
    avg_specificity: float,
    inefficient_ratio: float,
    avg_pattern_to_action: float,
    duplicate_ratio: float,
    avg_efficiency: float,
) -> list[str]:
    """Generate actionable recommendations based on metrics."""
    recommendations = []

    if avg_specificity < 0.7:
        recommendations.append(
            "Use more specific patterns (exact extensions, targeted directories) "
            "instead of broad wildcards"
        )

    if inefficient_ratio > 20.0:
        recommendations.append(
            "Reduce overly broad searches returning >50 files; "
            "refine patterns to target specific subdirectories or file types"
        )

    if avg_pattern_to_action < 40.0:
        recommendations.append(
            "Improve pattern-to-action ratio by using tighter patterns that match "
            "only files you intend to use"
        )

    if duplicate_ratio > 15.0:
        recommendations.append(
            "Combine similar patterns into single broader Glob call to reduce "
            "duplicate searches"
        )

    if avg_efficiency < 75.0:
        recommendations.append(
            "Overall search efficiency is low; consider using targeted patterns "
            "before resorting to broad exploratory searches"
        )

    if not recommendations:
        recommendations.append("Glob usage is efficient; maintain current strategy")

    return recommendations


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_glob_calls": 0,
        "avg_specificity_score": 0.0,
        "inefficient_searches": 0,
        "inefficient_search_ratio": 0.0,
        "avg_files_returned": 0.0,
        "avg_files_used": 0.0,
        "pattern_to_action_ratio": 0.0,
        "duplicate_patterns": 0,
        "duplicate_pattern_ratio": 0.0,
        "search_efficiency_score": 0.0,
        "high_efficiency_searches": 0,
        "low_efficiency_searches": 0,
        "patterns_by_type": {},
        "inefficient_patterns": [],
        "recommendations": [],
    }


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value (int or float) if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
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
