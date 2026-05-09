"""Pack code complexity and over-engineering detection analyzer.

Analyzes code complexity and over-engineering patterns across execution packs. Tracks
lines added vs changed, abstraction layering, premature optimization indicators,
unnecessary additions, and feature creep to detect over-engineering tendencies.

Code complexity metrics:
- Lines added vs changed ratio: New code vs modifications
- Abstraction layering: New helpers/utils for one-time use
- Premature optimization: Feature flags, compatibility shims, unused parameters
- Unnecessary additions: Comments/types on unchanged code, defensive validation
- Feature creep: Additions beyond user request
- Complexity score: 0-100 score (minimal changes = higher, abstractions = lower)

Quality indicators:
- Low lines added ratio (<30%): More modifications than new code
- Low abstraction count (<2): Minimal helper/utility creation
- No premature optimization: No feature flags or compatibility code
- Low unnecessary additions (<15%): Focused changes only
- No feature creep: Implementation matches requirements exactly
- High simplicity score (>80): Minimal, focused implementation
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_code_complexity(records: object) -> dict[str, Any]:
    """Analyze code complexity and over-engineering patterns in packs.

    Tracks complexity indicators and over-engineering detection.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_lines_changed: Total lines modified
            - lines_added: Lines of new code added
            - lines_modified: Lines of existing code changed
            - new_functions: Number of new function definitions
            - new_classes: Number of new class definitions
            - helper_utils_created: One-time use helpers/utils
            - feature_flags_added: Feature flag/toggle additions
            - compatibility_shims: Backwards-compatibility code
            - unused_params_added: Parameters renamed to _unused or similar
            - comments_on_unchanged: Comments added to unchanged code
            - types_on_unchanged: Type annotations on unchanged code
            - defensive_validation: Validation for impossible scenarios
            - features_beyond_request: Features not in requirements
            - pack_title: Optional pack title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_total_lines_changed: Average lines changed per pack
            - avg_lines_added_ratio: Average % new lines vs total
            - avg_abstraction_count: Average new functions + classes
            - avg_helper_utils_ratio: Average % one-time helpers
            - packs_with_premature_optimization: Count with flags/shims
            - avg_unnecessary_additions_ratio: Average % unnecessary code
            - packs_with_feature_creep: Count exceeding requirements
            - complexity_score: Score 0-100 (higher = simpler)
            - simple_implementation_packs: Count with score >80
            - over_engineered_packs: Count with score <50

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    lines_changed: list[int | float] = []
    added_ratios: list[float] = []
    abstraction_counts: list[int | float] = []
    helper_ratios: list[float] = []
    unnecessary_ratios: list[float] = []
    complexity_scores: list[float] = []

    packs_with_premature = 0
    packs_with_creep = 0
    simple_implementation_packs = 0  # >80 score
    over_engineered_packs = 0  # <50 score

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        total_lines = _extract_number(record.get("total_lines_changed"))
        lines_add = _extract_number(record.get("lines_added"))
        new_funcs = _extract_number(record.get("new_functions"))
        new_classes = _extract_number(record.get("new_classes"))
        helpers = _extract_number(record.get("helper_utils_created"))
        feature_flags = _extract_number(record.get("feature_flags_added"))
        compat_shims = _extract_number(record.get("compatibility_shims"))
        unused_params = _extract_number(record.get("unused_params_added"))
        comments_unchanged = _extract_number(record.get("comments_on_unchanged"))
        types_unchanged = _extract_number(record.get("types_on_unchanged"))
        defensive = _extract_number(record.get("defensive_validation"))
        beyond_request = _extract_number(record.get("features_beyond_request"))

        # Track lines changed
        if total_lines is not None and total_lines > 0:
            lines_changed.append(total_lines)

            # Calculate added ratio
            if lines_add is not None:
                added_ratios.append(_percentage(lines_add, total_lines))

        # Calculate abstraction count
        total_abstractions = 0
        if new_funcs is not None:
            total_abstractions += new_funcs
        if new_classes is not None:
            total_abstractions += new_classes
        if total_abstractions > 0:
            abstraction_counts.append(total_abstractions)

        # Calculate helper utils ratio
        if helpers is not None and total_abstractions > 0:
            helper_ratios.append(_percentage(helpers, total_abstractions))

        # Detect premature optimization
        has_premature = False
        if feature_flags is not None and feature_flags > 0:
            has_premature = True
        if compat_shims is not None and compat_shims > 0:
            has_premature = True
        if unused_params is not None and unused_params > 0:
            has_premature = True
        if has_premature:
            packs_with_premature += 1

        # Calculate unnecessary additions ratio
        unnecessary_total = 0
        if comments_unchanged is not None:
            unnecessary_total += comments_unchanged
        if types_unchanged is not None:
            unnecessary_total += types_unchanged
        if defensive is not None:
            unnecessary_total += defensive

        if total_lines is not None and total_lines > 0:
            unnecessary_ratios.append(_percentage(unnecessary_total, total_lines))

        # Detect feature creep
        if beyond_request is not None and beyond_request > 0:
            packs_with_creep += 1

        # Calculate complexity score
        complexity_score = _calculate_complexity_score(
            added_ratio=added_ratios[-1] if added_ratios and len(added_ratios) > len(complexity_scores) else None,
            abstraction_count=abstraction_counts[-1] if abstraction_counts and len(abstraction_counts) > len(complexity_scores) else None,
            has_premature=has_premature,
            unnecessary_ratio=unnecessary_ratios[-1] if unnecessary_ratios and len(unnecessary_ratios) > len(complexity_scores) else None,
            has_creep=beyond_request is not None and beyond_request > 0,
        )
        complexity_scores.append(complexity_score)

        if complexity_score > 80.0:
            simple_implementation_packs += 1
        elif complexity_score < 50.0:
            over_engineered_packs += 1

    # Calculate aggregate metrics
    avg_lines = _average(lines_changed)
    avg_added = _average(added_ratios)
    avg_abstractions = _average(abstraction_counts)
    avg_helpers = _average(helper_ratios)
    avg_unnecessary = _average(unnecessary_ratios)
    avg_complexity = _average(complexity_scores)

    return {
        "total_packs": total_packs,
        "avg_total_lines_changed": avg_lines,
        "avg_lines_added_ratio": avg_added,
        "avg_abstraction_count": avg_abstractions,
        "avg_helper_utils_ratio": avg_helpers,
        "packs_with_premature_optimization": packs_with_premature,
        "avg_unnecessary_additions_ratio": avg_unnecessary,
        "packs_with_feature_creep": packs_with_creep,
        "complexity_score": avg_complexity,
        "simple_implementation_packs": simple_implementation_packs,
        "over_engineered_packs": over_engineered_packs,
    }


def _calculate_complexity_score(
    added_ratio: float | None,
    abstraction_count: int | float | None,
    has_premature: bool,
    unnecessary_ratio: float | None,
    has_creep: bool,
) -> float:
    """Calculate complexity/simplicity score (0-100).

    Higher scores indicate simpler, more focused implementation:
    - Low added ratio (<30%): More modifications than new code
    - Low abstraction count (<2): Minimal helper creation
    - No premature optimization: No feature flags or shims
    - Low unnecessary additions (<15%): Focused changes
    - No feature creep: Implementation matches requirements

    Scoring breakdown:
    - Added ratio: 30 points (30% threshold)
    - Abstraction count: 25 points (<2 threshold)
    - Premature optimization: 20 points (none)
    - Unnecessary additions: 15 points (15% threshold)
    - Feature creep: 10 points (none)
    """
    score = 0.0

    # Added ratio component (30 points)
    if added_ratio is not None:
        if added_ratio < 30:  # <30% = excellent
            score += 30.0
        elif added_ratio < 50:  # <50% = good
            score += 22.0
        elif added_ratio < 70:  # <70% = acceptable
            score += 15.0

    # Abstraction count component (25 points)
    if abstraction_count is not None:
        if abstraction_count < 2:  # <2 = excellent
            score += 25.0
        elif abstraction_count < 4:  # <4 = good
            score += 18.0
        elif abstraction_count < 6:  # <6 = acceptable
            score += 10.0

    # Premature optimization component (20 points)
    if not has_premature:
        score += 20.0

    # Unnecessary additions component (15 points)
    if unnecessary_ratio is not None:
        if unnecessary_ratio < 15:  # <15% = excellent
            score += 15.0
        elif unnecessary_ratio < 30:  # <30% = good
            score += 10.0
        elif unnecessary_ratio < 50:  # <50% = acceptable
            score += 5.0

    # Feature creep component (10 points)
    if not has_creep:
        score += 10.0

    return round(score, 2)


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
