"""Pack task acceptance criteria coverage analyzer for quality validation.

Analyzes how well pack executions validate acceptance criteria to ensure
tasks meet quality standards and requirements are fully tested.

Acceptance criteria coverage metrics:
- Criteria validation rate: Percentage of criteria with validation
- Untested criteria count: Criteria without verification
- Verification-to-criteria alignment: How well tests match criteria
- Common validation gaps: Frequently missed criteria patterns
- Criteria specificity score: How measurable criteria are

Coverage patterns:
- Comprehensive: All criteria validated with passing tests
- Partial: Some criteria validated but gaps exist
- Weak: Few or no criteria explicitly validated
- Misaligned: Verification doesn't match stated criteria
"""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_pack_task_acceptance_criteria_coverage(records: object) -> dict[str, Any]:
    """Analyze acceptance criteria coverage in pack executions.

    Evaluates how well verification validates acceptance criteria, identifying
    gaps and measuring alignment between criteria and verification.

    Args:
        records: List of pack task dictionaries with keys:
            - pack_id: Execution pack identifier
            - task_id: Task identifier
            - acceptance_criteria: List of criteria strings or dict with criteria
            - verification_command: Command used to verify
            - verification_passed: Boolean indicating verification success
            - expected_files: Optional list of files that should be modified

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - total_tasks: Total number of tasks across packs
            - tasks_with_criteria: Count of tasks with defined criteria
            - total_criteria: Total acceptance criteria across all tasks
            - criteria_validation_rate: Percentage of criteria validated
            - untested_criteria_count: Criteria without validation
            - verification_to_criteria_alignment_score: Alignment score (0.0-1.0)
            - common_validation_gaps: List of common gap patterns
            - criteria_specificity_score: Measurability score (0.0-1.0)
            - fully_validated_tasks: Tasks with all criteria validated

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack task dictionaries")

    total_packs = set()
    total_tasks = 0
    tasks_with_criteria = 0
    total_criteria = 0
    validated_criteria = 0
    untested_criteria = 0

    validation_gaps: list[str] = []
    specificity_scores: list[float] = []
    alignment_scores: list[float] = []

    fully_validated_tasks = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        pack_id = record.get("pack_id")
        if pack_id:
            total_packs.add(str(pack_id))

        total_tasks += 1

        # Extract acceptance criteria
        criteria = _extract_criteria(record.get("acceptance_criteria"))
        if not criteria:
            continue

        tasks_with_criteria += 1
        task_criteria_count = len(criteria)
        total_criteria += task_criteria_count

        # Check verification
        verification_cmd = record.get("verification_command", "")
        verification_passed = record.get("verification_passed")

        # Analyze each criterion
        task_validated = 0
        task_untested = 0

        for criterion in criteria:
            # Measure specificity
            spec_score = _measure_specificity(criterion)
            specificity_scores.append(spec_score)

            # Check if criterion is validated
            if verification_cmd and _criterion_validated(criterion, verification_cmd):
                task_validated += 1
                validated_criteria += 1
            else:
                task_untested += 1
                untested_criteria += 1
                # Track gap pattern
                gap_pattern = _identify_gap_pattern(criterion)
                if gap_pattern:
                    validation_gaps.append(gap_pattern)

        # Calculate alignment for this task
        if task_criteria_count > 0:
            task_alignment = task_validated / task_criteria_count
            alignment_scores.append(task_alignment)

            if task_validated == task_criteria_count and verification_passed:
                fully_validated_tasks += 1

    # Calculate metrics
    criteria_validation_rate = _percentage(validated_criteria, total_criteria)
    avg_alignment = _average(alignment_scores)
    avg_specificity = _average(specificity_scores)

    # Count common validation gaps
    from collections import Counter
    gap_counts = Counter(validation_gaps)
    common_gaps = [
        {"gap_pattern": gap, "count": count}
        for gap, count in gap_counts.most_common(10)
    ]

    return {
        "total_packs": len(total_packs),
        "total_tasks": total_tasks,
        "tasks_with_criteria": tasks_with_criteria,
        "total_criteria": total_criteria,
        "criteria_validation_rate": criteria_validation_rate,
        "untested_criteria_count": untested_criteria,
        "verification_to_criteria_alignment_score": avg_alignment,
        "common_validation_gaps": common_gaps,
        "criteria_specificity_score": avg_specificity,
        "fully_validated_tasks": fully_validated_tasks,
    }


def _extract_criteria(value: object) -> list[str]:
    """Extract acceptance criteria from various formats."""
    criteria: list[str] = []

    if isinstance(value, str):
        # Single criterion as string
        if value.strip():
            criteria.append(value.strip())
    elif isinstance(value, list):
        # List of criteria
        for item in value:
            if isinstance(item, str) and item.strip():
                criteria.append(item.strip())
            elif isinstance(item, Mapping) and "criterion" in item:
                crit = item.get("criterion")
                if isinstance(crit, str) and crit.strip():
                    criteria.append(crit.strip())
    elif isinstance(value, Mapping):
        # Dict with criteria list
        crit_list = value.get("criteria", [])
        if isinstance(crit_list, list):
            for item in crit_list:
                if isinstance(item, str) and item.strip():
                    criteria.append(item.strip())

    return criteria


def _measure_specificity(criterion: str) -> float:
    """Measure how specific/measurable a criterion is (0.0-1.0).

    Higher scores for criteria with:
    - Specific numbers or percentages
    - Measurable outcomes
    - Concrete verbs (pass, fail, return, raise)
    - Expected values or ranges
    """
    score = 0.0

    # Check for specific numbers or percentages
    if re.search(r'\d+%?', criterion):
        score += 0.3

    # Check for measurable verbs
    measurable_verbs = ['pass', 'fail', 'return', 'raise', 'produce', 'output', 'calculate', 'validate']
    if any(verb in criterion.lower() for verb in measurable_verbs):
        score += 0.3

    # Check for expected values
    if re.search(r'(?:equal|match|contain|include|exceed)', criterion.lower()):
        score += 0.2

    # Check for comparison operators
    if re.search(r'(?:[<>]=?|==|!=)', criterion):
        score += 0.2

    return min(score, 1.0)


def _criterion_validated(criterion: str, verification_cmd: str) -> bool:
    """Check if criterion appears to be validated by verification command.

    Looks for keywords from criterion in verification command.
    """
    # Extract key terms from criterion (words >3 chars)
    words = re.findall(r'\b\w{4,}\b', criterion.lower())

    if not words:
        return False

    # Check if any key terms appear in verification command
    cmd_lower = verification_cmd.lower()
    return any(word in cmd_lower for word in words)


def _identify_gap_pattern(criterion: str) -> str:
    """Identify pattern of validation gap from criterion text."""
    text = criterion.lower()

    if "coverage" in text or "test" in text:
        return "test_coverage"
    elif "type" in text or "typing" in text:
        return "type_checking"
    elif "performance" in text or "speed" in text:
        return "performance"
    elif "error" in text or "exception" in text:
        return "error_handling"
    elif "return" in text or "output" in text:
        return "output_validation"
    else:
        return "general"


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 3)
