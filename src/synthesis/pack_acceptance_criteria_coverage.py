"""Pack acceptance criteria coverage analyzer for test quality assessment.

Analyzes acceptance criteria (AC) quality within execution packs to ensure
tasks have clear, measurable success conditions. Evaluates AC presence,
measurability, alignment with verification commands, and coverage.

Coverage metrics:
- AC presence: Tasks with explicit acceptance criteria
- AC count per task: Number of criteria defined
- Measurability: Observable outcomes vs vague goals
- Verification alignment: Criteria testable by verification commands
- Unvalidated criteria: ACs without verification coverage

Quality indicators:
- Well-defined criteria: Specific, measurable, testable outcomes
- Poorly-defined criteria: Vague, subjective, unmeasurable goals
- Missing criteria: Tasks without any acceptance criteria
- Coverage gaps: ACs that verification commands cannot validate
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any, Mapping


def analyze_pack_acceptance_criteria_coverage(records: object) -> dict[str, Any]:
    """Analyze acceptance criteria quality and coverage within packs.

    Evaluates acceptance criteria for presence, measurability, and
    alignment with verification commands. Identifies gaps in validation
    coverage and criteria quality issues.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - acceptance_criteria: List of acceptance criteria strings
            - verification_command: Verification command string
            - expected_files: Optional list of expected files

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - has_acceptance_criteria: Tasks with at least one AC
            - avg_criteria_per_task: Mean number of ACs per task
            - total_criteria: Total number of criteria across all tasks
            - measurable_criteria_count: Criteria with observable outcomes
            - vague_criteria_count: Criteria with subjective/vague language
            - avg_measurability_score: Average measurability (0-100)
            - verification_aligned_count: Tasks where ACs align with tests
            - unvalidated_criteria_count: Criteria lacking verification
            - missing_criteria_count: Tasks without any ACs
            - well_defined_task_count: Tasks with high-quality ACs
            - poorly_defined_task_count: Tasks with vague/missing ACs
            - common_vague_terms: Most frequent vague terms in criteria

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    total_tasks = 0
    has_acceptance_criteria = 0
    criteria_counts: list[int] = []
    total_criteria = 0

    measurable_criteria_count = 0
    vague_criteria_count = 0
    measurability_scores: list[float] = []

    verification_aligned_count = 0
    unvalidated_criteria_count = 0
    missing_criteria_count = 0

    well_defined_task_count = 0
    poorly_defined_task_count = 0

    vague_term_counter: Counter[str] = Counter()

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

        acceptance_criteria = record.get("acceptance_criteria")
        verification_command = _string(record.get("verification_command", ""))

        # Check for acceptance criteria presence
        criteria_list = _get_criteria_list(acceptance_criteria)
        criteria_count = len(criteria_list)
        criteria_counts.append(criteria_count)

        if criteria_count > 0:
            has_acceptance_criteria += 1
            total_criteria += criteria_count
        else:
            missing_criteria_count += 1

        # Analyze each criterion for measurability
        task_measurable = 0
        task_vague = 0
        task_scores: list[float] = []
        task_validated = 0

        for criterion in criteria_list:
            score = _calculate_measurability_score(criterion)
            task_scores.append(score)
            measurability_scores.append(score)

            if score >= 70:
                measurable_criteria_count += 1
                task_measurable += 1
            elif score <= 30:
                vague_criteria_count += 1
                task_vague += 1

            # Detect vague terms
            vague_terms = _detect_vague_terms(criterion)
            vague_term_counter.update(vague_terms)

            # Check if criterion is validated by verification command
            if _is_criterion_validated(criterion, verification_command):
                task_validated += 1
            else:
                unvalidated_criteria_count += 1

        # Check verification alignment at task level
        if criteria_count > 0 and task_validated >= criteria_count * 0.75:
            # At least 75% of criteria are validated
            verification_aligned_count += 1

        # Classify task quality
        if criteria_count > 0:
            avg_task_score = sum(task_scores) / len(task_scores) if task_scores else 0
            validated_ratio = task_validated / criteria_count if criteria_count > 0 else 0

            if avg_task_score >= 60 and validated_ratio >= 0.75:
                well_defined_task_count += 1
            elif avg_task_score <= 40 or validated_ratio <= 0.25:
                poorly_defined_task_count += 1
        else:
            # No criteria = poorly defined
            poorly_defined_task_count += 1

    # Calculate averages
    avg_criteria_per_task = _average(criteria_counts)
    avg_measurability_score = _average(measurability_scores)

    # Format common vague terms
    common_vague_terms = [
        {"term": term, "count": count}
        for term, count in vague_term_counter.most_common(5)
    ]

    return {
        "total_tasks": total_tasks,
        "has_acceptance_criteria": has_acceptance_criteria,
        "avg_criteria_per_task": avg_criteria_per_task,
        "total_criteria": total_criteria,
        "measurable_criteria_count": measurable_criteria_count,
        "vague_criteria_count": vague_criteria_count,
        "avg_measurability_score": avg_measurability_score,
        "verification_aligned_count": verification_aligned_count,
        "unvalidated_criteria_count": unvalidated_criteria_count,
        "missing_criteria_count": missing_criteria_count,
        "well_defined_task_count": well_defined_task_count,
        "poorly_defined_task_count": poorly_defined_task_count,
        "common_vague_terms": common_vague_terms,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _get_criteria_list(acceptance_criteria: object) -> list[str]:
    """Extract list of criteria strings from various formats.

    Handles:
    - List of strings
    - Single string (split on newlines)
    - Empty/None values
    """
    if acceptance_criteria is None:
        return []

    if isinstance(acceptance_criteria, list):
        # Filter out non-string elements and empty strings
        return [
            _string(criterion)
            for criterion in acceptance_criteria
            if isinstance(criterion, str) and _string(criterion)
        ]

    if isinstance(acceptance_criteria, str):
        # Split on newlines and filter empties
        return [
            line.strip()
            for line in acceptance_criteria.split("\n")
            if line.strip()
        ]

    return []


def _calculate_measurability_score(criterion: str) -> float:
    """Calculate measurability score for a single criterion.

    Measurable criteria indicators:
    - Concrete verbs (passes, detects, extracts, identifies, tests)
    - Observable outcomes (file exists, output contains, error shown)
    - Specific metrics (count, ratio, percentage, time)
    - File/function references (specific targets)

    Vague criteria indicators:
    - Subjective language (good, clean, proper, appropriate)
    - Ambiguous verbs (handles, manages, works, ensures)
    - No observable outcome
    - Vague quantities (some, various, several)

    Score: 0-100, higher is more measurable
    """
    if not criterion:
        return 0.0

    criterion_lower = criterion.lower()
    score = 50.0  # Base score

    # Measurable indicators (+)
    measurable_verbs = [
        'passes', 'fails', 'detects', 'extracts', 'identifies', 'calculates',
        'counts', 'measures', 'tracks', 'tests', 'validates', 'verifies',
        'returns', 'outputs', 'generates', 'creates', 'removes', 'adds',
        'updates', 'deletes', 'contains', 'includes', 'matches'
    ]

    for verb in measurable_verbs:
        if re.search(rf'\b{verb}\b', criterion_lower):
            score += 20.0
            break

    # Observable outcome indicators
    observable_patterns = [
        r'\btest[s]?\s+(?:pass|cover|run)\b',
        r'\berror[s]?\s+(?:shown|displayed|raised)\b',
        r'\bfile[s]?\s+(?:exists?|created?|contains?)\b',
        r'\boutput[s]?\s+(?:contains?|includes?|shows?)\b',
        r'\b(?:count|ratio|percentage|score|metric|time|under|over|above|below|exceed)\b',
        r'\b(?:all|every|each|no|zero|equals?)\s+\w+\b',
        r'\b(?:success(?:ful)?|failure|error)\s+(?:message|count|status)\b'
    ]

    for pattern in observable_patterns:
        if re.search(pattern, criterion_lower):
            score += 15.0
            break

    # Additional boost if has specific numeric/quantifiable indicators
    if re.search(r'\b(?:\d+%?|zero|one|two|three|first|last)\b', criterion_lower):
        score += 10.0

    # Specific file/function references
    if re.search(r'\b[\w/]+\.[\w]{2,4}\b', criterion):
        score += 10.0

    # Vague indicators (-)
    vague_terms = [
        'good', 'clean', 'proper', 'appropriate', 'correct', 'better',
        'nice', 'well', 'quality', 'readable', 'maintainable'
    ]

    for term in vague_terms:
        if re.search(rf'\b{term}\b', criterion_lower):
            score -= 20.0
            break

    # Ambiguous verbs (-)
    ambiguous_verbs = [
        'handles', 'manages', 'works', 'deals with', 'addresses',
        'improves', 'enhances', 'optimizes'
    ]

    for verb in ambiguous_verbs:
        if re.search(rf'\b{verb}\b', criterion_lower):
            score -= 15.0
            break

    # Vague quantities (-)
    if re.search(r'\b(?:some|various|several|multiple|many|few)\b', criterion_lower):
        score -= 10.0

    # Normalize to 0-100
    return max(0.0, min(100.0, score))


def _detect_vague_terms(criterion: str) -> list[str]:
    """Detect vague/subjective terms in criterion.

    Returns list of vague terms found.
    """
    if not criterion:
        return []

    criterion_lower = criterion.lower()
    vague_terms = []

    # Subjective quality terms
    subjective = [
        'good', 'clean', 'proper', 'appropriate', 'correct', 'better',
        'nice', 'well', 'quality', 'readable', 'maintainable', 'clear'
    ]

    for term in subjective:
        if re.search(rf'\b{term}\b', criterion_lower):
            vague_terms.append(term)

    # Vague quantities
    vague_quantities = [
        'some', 'various', 'several', 'multiple', 'many', 'few'
    ]

    for term in vague_quantities:
        if re.search(rf'\b{term}\b', criterion_lower):
            vague_terms.append(term)

    # Ambiguous verbs
    ambiguous = [
        'handles', 'manages', 'works', 'deals', 'addresses',
        'improves', 'enhances', 'optimizes'
    ]

    for term in ambiguous:
        if re.search(rf'\b{term}\b', criterion_lower):
            vague_terms.append(term)

    return vague_terms


def _is_criterion_validated(criterion: str, verification_command: str) -> bool:
    """Check if criterion can be validated by verification command.

    Indicators:
    - Test-related criteria aligned with test commands
    - Type-related criteria aligned with type check commands
    - Lint-related criteria aligned with lint commands
    - File-specific criteria aligned with targeted commands
    """
    if not criterion or not verification_command:
        return False

    criterion_lower = criterion.lower()
    command_lower = verification_command.lower()

    # Check if any verification command exists first
    has_verification = re.search(r'\b(?:pytest|test|jest|mocha|mypy|pyright|tsc|ruff|pylint|eslint|black|lint)\b', command_lower)
    if not has_verification:
        return False

    # Test coverage indicators - broad matching
    test_patterns = [
        r'\btest[s]?\b',
        r'\bcoverage\b',
        r'\bpass(?:es|ed)?\b',
        r'\brun[s]?\b'
    ]

    test_criterion = False
    for pattern in test_patterns:
        if re.search(pattern, criterion_lower):
            test_criterion = True
            break

    if test_criterion:
        # Check if command has test execution
        if re.search(r'\b(?:pytest|test|jest|mocha)\b', command_lower):
            return True

    # Type check indicators - broad matching
    type_patterns = [
        r'\btype[s]?\b',
        r'\bmypy\b',
        r'\bpyright\b',
        r'\bvalidat(?:e|ion|es?)\b',
        r'\bcheck[s]?\b'
    ]

    type_criterion = False
    for pattern in type_patterns:
        if re.search(pattern, criterion_lower):
            type_criterion = True
            break

    if type_criterion:
        # Check if command has type checking
        if re.search(r'\b(?:mypy|pyright|tsc)\b', command_lower):
            return True

    # Lint/format indicators
    lint_patterns = [
        r'\blint[s]?\b',
        r'\bformatting\b',
        r'\bstyle\b'
    ]

    lint_criterion = False
    for pattern in lint_patterns:
        if re.search(pattern, criterion_lower):
            lint_criterion = True
            break

    if lint_criterion:
        # Check if command has linting
        if re.search(r'\b(?:ruff|pylint|eslint|black)\b', command_lower):
            return True

    # File-specific criteria
    file_match = re.search(r'\b([\w/]+\.[\w]{2,4})\b', criterion)
    if file_match:
        filename = file_match.group(1)
        # Check if file is referenced in command
        if filename in verification_command:
            return True

    # Measurable outcome criteria that can be validated by any test command
    measurable_patterns = [
        r'\b(?:detects?|extracts?|identifies?|calculates?|measures?|tracks?)\b',
        r'\b(?:count|ratio|percentage|score|metric)\b',
        r'\b(?:returns?|outputs?|generates?|creates?)\b',
        r'\b(?:contains?|includes?|matches?)\b',
        r'\b(?:exists?|created?|shown|displayed?|raised?)\b'
    ]

    for pattern in measurable_patterns:
        if re.search(pattern, criterion_lower):
            # Any test command can validate measurable outcomes
            if re.search(r'\b(?:pytest|test|jest|mocha)\b', command_lower):
                return True

    return False


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
