"""Pack acceptance criteria coverage analyzer for verification quality assessment.

Evaluates how well pack verification validates the stated acceptance criteria.
Identifies gaps where criteria lack corresponding verification, or verification
commands test aspects not covered by explicit acceptance criteria.

Acceptance criteria quality indicators:
- Measurability: Observable outcomes vs vague goals
- Specificity: Concrete assertions vs general statements
- Verification alignment: Test commands validate the stated criteria
- Coverage completeness: All criteria have corresponding verification
- Testability: Criteria can be objectively validated

Verification gaps:
- Unvalidated criteria: ACs without corresponding test coverage
- Vague criteria: Non-measurable goals ('improve', 'better', 'enhance')
- Missing criteria: Core functionality not covered by any AC
- Over-verification: Tests beyond stated criteria (may indicate missing ACs)
"""

from __future__ import annotations

import re
from typing import Any, Mapping


# Vague terms that indicate non-measurable criteria
VAGUE_CRITERIA_TERMS = (
    'improve',
    'better',
    'enhance',
    'optimize',
    'ensure quality',
    'good',
    'appropriate',
    'reasonable',
    'sufficient',
    'adequate',
    'various',
    'some',
    'properly',
)

# Measurable indicator keywords
MEASURABLE_KEYWORDS = (
    'test',
    'verify',
    'assert',
    'check',
    'validate',
    'detect',
    'identify',
    'measure',
    'calculate',
    'count',
    'pass',
    'fail',
    'return',
    'raise',
    'contain',
    'match',
    'equal',
)


def analyze_pack_acceptance_criteria_coverage(records: object) -> dict[str, Any]:
    """Analyze acceptance criteria quality and verification coverage in packs.

    Evaluates whether acceptance criteria are measurable, specific, and properly
    validated by verification commands.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - acceptance_criteria: List of acceptance criteria strings
            - test_command: Verification command to validate task
            - expected_files: List of files task expects to modify

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - tasks_with_criteria: Number of tasks with acceptance criteria
            - tasks_without_criteria: Tasks missing acceptance criteria
            - total_criteria: Total number of acceptance criteria across all tasks
            - avg_criteria_per_task: Average criteria count per task
            - measurable_criteria: Number of criteria with measurable indicators
            - vague_criteria: Number of criteria with vague terms
            - measurability_rate: Percentage of measurable criteria
            - tasks_with_test_commands: Tasks with verification commands
            - tasks_with_aligned_verification: Tasks where tests align with criteria
            - unvalidated_criteria_count: Criteria lacking verification coverage
            - coverage_score: Overall quality score (0-100)
            - examples: Sample criteria with quality assessments

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    total_tasks = 0
    tasks_with_criteria = 0
    tasks_without_criteria = 0
    total_criteria = 0
    measurable_criteria = 0
    vague_criteria = 0
    tasks_with_test_commands = 0
    tasks_with_aligned_verification = 0
    unvalidated_criteria_count = 0

    examples: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

        task_id = _string(record.get('task_id'))
        criteria_list = record.get('acceptance_criteria')
        test_command = _string(record.get('test_command'))
        expected_files = record.get('expected_files')

        # Check if task has criteria
        if not criteria_list or not isinstance(criteria_list, (list, tuple)):
            tasks_without_criteria += 1
            if len(examples) < 5:
                examples.append({
                    'task_id': task_id,
                    'issue': 'missing_criteria',
                    'description': 'Task has no acceptance criteria defined',
                })
            continue

        tasks_with_criteria += 1
        task_criteria_count = len(criteria_list)
        total_criteria += task_criteria_count

        # Check if task has test command
        has_test_command = bool(test_command)
        if has_test_command:
            tasks_with_test_commands += 1

        # Analyze each criterion
        task_measurable_count = 0
        task_vague_count = 0
        task_has_alignment = True

        for criterion in criteria_list:
            if not isinstance(criterion, str):
                continue

            criterion_text = criterion.strip()
            if not criterion_text:
                continue

            # Check if criterion is measurable
            is_measurable = _is_measurable_criterion(criterion_text)
            is_vague = _is_vague_criterion(criterion_text)

            if is_measurable:
                measurable_criteria += 1
                task_measurable_count += 1
            if is_vague:
                vague_criteria += 1
                task_vague_count += 1

            # Check if test command validates this criterion
            if has_test_command:
                is_validated = _criterion_validated_by_command(criterion_text, test_command, expected_files)
                if not is_validated:
                    unvalidated_criteria_count += 1
                    task_has_alignment = False
                    if len(examples) < 5:
                        examples.append({
                            'task_id': task_id,
                            'issue': 'unvalidated_criterion',
                            'criterion': criterion_text[:100],
                            'test_command': test_command[:100] if test_command else None,
                            'description': 'Criterion lacks verification coverage',
                        })
            else:
                # No test command means criteria cannot be validated
                unvalidated_criteria_count += 1
                task_has_alignment = False

        # Check if verification aligns with criteria
        if task_has_alignment and has_test_command and task_criteria_count > 0:
            tasks_with_aligned_verification += 1

        # Add example for vague criteria
        if task_vague_count > 0 and len(examples) < 5:
            vague_criterion_examples = [
                c for c in criteria_list if isinstance(c, str) and _is_vague_criterion(c)
            ]
            if vague_criterion_examples:
                examples.append({
                    'task_id': task_id,
                    'issue': 'vague_criteria',
                    'criterion': vague_criterion_examples[0][:100],
                    'description': 'Criterion uses vague, non-measurable terms',
                })

    # Calculate metrics
    avg_criteria_per_task = round(total_criteria / total_tasks, 2) if total_tasks > 0 else 0.0
    measurability_rate = _percentage(measurable_criteria, total_criteria)
    alignment_rate = _percentage(tasks_with_aligned_verification, tasks_with_criteria)
    criteria_presence_rate = _percentage(tasks_with_criteria, total_tasks)

    # Calculate overall coverage score (0-100)
    coverage_score = _calculate_coverage_score(
        criteria_presence_rate=criteria_presence_rate,
        measurability_rate=measurability_rate,
        alignment_rate=alignment_rate,
        vague_rate=_percentage(vague_criteria, total_criteria),
    )

    return {
        'total_tasks': total_tasks,
        'tasks_with_criteria': tasks_with_criteria,
        'tasks_without_criteria': tasks_without_criteria,
        'total_criteria': total_criteria,
        'avg_criteria_per_task': avg_criteria_per_task,
        'measurable_criteria': measurable_criteria,
        'vague_criteria': vague_criteria,
        'measurability_rate': measurability_rate,
        'tasks_with_test_commands': tasks_with_test_commands,
        'tasks_with_aligned_verification': tasks_with_aligned_verification,
        'unvalidated_criteria_count': unvalidated_criteria_count,
        'coverage_score': coverage_score,
        'examples': examples[:5],
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _is_measurable_criterion(criterion: str) -> bool:
    """Check if criterion contains measurable indicators."""
    if not criterion:
        return False
    normalized = criterion.lower()
    return any(keyword in normalized for keyword in MEASURABLE_KEYWORDS)


def _is_vague_criterion(criterion: str) -> bool:
    """Check if criterion uses vague, non-measurable terms."""
    if not criterion:
        return True
    normalized = criterion.lower()
    return any(term in normalized for term in VAGUE_CRITERIA_TERMS)


def _criterion_validated_by_command(
    criterion: str,
    test_command: str,
    expected_files: object,
) -> bool:
    """Check if test command validates the criterion.

    This is a heuristic check - we look for evidence that the test command
    covers the criterion through:
    1. File overlap (test command tests files mentioned in criterion or expected files)
    2. Keyword overlap (test command and criterion share domain terms)
    """
    if not test_command:
        return False

    criterion_normalized = criterion.lower()
    command_normalized = test_command.lower()

    # Check if test command tests any of the expected files
    if expected_files and isinstance(expected_files, (list, tuple)):
        for expected_file in expected_files:
            if isinstance(expected_file, str):
                file_name = expected_file.strip()
                # Check if test command references this file
                if file_name in command_normalized:
                    return True
                # Check if criterion mentions this file
                if file_name in criterion_normalized:
                    # And test command tests this file
                    if file_name in command_normalized:
                        return True

    # Check for keyword overlap between criterion and test command
    # Extract meaningful words (4+ chars) from criterion
    criterion_words = set(
        word for word in re.findall(r'\b\w{4,}\b', criterion_normalized)
        if word not in {'test', 'tests', 'that', 'with', 'from', 'have', 'this', 'when'}
    )
    command_words = set(re.findall(r'\b\w{4,}\b', command_normalized))

    # If there's significant word overlap, likely the test validates the criterion
    overlap = criterion_words.intersection(command_words)
    if len(overlap) >= 2:  # At least 2 meaningful words overlap
        return True

    return False


def _extract_file_paths(text: str) -> list[str]:
    """Extract file paths from text."""
    # Match common file patterns: path/to/file.ext
    file_pattern = r'\b[\w/.-]+\.\w{2,4}\b'
    matches = re.findall(file_pattern, text)
    return matches


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _calculate_coverage_score(
    criteria_presence_rate: float,
    measurability_rate: float,
    alignment_rate: float,
    vague_rate: float,
) -> float:
    """Calculate overall acceptance criteria coverage score (0-100).

    Args:
        criteria_presence_rate: Percentage of tasks with criteria
        measurability_rate: Percentage of measurable criteria
        alignment_rate: Percentage of tasks with aligned verification
        vague_rate: Percentage of vague criteria

    Returns:
        Score from 0-100 indicating overall coverage quality
    """
    # Weight different factors
    score = (
        criteria_presence_rate * 0.25 +
        measurability_rate * 0.30 +
        alignment_rate * 0.30 -
        vague_rate * 0.15  # Penalize vague criteria
    )

    # Ensure score stays in 0-100 range
    return round(max(0.0, min(100.0, score)), 2)
