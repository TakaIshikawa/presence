<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME
"""

from __future__ import annotations

import re
<<<<<<< HEAD
from collections import Counter
from typing import Any, Mapping


def analyze_pack_acceptance_criteria_coverage(records: object) -> dict[str, Any]:
    """Analyze acceptance criteria quality and coverage within packs.

    Evaluates acceptance criteria for presence, measurability, and
    alignment with verification commands. Identifies gaps in validation
    coverage and criteria quality issues.
=======
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
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - acceptance_criteria: List of acceptance criteria strings
<<<<<<< HEAD
            - verification_command: Verification command string
            - expected_files: Optional list of expected files
=======
            - test_command: Verification command to validate task
            - expected_files: List of files task expects to modify
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    total_tasks = 0
<<<<<<< HEAD
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
=======
    tasks_with_criteria = 0
    tasks_without_criteria = 0
    total_criteria = 0
    measurable_criteria = 0
    vague_criteria = 0
    tasks_with_test_commands = 0
    tasks_with_aligned_verification = 0
    unvalidated_criteria_count = 0

    examples: list[dict[str, Any]] = []
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME

    return False


<<<<<<< HEAD
def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
=======
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
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME
