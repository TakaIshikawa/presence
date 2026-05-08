"""Pack task description clarity analyzer for prompt quality assessment.

Analyzes task prompt quality within execution packs to identify clear,
actionable task descriptions versus vague, ambiguous prompts. Evaluates
specificity, scope boundedness, and presence of concrete file paths.

Clarity metrics:
- Concrete file paths: Presence of specific paths vs vague descriptions
- Acceptance criteria: Explicit AC presence
- Verb clarity: Use of clear imperative verbs
- Scope boundedness: Single concern indicators
- Ambiguity flags: Vague terms like 'improve', 'enhance', 'various'

Quality indicators:
- Clear prompts: Specific files, clear verbs, bounded scope, explicit ACs
- Vague prompts: No files, weak verbs, unbounded scope, ambiguity flags
- Red flags: Multiple ambiguity indicators suggesting unclear requirements
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any, Mapping


def analyze_pack_task_description_clarity(records: object) -> dict[str, Any]:
    """Analyze task prompt quality and clarity within execution packs.

    Evaluates task descriptions for specificity, clarity, and actionability.
    Identifies vague prompts that may lead to implementation confusion.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - prompt: Task description/prompt text
            - expected_files: List of files task expects to modify
            - acceptance_criteria: Optional list of acceptance criteria

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - has_specific_files_count: Tasks with concrete file paths
            - has_acceptance_criteria_count: Tasks with explicit ACs
            - avg_verb_clarity_score: Average verb clarity (0-100)
            - avg_scope_boundedness: Average scope focus score (0-100)
            - ambiguity_flag_count: Total ambiguity indicators found
            - tasks_with_ambiguity: Number of tasks with ambiguity flags
            - common_ambiguity_terms: Most frequent vague terms
            - clear_task_count: Tasks meeting clarity thresholds
            - vague_task_count: Tasks failing clarity thresholds
            - red_flag_task_count: Tasks with multiple ambiguity issues

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    total_tasks = 0
    has_specific_files_count = 0
    has_acceptance_criteria_count = 0

    verb_clarity_scores: list[float] = []
    scope_boundedness_scores: list[float] = []

    ambiguity_flag_count = 0
    tasks_with_ambiguity = 0
    ambiguity_term_counter: Counter[str] = Counter()

    clear_task_count = 0
    vague_task_count = 0
    red_flag_task_count = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

        prompt = _string(record.get("prompt", ""))
        expected_files = record.get("expected_files")
        acceptance_criteria = record.get("acceptance_criteria")

        # Check for specific file paths in expected_files or prompt
        has_files = _has_specific_files(expected_files, prompt)
        if has_files:
            has_specific_files_count += 1

        # Check for acceptance criteria
        has_acs = _has_acceptance_criteria(acceptance_criteria, prompt)
        if has_acs:
            has_acceptance_criteria_count += 1

        # Calculate verb clarity score
        verb_score = _calculate_verb_clarity(prompt)
        verb_clarity_scores.append(verb_score)

        # Calculate scope boundedness
        scope_score = _calculate_scope_boundedness(prompt)
        scope_boundedness_scores.append(scope_score)

        # Detect ambiguity flags
        task_ambiguity_terms = _detect_ambiguity_flags(prompt)
        ambiguity_flag_count += len(task_ambiguity_terms)
        if task_ambiguity_terms:
            tasks_with_ambiguity += 1
            ambiguity_term_counter.update(task_ambiguity_terms)

        # Classify task clarity
        clarity_signals = sum([has_files, has_acs, verb_score > 60, scope_score > 60])
        ambiguity_signals = len(task_ambiguity_terms)

        if clarity_signals >= 3 and ambiguity_signals == 0:
            clear_task_count += 1
        elif clarity_signals <= 1 or ambiguity_signals >= 3:
            vague_task_count += 1

        if ambiguity_signals >= 3:
            red_flag_task_count += 1

    # Calculate averages
    avg_verb_clarity = _average(verb_clarity_scores)
    avg_scope_boundedness = _average(scope_boundedness_scores)

    # Format common ambiguity terms
    common_ambiguity = [
        {"term": term, "count": count}
        for term, count in ambiguity_term_counter.most_common(5)
    ]

    return {
        "total_tasks": total_tasks,
        "has_specific_files_count": has_specific_files_count,
        "has_acceptance_criteria_count": has_acceptance_criteria_count,
        "avg_verb_clarity_score": avg_verb_clarity,
        "avg_scope_boundedness": avg_scope_boundedness,
        "ambiguity_flag_count": ambiguity_flag_count,
        "tasks_with_ambiguity": tasks_with_ambiguity,
        "common_ambiguity_terms": common_ambiguity,
        "clear_task_count": clear_task_count,
        "vague_task_count": vague_task_count,
        "red_flag_task_count": red_flag_task_count,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _has_specific_files(expected_files: object, prompt: str) -> bool:
    """Check if task has specific file paths.

    Looks for:
    - Non-empty expected_files list
    - Concrete file paths in prompt (e.g., src/main.py)
    """
    # Check expected_files list
    if isinstance(expected_files, list) and expected_files:
        return True

    # Check for file paths in prompt (path with extension or src/tests/etc)
    file_path_pattern = r'\b(?:src|tests|lib|bin)/[\w/]+\.\w+\b|\b\w+\.\w{2,4}\b'
    if re.search(file_path_pattern, prompt):
        return True

    return False


def _has_acceptance_criteria(acceptance_criteria: object, prompt: str) -> bool:
    """Check if task has explicit acceptance criteria.

    Looks for:
    - Non-empty acceptance_criteria list
    - AC keywords in prompt (acceptance criteria, AC, must, should, verify)
    """
    # Check acceptance_criteria list
    if isinstance(acceptance_criteria, list) and acceptance_criteria:
        return True

    # Check for AC indicators in prompt
    ac_pattern = r'\b(?:acceptance criteria|AC|must|should|verify|ensure|requirement)\b'
    if re.search(ac_pattern, prompt, re.IGNORECASE):
        return True

    return False


def _calculate_verb_clarity(prompt: str) -> float:
    """Calculate verb clarity score for prompt.

    Clear imperative verbs: create, implement, add, fix, remove, update, refactor
    Weak verbs: improve, enhance, optimize, handle, manage, work on
    Vague verbs: adjust, modify, change, deal with, look at

    Score: 100 for clear verbs, 50 for weak, 0 for vague/none
    """
    if not prompt:
        return 0.0

    prompt_lower = prompt.lower()

    # Clear imperative verbs
    clear_verbs = [
        'create', 'implement', 'add', 'fix', 'remove', 'delete', 'update',
        'refactor', 'build', 'write', 'test', 'validate', 'extract',
        'move', 'rename', 'replace', 'integrate', 'configure'
    ]

    # Weak verbs
    weak_verbs = [
        'improve', 'enhance', 'optimize', 'handle', 'manage', 'work on',
        'address', 'deal with', 'review', 'check', 'ensure'
    ]

    # Check for clear verbs first
    for verb in clear_verbs:
        if re.search(rf'\b{verb}\b', prompt_lower):
            return 100.0

    # Check for weak verbs
    for verb in weak_verbs:
        if re.search(rf'\b{verb}\b', prompt_lower):
            return 50.0

    # No clear verb found
    return 0.0


def _calculate_scope_boundedness(prompt: str) -> float:
    """Calculate scope boundedness score.

    Single concern indicators:
    - Specific feature/bug mention
    - Concrete action on specific target
    - Short, focused prompt (< 150 chars)

    Unbounded indicators:
    - Multiple concerns (and, also, additionally)
    - Vague targets (system, application, codebase)
    - Very long prompt (> 500 chars)

    Score: 0-100, higher is more bounded
    """
    if not prompt:
        return 0.0

    score = 50.0  # Base score

    # Length-based scoring
    length = len(prompt)
    if length < 150:
        score += 20.0
    elif length > 500:
        score -= 30.0

    # Check for multiple concerns
    multi_concern_pattern = r'\b(?:and|also|additionally|furthermore|plus)\b'
    multi_concerns = len(re.findall(multi_concern_pattern, prompt, re.IGNORECASE))
    score -= multi_concerns * 10.0

    # Check for vague targets
    vague_targets = ['system', 'application', 'codebase', 'project', 'entire', 'all']
    for target in vague_targets:
        if re.search(rf'\b{target}\b', prompt, re.IGNORECASE):
            score -= 15.0

    # Check for specific targets (file, function, class, component names)
    specific_pattern = r'\b(?:function|class|component|file|module)\s+\w+\b'
    if re.search(specific_pattern, prompt, re.IGNORECASE):
        score += 20.0

    # Normalize to 0-100
    return max(0.0, min(100.0, score))


def _detect_ambiguity_flags(prompt: str) -> list[str]:
    """Detect ambiguity flags in prompt.

    Ambiguous terms:
    - improve, enhance, optimize (without specific metric)
    - various, some, several, multiple (vague quantities)
    - better, good, clean (subjective quality)
    - fix issues, handle errors (vague problem description)
    """
    if not prompt:
        return []

    prompt_lower = prompt.lower()
    flags = []

    # Ambiguous action terms
    ambiguous_actions = [
        'improve', 'enhance', 'optimize', 'clean up', 'tidy',
        'polish', 'beautify', 'simplify'
    ]

    for term in ambiguous_actions:
        if re.search(rf'\b{term}\b', prompt_lower):
            flags.append(term)

    # Vague quantity terms
    vague_quantities = [
        'various', 'some', 'several', 'multiple', 'many', 'few', 'couple'
    ]

    for term in vague_quantities:
        if re.search(rf'\b{term}\b', prompt_lower):
            flags.append(term)

    # Subjective quality terms
    subjective_terms = [
        'better', 'good', 'clean', 'nice', 'proper', 'appropriate'
    ]

    for term in subjective_terms:
        if re.search(rf'\b{term}\b', prompt_lower):
            flags.append(term)

    return flags


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
