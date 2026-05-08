"""Final answer summary completeness analyzer for session quality assessment.

Analyzes the quality of final answers provided to users after completing tasks.
Evaluates completeness, clarity, and whether answers provide appropriate
context about work completed, verification results, and next steps.

Completeness metrics:
- Completion statement: Explicit indication task is complete
- File references: Mentions specific files changed
- Verification results: Includes test/build/lint results
- Next steps: Provides guidance for follow-up actions
- Summary-to-action ratio: Balance of explanation vs work done

Quality indicators:
- Complete answers: All key elements present
- Minimal summaries: Lacks important context or details
- Verbose explanations: Too much explanation, not enough substance
- Well-balanced: Right mix of context and results
"""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_final_answer_summary_completeness(records: object) -> dict[str, Any]:
    """Analyze final answer quality and completeness.

    Evaluates final answers for completeness, appropriate context,
    and balance between explanation and results.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - final_answer: Final answer text provided to user
            - changed_files: Optional list of files actually changed
            - verification_passed: Optional boolean for verification results
            - task_completed: Optional boolean for task completion

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - has_completion_statement: Answers with explicit completion
            - references_changed_files: Answers mentioning changed files
            - includes_verification_results: Answers with test/build results
            - provides_next_steps: Answers with follow-up guidance
            - avg_summary_to_action_ratio: Mean ratio of explanation to work
            - complete_answer_count: Answers meeting all criteria
            - minimal_summary_count: Answers lacking important context
            - verbose_summary_count: Answers with excessive explanation
            - well_balanced_count: Answers with good balance

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    has_completion_statement = 0
    references_changed_files = 0
    includes_verification_results = 0
    provides_next_steps = 0

    summary_ratios: list[float] = []

    complete_answer_count = 0
    minimal_summary_count = 0
    verbose_summary_count = 0
    well_balanced_count = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        final_answer = _string(record.get("final_answer", ""))
        changed_files = record.get("changed_files")
        verification_passed = record.get("verification_passed")

        if not final_answer:
            minimal_summary_count += 1
            continue

        # Check for completion statement
        has_complete = _has_completion_statement(final_answer)
        if has_complete:
            has_completion_statement += 1

        # Check for file references
        has_files = _references_files(final_answer, changed_files)
        if has_files:
            references_changed_files += 1

        # Check for verification results
        has_verification = _includes_verification(final_answer, verification_passed)
        if has_verification:
            includes_verification_results += 1

        # Check for next steps
        has_steps = _provides_next_steps(final_answer)
        if has_steps:
            provides_next_steps += 1

        # Calculate summary-to-action ratio
        ratio = _calculate_summary_ratio(final_answer)
        summary_ratios.append(ratio)

        # Classify answer quality
        completeness_score = sum([has_complete, has_files, has_verification, has_steps])

        if completeness_score >= 3:
            complete_answer_count += 1
            if 30.0 <= ratio <= 70.0:
                well_balanced_count += 1
        elif completeness_score <= 1 or ratio < 20.0:
            minimal_summary_count += 1
        elif ratio > 80.0:
            verbose_summary_count += 1
        elif completeness_score >= 2 and 25.0 <= ratio <= 75.0:
            well_balanced_count += 1

    # Calculate averages
    avg_summary_ratio = _average(summary_ratios)

    return {
        "total_sessions": total_sessions,
        "has_completion_statement": has_completion_statement,
        "references_changed_files": references_changed_files,
        "includes_verification_results": includes_verification_results,
        "provides_next_steps": provides_next_steps,
        "avg_summary_to_action_ratio": avg_summary_ratio,
        "complete_answer_count": complete_answer_count,
        "minimal_summary_count": minimal_summary_count,
        "verbose_summary_count": verbose_summary_count,
        "well_balanced_count": well_balanced_count,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _has_completion_statement(text: str) -> bool:
    """Check if text has explicit completion statement.

    Indicators:
    - "completed", "done", "finished", "successfully"
    - "All tests pass", "implementation complete"
    - "ready for review", "task finished"
    """
    completion_patterns = [
        r'\b(?:completed|finished|successfully)\b',
        r'\bAll tests pass\b',
        r'\bimplementation (?:complete|finished)\b',
        r'\bready for review\b',
        r'\btask (?:completed|finished)\b',
        r'\bsuccessfully (?:implemented|created|fixed|added)\b'
    ]

    for pattern in completion_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True

    return False


def _references_files(text: str, changed_files: object) -> bool:
    """Check if text references changed files.

    Looks for:
    - File paths (src/main.py, tests/test_main.py)
    - File count mentions ("3 files changed", "modified 2 files")
    - Specific files from changed_files list
    """
    # Check for file path patterns
    file_path_pattern = r'\b(?:src|tests|lib|bin)/[\w/]+\.\w+\b|\b\w+\.\w{2,4}\b'
    if re.search(file_path_pattern, text):
        return True

    # Check for file count mentions
    file_count_pattern = r'\b(?:modified|created|updated|changed)\s+\d+\s+files?\b|\b\d+\s+files?\s+(?:changed|modified|created|updated)\b'
    if re.search(file_count_pattern, text, re.IGNORECASE):
        return True

    # Check for references to changed_files
    if isinstance(changed_files, list):
        for file in changed_files:
            if isinstance(file, str) and file in text:
                return True

    return False


def _includes_verification(text: str, verification_passed: object) -> bool:
    """Check if text includes verification results.

    Indicators:
    - "tests pass", "build successful", "lint clean"
    - "pytest", "mypy", "ruff" with results
    - Explicit verification_passed mention
    """
    verification_patterns = [
        r'\b(?:tests?|pytest)\s+(?:pass|passed|passing)\b',
        r'\bbuild\s+(?:successful|succeeded|passed)\b',
        r'\blint\s+(?:clean|passed|successful)\b',
        r'\b(?:mypy|pyright|tsc)\s+(?:type\s+)?(?:check|checking)\s+(?:passed|clean|successful)\b',
        r'\b(?:ruff|eslint|pylint)\s+(?:check\s+)?(?:passed|clean)\b',
        r'\bverification\s+(?:passed|successful)\b',
        r'\ball tests pass\b'
    ]

    for pattern in verification_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True

    # Check explicit flag
    if verification_passed is True:
        # Look for any mention of verification/testing
        if re.search(r'\b(?:test|verify|validation|check)\b', text, re.IGNORECASE):
            return True

    return False


def _provides_next_steps(text: str) -> bool:
    """Check if text provides next steps or follow-up guidance.

    Indicators:
    - "next steps", "follow-up", "you can now"
    - "to use this", "to test", "to deploy"
    - Numbered/bulleted lists of actions
    """
    next_steps_patterns = [
        r'\bnext\s+steps?\b',
        r'\bfollow[- ]up\b',
        r'\byou can now\b',
        r'\bto (?:use|test|deploy|run|build)\b',
        r'\bconsider\b',
        r'\bmay want to\b',
        r'\bshould\b.*\b(?:test|verify|check|review)\b'
    ]

    for pattern in next_steps_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True

    return False


def _calculate_summary_ratio(text: str) -> float:
    """Calculate summary-to-action ratio.

    Higher ratio = more explanation-heavy
    Lower ratio = more action-focused

    Heuristic:
    - Count explanation words (because, however, therefore, etc.)
    - Count action words (created, implemented, fixed, tested, etc.)
    - Ratio = explanation / (explanation + action) * 100

    Returns:
        Ratio score (0-100)
    """
    if not text:
        return 0.0

    text_lower = text.lower()

    # Explanation indicators
    explanation_words = [
        'because', 'however', 'therefore', 'thus', 'hence', 'although',
        'moreover', 'furthermore', 'additionally', 'specifically',
        'essentially', 'basically', 'generally', 'typically'
    ]

    # Action indicators
    action_words = [
        'created', 'implemented', 'fixed', 'added', 'updated', 'removed',
        'refactored', 'tested', 'validated', 'built', 'deployed',
        'configured', 'integrated', 'modified', 'changed'
    ]

    explanation_count = sum(text_lower.count(word) for word in explanation_words)
    action_count = sum(text_lower.count(word) for word in action_words)

    total = explanation_count + action_count
    if total == 0:
        # No clear indicators, use length as proxy
        length = len(text)
        if length < 200:
            return 30.0  # Short = action-focused
        elif length > 800:
            return 70.0  # Long = explanation-heavy
        else:
            return 50.0  # Medium = balanced

    # Calculate ratio
    ratio = (explanation_count / total) * 100.0
    return round(ratio, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
