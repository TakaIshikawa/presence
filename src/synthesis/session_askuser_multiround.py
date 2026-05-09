"""Session AskUserQuestion multi-turn conversation flow analyzer.

Analyzes AskUserQuestion usage patterns and conversation flow quality.
Tracks batching efficiency, timing appropriateness, question quality, and anti-patterns.

Metrics:
- Questions per session: Number of AskUserQuestion calls
- Batch efficiency: Sequential vs parallel question batching
- Early planning rate: Questions asked during planning phase vs mid-execution
- Option quality: 2-4 options, distinct choices, clear descriptions
- MultiSelect appropriateness: Non-mutually-exclusive choices
- ExitPlanMode confusion: Asking 'should I proceed?' instead of using ExitPlanMode

Quality indicators:
- High batch efficiency: >80% questions batched in single calls
- High early planning rate: >70% questions during planning phase
- High option quality score: >0.8 average option quality
- Appropriate multiSelect usage: >90% correct usage
- Low ExitPlanMode confusion: <10% anti-pattern occurrences
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_askuser_multiround(records: object) -> dict[str, Any]:
    """Analyze AskUserQuestion usage patterns and conversation flow quality.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number
            - tool_name: Tool used (AskUserQuestion, EnterPlanMode, Edit, etc.)
            - questions: List of question dicts with:
                - question: Question text
                - options: List of option dicts with:
                    - label: Option label
                    - description: Option description
                - multiSelect: Boolean for multi-select questions
            - in_plan_mode: Boolean if currently in plan mode
            - after_implementation_start: Boolean if implementation has started

    Returns:
        Dict with metrics including questions_per_session, batch_efficiency_rate,
        early_planning_rate, option_quality_score, multiselect_appropriate_rate,
        and exitplanmode_confusion_rate.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    total_turns = 0
    askuser_invocations = 0
    total_questions_asked = 0

    # Batching efficiency tracking
    sequential_questions = 0  # Questions asked one at a time
    batched_questions = 0  # Multiple questions in single call

    # Timing appropriateness
    early_planning_questions = 0
    mid_execution_questions = 0

    # Option quality tracking
    option_quality_scores: list[float] = []

    # MultiSelect appropriateness
    multiselect_total = 0
    multiselect_appropriate = 0

    # Anti-pattern detection
    exitplanmode_confusion_count = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1
        tool_name = _string(record.get("tool_name"))

        if tool_name.lower() != "askuserquestion":
            continue

        askuser_invocations += 1

        # Extract questions list
        questions = record.get("questions")
        if not isinstance(questions, list):
            continue

        question_count = len(questions)
        total_questions_asked += question_count

        # Track batching efficiency
        if question_count == 1:
            sequential_questions += 1
        elif question_count > 1:
            batched_questions += question_count

        # Track timing appropriateness
        in_plan_mode = _bool(record.get("in_plan_mode", False))
        after_implementation_start = _bool(record.get("after_implementation_start", False))

        if in_plan_mode or not after_implementation_start:
            early_planning_questions += question_count
        else:
            mid_execution_questions += question_count

        # Analyze each question
        for question_data in questions:
            if not isinstance(question_data, Mapping):
                continue

            question_text = _string(question_data.get("question", ""))

            # Check for ExitPlanMode confusion anti-pattern
            if _is_exitplanmode_confusion(question_text):
                exitplanmode_confusion_count += 1

            # Evaluate option quality
            options = question_data.get("options")
            if isinstance(options, list):
                quality_score = _evaluate_option_quality(options)
                option_quality_scores.append(quality_score)

                # Check multiSelect appropriateness
                multiselect = _bool(question_data.get("multiSelect", False))
                multiselect_total += 1

                if _is_multiselect_appropriate(question_text, options, multiselect):
                    multiselect_appropriate += 1

    # Calculate metrics
    questions_per_session = askuser_invocations

    # Batch efficiency: percentage of questions batched vs sequential
    total_q = sequential_questions + batched_questions
    batch_efficiency_rate = _percentage(batched_questions, total_q)

    # Early planning rate
    total_q_for_timing = early_planning_questions + mid_execution_questions
    early_planning_rate = _percentage(early_planning_questions, total_q_for_timing)

    # Option quality score
    option_quality_score = _average(option_quality_scores)

    # MultiSelect appropriate rate
    multiselect_appropriate_rate = _percentage(multiselect_appropriate, multiselect_total)

    # ExitPlanMode confusion rate
    exitplanmode_confusion_rate = _percentage(exitplanmode_confusion_count, askuser_invocations)

    return {
        "total_turns": total_turns,
        "askuser_invocations": askuser_invocations,
        "total_questions_asked": total_questions_asked,
        "questions_per_session": questions_per_session,
        "sequential_questions": sequential_questions,
        "batched_questions": batched_questions,
        "batch_efficiency_rate": batch_efficiency_rate,
        "early_planning_questions": early_planning_questions,
        "mid_execution_questions": mid_execution_questions,
        "early_planning_rate": early_planning_rate,
        "option_quality_scores": option_quality_scores,
        "option_quality_score": option_quality_score,
        "multiselect_total": multiselect_total,
        "multiselect_appropriate": multiselect_appropriate,
        "multiselect_appropriate_rate": multiselect_appropriate_rate,
        "exitplanmode_confusion_count": exitplanmode_confusion_count,
        "exitplanmode_confusion_rate": exitplanmode_confusion_rate,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "askuser_invocations": 0,
        "total_questions_asked": 0,
        "questions_per_session": 0,
        "sequential_questions": 0,
        "batched_questions": 0,
        "batch_efficiency_rate": 0.0,
        "early_planning_questions": 0,
        "mid_execution_questions": 0,
        "early_planning_rate": 0.0,
        "option_quality_scores": [],
        "option_quality_score": 0.0,
        "multiselect_total": 0,
        "multiselect_appropriate": 0,
        "multiselect_appropriate_rate": 0.0,
        "exitplanmode_confusion_count": 0,
        "exitplanmode_confusion_rate": 0.0,
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


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 3)


def _is_exitplanmode_confusion(question_text: str) -> bool:
    """Detect anti-pattern of asking 'should I proceed?' instead of ExitPlanMode.

    Args:
        question_text: The question text to analyze

    Returns:
        True if question shows ExitPlanMode confusion anti-pattern
    """
    question_lower = question_text.lower()

    confusion_phrases = [
        "should i proceed",
        "should we proceed",
        "can i proceed",
        "can we proceed",
        "may i proceed",
        "ready to proceed",
        "proceed with",
        "is this plan okay",
        "is my plan ready",
        "is the plan okay",
        "approve the plan",
        "is the plan acceptable",
        "plan looks good",
        "does the plan look",
        "does this look good",
    ]

    return any(phrase in question_lower for phrase in confusion_phrases)


def _evaluate_option_quality(options: list) -> float:
    """Evaluate quality of options for a question.

    Quality criteria:
    - 2-4 options (optimal range)
    - Distinct labels (concise, 1-5 words)
    - Clear descriptions (non-empty, informative)

    Args:
        options: List of option dicts with label and description

    Returns:
        Quality score from 0.0 to 1.0
    """
    if not isinstance(options, list):
        return 0.0

    option_count = len(options)

    # Score for option count (2-4 is ideal)
    if 2 <= option_count <= 4:
        count_score = 1.0
    elif option_count < 2:
        count_score = 0.3
    else:
        # Penalize for too many options
        count_score = max(0.3, 1.0 - (option_count - 4) * 0.1)

    # Score for label and description quality
    quality_scores: list[float] = []

    for option in options:
        if not isinstance(option, Mapping):
            quality_scores.append(0.0)
            continue

        label = _string(option.get("label", ""))
        description = _string(option.get("description", ""))

        # Label quality (1-5 words is ideal)
        label_words = len(label.split()) if label else 0
        if 1 <= label_words <= 5:
            label_score = 1.0
        elif label_words == 0:
            label_score = 0.0
        else:
            label_score = max(0.3, 1.0 - (label_words - 5) * 0.1)

        # Description quality (non-empty, >15 chars for good quality)
        if len(description) >= 15:
            desc_score = 1.0
        elif len(description) >= 5:
            desc_score = 0.4
        else:
            desc_score = 0.0

        # Average label and description scores
        option_score = (label_score + desc_score) / 2.0
        quality_scores.append(option_score)

    # Calculate overall quality
    avg_option_quality = _average(quality_scores) if quality_scores else 0.0

    # Combine count and option quality scores
    overall_score = (count_score + avg_option_quality) / 2.0

    return overall_score


def _is_multiselect_appropriate(question_text: str, options: list, multiselect: bool) -> bool:
    """Determine if multiSelect usage is appropriate for the given options.

    MultiSelect is appropriate when options are not mutually exclusive
    (e.g., "which features to enable" vs "which approach to use").

    Args:
        question_text: The question text
        options: List of option dicts
        multiselect: Whether multiSelect is enabled

    Returns:
        True if multiSelect usage is appropriate
    """
    if not isinstance(options, list) or len(options) < 2:
        return not multiselect  # Default to False for edge cases

    # Heuristic: Check question text and option labels for keywords
    question_lower = question_text.lower()
    labels = [_string(opt.get("label", "")).lower() for opt in options if isinstance(opt, Mapping)]
    all_text = question_lower + " " + " ".join(labels)

    # Keywords suggesting mutually exclusive choices (should NOT use multiSelect)
    exclusive_keywords = [
        "approach",
        "method",
        "strategy",
        "option",
        "vs",
        "or",
        "instead",
    ]

    # Keywords suggesting non-exclusive choices (SHOULD use multiSelect)
    non_exclusive_keywords = [
        "features",
        "feature",
        "include",
        "enable",
        "add",
        "which to",
        "select all",
        "choose multiple",
        "all that apply",
    ]

    # Check for exclusive keywords
    has_exclusive = any(keyword in all_text for keyword in exclusive_keywords)

    # Check for non-exclusive keywords
    has_non_exclusive = any(keyword in all_text for keyword in non_exclusive_keywords)

    # If we can't determine, assume multiselect is inappropriate (conservative)
    if not has_exclusive and not has_non_exclusive:
        return not multiselect

    # If clearly exclusive, multiselect should be False
    if has_exclusive and not has_non_exclusive:
        return not multiselect

    # If clearly non-exclusive, multiselect should be True
    if has_non_exclusive and not has_exclusive:
        return multiselect

    # Mixed signals - default to current usage is appropriate
    return True
