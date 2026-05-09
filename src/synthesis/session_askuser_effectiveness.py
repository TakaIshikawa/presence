"""Session AskUserQuestion effectiveness analyzer for interactive clarification patterns.

Analyzes Claude Code session transcripts for AskUserQuestion tool usage patterns to
measure interactive clarification effectiveness, question clarity, and correlation
with task completion. Evaluates whether questions lead to productive outcomes or
workflow abandonment.

AskUserQuestion metrics:
- Total questions asked: Count of AskUserQuestion tool calls
- Questions per turn: Average questions per turn with questions
- MultiSelect vs single-choice ratio: Balance of selection types
- Task completion correlation: Questions leading to completion vs abandonment
- User response latency patterns: Average time to respond
- Question clarity score: Based on option count and description length

Quality indicators:
- Low question frequency: Agent autonomy vs over-reliance on user input
- High clarity score: Well-structured questions with clear options
- High completion rate: Questions lead to productive task completion
- Appropriate multiSelect usage: Used when truly non-exclusive choices
- Reasonable response latency: User can respond without excessive delays
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_askuser_effectiveness(records: object) -> dict[str, Any]:
    """Analyze AskUserQuestion tool usage effectiveness in agent sessions.

    Evaluates interactive clarification patterns, question clarity, and correlation
    with task completion outcomes.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_name: Name of the tool (AskUserQuestion, etc.)
            - questions: Optional list of question dicts with:
                - question: Question text
                - options: List of option dicts
                - multiSelect: Boolean for multi-select
            - user_responded: Optional boolean if user responded
            - response_latency_seconds: Optional response time
            - task_completed: Optional boolean if task completed after question
            - task_abandoned: Optional boolean if task abandoned after question

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - total_questions_asked: Total AskUserQuestion calls
            - turns_with_questions: Turns containing questions
            - questions_per_turn: Average questions per turn (when questions exist)
            - total_question_count: Sum of all individual questions
            - avg_questions_per_call: Average questions per AskUserQuestion call
            - multiselect_questions: Count of multiSelect questions
            - single_choice_questions: Count of single-choice questions
            - multiselect_ratio: Percentage of multiSelect questions
            - questions_with_responses: Questions that received user responses
            - response_rate: Percentage of questions answered
            - avg_response_latency_seconds: Average user response time
            - questions_leading_to_completion: Questions followed by task completion
            - questions_leading_to_abandonment: Questions followed by abandonment
            - completion_correlation_rate: Percentage leading to completion
            - avg_options_per_question: Average number of options per question
            - avg_description_length: Average description length per option
            - question_clarity_score: 0-100 score based on structure
            - overall_effectiveness_score: 0-1 score combining all metrics

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    total_turns = 0
    total_questions_asked = 0  # AskUserQuestion calls
    turns_with_questions = 0
    total_question_count = 0  # Sum of individual questions

    multiselect_questions = 0
    single_choice_questions = 0

    questions_with_responses = 0
    response_latencies: list[float] = []

    questions_leading_to_completion = 0
    questions_leading_to_abandonment = 0

    options_per_question: list[int] = []
    description_lengths: list[int] = []
    questions_per_call: list[int] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1
        tool_name = _string(record.get("tool_name"))

        if tool_name.lower() != "askuserquestion":
            continue

        total_questions_asked += 1
        turns_with_questions += 1

        # Extract questions list
        questions = record.get("questions")
        if not isinstance(questions, list):
            continue

        question_count = len(questions)
        total_question_count += question_count
        questions_per_call.append(question_count)

        # Analyze each question
        for question in questions:
            if not isinstance(question, Mapping):
                continue

            # Check multiSelect
            is_multiselect = _bool(question.get("multiSelect", False))
            if is_multiselect:
                multiselect_questions += 1
            else:
                single_choice_questions += 1

            # Analyze options
            options = question.get("options")
            if isinstance(options, list):
                options_per_question.append(len(options))

                # Analyze description lengths
                for option in options:
                    if isinstance(option, Mapping):
                        description = _string(option.get("description", ""))
                        if description:
                            description_lengths.append(len(description))

        # Check if user responded
        user_responded = _bool(record.get("user_responded", False))
        if user_responded:
            questions_with_responses += 1

        # Track response latency
        latency = _float(record.get("response_latency_seconds"))
        if latency > 0:
            response_latencies.append(latency)

        # Track task outcome
        task_completed = _bool(record.get("task_completed", False))
        task_abandoned = _bool(record.get("task_abandoned", False))

        if task_completed:
            questions_leading_to_completion += 1
        if task_abandoned:
            questions_leading_to_abandonment += 1

    # Calculate aggregate metrics
    questions_per_turn = (
        total_questions_asked / turns_with_questions
        if turns_with_questions > 0
        else 0.0
    )

    avg_questions_per_call = _average(questions_per_call)

    total_choice_questions = multiselect_questions + single_choice_questions
    multiselect_ratio = _percentage(multiselect_questions, total_choice_questions)

    response_rate = _percentage(questions_with_responses, total_questions_asked)
    avg_response_latency = _average(response_latencies)

    total_outcomes = questions_leading_to_completion + questions_leading_to_abandonment
    completion_correlation_rate = _percentage(
        questions_leading_to_completion,
        total_outcomes
    )

    avg_options = _average(options_per_question)
    avg_desc_length = _average(description_lengths)

    # Calculate question clarity score (0-100)
    clarity_score = _calculate_clarity_score(
        avg_options,
        avg_desc_length,
        total_choice_questions,
    )

    # Calculate overall effectiveness score (0-1)
    effectiveness_score = _calculate_effectiveness_score(
        total_questions_asked,
        total_turns,
        response_rate,
        completion_correlation_rate,
        clarity_score,
    )

    return {
        "total_turns": total_turns,
        "total_questions_asked": total_questions_asked,
        "turns_with_questions": turns_with_questions,
        "questions_per_turn": round(questions_per_turn, 2),
        "total_question_count": total_question_count,
        "avg_questions_per_call": avg_questions_per_call,
        "multiselect_questions": multiselect_questions,
        "single_choice_questions": single_choice_questions,
        "multiselect_ratio": multiselect_ratio,
        "questions_with_responses": questions_with_responses,
        "response_rate": response_rate,
        "avg_response_latency_seconds": avg_response_latency,
        "questions_leading_to_completion": questions_leading_to_completion,
        "questions_leading_to_abandonment": questions_leading_to_abandonment,
        "completion_correlation_rate": completion_correlation_rate,
        "avg_options_per_question": avg_options,
        "avg_description_length": avg_desc_length,
        "question_clarity_score": clarity_score,
        "overall_effectiveness_score": effectiveness_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "total_questions_asked": 0,
        "turns_with_questions": 0,
        "questions_per_turn": 0.0,
        "total_question_count": 0,
        "avg_questions_per_call": 0.0,
        "multiselect_questions": 0,
        "single_choice_questions": 0,
        "multiselect_ratio": 0.0,
        "questions_with_responses": 0,
        "response_rate": 0.0,
        "avg_response_latency_seconds": 0.0,
        "questions_leading_to_completion": 0,
        "questions_leading_to_abandonment": 0,
        "completion_correlation_rate": 0.0,
        "avg_options_per_question": 0.0,
        "avg_description_length": 0.0,
        "question_clarity_score": 0.0,
        "overall_effectiveness_score": 0.0,
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


def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


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


def _calculate_clarity_score(
    avg_options: float,
    avg_desc_length: float,
    total_questions: int,
) -> float:
    """Calculate question clarity score (0-100).

    Score components:
    - 40 points: Optimal option count (2-4 options)
    - 40 points: Good description length (50-200 chars)
    - 20 points: Consistency (non-zero questions)
    """
    if total_questions == 0:
        return 0.0

    # Option count component (0-40)
    # Optimal: 2-4 options, penalize <2 or >4
    if avg_options < 2:
        option_component = avg_options / 2 * 40.0
    elif avg_options <= 4:
        option_component = 40.0
    else:
        # Penalize >4 options (too many choices)
        option_component = max(0, 40.0 - (avg_options - 4) * 5.0)

    # Description length component (0-40)
    # Optimal: 50-200 chars, penalize too short or too long
    if avg_desc_length < 50:
        desc_component = avg_desc_length / 50 * 40.0
    elif avg_desc_length <= 200:
        desc_component = 40.0
    else:
        # Penalize >200 chars (too verbose)
        desc_component = max(0, 40.0 - (avg_desc_length - 200) / 20.0)

    # Consistency component (0-20)
    # Just having questions gets points
    consistency_component = 20.0

    score = option_component + desc_component + consistency_component
    return round(max(0.0, min(100.0, score)), 2)


def _calculate_effectiveness_score(
    total_questions: int,
    total_turns: int,
    response_rate: float,
    completion_rate: float,
    clarity_score: float,
) -> float:
    """Calculate overall effectiveness score (0-1).

    Score components:
    - 0.3: Autonomy (fewer questions per turn is better)
    - 0.3: Completion correlation (questions lead to completion)
    - 0.2: Response rate (questions get answered)
    - 0.2: Clarity (well-structured questions)
    """
    if total_turns == 0:
        return 0.0

    # Autonomy component (0-0.3)
    # Lower question frequency is better (target: <10% of turns)
    question_frequency = (total_questions / total_turns) * 100
    if question_frequency <= 10:
        autonomy_component = 0.3
    else:
        # Penalize higher frequencies
        autonomy_component = max(0, 0.3 - (question_frequency - 10) / 100)

    # Completion component (0-0.3)
    completion_component = (completion_rate / 100.0) * 0.3

    # Response component (0-0.2)
    response_component = (response_rate / 100.0) * 0.2

    # Clarity component (0-0.2)
    clarity_component = (clarity_score / 100.0) * 0.2

    score = (
        autonomy_component +
        completion_component +
        response_component +
        clarity_component
    )
    return round(max(0.0, min(1.0, score)), 3)
