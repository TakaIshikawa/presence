"""Session AskUserQuestion usage pattern analyzer for consultation patterns.

Analyzes AskUserQuestion tool usage in Claude Code sessions to measure how effectively
the agent consults users during development. Evaluates question quality, timing,
response handling, and redundancy detection.

AskUserQuestion pattern metrics:
- Total questions asked: Number of AskUserQuestion tool invocations
- Question type distribution: Clarification, preference, decision counts
- Question timing distribution: Early planning, mid-implementation, late validation
- User response types: Selected option, custom text, skipped
- Answer utilization rate: % of answers used in subsequent tool calls
- Redundant question count: Similar questions within same session

Quality indicators:
- Balanced question types: Mix of clarification, preference, and decision questions
- Early planning questions (>40%): Consulting before implementation
- High answer utilization (>70%): Questions lead to action
- Low redundant questions (<10%): Efficient consultation without repetition
- Moderate question rate (5-15 per session): Balanced autonomy and consultation
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_ask_user_question_pattern(records: object) -> dict[str, Any]:
    """Analyze AskUserQuestion tool usage patterns in Claude Code sessions.

    Evaluates consultation quality through question categorization, timing analysis,
    response tracking, and redundancy detection.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_ask_user_questions: Number of AskUserQuestion calls
            - clarification_questions: Questions for requirement clarification
            - preference_questions: Questions about user preferences
            - decision_questions: Questions for implementation decisions
            - early_planning_questions: Questions in first 25% of session
            - mid_implementation_questions: Questions in middle 50% of session
            - late_validation_questions: Questions in last 25% of session
            - selected_option_responses: User selected from options
            - custom_text_responses: User provided custom text
            - skipped_responses: User skipped question
            - answers_utilized: Answers referenced in subsequent tools
            - redundant_questions: Similar questions within session
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_questions: Count of sessions using AskUserQuestion
            - avg_questions_per_session: Average AskUserQuestion calls
            - avg_clarification_ratio: Average % clarification questions
            - avg_preference_ratio: Average % preference questions
            - avg_decision_ratio: Average % decision questions
            - avg_early_planning_ratio: Average % early questions
            - avg_mid_implementation_ratio: Average % mid questions
            - avg_late_validation_ratio: Average % late questions
            - avg_selected_option_rate: Average % selected options
            - avg_custom_text_rate: Average % custom text
            - avg_skipped_rate: Average % skipped questions
            - avg_answer_utilization_rate: Average % answers used
            - avg_redundant_question_rate: Average % redundant questions
            - high_consultation_sessions: Count with >70% answer utilization
            - low_consultation_sessions: Count with <40% answer utilization

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_questions = 0

    questions_per_session: list[int | float] = []
    clarification_ratios: list[float] = []
    preference_ratios: list[float] = []
    decision_ratios: list[float] = []
    early_planning_ratios: list[float] = []
    mid_implementation_ratios: list[float] = []
    late_validation_ratios: list[float] = []
    selected_option_rates: list[float] = []
    custom_text_rates: list[float] = []
    skipped_rates: list[float] = []
    answer_utilization_rates: list[float] = []
    redundant_question_rates: list[float] = []

    high_consultation_sessions = 0  # >70% answer utilization
    low_consultation_sessions = 0   # <40% answer utilization

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_questions = _extract_int(record.get("total_ask_user_questions"))
        clarification = _extract_int(record.get("clarification_questions"))
        preference = _extract_int(record.get("preference_questions"))
        decision = _extract_int(record.get("decision_questions"))
        early_planning = _extract_int(record.get("early_planning_questions"))
        mid_implementation = _extract_int(record.get("mid_implementation_questions"))
        late_validation = _extract_int(record.get("late_validation_questions"))
        selected_option = _extract_int(record.get("selected_option_responses"))
        custom_text = _extract_int(record.get("custom_text_responses"))
        skipped = _extract_int(record.get("skipped_responses"))
        answers_utilized = _extract_int(record.get("answers_utilized"))
        redundant = _extract_int(record.get("redundant_questions"))

        # Track sessions with questions
        if total_questions is not None and total_questions > 0:
            sessions_with_questions += 1
            questions_per_session.append(total_questions)

            # Calculate question type ratios
            if clarification is not None:
                clarification_ratios.append(_percentage(clarification, total_questions))
            if preference is not None:
                preference_ratios.append(_percentage(preference, total_questions))
            if decision is not None:
                decision_ratios.append(_percentage(decision, total_questions))

            # Calculate timing distribution
            if early_planning is not None:
                early_planning_ratios.append(_percentage(early_planning, total_questions))
            if mid_implementation is not None:
                mid_implementation_ratios.append(_percentage(mid_implementation, total_questions))
            if late_validation is not None:
                late_validation_ratios.append(_percentage(late_validation, total_questions))

            # Calculate response type rates
            if selected_option is not None:
                selected_option_rates.append(_percentage(selected_option, total_questions))
            if custom_text is not None:
                custom_text_rates.append(_percentage(custom_text, total_questions))
            if skipped is not None:
                skipped_rates.append(_percentage(skipped, total_questions))

            # Calculate answer utilization rate
            if answers_utilized is not None:
                utilization = _percentage(answers_utilized, total_questions)
                answer_utilization_rates.append(utilization)

                # Classify consultation quality
                if utilization > 70.0:
                    high_consultation_sessions += 1
                elif utilization < 40.0:
                    low_consultation_sessions += 1

            # Calculate redundant question rate
            if redundant is not None:
                redundant_question_rates.append(_percentage(redundant, total_questions))

    # Calculate aggregate metrics
    avg_questions = _average(questions_per_session)
    avg_clarification = _average(clarification_ratios)
    avg_preference = _average(preference_ratios)
    avg_decision = _average(decision_ratios)
    avg_early = _average(early_planning_ratios)
    avg_mid = _average(mid_implementation_ratios)
    avg_late = _average(late_validation_ratios)
    avg_selected = _average(selected_option_rates)
    avg_custom = _average(custom_text_rates)
    avg_skipped = _average(skipped_rates)
    avg_utilization = _average(answer_utilization_rates)
    avg_redundant = _average(redundant_question_rates)

    return {
        "total_sessions": total_sessions,
        "sessions_with_questions": sessions_with_questions,
        "avg_questions_per_session": avg_questions,
        "avg_clarification_ratio": avg_clarification,
        "avg_preference_ratio": avg_preference,
        "avg_decision_ratio": avg_decision,
        "avg_early_planning_ratio": avg_early,
        "avg_mid_implementation_ratio": avg_mid,
        "avg_late_validation_ratio": avg_late,
        "avg_selected_option_rate": avg_selected,
        "avg_custom_text_rate": avg_custom,
        "avg_skipped_rate": avg_skipped,
        "avg_answer_utilization_rate": avg_utilization,
        "avg_redundant_question_rate": avg_redundant,
        "high_consultation_sessions": high_consultation_sessions,
        "low_consultation_sessions": low_consultation_sessions,
    }


def _extract_int(value: object) -> int | None:
    """Extract integer from value if available."""
    if isinstance(value, int) and not isinstance(value, bool):
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
