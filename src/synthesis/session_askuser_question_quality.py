"""Session AskUserQuestion frequency and question quality analyzer.

Analyzes how effectively Claude Code sessions use the AskUserQuestion tool,
measuring question timing, specificity, redundancy, and action linkage.

Analyzed dimensions:
1. Question frequency: Rate of AskUserQuestion calls relative to session complexity
2. Question timing: Whether questions are asked early (planning phase) vs late
   (implementation phase)
3. Question specificity: Number of options provided, use of multiSelect, header quality
4. Redundant questions: Questions that could have been answered by reading code/docs first
5. Question-to-action ratio: Whether answers actually influence subsequent tool calls

Quality indicators:
- High early-phase question rate (>60%): Questions asked during planning
- Low redundant question rate (<15%): Not asking about readable information
- High question-to-action ratio (>80%): Answers drive subsequent actions
- Appropriate option count (2-4): Well-structured choice presentation
- Selective multiSelect usage (<30%): Used only when choices aren't mutually exclusive
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_askuser_question_quality(records: object) -> dict[str, Any]:
    """Analyze AskUserQuestion usage quality across sessions.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_askuser_calls: Total AskUserQuestion invocations
            - early_phase_questions: Questions asked before implementation
            - late_phase_questions: Questions asked during/after implementation
            - total_options_provided: Sum of options across all questions
            - multiselect_questions: Questions using multiSelect=true
            - questions_with_followup_action: Questions whose answers influenced
              subsequent tool calls
            - redundant_questions: Questions about information available in
              code/docs that could have been read first
            - session_total_tool_calls: Total tool calls in the session

    Returns:
        Dict with:
            - total_sessions: Number of sessions analyzed
            - total_questions: Total AskUserQuestion calls
            - questions_per_session: Average questions per session
            - early_phase_question_rate: % questions asked during planning
            - late_phase_question_rate: % questions asked during implementation
            - avg_options_per_question: Average options provided per question
            - multiselect_usage_rate: % questions using multiSelect
            - question_to_action_ratio: % questions leading to follow-up actions
            - redundant_question_rate: % questions that were redundant
            - high_quality_sessions: Sessions with score > 0.7
            - low_quality_sessions: Sessions with score < 0.4
            - askuser_question_quality_score: Overall quality score 0-1

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    total_questions = 0
    total_early = 0
    total_late = 0
    total_options = 0
    total_multiselect = 0
    total_with_action = 0
    total_redundant = 0

    session_scores: list[float] = []
    questions_per_session_vals: list[float] = []
    high_quality_sessions = 0
    low_quality_sessions = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        askuser_calls = _int(record.get("total_askuser_calls", 0))
        early = _int(record.get("early_phase_questions", 0))
        late = _int(record.get("late_phase_questions", 0))
        options = _int(record.get("total_options_provided", 0))
        multiselect = _int(record.get("multiselect_questions", 0))
        with_action = _int(record.get("questions_with_followup_action", 0))
        redundant = _int(record.get("redundant_questions", 0))
        total_tool_calls = _int(record.get("session_total_tool_calls", 0))

        total_questions += askuser_calls
        total_early += early
        total_late += late
        total_options += options
        total_multiselect += multiselect
        total_with_action += with_action
        total_redundant += redundant

        questions_per_session_vals.append(float(askuser_calls))

        session_score = _calculate_session_score(
            askuser_calls=askuser_calls,
            early_questions=early,
            late_questions=late,
            total_options=options,
            multiselect_questions=multiselect,
            questions_with_action=with_action,
            redundant_questions=redundant,
            total_tool_calls=total_tool_calls,
        )
        session_scores.append(session_score)

        if session_score > 0.7:
            high_quality_sessions += 1
        elif session_score < 0.4:
            low_quality_sessions += 1

    # Calculate aggregate rates
    all_questions = total_early + total_late
    early_phase_question_rate = _percentage(total_early, all_questions)
    late_phase_question_rate = _percentage(total_late, all_questions)
    avg_options_per_question = (
        round(total_options / total_questions, 2) if total_questions > 0 else 0.0
    )
    multiselect_usage_rate = _percentage(total_multiselect, total_questions)
    question_to_action_ratio = _percentage(total_with_action, total_questions)
    redundant_question_rate = _percentage(total_redundant, total_questions)
    questions_per_session = _average(questions_per_session_vals)

    # Overall score
    askuser_question_quality_score = _calculate_overall_score(
        early_phase_question_rate=early_phase_question_rate,
        redundant_question_rate=redundant_question_rate,
        question_to_action_ratio=question_to_action_ratio,
        avg_options_per_question=avg_options_per_question,
        multiselect_usage_rate=multiselect_usage_rate,
    )

    return {
        "total_sessions": total_sessions,
        "total_questions": total_questions,
        "questions_per_session": questions_per_session,
        "early_phase_question_rate": early_phase_question_rate,
        "late_phase_question_rate": late_phase_question_rate,
        "avg_options_per_question": avg_options_per_question,
        "multiselect_usage_rate": multiselect_usage_rate,
        "question_to_action_ratio": question_to_action_ratio,
        "redundant_question_rate": redundant_question_rate,
        "high_quality_sessions": high_quality_sessions,
        "low_quality_sessions": low_quality_sessions,
        "askuser_question_quality_score": askuser_question_quality_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "total_questions": 0,
        "questions_per_session": 0.0,
        "early_phase_question_rate": 0.0,
        "late_phase_question_rate": 0.0,
        "avg_options_per_question": 0.0,
        "multiselect_usage_rate": 0.0,
        "question_to_action_ratio": 0.0,
        "redundant_question_rate": 0.0,
        "high_quality_sessions": 0,
        "low_quality_sessions": 0,
        "askuser_question_quality_score": 0.0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_session_score(
    askuser_calls: int,
    early_questions: int,
    late_questions: int,
    total_options: int,
    multiselect_questions: int,
    questions_with_action: int,
    redundant_questions: int,
    total_tool_calls: int,
) -> float:
    """Calculate session-level question quality score (0-1).

    Scoring components:
    - Early-phase question rate (0-0.30): >60% questions asked during planning
    - Question-to-action linkage (0-0.30): >80% questions lead to actions
    - Low redundancy (0-0.20): <15% redundant questions
    - Option quality (0-0.20): 2-4 options per question average

    Returns:
        Session score from 0.0 to 1.0
    """
    if askuser_calls == 0:
        # No questions asked — neutral score (depends on session complexity)
        if total_tool_calls > 20:
            # Complex session with no questions might be fine or might be missing
            return 0.5
        return 0.6

    score = 0.0
    total_q = early_questions + late_questions

    # Early-phase question rate component (0-0.30)
    if total_q > 0:
        early_rate = _percentage(early_questions, total_q)
        if early_rate >= 60:
            score += 0.30
        elif early_rate >= 40:
            score += 0.20
        elif early_rate >= 20:
            score += 0.10
    else:
        score += 0.15  # No phase data

    # Question-to-action linkage component (0-0.30)
    action_rate = _percentage(questions_with_action, askuser_calls)
    if action_rate >= 80:
        score += 0.30
    elif action_rate >= 60:
        score += 0.20
    elif action_rate >= 40:
        score += 0.10

    # Low redundancy component (0-0.20)
    redundant_rate = _percentage(redundant_questions, askuser_calls)
    if redundant_rate <= 10:
        score += 0.20
    elif redundant_rate <= 20:
        score += 0.15
    elif redundant_rate <= 30:
        score += 0.10
    elif redundant_rate <= 50:
        score += 0.05

    # Option quality component (0-0.20)
    avg_options = total_options / askuser_calls if askuser_calls > 0 else 0
    if 2 <= avg_options <= 4:
        score += 0.20
    elif 1 <= avg_options < 2 or 4 < avg_options <= 5:
        score += 0.10
    elif avg_options > 5:
        score += 0.05

    return round(max(0.0, min(1.0, score)), 3)


def _calculate_overall_score(
    early_phase_question_rate: float,
    redundant_question_rate: float,
    question_to_action_ratio: float,
    avg_options_per_question: float,
    multiselect_usage_rate: float,
) -> float:
    """Calculate overall AskUserQuestion quality score (0-1).

    Scoring components:
    - Early-phase question rate (0-0.30): >60% asked during planning
    - Low redundancy (0-0.25): <15% redundant questions
    - Action linkage (0-0.25): >80% questions lead to actions
    - Option quality (0-0.10): Average 2-4 options per question
    - Selective multiSelect (0-0.10): <30% multiSelect usage

    Returns:
        Overall score from 0.0 to 1.0
    """
    score = 0.0

    # Early-phase question rate component (0-0.30)
    if early_phase_question_rate >= 60:
        score += 0.30
    elif early_phase_question_rate >= 40:
        score += 0.20
    elif early_phase_question_rate >= 20:
        score += 0.10

    # Low redundancy component (0-0.25)
    if redundant_question_rate <= 10:
        score += 0.25
    elif redundant_question_rate <= 20:
        score += 0.18
    elif redundant_question_rate <= 30:
        score += 0.10

    # Action linkage component (0-0.25)
    if question_to_action_ratio >= 80:
        score += 0.25
    elif question_to_action_ratio >= 60:
        score += 0.18
    elif question_to_action_ratio >= 40:
        score += 0.10

    # Option quality component (0-0.10)
    if 2 <= avg_options_per_question <= 4:
        score += 0.10
    elif 1 <= avg_options_per_question < 2 or 4 < avg_options_per_question <= 5:
        score += 0.05

    # Selective multiSelect component (0-0.10)
    if multiselect_usage_rate <= 30:
        score += 0.10
    elif multiselect_usage_rate <= 50:
        score += 0.07
    elif multiselect_usage_rate <= 70:
        score += 0.04

    return round(max(0.0, min(1.0, score)), 3)
