"""Final answer user facing summary analyzer.

Analyzes quality and completeness of final answer summaries presented to users.
Tracks summary clarity, evidence inclusion, actionability, length appropriateness,
and alignment with task completion status.

Final answer summary metrics:
- Summary presence: Tasks with explicit final summary
- Summary length: Character/word count of summaries
- Evidence inclusion: Links to files, line numbers, specific examples
- Actionability: Clear next steps or completion status
- Task alignment: Summary matches task completion state
- Clarity metrics: Readability, structure, conciseness
- User-facing language: Appropriate tone and technical level

Quality indicators:
- High summary presence (>95%): Most tasks have summaries
- Appropriate length (100-500 words): Not too brief or verbose
- High evidence inclusion (>80%): Summaries reference concrete evidence
- High actionability (>90%): Clear completion status or next steps
- Perfect task alignment (100%): Summaries match actual completion
- High clarity score (>80%): Well-structured, readable summaries
- Appropriate tone (>85%): User-friendly, professional language
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_final_answer_user_facing_summary(records: object) -> dict[str, Any]:
    """Analyze quality and completeness of final answer user-facing summaries.

    Tracks summary presence, quality, evidence, and alignment with completion.

    Args:
        records: List of session/task dictionaries with keys:
            - session_id: Session or task identifier
            - has_final_summary: Boolean indicating summary presence
            - summary_length_words: Word count of summary
            - summary_length_chars: Character count of summary
            - has_evidence_references: Summary includes file/line references
            - has_actionable_next_steps: Clear next steps provided
            - task_completed: Task completion status
            - summary_matches_completion: Summary aligns with completion state
            - clarity_score: Readability/structure score (0-100)
            - tone_appropriateness_score: User-facing tone quality (0-100)
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions/tasks analyzed
            - summary_presence_rate: % of tasks with final summaries
            - avg_summary_length_words: Average word count
            - avg_summary_length_chars: Average character count
            - evidence_inclusion_rate: % summaries with evidence references
            - actionability_rate: % summaries with next steps
            - task_alignment_rate: % summaries matching completion state
            - avg_clarity_score: Average clarity/readability score
            - avg_tone_score: Average tone appropriateness score
            - appropriate_length_summaries: Count with 100-500 words
            - too_brief_summaries: Count with <100 words
            - too_verbose_summaries: Count with >500 words
            - high_quality_summaries: Count with clarity >80 and tone >85

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_summary = 0
    summary_lengths_words: list[int | float] = []
    summary_lengths_chars: list[int | float] = []
    sessions_with_evidence = 0
    sessions_with_next_steps = 0
    sessions_aligned = 0
    clarity_scores: list[float] = []
    tone_scores: list[float] = []

    appropriate_length_summaries = 0  # 100-500 words
    too_brief_summaries = 0  # <100 words
    too_verbose_summaries = 0  # >500 words
    high_quality_summaries = 0  # clarity >80 and tone >85

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        has_summary = record.get("has_final_summary")
        length_words = _extract_number(record.get("summary_length_words"))
        length_chars = _extract_number(record.get("summary_length_chars"))
        has_evidence = record.get("has_evidence_references")
        has_next_steps = record.get("has_actionable_next_steps")
        task_completed = record.get("task_completed")
        summary_matches = record.get("summary_matches_completion")
        clarity_score = _extract_number(record.get("clarity_score"))
        tone_score = _extract_number(record.get("tone_appropriateness_score"))

        # Track summary presence
        if has_summary is True:
            sessions_with_summary += 1

        # Track summary length
        if length_words is not None:
            summary_lengths_words.append(length_words)

            if 100 <= length_words <= 500:
                appropriate_length_summaries += 1
            elif length_words < 100:
                too_brief_summaries += 1
            elif length_words > 500:
                too_verbose_summaries += 1

        if length_chars is not None:
            summary_lengths_chars.append(length_chars)

        # Track evidence inclusion
        if has_evidence is True:
            sessions_with_evidence += 1

        # Track actionability
        if has_next_steps is True:
            sessions_with_next_steps += 1

        # Track task alignment
        if summary_matches is True:
            sessions_aligned += 1

        # Track clarity and tone scores
        if clarity_score is not None:
            clarity_scores.append(clarity_score)

        if tone_score is not None:
            tone_scores.append(tone_score)

        # Detect high-quality summaries
        if (clarity_score is not None and clarity_score > 80.0 and
            tone_score is not None and tone_score > 85.0):
            high_quality_summaries += 1

    # Calculate aggregate metrics
    summary_presence = _percentage(sessions_with_summary, total_sessions)
    avg_length_words = _average(summary_lengths_words)
    avg_length_chars = _average(summary_lengths_chars)
    evidence_rate = _percentage(sessions_with_evidence, total_sessions)
    actionability = _percentage(sessions_with_next_steps, total_sessions)
    alignment_rate = _percentage(sessions_aligned, total_sessions)
    avg_clarity = _average(clarity_scores)
    avg_tone = _average(tone_scores)

    return {
        "total_sessions": total_sessions,
        "summary_presence_rate": summary_presence,
        "avg_summary_length_words": avg_length_words,
        "avg_summary_length_chars": avg_length_chars,
        "evidence_inclusion_rate": evidence_rate,
        "actionability_rate": actionability,
        "task_alignment_rate": alignment_rate,
        "avg_clarity_score": avg_clarity,
        "avg_tone_score": avg_tone,
        "appropriate_length_summaries": appropriate_length_summaries,
        "too_brief_summaries": too_brief_summaries,
        "too_verbose_summaries": too_verbose_summaries,
        "high_quality_summaries": high_quality_summaries,
    }


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value (int or float) if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
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
