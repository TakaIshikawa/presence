"""Pack Skill invocation coverage analyzer.

Measures how effectively packs leverage available Skills. Tracks skill invocation
opportunities, actual usage, matching accuracy, and discipline score. Evaluates
whether skills are invoked when appropriate versus manual implementation.

Skill usage metrics:
- Skill invocation rate: Invoked / opportunities
- Skill matching accuracy: Correct skill selected when invoked
- User-invocable skill recognition: Detection of skill opportunities
- Skill vs manual implementation ratio
- Overall skill discipline score (0.0-1.0)

Quality indicators:
- High invocation rate (>80%): Most opportunities result in skill use
- High matching accuracy (>90%): Correct skills selected
- High recognition rate (>70%): Skill opportunities detected
- Low manual implementation ratio (<20%): Prefers skills over manual work
- High discipline score (>0.8): Strong skill usage adherence
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_skill_invocation(records: object) -> dict[str, Any]:
    """Analyze Skill invocation coverage in execution packs.

    Evaluates how effectively packs use available Skills.

    Args:
        records: List of skill event dictionaries with keys:
            - event_type: "skill_available", "skill_opportunity", or "skill_invoked"
            - skill_name: Name of the skill
            - was_invoked: Boolean indicating actual invocation
            - correct_skill: Boolean indicating correct skill selected
            - user_request: User message that could use skill
            - manual_implementation: Boolean indicating manual work instead
            - turn_index: Turn number when event occurred

    Returns:
        Dict with:
            - total_skill_opportunities: Count of detected opportunities
            - skills_invoked: Count of actual Skill tool invocations
            - skills_available: Count of available skills
            - skill_invocation_rate: Percentage of opportunities resulting in use
            - correct_skill_selections: Count of correct skill choices
            - skill_matching_accuracy: Percentage of correct selections
            - user_invocable_recognized: Count of recognized user-invocable skills
            - recognition_rate: Percentage of user skills recognized
            - manual_implementations: Count of manual work instead of skills
            - skill_vs_manual_ratio: Ratio of skill use to manual (0.0-1.0)
            - skills_mentioned_not_invoked: Opportunities where skill mentioned but not used
            - discipline_score: Overall skill usage discipline (0.0-1.0)
            - common_skills_used: Dict of skill name to usage count
            - missed_opportunities: Count of clear skill opportunities missed

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of skill event dictionaries")

    if not records:
        return _empty_result()

    total_opportunities = 0
    skills_invoked = 0
    skills_available_count = 0
    correct_selections = 0
    user_invocable_recognized = 0
    manual_implementations = 0
    mentioned_not_invoked = 0
    skill_usage_counts: dict[str, int] = {}
    missed_opportunities = 0

    # Track available skills
    available_skills: set[str] = set()
    # Track user-invocable skills
    user_invocable_skills: set[str] = set()

    for record in records:
        if not isinstance(record, Mapping):
            continue

        event_type = record.get("event_type")
        skill_name = record.get("skill_name")
        was_invoked = record.get("was_invoked")
        correct_skill = record.get("correct_skill")
        manual_impl = record.get("manual_implementation")

        # Track available skills
        if event_type == "skill_available" and isinstance(skill_name, str):
            available_skills.add(skill_name)
            skills_available_count += 1

        # Track skill opportunities
        if event_type == "skill_opportunity":
            total_opportunities += 1

            # Check if invoked
            if was_invoked is True:
                skills_invoked += 1

                # Track usage counts (only for opportunities, not separate invoked events)
                if isinstance(skill_name, str):
                    skill_usage_counts[skill_name] = skill_usage_counts.get(skill_name, 0) + 1

                # Track correct skill from opportunity
                if correct_skill is True:
                    correct_selections += 1
            else:
                # Opportunity identified but not invoked
                mentioned_not_invoked += 1

                # Check if manual implementation done instead
                if manual_impl is True:
                    manual_implementations += 1
                    missed_opportunities += 1

        # Track skill invocations (standalone, not from opportunity)
        elif event_type == "skill_invoked":
            if isinstance(skill_name, str):
                skill_usage_counts[skill_name] = skill_usage_counts.get(skill_name, 0) + 1

            # Check if correct skill
            if correct_skill is True:
                correct_selections += 1

        # Track user-invocable skill recognition
        if event_type == "user_invocable_skill":
            if isinstance(skill_name, str):
                user_invocable_skills.add(skill_name)
            if was_invoked is True:
                user_invocable_recognized += 1

    # Calculate aggregate metrics
    invocation_rate = _percentage(skills_invoked, total_opportunities)
    matching_accuracy = _percentage(correct_selections, skills_invoked)
    recognition_rate = _percentage(
        user_invocable_recognized,
        len(user_invocable_skills) if user_invocable_skills else 1
    )

    # Skill vs manual ratio (higher is better)
    total_work = skills_invoked + manual_implementations
    skill_ratio = _ratio(skills_invoked, total_work)

    # Discipline score (0.0-1.0)
    discipline = _calculate_discipline_score(
        invocation_rate=invocation_rate,
        matching_accuracy=matching_accuracy,
        recognition_rate=recognition_rate,
        skill_vs_manual_ratio=skill_ratio,
    )

    return {
        "total_skill_opportunities": total_opportunities,
        "skills_invoked": skills_invoked,
        "skills_available": len(available_skills),
        "skill_invocation_rate": invocation_rate,
        "correct_skill_selections": correct_selections,
        "skill_matching_accuracy": matching_accuracy,
        "user_invocable_recognized": user_invocable_recognized,
        "recognition_rate": recognition_rate,
        "manual_implementations": manual_implementations,
        "skill_vs_manual_ratio": skill_ratio,
        "skills_mentioned_not_invoked": mentioned_not_invoked,
        "discipline_score": discipline,
        "common_skills_used": skill_usage_counts,
        "missed_opportunities": missed_opportunities,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_skill_opportunities": 0,
        "skills_invoked": 0,
        "skills_available": 0,
        "skill_invocation_rate": 0.0,
        "correct_skill_selections": 0,
        "skill_matching_accuracy": 0.0,
        "user_invocable_recognized": 0,
        "recognition_rate": 0.0,
        "manual_implementations": 0,
        "skill_vs_manual_ratio": 1.0,  # Perfect when no work done
        "skills_mentioned_not_invoked": 0,
        "discipline_score": 1.0,  # Perfect when no opportunities
        "common_skills_used": {},
        "missed_opportunities": 0,
    }


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _ratio(numerator: int | float, denominator: int | float) -> float:
    """Calculate ratio (0.0-1.0), handling zero denominator."""
    if denominator <= 0:
        return 1.0  # Perfect ratio when no denominator
    return round(numerator / denominator, 2)


def _calculate_discipline_score(
    invocation_rate: float,
    matching_accuracy: float,
    recognition_rate: float,
    skill_vs_manual_ratio: float,
) -> float:
    """Calculate overall skill discipline score (0.0-1.0).

    Components weighted by importance:
    - 40% invocation rate (most important)
    - 25% matching accuracy
    - 20% recognition rate
    - 15% skill vs manual ratio

    Args:
        invocation_rate: Percentage of opportunities resulting in use (0-100)
        matching_accuracy: Percentage of correct skill selections (0-100)
        recognition_rate: Percentage of user skills recognized (0-100)
        skill_vs_manual_ratio: Ratio of skill use to manual work (0.0-1.0)

    Returns:
        Discipline score normalized to 0.0-1.0 range
    """
    # Normalize percentages to 0.0-1.0
    invocation_component = invocation_rate / 100.0
    matching_component = matching_accuracy / 100.0
    recognition_component = recognition_rate / 100.0

    # skill_vs_manual_ratio is already 0.0-1.0

    # Weighted combination
    discipline = (
        0.4 * invocation_component
        + 0.25 * matching_component
        + 0.2 * recognition_component
        + 0.15 * skill_vs_manual_ratio
    )

    return round(discipline, 2)
