"""Pack EnterPlanMode usage analyzer for planning discipline and quality.

Analyzes execution pack transcripts for EnterPlanMode tool usage and plan quality.
Measures planning discipline, plan comprehensiveness, alignment with implementation,
and effectiveness of plan revisions based on user feedback.

Planning metrics:
- EnterPlanMode usage: Whether planning was used for complex tasks
- Plan file size: Lines in plan documents (comprehensiveness indicator)
- ExitPlanMode calls: Number of times agent requested plan approval
- Plan-to-implementation alignment: Match between plan ACs and final changes
- Plan revisions: Whether plans were revised after user feedback
- Planning for multi-file tasks: Whether complex changes used planning

Quality indicators:
- Good planning discipline: EnterPlanMode for multi-file/complex tasks
- Comprehensive plans: 20-100 lines (detailed but focused)
- High alignment score: >80% match between plan and implementation
- Responsive to feedback: Plans revised when users request changes
- Skipped planning penalty: Multi-file tasks without planning
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_planmode_usage(records: object) -> dict[str, Any]:
    """Analyze EnterPlanMode usage and plan quality in execution packs.

    Evaluates planning discipline, plan comprehensiveness, and alignment with
    final implementation. Penalizes skipped planning for complex tasks.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Pack identifier
            - enterplanmode_used: Boolean if EnterPlanMode was called
            - plan_file_lines: Number of lines in plan file
            - exitplanmode_calls: Number of ExitPlanMode calls
            - plan_acceptance_criteria: List of ACs from plan
            - final_acceptance_criteria: List of ACs from implementation
            - plan_revised: Boolean if plan was revised after feedback
            - user_feedback_on_plan: Optional user feedback text
            - files_changed_count: Number of files modified
            - task_complexity: Optional complexity indicator (high/medium/low)

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - packs_with_planning: Packs that used EnterPlanMode
            - planning_usage_rate: Percentage of packs with planning
            - avg_plan_file_lines: Average plan file size
            - plans_within_optimal_size: Count of 20-100 line plans
            - total_exitplanmode_calls: Total ExitPlanMode calls
            - avg_exitplanmode_per_pack: Average exits per pack with planning
            - plans_revised_after_feedback: Count of revised plans
            - revision_rate: Percentage of plans revised
            - avg_plan_alignment_score: Average AC alignment (0-100)
            - high_alignment_packs: Count with >80% alignment
            - low_alignment_packs: Count with <50% alignment
            - multi_file_tasks_without_planning: Penalty count
            - planning_discipline_score: 0-1 score for planning usage

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    packs_with_planning = 0
    plan_file_lines: list[int] = []
    plans_within_optimal_size = 0  # 20-100 lines

    total_exitplanmode_calls = 0
    exitplanmode_per_pack: list[int] = []

    plans_revised_after_feedback = 0
    revision_count = 0

    alignment_scores: list[float] = []
    high_alignment_packs = 0  # >80%
    low_alignment_packs = 0    # <50%

    multi_file_tasks_without_planning = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        enterplanmode_used = _bool(record.get("enterplanmode_used", False))
        files_changed = _int(record.get("files_changed_count", 0))
        task_complexity = _string(record.get("task_complexity", "")).lower()

        # Track planning usage
        if enterplanmode_used:
            packs_with_planning += 1

            # Track plan file size
            plan_lines = _int(record.get("plan_file_lines", 0))
            if plan_lines > 0:
                plan_file_lines.append(plan_lines)

                # Check optimal size
                if 20 <= plan_lines <= 100:
                    plans_within_optimal_size += 1

            # Track ExitPlanMode calls
            exit_calls = _int(record.get("exitplanmode_calls", 0))
            total_exitplanmode_calls += exit_calls
            exitplanmode_per_pack.append(exit_calls)

            # Track plan revisions
            plan_revised = _bool(record.get("plan_revised", False))
            if plan_revised:
                plans_revised_after_feedback += 1
                revision_count += 1

            # Calculate plan-to-implementation alignment
            plan_acs = _get_list(record.get("plan_acceptance_criteria"))
            final_acs = _get_list(record.get("final_acceptance_criteria"))

            if plan_acs and final_acs:
                alignment = _calculate_alignment_score(plan_acs, final_acs)
                alignment_scores.append(alignment)

                if alignment > 80:
                    high_alignment_packs += 1
                elif alignment < 50:
                    low_alignment_packs += 1

        # Penalize multi-file tasks without planning
        if not enterplanmode_used:
            # Multi-file or high complexity without planning is problematic
            if files_changed > 2 or task_complexity == "high":
                multi_file_tasks_without_planning += 1

    # Calculate aggregate metrics
    planning_usage_rate = _percentage(packs_with_planning, total_packs)
    avg_plan_lines = _average(plan_file_lines)
    avg_exitplanmode = _average(exitplanmode_per_pack)
    revision_rate = _percentage(plans_revised_after_feedback, packs_with_planning)
    avg_alignment = _average(alignment_scores)

    # Calculate planning discipline score (0-1)
    discipline_score = _calculate_discipline_score(
        packs_with_planning,
        total_packs,
        multi_file_tasks_without_planning,
        avg_alignment,
        plans_within_optimal_size,
        len(plan_file_lines),
    )

    return {
        "total_packs": total_packs,
        "packs_with_planning": packs_with_planning,
        "planning_usage_rate": planning_usage_rate,
        "avg_plan_file_lines": avg_plan_lines,
        "plans_within_optimal_size": plans_within_optimal_size,
        "total_exitplanmode_calls": total_exitplanmode_calls,
        "avg_exitplanmode_per_pack": avg_exitplanmode,
        "plans_revised_after_feedback": plans_revised_after_feedback,
        "revision_rate": revision_rate,
        "avg_plan_alignment_score": avg_alignment,
        "high_alignment_packs": high_alignment_packs,
        "low_alignment_packs": low_alignment_packs,
        "multi_file_tasks_without_planning": multi_file_tasks_without_planning,
        "planning_discipline_score": discipline_score,
    }


def _bool(value: object) -> bool:
    """Convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1")
    return bool(value)


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _get_list(value: object) -> list[str]:
    """Extract list of strings from value."""
    if value is None:
        return []
    if isinstance(value, list):
        return [_string(item) for item in value if isinstance(item, str) and _string(item)]
    if isinstance(value, str) and _string(value):
        return [_string(value)]
    return []


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_alignment_score(plan_acs: list[str], final_acs: list[str]) -> float:
    """Calculate alignment between plan and final ACs (0-100).

    Measures how well the plan matched the final implementation by comparing
    acceptance criteria. Uses fuzzy matching for partial overlap.
    """
    if not plan_acs or not final_acs:
        return 0.0

    # Count how many plan ACs are present in final ACs (fuzzy match)
    matched_count = 0
    for plan_ac in plan_acs:
        plan_lower = plan_ac.lower()
        for final_ac in final_acs:
            final_lower = final_ac.lower()
            # Consider it a match if >60% of words overlap
            if _fuzzy_match(plan_lower, final_lower):
                matched_count += 1
                break

    # Alignment based on how many plan ACs made it to final
    alignment_percentage = (matched_count / len(plan_acs)) * 100.0
    return round(alignment_percentage, 2)


def _fuzzy_match(text1: str, text2: str) -> bool:
    """Check if two texts have >60% word overlap."""
    words1 = set(text1.split())
    words2 = set(text2.split())

    if not words1 or not words2:
        return False

    overlap = len(words1 & words2)
    min_words = min(len(words1), len(words2))

    return (overlap / min_words) > 0.6


def _calculate_discipline_score(
    packs_with_planning: int,
    total_packs: int,
    missed_planning: int,
    avg_alignment: float,
    optimal_size_count: int,
    total_plans: int,
) -> float:
    """Calculate planning discipline score (0-1).

    Score components:
    - 0.4: Planning usage for complex tasks (penalize missed planning)
    - 0.3: Plan-to-implementation alignment
    - 0.3: Plan quality (optimal size)
    """
    if total_packs == 0:
        return 0.0

    # Usage component (0-0.4)
    # Penalize heavily for missed planning on complex tasks
    if total_packs > 0:
        missed_ratio = missed_planning / total_packs
        usage_component = max(0, 0.4 - (missed_ratio * 0.8))
    else:
        usage_component = 0.4

    # Alignment component (0-0.3)
    alignment_component = (avg_alignment / 100.0) * 0.3

    # Quality component (0-0.3)
    # Reward optimal-sized plans
    if total_plans > 0:
        optimal_ratio = optimal_size_count / total_plans
        quality_component = optimal_ratio * 0.3
    else:
        quality_component = 0.0

    score = usage_component + alignment_component + quality_component
    return round(max(0.0, min(1.0, score)), 3)
