"""Session EnterPlanMode usage appropriateness analyzer.

Analyzes when agents use EnterPlanMode for planning vs implementing directly,
measuring plan mode adoption, task complexity correlation, and appropriateness
of planning decisions. Detects anti-patterns like planning for research tasks
or skipping planning for non-trivial implementations.

Plan mode usage metrics:
- EnterPlanMode invocations: Times plan mode was used
- Task type categorization: Research, simple, multi-file, complex
- Direct implementations: Tasks implemented without planning
- Plan exit patterns: ExitPlanMode usage, user approval
- Over-planning detection: Plan mode for simple/trivial tasks
- Skipped planning detection: Missing planning for non-trivial tasks

Anti-patterns detected:
- Planning for research tasks (should explore directly)
- Skipping planning for multi-file changes
- Skipping planning for complex/ambiguous tasks
- Abandoning plans without implementation
- Over-planning for single-file trivial edits

Quality indicators:
- High appropriateness score (>0.8): Good planning judgment
- Low anti-pattern rate (<10%): Few inappropriate planning decisions
- High complex task planning (>80%): Plans when needed
- Low simple task planning (<20%): Avoids over-planning
- High plan completion rate (>90%): Plans lead to implementation
"""

from __future__ import annotations

from typing import Any, Mapping


# Task types that should NOT trigger plan mode
# Use word boundaries to avoid false matches (e.g., "read" in "README")
RESEARCH_TASK_INDICATORS = (
    " explore ",
    " search ",
    " find ",
    " investigate ",
    " understand ",
    " read ",
    " examine ",
    "explore the",
    "search for",
    "find all",
    "find the",
    "analyze codebase",
    "what files",
    "where is",
)

# Task types that SHOULD trigger plan mode
COMPLEX_TASK_INDICATORS = (
    "implement",
    "add feature",
    "refactor",
    "multi-file",
    "architecture",
    "design",
    "migration",
    "integration",
)

# Simple task indicators (over-planning if plan mode used)
SIMPLE_TASK_INDICATORS = (
    "fix typo",
    "add comment",
    "single line",
    "trivial",
    "small fix",
    "rename variable",
)


def analyze_session_enterplanmode_usage(records: object) -> dict[str, Any]:
    """Analyze EnterPlanMode usage appropriateness in agent sessions.

    Evaluates whether plan mode was used appropriately based on task type,
    complexity, and context. Detects anti-patterns and measures effectiveness.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_name: Name of the tool (EnterPlanMode, ExitPlanMode, etc.)
            - task_type: Type of task (research, simple, multi_file, complex)
            - task_description: Optional text description of the task
            - is_implementation: Boolean if this is an implementation turn
            - used_planning: Boolean if planning was used before implementation
            - task_completed: Boolean if task completed successfully
            - is_complex_task: Boolean if task is complex/non-trivial
            - is_multi_file: Boolean if task affects multiple files
            - plan_abandoned: Boolean if plan was started but not completed
            - user_approved_plan: Boolean if user explicitly approved plan
            - exit_plan_mode_used: Boolean if ExitPlanMode was called
            - files_affected_count: Number of files affected by implementation

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - enterplanmode_invocations: Total EnterPlanMode calls
            - exitplanmode_invocations: Total ExitPlanMode calls
            - plan_mode_count: Number of times plan mode was entered
            - direct_implementations: Implementations without planning
            - tasks_with_planning: Implementations preceded by planning
            - tasks_without_planning: Implementations without planning
            - total_implementations: Total implementation tasks
            - plan_mode_ratio: Percentage using plan mode

            Task categorization:
            - research_tasks_planned: Research tasks that used plan mode (anti-pattern)
            - simple_tasks_planned: Simple tasks that used plan mode (over-planning)
            - complex_tasks_planned: Complex tasks that used plan mode (appropriate)
            - multi_file_tasks_planned: Multi-file tasks that used plan mode (appropriate)
            - complex_tasks_direct: Complex tasks without planning (skipped opportunity)
            - multi_file_tasks_direct: Multi-file tasks without planning (skipped opportunity)

            Effectiveness metrics:
            - planning_successes: Successful completions after planning
            - direct_successes: Successful completions without planning
            - planning_success_rate: Percentage of planned tasks completed
            - direct_success_rate: Percentage of direct tasks completed
            - plan_abandonments: Plans started but not completed
            - abandonment_ratio: Percentage of plans abandoned

            Exit pattern metrics:
            - plans_with_exit_tool: Plans that used ExitPlanMode
            - plans_with_user_approval: Plans explicitly approved by user
            - exit_pattern_adherence: Percentage following proper exit pattern

            Anti-pattern detection:
            - skipped_planning_opportunities: Complex/multi-file tasks without planning
            - over_planning_count: Simple/trivial tasks with planning
            - research_planning_count: Research tasks with planning (anti-pattern)
            - total_anti_patterns: Sum of all anti-pattern occurrences
            - anti_pattern_rate: Percentage of decisions that are anti-patterns

            Correlation metrics:
            - complex_planning_ratio: Percentage of complex tasks that used planning
            - simple_planning_ratio: Percentage of simple tasks that used planning
            - multi_file_planning_ratio: Percentage of multi-file tasks that used planning

            Overall scores:
            - appropriateness_score: Overall planning appropriateness (0.0-1.0)
            - usage_score: Combined effectiveness metric (0.0-1.0)

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    # Basic counts
    total_turns = 0
    enterplanmode_invocations = 0
    exitplanmode_invocations = 0
    direct_implementations = 0
    tasks_with_planning = 0
    tasks_without_planning = 0

    # Success tracking
    planning_successes = 0
    direct_successes = 0
    plan_abandonments = 0

    # Task categorization with planning
    research_tasks_planned = 0
    simple_tasks_planned = 0
    complex_tasks_planned = 0
    multi_file_tasks_planned = 0

    # Task categorization without planning
    complex_tasks_direct = 0
    multi_file_tasks_direct = 0
    simple_tasks_direct = 0
    research_tasks_direct = 0

    # Exit pattern tracking
    plans_with_exit_tool = 0
    plans_with_user_approval = 0

    # Anti-pattern tracking
    skipped_planning_opportunities = 0
    over_planning_count = 0
    research_planning_count = 0

    for record in records:
        total_turns += 1

        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))

        # Track EnterPlanMode invocations
        if tool_name.lower() == "enterplanmode":
            enterplanmode_invocations += 1

        # Track ExitPlanMode invocations
        if tool_name.lower() == "exitplanmode":
            exitplanmode_invocations += 1
            if _bool(record.get("user_approved_plan")):
                plans_with_user_approval += 1
            plans_with_exit_tool += 1

        # Track plan abandonments
        if _bool(record.get("plan_abandoned")):
            plan_abandonments += 1

        # Analyze implementation tasks
        is_implementation = _bool(record.get("is_implementation", False))
        if not is_implementation:
            continue

        used_planning = _bool(record.get("used_planning", False))
        task_completed = _bool(record.get("task_completed", False))
        is_complex = _bool(record.get("is_complex_task", False))
        is_multi_file = _bool(record.get("is_multi_file", False))
        task_type = _string(record.get("task_type", "")).lower()
        task_description = _string(record.get("task_description", "")).lower()

        # Infer task characteristics if not explicitly provided
        is_research = task_type == "research" or _is_research_task(task_description)
        is_simple = task_type == "simple" or _is_simple_task(task_description)

        if used_planning:
            tasks_with_planning += 1
            if task_completed:
                planning_successes += 1

            # Categorize planned tasks
            if is_research:
                research_tasks_planned += 1
                research_planning_count += 1  # Anti-pattern
            elif is_simple:
                simple_tasks_planned += 1
                over_planning_count += 1  # Anti-pattern
            elif is_complex:
                complex_tasks_planned += 1
            elif is_multi_file:
                multi_file_tasks_planned += 1
        else:
            tasks_without_planning += 1
            direct_implementations += 1
            if task_completed:
                direct_successes += 1

            # Categorize direct implementations
            if is_research:
                research_tasks_direct += 1
            elif is_simple:
                simple_tasks_direct += 1
            elif is_complex:
                complex_tasks_direct += 1
                skipped_planning_opportunities += 1  # Anti-pattern
            elif is_multi_file:
                multi_file_tasks_direct += 1
                skipped_planning_opportunities += 1  # Anti-pattern

    # Calculate derived metrics
    total_implementations = tasks_with_planning + tasks_without_planning
    plan_mode_ratio = _percentage(tasks_with_planning, total_implementations)

    planning_success_rate = _percentage(planning_successes, tasks_with_planning)
    direct_success_rate = _percentage(direct_successes, tasks_without_planning)

    abandonment_ratio = _percentage(plan_abandonments, enterplanmode_invocations)
    exit_pattern_adherence = _percentage(plans_with_exit_tool, enterplanmode_invocations)

    # Calculate correlation metrics
    total_complex = complex_tasks_planned + complex_tasks_direct
    complex_planning_ratio = _percentage(complex_tasks_planned, total_complex)

    total_simple = simple_tasks_planned + simple_tasks_direct
    simple_planning_ratio = _percentage(simple_tasks_planned, total_simple)

    total_multi_file = multi_file_tasks_planned + multi_file_tasks_direct
    multi_file_planning_ratio = _percentage(multi_file_tasks_planned, total_multi_file)

    # Calculate anti-pattern metrics
    total_anti_patterns = (
        skipped_planning_opportunities +
        over_planning_count +
        research_planning_count
    )
    total_decisions = total_implementations
    anti_pattern_rate = _percentage(total_anti_patterns, total_decisions)

    # Calculate overall scores
    appropriateness_score = _calculate_appropriateness_score(
        complex_planning_ratio,
        simple_planning_ratio,
        anti_pattern_rate,
        exit_pattern_adherence,
    )

    usage_score = _calculate_usage_score(
        planning_success_rate,
        direct_success_rate,
        complex_planning_ratio,
        abandonment_ratio,
    )

    return {
        # Basic metrics
        "total_turns": total_turns,
        "enterplanmode_invocations": enterplanmode_invocations,
        "exitplanmode_invocations": exitplanmode_invocations,
        "plan_mode_count": enterplanmode_invocations,
        "direct_implementations": direct_implementations,
        "tasks_with_planning": tasks_with_planning,
        "tasks_without_planning": tasks_without_planning,
        "total_implementations": total_implementations,
        "plan_mode_ratio": plan_mode_ratio,

        # Task categorization with planning
        "research_tasks_planned": research_tasks_planned,
        "simple_tasks_planned": simple_tasks_planned,
        "complex_tasks_planned": complex_tasks_planned,
        "multi_file_tasks_planned": multi_file_tasks_planned,

        # Task categorization without planning
        "complex_tasks_direct": complex_tasks_direct,
        "multi_file_tasks_direct": multi_file_tasks_direct,
        "simple_tasks_direct": simple_tasks_direct,
        "research_tasks_direct": research_tasks_direct,

        # Effectiveness metrics
        "planning_successes": planning_successes,
        "direct_successes": direct_successes,
        "planning_success_rate": planning_success_rate,
        "direct_success_rate": direct_success_rate,
        "plan_abandonments": plan_abandonments,
        "abandonment_ratio": abandonment_ratio,

        # Exit pattern metrics
        "plans_with_exit_tool": plans_with_exit_tool,
        "plans_with_user_approval": plans_with_user_approval,
        "exit_pattern_adherence": exit_pattern_adherence,

        # Anti-pattern detection
        "skipped_planning_opportunities": skipped_planning_opportunities,
        "over_planning_count": over_planning_count,
        "research_planning_count": research_planning_count,
        "total_anti_patterns": total_anti_patterns,
        "anti_pattern_rate": anti_pattern_rate,

        # Correlation metrics
        "complex_planning_ratio": complex_planning_ratio,
        "simple_planning_ratio": simple_planning_ratio,
        "multi_file_planning_ratio": multi_file_planning_ratio,

        # Overall scores
        "appropriateness_score": appropriateness_score,
        "usage_score": usage_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "enterplanmode_invocations": 0,
        "exitplanmode_invocations": 0,
        "plan_mode_count": 0,
        "direct_implementations": 0,
        "tasks_with_planning": 0,
        "tasks_without_planning": 0,
        "total_implementations": 0,
        "plan_mode_ratio": 0.0,
        "research_tasks_planned": 0,
        "simple_tasks_planned": 0,
        "complex_tasks_planned": 0,
        "multi_file_tasks_planned": 0,
        "complex_tasks_direct": 0,
        "multi_file_tasks_direct": 0,
        "simple_tasks_direct": 0,
        "research_tasks_direct": 0,
        "planning_successes": 0,
        "direct_successes": 0,
        "planning_success_rate": 0.0,
        "direct_success_rate": 0.0,
        "plan_abandonments": 0,
        "abandonment_ratio": 0.0,
        "plans_with_exit_tool": 0,
        "plans_with_user_approval": 0,
        "exit_pattern_adherence": 0.0,
        "skipped_planning_opportunities": 0,
        "over_planning_count": 0,
        "research_planning_count": 0,
        "total_anti_patterns": 0,
        "anti_pattern_rate": 0.0,
        "complex_planning_ratio": 0.0,
        "simple_planning_ratio": 0.0,
        "multi_file_planning_ratio": 0.0,
        "appropriateness_score": 0.0,
        "usage_score": 0.0,
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


def _is_research_task(description: str) -> bool:
    """Detect if task description indicates research/exploration task."""
    # Pad description with spaces to enable word boundary matching
    padded = f" {description} "
    return any(indicator in padded for indicator in RESEARCH_TASK_INDICATORS)


def _is_simple_task(description: str) -> bool:
    """Detect if task description indicates simple/trivial task."""
    return any(indicator in description for indicator in SIMPLE_TASK_INDICATORS)


def _calculate_appropriateness_score(
    complex_planning_ratio: float,
    simple_planning_ratio: float,
    anti_pattern_rate: float,
    exit_pattern_adherence: float,
) -> float:
    """Calculate overall planning appropriateness score (0-1).

    High score indicates good judgment in when to use plan mode:
    - High planning ratio for complex tasks (target: >80%)
    - Low planning ratio for simple tasks (target: <20%)
    - Low anti-pattern rate (target: <10%)
    - High exit pattern adherence (target: >80%)
    """
    # Complex task planning component (0-0.35)
    # Target: 80-100% of complex tasks use planning
    complex_normalized = min(complex_planning_ratio / 80.0, 1.0)
    complex_component = complex_normalized * 0.35

    # Simple task avoidance component (0-0.25)
    # Target: <20% of simple tasks use planning
    if simple_planning_ratio <= 20.0:
        simple_component = 0.25
    else:
        # Penalty for over-planning simple tasks
        excess = min(simple_planning_ratio - 20.0, 80.0) / 80.0
        simple_component = 0.25 * (1.0 - excess)

    # Anti-pattern avoidance component (0-0.25)
    # Target: <10% anti-pattern rate
    if anti_pattern_rate <= 10.0:
        anti_pattern_component = 0.25
    else:
        penalty = min(anti_pattern_rate - 10.0, 90.0) / 90.0
        anti_pattern_component = 0.25 * (1.0 - penalty)

    # Exit pattern adherence component (0-0.15)
    # Target: >80% of plans use proper exit pattern
    exit_normalized = min(exit_pattern_adherence / 80.0, 1.0)
    exit_component = exit_normalized * 0.15

    score = (
        complex_component +
        simple_component +
        anti_pattern_component +
        exit_component
    )
    return round(max(0.0, min(1.0, score)), 3)


def _calculate_usage_score(
    planning_success_rate: float,
    direct_success_rate: float,
    complex_planning_ratio: float,
    abandonment_ratio: float,
) -> float:
    """Calculate overall plan mode usage score (0-1).

    Measures effectiveness of plan mode usage:
    - High success rate with planning (target: >90%)
    - Good success rate without planning (target: >75%)
    - Strong correlation with complex tasks (target: >80%)
    - Low abandonment rate (target: <10%)
    """
    # Planning success component (0-0.35)
    planning_component = (planning_success_rate / 100.0) * 0.35

    # Direct success component (0-0.25)
    direct_component = (direct_success_rate / 100.0) * 0.25

    # Complex task planning component (0-0.25)
    complex_component = (complex_planning_ratio / 100.0) * 0.25

    # Abandonment penalty (0-0.15)
    if abandonment_ratio <= 10.0:
        abandonment_component = 0.15
    else:
        penalty = min(abandonment_ratio - 10.0, 90.0) / 90.0
        abandonment_component = 0.15 * (1.0 - penalty)

    score = (
        planning_component +
        direct_component +
        complex_component +
        abandonment_component
    )
    return round(max(0.0, min(1.0, score)), 3)
