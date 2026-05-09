"""Session Task tool delegation depth and agent selection analyzer.

Analyzes Task tool usage and agent spawning patterns in Claude Code sessions to
measure delegation discipline, agent selection appropriateness, and delegation depth.
Tracks how effectively the agent delegates work to specialized subagents.

Task delegation metrics:
- Total Task calls: Number of Task tool invocations
- Agent type distribution: Count by type (Bash, Explore, Plan, general-purpose, etc.)
- Delegation depth: Maximum nesting level of agent spawning
- Task success rate by agent type: Success percentage per agent category
- Task duration: Average task completion time
- Agent selection appropriateness: Match between agent type and task description
- Resume usage: Frequency of agent resume vs new spawns
- Delegation discipline score: 0-100 score (appropriate selection and flat delegation = higher)

Quality indicators:
- Balanced agent distribution: Appropriate mix across agent types
- Low delegation depth (1-2 levels): Flat delegation structure
- High task success rate (>85%): Effective agent selection
- High selection appropriateness (>80%): Right agent for task
- Low resume ratio (<20%): Fresh agents for most tasks
- High discipline score (>80): Optimal delegation patterns
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_task_delegation(records: object) -> dict[str, Any]:
    """Analyze Task tool delegation patterns and agent selection in sessions.

    Evaluates delegation discipline through agent type distribution, depth,
    success rates, and selection appropriateness.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_task_calls: Number of Task tool invocations
            - bash_agent_calls: Task calls with subagent_type "Bash"
            - explore_agent_calls: Task calls with subagent_type "Explore"
            - plan_agent_calls: Task calls with subagent_type "Plan"
            - general_agent_calls: Task calls with subagent_type "general-purpose"
            - other_agent_calls: Task calls with other agent types
            - max_delegation_depth: Maximum nesting level of agents
            - avg_delegation_depth: Average nesting level
            - bash_success_count: Successful Bash agent tasks
            - explore_success_count: Successful Explore agent tasks
            - plan_success_count: Successful Plan agent tasks
            - general_success_count: Successful general-purpose agent tasks
            - appropriate_selections: Tasks with matching agent type
            - resume_calls: Task calls using resume parameter
            - avg_task_duration_seconds: Average task completion time
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_task_tool: Count using Task tool
            - avg_task_calls: Average Task invocations per session
            - avg_bash_agent_ratio: Average % Bash agent usage
            - avg_explore_agent_ratio: Average % Explore agent usage
            - avg_plan_agent_ratio: Average % Plan agent usage
            - avg_general_agent_ratio: Average % general-purpose usage
            - avg_other_agent_ratio: Average % other agent types
            - avg_max_delegation_depth: Average maximum depth
            - avg_delegation_depth: Average overall delegation depth
            - avg_bash_success_rate: Average success rate for Bash agents
            - avg_explore_success_rate: Average success rate for Explore agents
            - avg_plan_success_rate: Average success rate for Plan agents
            - avg_general_success_rate: Average success rate for general-purpose
            - avg_selection_appropriateness: Average % appropriate selections
            - avg_resume_ratio: Average % of resume usage
            - avg_task_duration: Average task duration in seconds
            - delegation_discipline_score: Score 0-100 (higher = better)
            - high_discipline_sessions: Count with score >80
            - low_discipline_sessions: Count with score <50
            - deep_delegation_sessions: Count with max depth >2

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_task = 0

    task_calls: list[int | float] = []
    bash_ratios: list[float] = []
    explore_ratios: list[float] = []
    plan_ratios: list[float] = []
    general_ratios: list[float] = []
    other_ratios: list[float] = []
    max_depths: list[float] = []
    avg_depths: list[float] = []
    bash_success_rates: list[float] = []
    explore_success_rates: list[float] = []
    plan_success_rates: list[float] = []
    general_success_rates: list[float] = []
    appropriateness_scores: list[float] = []
    resume_ratios: list[float] = []
    task_durations: list[float] = []
    discipline_scores: list[float] = []

    high_discipline_sessions = 0  # >80 score
    low_discipline_sessions = 0   # <50 score
    deep_delegation_sessions = 0  # max depth >2

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_tasks = _extract_number(record.get("total_task_calls"))
        bash_calls = _extract_number(record.get("bash_agent_calls"))
        explore_calls = _extract_number(record.get("explore_agent_calls"))
        plan_calls = _extract_number(record.get("plan_agent_calls"))
        general_calls = _extract_number(record.get("general_agent_calls"))
        other_calls = _extract_number(record.get("other_agent_calls"))
        max_depth = _extract_number(record.get("max_delegation_depth"))
        avg_depth = _extract_number(record.get("avg_delegation_depth"))
        bash_success = _extract_number(record.get("bash_success_count"))
        explore_success = _extract_number(record.get("explore_success_count"))
        plan_success = _extract_number(record.get("plan_success_count"))
        general_success = _extract_number(record.get("general_success_count"))
        appropriate = _extract_number(record.get("appropriate_selections"))
        resume_count = _extract_number(record.get("resume_calls"))
        duration = _extract_number(record.get("avg_task_duration_seconds"))

        # Track sessions using Task tool
        if total_tasks is not None and total_tasks > 0:
            sessions_with_task += 1
            task_calls.append(total_tasks)

            # Calculate agent type ratios
            if bash_calls is not None:
                bash_ratios.append(_percentage(bash_calls, total_tasks))
            if explore_calls is not None:
                explore_ratios.append(_percentage(explore_calls, total_tasks))
            if plan_calls is not None:
                plan_ratios.append(_percentage(plan_calls, total_tasks))
            if general_calls is not None:
                general_ratios.append(_percentage(general_calls, total_tasks))
            if other_calls is not None:
                other_ratios.append(_percentage(other_calls, total_tasks))

            # Track delegation depth
            if max_depth is not None:
                max_depths.append(max_depth)
                if max_depth > 2:
                    deep_delegation_sessions += 1
            if avg_depth is not None:
                avg_depths.append(avg_depth)

            # Calculate success rates by agent type
            if bash_calls is not None and bash_calls > 0 and bash_success is not None:
                bash_success_rates.append(_percentage(bash_success, bash_calls))
            if explore_calls is not None and explore_calls > 0 and explore_success is not None:
                explore_success_rates.append(_percentage(explore_success, explore_calls))
            if plan_calls is not None and plan_calls > 0 and plan_success is not None:
                plan_success_rates.append(_percentage(plan_success, plan_calls))
            if general_calls is not None and general_calls > 0 and general_success is not None:
                general_success_rates.append(_percentage(general_success, general_calls))

            # Calculate selection appropriateness
            if appropriate is not None:
                appropriateness_scores.append(_percentage(appropriate, total_tasks))

            # Calculate resume ratio
            if resume_count is not None:
                resume_ratios.append(_percentage(resume_count, total_tasks))

            # Track task duration
            if duration is not None:
                task_durations.append(duration)

            # Calculate delegation discipline score
            discipline_score = _calculate_discipline_score(
                max_depth=max_depths[-1] if max_depths and len(max_depths) > len(discipline_scores) else None,
                appropriateness=appropriateness_scores[-1] if appropriateness_scores and len(appropriateness_scores) > len(discipline_scores) else None,
                avg_success_rate=_calculate_overall_success_rate(
                    bash_success, bash_calls, explore_success, explore_calls,
                    plan_success, plan_calls, general_success, general_calls
                ),
                resume_ratio=resume_ratios[-1] if resume_ratios and len(resume_ratios) > len(discipline_scores) else None,
            )
            discipline_scores.append(discipline_score)

            # Classify discipline quality
            if discipline_score > 80.0:
                high_discipline_sessions += 1
            elif discipline_score < 50.0:
                low_discipline_sessions += 1

    # Calculate aggregate metrics
    avg_tasks = _average(task_calls)
    avg_bash = _average(bash_ratios)
    avg_explore = _average(explore_ratios)
    avg_plan = _average(plan_ratios)
    avg_general = _average(general_ratios)
    avg_other = _average(other_ratios)
    avg_max_depth = _average(max_depths)
    avg_avg_depth = _average(avg_depths)
    avg_bash_success = _average(bash_success_rates)
    avg_explore_success = _average(explore_success_rates)
    avg_plan_success = _average(plan_success_rates)
    avg_general_success = _average(general_success_rates)
    avg_appropriateness = _average(appropriateness_scores)
    avg_resume = _average(resume_ratios)
    avg_duration = _average(task_durations)
    avg_discipline = _average(discipline_scores)

    return {
        "total_sessions": total_sessions,
        "sessions_with_task_tool": sessions_with_task,
        "avg_task_calls": avg_tasks,
        "avg_bash_agent_ratio": avg_bash,
        "avg_explore_agent_ratio": avg_explore,
        "avg_plan_agent_ratio": avg_plan,
        "avg_general_agent_ratio": avg_general,
        "avg_other_agent_ratio": avg_other,
        "avg_max_delegation_depth": avg_max_depth,
        "avg_delegation_depth": avg_avg_depth,
        "avg_bash_success_rate": avg_bash_success,
        "avg_explore_success_rate": avg_explore_success,
        "avg_plan_success_rate": avg_plan_success,
        "avg_general_success_rate": avg_general_success,
        "avg_selection_appropriateness": avg_appropriateness,
        "avg_resume_ratio": avg_resume,
        "avg_task_duration": avg_duration,
        "delegation_discipline_score": avg_discipline,
        "high_discipline_sessions": high_discipline_sessions,
        "low_discipline_sessions": low_discipline_sessions,
        "deep_delegation_sessions": deep_delegation_sessions,
    }


def _calculate_overall_success_rate(
    bash_success: int | float | None, bash_calls: int | float | None,
    explore_success: int | float | None, explore_calls: int | float | None,
    plan_success: int | float | None, plan_calls: int | float | None,
    general_success: int | float | None, general_calls: int | float | None,
) -> float | None:
    """Calculate overall success rate across all agent types."""
    total_success = 0
    total_calls = 0

    if bash_success is not None and bash_calls is not None:
        total_success += bash_success
        total_calls += bash_calls
    if explore_success is not None and explore_calls is not None:
        total_success += explore_success
        total_calls += explore_calls
    if plan_success is not None and plan_calls is not None:
        total_success += plan_success
        total_calls += plan_calls
    if general_success is not None and general_calls is not None:
        total_success += general_success
        total_calls += general_calls

    if total_calls > 0:
        return _percentage(total_success, total_calls)
    return None


def _calculate_discipline_score(
    max_depth: float | None,
    appropriateness: float | None,
    avg_success_rate: float | None,
    resume_ratio: float | None,
) -> float:
    """Calculate delegation discipline score (0-100).

    Higher scores indicate better discipline:
    - Low delegation depth (flat structure)
    - High agent selection appropriateness
    - High task success rate
    - Low resume usage (fresh agents)

    Scoring breakdown:
    - Delegation depth: 35 points (depth 1-2 threshold)
    - Selection appropriateness: 30 points (80% threshold)
    - Task success rate: 25 points (85% threshold)
    - Resume discipline: 10 points (20% threshold)
    """
    score = 0.0

    # Delegation depth component (35 points)
    if max_depth is not None:
        if max_depth <= 1:  # Flat = excellent
            score += 35.0
        elif max_depth <= 2:  # One level = good
            score += 25.0
        elif max_depth <= 3:  # Two levels = acceptable
            score += 15.0
        # >3 levels = 0 points (too deep)

    # Selection appropriateness component (30 points)
    if appropriateness is not None:
        if appropriateness >= 80:  # >=80% = excellent
            score += 30.0
        elif appropriateness >= 65:  # >=65% = good
            score += 20.0
        elif appropriateness >= 50:  # >=50% = acceptable
            score += 10.0
        # <50% = 0 points

    # Success rate component (25 points)
    if avg_success_rate is not None:
        if avg_success_rate >= 85:  # >=85% = excellent
            score += 25.0
        elif avg_success_rate >= 70:  # >=70% = good
            score += 20.0
        elif avg_success_rate >= 55:  # >=55% = acceptable
            score += 15.0
        # <55% = 0 points

    # Resume discipline component (10 points)
    if resume_ratio is not None:
        if resume_ratio < 20:  # <20% = excellent (mostly fresh)
            score += 10.0
        elif resume_ratio < 40:  # <40% = good
            score += 7.0
        elif resume_ratio < 60:  # <60% = acceptable
            score += 4.0
        # >60% = 0 points (too much resume)

    return round(score, 2)


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
