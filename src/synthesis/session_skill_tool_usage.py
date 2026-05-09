"""Session skill tool usage analyzer.

Analyzes Skill tool invocation patterns during session execution. Tracks which
skills are used, success rates, appropriateness of skill timing, and redundant
invocations (e.g., multiple /verify calls without intervening edits).

Skill tool usage metrics:
- Total skill invocations: Number of Skill tool calls
- Skill frequency distribution: Which skills are used most often
- Skill success vs failure rate: Percentage of successful skill executions
- Appropriate timing: Skills invoked at appropriate times per guidelines
- Redundant invocations: Back-to-back identical skill calls without changes
- Skill diversity: Number of different skills used in session
- Correlation with task efficiency: How skill usage affects completion time

Quality indicators:
- Moderate skill usage (3-8 per session): Balanced use of specialized tools
- High success rate (>90%): Skills execute successfully
- Low redundancy (<10%): No unnecessary repeated skill calls
- High skill diversity (>3 unique): Using multiple specialized tools
- Appropriate timing (>80%): Skills used at right moments
- Common skills: verify, commit, cache are used when needed
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_skill_tool_usage(records: object) -> dict[str, Any]:
    """Analyze Skill tool invocation patterns and effectiveness.

    Tracks skill usage frequency, success rates, and identifies redundant invocations.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_skill_invocations: Total Skill tool calls
            - successful_skills: Number of successful skill executions
            - failed_skills: Number of failed skill executions
            - skill_verify_count: Number of /verify invocations
            - skill_commit_count: Number of /commit invocations
            - skill_cache_count: Number of /cache invocations
            - skill_other_count: Number of other skill invocations
            - unique_skills_used: Number of different skills used
            - redundant_skill_calls: Back-to-back identical invocations
            - appropriate_timing_count: Skills used at appropriate times
            - inappropriate_timing_count: Skills used at wrong times
            - total_tasks: Total tasks in session
            - session_duration_seconds: Total session duration
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_skills: Sessions using Skill tool
            - avg_skill_invocations: Average skill calls per session
            - avg_skills_per_task: Average skill calls per task
            - avg_success_rate: Average % successful skill executions
            - avg_verify_usage: Average /verify calls per session
            - avg_commit_usage: Average /commit calls per session
            - avg_cache_usage: Average /cache calls per session
            - avg_other_usage: Average other skill calls per session
            - avg_skill_diversity: Average unique skills per session
            - avg_redundant_call_rate: Average % redundant invocations
            - avg_appropriate_timing_rate: Average % appropriate timing
            - high_skill_usage_sessions: Count with >10 skill calls
            - low_skill_usage_sessions: Count with <3 skill calls
            - sessions_with_redundant_calls: Count with redundant calls
            - high_diversity_sessions: Count with >4 unique skills

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_skills = 0

    skill_invocations: list[int | float] = []
    skills_per_task: list[float] = []
    success_rates: list[float] = []
    verify_usage: list[int | float] = []
    commit_usage: list[int | float] = []
    cache_usage: list[int | float] = []
    other_usage: list[int | float] = []
    skill_diversity: list[int | float] = []
    redundant_call_rates: list[float] = []
    appropriate_timing_rates: list[float] = []

    high_skill_usage_sessions = 0  # >10 skill calls
    low_skill_usage_sessions = 0   # <3 skill calls
    sessions_with_redundant_calls = 0
    high_diversity_sessions = 0  # >4 unique skills

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_invocations = _extract_number(record.get("total_skill_invocations"))
        successful = _extract_number(record.get("successful_skills"))
        failed = _extract_number(record.get("failed_skills"))
        verify_count = _extract_number(record.get("skill_verify_count"))
        commit_count = _extract_number(record.get("skill_commit_count"))
        cache_count = _extract_number(record.get("skill_cache_count"))
        other_count = _extract_number(record.get("skill_other_count"))
        unique_skills = _extract_number(record.get("unique_skills_used"))
        redundant_calls = _extract_number(record.get("redundant_skill_calls"))
        appropriate_timing = _extract_number(record.get("appropriate_timing_count"))
        inappropriate_timing = _extract_number(record.get("inappropriate_timing_count"))
        total_tasks = _extract_number(record.get("total_tasks"))
        session_duration = _extract_number(record.get("session_duration_seconds"))

        # Track sessions with skills
        if total_invocations is not None and total_invocations > 0:
            sessions_with_skills += 1
            skill_invocations.append(total_invocations)

            if total_invocations > 10:
                high_skill_usage_sessions += 1
            elif total_invocations < 3:
                low_skill_usage_sessions += 1

            # Calculate skills per task
            if total_tasks is not None and total_tasks > 0:
                skills_per_task.append(total_invocations / total_tasks)

        # Calculate success rate
        if successful is not None and failed is not None:
            total_attempts = successful + failed
            if total_attempts > 0:
                success_rates.append(_percentage(successful, total_attempts))

        # Track individual skill usage
        if verify_count is not None:
            verify_usage.append(verify_count)
        if commit_count is not None:
            commit_usage.append(commit_count)
        if cache_count is not None:
            cache_usage.append(cache_count)
        if other_count is not None:
            other_usage.append(other_count)

        # Track skill diversity
        if unique_skills is not None:
            skill_diversity.append(unique_skills)

            if unique_skills > 4:
                high_diversity_sessions += 1

        # Calculate redundant call rate
        if total_invocations is not None and total_invocations > 0:
            if redundant_calls is not None:
                redundant_call_rates.append(_percentage(redundant_calls, total_invocations))

                if redundant_calls > 0:
                    sessions_with_redundant_calls += 1

        # Calculate appropriate timing rate
        if appropriate_timing is not None and inappropriate_timing is not None:
            total_timing_events = appropriate_timing + inappropriate_timing
            if total_timing_events > 0:
                appropriate_timing_rates.append(_percentage(appropriate_timing, total_timing_events))

    # Calculate aggregate metrics
    avg_invocations = _average(skill_invocations)
    avg_per_task = _average(skills_per_task)
    avg_success = _average(success_rates)
    avg_verify = _average(verify_usage)
    avg_commit = _average(commit_usage)
    avg_cache = _average(cache_usage)
    avg_other = _average(other_usage)
    avg_diversity = _average(skill_diversity)
    avg_redundant = _average(redundant_call_rates)
    avg_appropriate = _average(appropriate_timing_rates)

    return {
        "total_sessions": total_sessions,
        "sessions_with_skills": sessions_with_skills,
        "avg_skill_invocations": avg_invocations,
        "avg_skills_per_task": avg_per_task,
        "avg_success_rate": avg_success,
        "avg_verify_usage": avg_verify,
        "avg_commit_usage": avg_commit,
        "avg_cache_usage": avg_cache,
        "avg_other_usage": avg_other,
        "avg_skill_diversity": avg_diversity,
        "avg_redundant_call_rate": avg_redundant,
        "avg_appropriate_timing_rate": avg_appropriate,
        "high_skill_usage_sessions": high_skill_usage_sessions,
        "low_skill_usage_sessions": low_skill_usage_sessions,
        "sessions_with_redundant_calls": sessions_with_redundant_calls,
        "high_diversity_sessions": high_diversity_sessions,
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
