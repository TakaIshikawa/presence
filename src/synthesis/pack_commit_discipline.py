"""Pack git commit discipline and message quality analyzer.

Analyzes git commit behavior across execution packs. Tracks commit message quality,
staging discipline, pre-commit hook compliance, and unauthorized destructive command
usage to measure commit discipline and adherence to best practices.

Git commit discipline metrics:
- Total commits created: Number of git commits in pack
- Commit message quality: Conventional commit format adherence, length, clarity
- Commits per task: Average commits per pack task
- Staging discipline: Specific file staging vs bulk adds (git add -A)
- Pre-commit hook compliance: Hook execution and pass rate
- Unauthorized destructive commands: Force push, reset --hard without user request
- Co-author attribution: Proper Claude co-authorship tagging
- Discipline score: 0-100 score (good messages, specific staging, no unauthorized ops)

Quality indicators:
- High message quality (>80%): Most commits follow conventional format
- Appropriate commit frequency (1-2 per task): Not too many, not too few
- High staging discipline (>75%): Specific file staging over bulk adds
- Perfect hook compliance (100%): All hooks pass
- No unauthorized destructive commands: Safe git operations only
- High discipline score (>80): Excellent git discipline
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_commit_discipline(records: object) -> dict[str, Any]:
    """Analyze git commit behavior and discipline in execution packs.

    Tracks commit quality, staging patterns, hook compliance, and safety.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_commits: Number of git commits created
            - total_tasks: Number of tasks in pack
            - conventional_commits: Commits following conventional format
            - good_length_commits: Commits with appropriate message length
            - clear_message_commits: Commits with clear, descriptive messages
            - specific_staging_commits: Commits using specific file staging
            - bulk_staging_commits: Commits using git add -A or similar
            - hook_executions: Number of pre-commit hook runs
            - hook_passes: Number of successful hook runs
            - hook_failures: Number of failed hook runs
            - unauthorized_force_push: Force push without user request
            - unauthorized_reset_hard: Reset --hard without user request
            - coauthor_tagged_commits: Commits with Claude co-authorship
            - pack_title: Optional pack title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_total_commits: Average commits per pack
            - avg_commits_per_task: Average commits per task
            - avg_conventional_format_rate: Average % conventional commits
            - avg_good_length_rate: Average % appropriate length
            - avg_clear_message_rate: Average % clear messages
            - avg_specific_staging_rate: Average % specific staging
            - avg_hook_pass_rate: Average % hooks passing
            - packs_with_unauthorized_commands: Count with unsafe git commands
            - avg_coauthor_rate: Average % commits with co-author tag
            - commit_discipline_score: Score 0-100 (higher = better)
            - high_discipline_packs: Count with score >80
            - low_discipline_packs: Count with score <50

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    commit_counts: list[int | float] = []
    commits_per_task: list[float] = []
    conventional_rates: list[float] = []
    good_length_rates: list[float] = []
    clear_message_rates: list[float] = []
    specific_staging_rates: list[float] = []
    hook_pass_rates: list[float] = []
    coauthor_rates: list[float] = []
    discipline_scores: list[float] = []

    packs_with_unauthorized = 0
    high_discipline_packs = 0  # >80 score
    low_discipline_packs = 0   # <50 score

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        total_commits = _extract_number(record.get("total_commits"))
        total_tasks = _extract_number(record.get("total_tasks"))
        conventional = _extract_number(record.get("conventional_commits"))
        good_length = _extract_number(record.get("good_length_commits"))
        clear_messages = _extract_number(record.get("clear_message_commits"))
        specific_staging = _extract_number(record.get("specific_staging_commits"))
        bulk_staging = _extract_number(record.get("bulk_staging_commits"))
        hook_execs = _extract_number(record.get("hook_executions"))
        hook_passes = _extract_number(record.get("hook_passes"))
        unauthorized_force = _extract_number(record.get("unauthorized_force_push"))
        unauthorized_reset = _extract_number(record.get("unauthorized_reset_hard"))
        coauthor_tagged = _extract_number(record.get("coauthor_tagged_commits"))

        # Track commit counts
        if total_commits is not None and total_commits > 0:
            commit_counts.append(total_commits)

            # Calculate commits per task
            if total_tasks is not None and total_tasks > 0:
                commits_per_task.append(total_commits / total_tasks)

            # Calculate message quality rates
            if conventional is not None:
                conventional_rates.append(_percentage(conventional, total_commits))
            if good_length is not None:
                good_length_rates.append(_percentage(good_length, total_commits))
            if clear_messages is not None:
                clear_message_rates.append(_percentage(clear_messages, total_commits))

            # Calculate staging discipline
            if specific_staging is not None and bulk_staging is not None:
                total_staging = specific_staging + bulk_staging
                if total_staging > 0:
                    specific_staging_rates.append(_percentage(specific_staging, total_staging))
            elif specific_staging is not None:
                # Assume specific staging if only that metric provided
                specific_staging_rates.append(_percentage(specific_staging, total_commits))

            # Calculate hook pass rate
            if hook_execs is not None and hook_execs > 0 and hook_passes is not None:
                hook_pass_rates.append(_percentage(hook_passes, hook_execs))

            # Track unauthorized commands
            has_unauthorized = False
            if unauthorized_force is not None and unauthorized_force > 0:
                has_unauthorized = True
            if unauthorized_reset is not None and unauthorized_reset > 0:
                has_unauthorized = True
            if has_unauthorized:
                packs_with_unauthorized += 1

            # Calculate co-author rate
            if coauthor_tagged is not None:
                coauthor_rates.append(_percentage(coauthor_tagged, total_commits))

        # Calculate discipline score
        discipline_score = _calculate_discipline_score(
            conventional_rate=conventional_rates[-1] if conventional_rates and len(conventional_rates) > len(discipline_scores) else None,
            clear_message_rate=clear_message_rates[-1] if clear_message_rates and len(clear_message_rates) > len(discipline_scores) else None,
            specific_staging_rate=specific_staging_rates[-1] if specific_staging_rates and len(specific_staging_rates) > len(discipline_scores) else None,
            hook_pass_rate=hook_pass_rates[-1] if hook_pass_rates and len(hook_pass_rates) > len(discipline_scores) else None,
            has_unauthorized=packs_with_unauthorized > len(discipline_scores),
        )
        discipline_scores.append(discipline_score)

        if discipline_score > 80.0:
            high_discipline_packs += 1
        elif discipline_score < 50.0:
            low_discipline_packs += 1

    # Calculate aggregate metrics
    avg_commits = _average(commit_counts)
    avg_cpt = _average(commits_per_task)
    avg_conventional = _average(conventional_rates)
    avg_good_length = _average(good_length_rates)
    avg_clear = _average(clear_message_rates)
    avg_specific = _average(specific_staging_rates)
    avg_hook_pass = _average(hook_pass_rates)
    avg_coauthor = _average(coauthor_rates)
    avg_discipline = _average(discipline_scores)

    return {
        "total_packs": total_packs,
        "avg_total_commits": avg_commits,
        "avg_commits_per_task": avg_cpt,
        "avg_conventional_format_rate": avg_conventional,
        "avg_good_length_rate": avg_good_length,
        "avg_clear_message_rate": avg_clear,
        "avg_specific_staging_rate": avg_specific,
        "avg_hook_pass_rate": avg_hook_pass,
        "packs_with_unauthorized_commands": packs_with_unauthorized,
        "avg_coauthor_rate": avg_coauthor,
        "commit_discipline_score": avg_discipline,
        "high_discipline_packs": high_discipline_packs,
        "low_discipline_packs": low_discipline_packs,
    }


def _calculate_discipline_score(
    conventional_rate: float | None,
    clear_message_rate: float | None,
    specific_staging_rate: float | None,
    hook_pass_rate: float | None,
    has_unauthorized: bool,
) -> float:
    """Calculate commit discipline score (0-100).

    Higher scores indicate better discipline:
    - High conventional format rate (>80%)
    - High clear message rate (>85%)
    - High specific staging rate (>75%)
    - Perfect hook pass rate (100%)
    - No unauthorized destructive commands

    Scoring breakdown:
    - Conventional format: 25 points (80% threshold)
    - Clear messages: 25 points (85% threshold)
    - Specific staging: 25 points (75% threshold)
    - Hook compliance: 15 points (100% threshold)
    - Safety: 10 points (no unauthorized commands)
    """
    score = 0.0

    # Conventional format component (25 points)
    if conventional_rate is not None:
        if conventional_rate >= 80:
            score += 25.0
        elif conventional_rate >= 60:
            score += 18.0
        elif conventional_rate >= 40:
            score += 10.0

    # Clear messages component (25 points)
    if clear_message_rate is not None:
        if clear_message_rate >= 85:
            score += 25.0
        elif clear_message_rate >= 70:
            score += 18.0
        elif clear_message_rate >= 50:
            score += 10.0

    # Specific staging component (25 points)
    if specific_staging_rate is not None:
        if specific_staging_rate >= 75:
            score += 25.0
        elif specific_staging_rate >= 50:
            score += 18.0
        elif specific_staging_rate >= 25:
            score += 10.0

    # Hook compliance component (15 points)
    if hook_pass_rate is not None:
        if hook_pass_rate >= 100:
            score += 15.0
        elif hook_pass_rate >= 85:
            score += 10.0
        elif hook_pass_rate >= 70:
            score += 5.0

    # Safety component (10 points) - deduct if unauthorized
    if not has_unauthorized:
        score += 10.0

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
