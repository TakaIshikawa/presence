"""Session git commit message quality and discipline analyzer.

Analyzes git commit message quality and adherence to commit discipline
guidelines from CLAUDE.md and system instructions.

Commit quality dimensions:
1. Message structure:
   - Conventional commit format compliance (type(scope): subject)
   - 'Why' vs 'what' focus in message body
   - Conciseness (1-2 sentences for simple commits)
   - Multi-line body usage for complex commits

2. Co-authorship discipline:
   - Claude Co-Authored-By presence (anti-pattern per CLAUDE.md)
   - Adherence to "Don't include Claude as co-author" guideline

3. Change verb accuracy:
   - 'add' for wholly new features (not enhancements)
   - 'update'/'enhance' for improvements to existing features
   - 'fix' for bug fixes
   - Accurate categorization vs misleading verbs

4. Pre-commit workflow:
   - git status before commit
   - git diff before commit
   - git log review (for style consistency)
   - Staged file selection discipline (specific files vs git add -A)

Quality indicators:
- High format compliance (>85%): Conventional commit format
- Zero Claude co-author (0%): No CLAUDE.md violations
- High verb accuracy (>80%): Correct add/update/fix usage
- High workflow compliance (>90%): Pre-commit status/diff checks
- Low git add -A usage (<20%): Specific file staging
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_commit_message_quality(records: object) -> dict[str, Any]:
    """Analyze git commit message quality and discipline.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_commits: Number of commits created
            - conventional_format_commits: Commits using type(scope): format
            - commits_with_claude_coauthor: Commits including Claude (violation)
            - commits_with_why_focus: Commits explaining 'why' not just 'what'
            - concise_commits: Commits with 1-2 sentence messages
            - verbose_commits: Overly long commit messages
            - accurate_add_verbs: 'add' used for new features correctly
            - inaccurate_add_verbs: 'add' misused for updates
            - accurate_update_verbs: 'update' used correctly
            - inaccurate_update_verbs: 'update' misused
            - accurate_fix_verbs: 'fix' used correctly
            - commits_with_prior_status: Commits preceded by git status
            - commits_with_prior_diff: Commits preceded by git diff
            - commits_with_prior_log: Commits preceded by git log
            - commits_using_add_all: Commits using 'git add -A' or '.'
            - commits_with_specific_staging: Commits staging specific files

    Returns:
        Dict with:
            - total_sessions: Number of sessions analyzed
            - sessions_with_commits: Sessions creating commits
            - total_commits: Total commits across sessions
            - conventional_format_rate: % commits using conventional format
            - claude_coauthor_violation_rate: % commits with Claude co-author
            - why_focus_rate: % commits explaining 'why'
            - conciseness_rate: % commits with appropriate length
            - verb_accuracy_rate: % commits with accurate add/update/fix verbs
            - pre_commit_status_rate: % commits with prior git status
            - pre_commit_diff_rate: % commits with prior git diff
            - pre_commit_log_rate: % commits with prior git log
            - specific_staging_rate: % commits staging specific files
            - git_add_all_rate: % commits using add -A or .
            - commit_quality_score: Overall quality score 0-1
            - high_quality_sessions: Count with score >0.8
            - low_quality_sessions: Count with score <0.5

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
    sessions_with_commits = 0
    total_commits = 0
    conventional_format = 0
    claude_coauthor = 0
    why_focus = 0
    concise = 0
    accurate_add = 0
    inaccurate_add = 0
    accurate_update = 0
    accurate_fix = 0
    prior_status = 0
    prior_diff = 0
    prior_log = 0
    add_all = 0
    specific_staging = 0

    session_scores: list[int | float] = []
    high_quality_sessions = 0
    low_quality_sessions = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        commits = _int(record.get("total_commits", 0))
        conv_format = _int(record.get("conventional_format_commits", 0))
        claude_co = _int(record.get("commits_with_claude_coauthor", 0))
        why_foc = _int(record.get("commits_with_why_focus", 0))
        conc = _int(record.get("concise_commits", 0))
        acc_add = _int(record.get("accurate_add_verbs", 0))
        inacc_add = _int(record.get("inaccurate_add_verbs", 0))
        acc_upd = _int(record.get("accurate_update_verbs", 0))
        acc_fix = _int(record.get("accurate_fix_verbs", 0))
        pr_status = _int(record.get("commits_with_prior_status", 0))
        pr_diff = _int(record.get("commits_with_prior_diff", 0))
        pr_log = _int(record.get("commits_with_prior_log", 0))
        add_a = _int(record.get("commits_using_add_all", 0))
        spec_stage = _int(record.get("commits_with_specific_staging", 0))

        if commits > 0:
            sessions_with_commits += 1

        total_commits += commits
        conventional_format += conv_format
        claude_coauthor += claude_co
        why_focus += why_foc
        concise += conc
        accurate_add += acc_add
        inaccurate_add += inacc_add
        accurate_update += acc_upd
        accurate_fix += acc_fix
        prior_status += pr_status
        prior_diff += pr_diff
        prior_log += pr_log
        add_all += add_a
        specific_staging += spec_stage

        # Calculate session score
        session_score = _calculate_session_score(
            total_commits=commits,
            conventional_format=conv_format,
            claude_coauthor=claude_co,
            why_focus=why_foc,
            concise=conc,
            accurate_add=acc_add,
            inaccurate_add=inacc_add,
            prior_status=pr_status,
            prior_diff=pr_diff,
            add_all=add_a,
        )
        session_scores.append(session_score)

        if session_score > 0.8:
            high_quality_sessions += 1
        elif session_score < 0.5:
            low_quality_sessions += 1

    # Calculate rates
    conventional_format_rate = _percentage(conventional_format, total_commits)
    claude_coauthor_violation_rate = _percentage(claude_coauthor, total_commits)
    why_focus_rate = _percentage(why_focus, total_commits)
    conciseness_rate = _percentage(concise, total_commits)

    total_verb_usage = accurate_add + inaccurate_add + accurate_update + accurate_fix
    verb_accuracy_rate = _percentage(
        accurate_add + accurate_update + accurate_fix, total_verb_usage
    )

    pre_commit_status_rate = _percentage(prior_status, total_commits)
    pre_commit_diff_rate = _percentage(prior_diff, total_commits)
    pre_commit_log_rate = _percentage(prior_log, total_commits)

    total_staging = add_all + specific_staging
    specific_staging_rate = _percentage(specific_staging, total_staging)
    git_add_all_rate = _percentage(add_all, total_staging)

    # Calculate overall score
    commit_quality_score = _calculate_pack_score(
        conventional_format_rate=conventional_format_rate,
        claude_coauthor_violation_rate=claude_coauthor_violation_rate,
        why_focus_rate=why_focus_rate,
        verb_accuracy_rate=verb_accuracy_rate,
        pre_commit_status_rate=pre_commit_status_rate,
        specific_staging_rate=specific_staging_rate,
    )

    return {
        "total_sessions": total_sessions,
        "sessions_with_commits": sessions_with_commits,
        "total_commits": total_commits,
        "conventional_format_rate": conventional_format_rate,
        "claude_coauthor_violation_rate": claude_coauthor_violation_rate,
        "why_focus_rate": why_focus_rate,
        "conciseness_rate": conciseness_rate,
        "verb_accuracy_rate": verb_accuracy_rate,
        "pre_commit_status_rate": pre_commit_status_rate,
        "pre_commit_diff_rate": pre_commit_diff_rate,
        "pre_commit_log_rate": pre_commit_log_rate,
        "specific_staging_rate": specific_staging_rate,
        "git_add_all_rate": git_add_all_rate,
        "commit_quality_score": commit_quality_score,
        "high_quality_sessions": high_quality_sessions,
        "low_quality_sessions": low_quality_sessions,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "sessions_with_commits": 0,
        "total_commits": 0,
        "conventional_format_rate": 0.0,
        "claude_coauthor_violation_rate": 0.0,
        "why_focus_rate": 0.0,
        "conciseness_rate": 0.0,
        "verb_accuracy_rate": 0.0,
        "pre_commit_status_rate": 0.0,
        "pre_commit_diff_rate": 0.0,
        "pre_commit_log_rate": 0.0,
        "specific_staging_rate": 0.0,
        "git_add_all_rate": 0.0,
        "commit_quality_score": 0.0,
        "high_quality_sessions": 0,
        "low_quality_sessions": 0,
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


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _calculate_session_score(
    total_commits: int,
    conventional_format: int,
    claude_coauthor: int,
    why_focus: int,
    concise: int,
    accurate_add: int,
    inaccurate_add: int,
    prior_status: int,
    prior_diff: int,
    add_all: int,
) -> float:
    """Calculate session-level commit quality score (0-1).

    Scoring components:
    - Conventional format (0-0.25): Using type(scope): format
    - No Claude co-author (0-0.20): CLAUDE.md compliance
    - Why focus (0-0.20): Explaining 'why' not just 'what'
    - Verb accuracy (0-0.15): Correct add/update/fix usage
    - Pre-commit checks (0-0.10): git status/diff before commit
    - Specific staging (0-0.10): Not using git add -A

    Returns:
        Session score from 0.0 to 1.0
    """
    if total_commits == 0:
        return 1.0  # No commits = perfect score

    score = 0.0

    # Conventional format component (0-0.25)
    format_rate = _percentage(conventional_format, total_commits)
    if format_rate >= 85:
        score += 0.25
    elif format_rate >= 70:
        score += 0.18
    elif format_rate >= 50:
        score += 0.10

    # No Claude co-author component (0-0.20)
    coauthor_rate = _percentage(claude_coauthor, total_commits)
    if coauthor_rate == 0:
        score += 0.20
    elif coauthor_rate <= 10:
        score += 0.10
    # >10% = 0 points (serious violation)

    # Why focus component (0-0.20)
    why_rate = _percentage(why_focus, total_commits)
    if why_rate >= 80:
        score += 0.20
    elif why_rate >= 60:
        score += 0.14
    elif why_rate >= 40:
        score += 0.08

    # Verb accuracy component (0-0.15)
    if accurate_add + inaccurate_add > 0:
        verb_acc_rate = _percentage(accurate_add, accurate_add + inaccurate_add)
        if verb_acc_rate >= 80:
            score += 0.15
        elif verb_acc_rate >= 60:
            score += 0.10
        elif verb_acc_rate >= 40:
            score += 0.05

    # Pre-commit checks component (0-0.10)
    check_rate = _percentage(prior_status + prior_diff, total_commits * 2)
    if check_rate >= 90:
        score += 0.10
    elif check_rate >= 70:
        score += 0.07
    elif check_rate >= 50:
        score += 0.04

    # Specific staging component (0-0.10)
    total_staging = total_commits  # Assume each commit has staging
    if total_staging > 0:
        add_all_rate = _percentage(add_all, total_staging)
        if add_all_rate <= 20:
            score += 0.10
        elif add_all_rate <= 40:
            score += 0.07
        elif add_all_rate <= 60:
            score += 0.04

    return round(score, 3)


def _calculate_pack_score(
    conventional_format_rate: float,
    claude_coauthor_violation_rate: float,
    why_focus_rate: float,
    verb_accuracy_rate: float,
    pre_commit_status_rate: float,
    specific_staging_rate: float,
) -> float:
    """Calculate overall pack commit quality score (0-1).

    Scoring components:
    - Conventional format (0-0.25): >85% compliance
    - No Claude co-author (0-0.20): 0% violation
    - Why focus (0-0.20): >80% explaining why
    - Verb accuracy (0-0.15): >80% correct verbs
    - Pre-commit status (0-0.10): >90% with git status
    - Specific staging (0-0.10): >80% specific files

    Returns:
        Pack score from 0.0 to 1.0
    """
    score = 0.0

    # Conventional format component (0-0.25)
    if conventional_format_rate >= 85:
        score += 0.25
    elif conventional_format_rate >= 70:
        score += 0.18
    elif conventional_format_rate >= 50:
        score += 0.10

    # No Claude co-author component (0-0.20)
    if claude_coauthor_violation_rate == 0:
        score += 0.20
    elif claude_coauthor_violation_rate <= 10:
        score += 0.10

    # Why focus component (0-0.20)
    if why_focus_rate >= 80:
        score += 0.20
    elif why_focus_rate >= 60:
        score += 0.14
    elif why_focus_rate >= 40:
        score += 0.08

    # Verb accuracy component (0-0.15)
    if verb_accuracy_rate >= 80:
        score += 0.15
    elif verb_accuracy_rate >= 60:
        score += 0.10
    elif verb_accuracy_rate >= 40:
        score += 0.05

    # Pre-commit status component (0-0.10)
    if pre_commit_status_rate >= 90:
        score += 0.10
    elif pre_commit_status_rate >= 70:
        score += 0.07
    elif pre_commit_status_rate >= 50:
        score += 0.04

    # Specific staging component (0-0.10)
    if specific_staging_rate >= 80:
        score += 0.10
    elif specific_staging_rate >= 60:
        score += 0.07
    elif specific_staging_rate >= 40:
        score += 0.04

    return round(max(0.0, min(1.0, score)), 3)
