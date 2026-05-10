"""Pack branch naming hygiene and PR workflow discipline analyzer.

Dimensions: branch naming convention compliance, branch-to-task alignment,
PR creation workflow, commit-to-PR ratio, branch cleanup.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def analyze_pack_branch_naming_hygiene(records: object) -> dict[str, Any]:
    """Analyze pack branch naming hygiene and PR workflow discipline.

    Args:
        records: A list of pack dictionaries containing branch and PR metrics.

    Returns:
        A dictionary with aggregated branch naming hygiene metrics.

    Raises:
        ValueError: If records is not a list of pack dictionaries.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    pack_scores: list[float] = []
    total_packs = 0
    agg_branches_created = 0
    agg_compliant = 0
    agg_task_aligned = 0
    agg_workflow_followed = 0
    agg_workflow_total = 0
    agg_commits = 0
    agg_prs = 0
    agg_oversized = 0
    agg_undersized = 0
    agg_branch_reuse = 0
    high_quality_packs = 0
    low_quality_packs = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        total_branches_created = _int(record.get("total_branches_created"))
        convention_compliant = _int(record.get("convention_compliant_branches"))
        task_aligned = _int(record.get("task_aligned_branches"))
        workflow_followed = _int(record.get("pr_workflow_steps_followed"))
        workflow_total = _int(record.get("pr_workflow_steps_total"))
        total_commits = _int(record.get("total_commits"))
        total_prs = _int(record.get("total_prs"))
        oversized = _int(record.get("oversized_prs"))
        undersized = _int(record.get("undersized_prs"))
        branch_reuse = _int(record.get("branch_reuse_count"))

        agg_branches_created += total_branches_created
        agg_compliant += convention_compliant
        agg_task_aligned += task_aligned
        agg_workflow_followed += workflow_followed
        agg_workflow_total += workflow_total
        agg_commits += total_commits
        agg_prs += total_prs
        agg_oversized += oversized
        agg_undersized += undersized
        agg_branch_reuse += branch_reuse

        # Convention compliance (0-0.30): >80% compliant = full
        compliance_rate = _percentage(convention_compliant, total_branches_created)
        if compliance_rate >= 80.0:
            convention_score = 0.30
        else:
            convention_score = round((compliance_rate / 80.0) * 0.30, 4)

        # Task alignment (0-0.25): >70% aligned = full
        alignment_rate = _percentage(task_aligned, total_branches_created)
        if alignment_rate >= 70.0:
            alignment_score = 0.25
        else:
            alignment_score = round((alignment_rate / 70.0) * 0.25, 4)

        # PR workflow adherence (0-0.25): >80% steps followed = full
        workflow_rate = _percentage(workflow_followed, workflow_total)
        if workflow_rate >= 80.0:
            workflow_score = 0.25
        else:
            workflow_score = round((workflow_rate / 80.0) * 0.25, 4)

        # Appropriate PR size (0-0.20): <10% oversized and <20% undersized = full
        oversized_rate = _percentage(oversized, total_prs)
        undersized_rate = _percentage(undersized, total_prs)
        if oversized_rate < 10.0 and undersized_rate < 20.0:
            size_score = 0.20
        else:
            oversized_penalty = min(oversized_rate / 10.0, 1.0)
            undersized_penalty = min(undersized_rate / 20.0, 1.0)
            combined_penalty = max(oversized_penalty, undersized_penalty)
            size_score = round((1.0 - combined_penalty) * 0.20, 4)
            size_score = max(size_score, 0.0)

        pack_score = round(convention_score + alignment_score + workflow_score + size_score, 4)
        pack_scores.append(pack_score)

        if pack_score > 0.7:
            high_quality_packs += 1
        elif pack_score < 0.4:
            low_quality_packs += 1

    # Aggregate metrics
    convention_compliant_rate = _percentage(agg_compliant, agg_branches_created)
    task_aligned_rate = _percentage(agg_task_aligned, agg_branches_created)
    pr_workflow_adherence_rate = _percentage(agg_workflow_followed, agg_workflow_total)
    avg_commits_per_pr = round(agg_commits / agg_prs, 2) if agg_prs > 0 else 0.0
    oversized_pr_rate = _percentage(agg_oversized, agg_prs)
    undersized_pr_rate = _percentage(agg_undersized, agg_prs)
    branch_reuse_rate = _percentage(agg_branch_reuse, agg_branches_created)

    # Overall score: aggregate with same weights
    if total_packs == 0:
        overall_score = 0.0
        return {
            "total_packs": 0,
            "total_branches_created": 0,
            "convention_compliant_rate": 0.0,
            "task_aligned_rate": 0.0,
            "pr_workflow_adherence_rate": 0.0,
            "avg_commits_per_pr": 0.0,
            "oversized_pr_rate": 0.0,
            "undersized_pr_rate": 0.0,
            "branch_reuse_rate": 0.0,
            "high_quality_packs": 0,
            "low_quality_packs": 0,
            "branch_naming_hygiene_score": 0.0,
        }

    agg_compliance = _percentage(agg_compliant, agg_branches_created)
    if agg_compliance >= 80.0:
        agg_convention_score = 0.30
    else:
        agg_convention_score = round((agg_compliance / 80.0) * 0.30, 4)

    agg_alignment = _percentage(agg_task_aligned, agg_branches_created)
    if agg_alignment >= 70.0:
        agg_alignment_score = 0.25
    else:
        agg_alignment_score = round((agg_alignment / 70.0) * 0.25, 4)

    agg_workflow = _percentage(agg_workflow_followed, agg_workflow_total)
    if agg_workflow >= 80.0:
        agg_workflow_score = 0.25
    else:
        agg_workflow_score = round((agg_workflow / 80.0) * 0.25, 4)

    agg_oversized_r = _percentage(agg_oversized, agg_prs)
    agg_undersized_r = _percentage(agg_undersized, agg_prs)
    if agg_oversized_r < 10.0 and agg_undersized_r < 20.0:
        agg_size_score = 0.20
    else:
        o_pen = min(agg_oversized_r / 10.0, 1.0)
        u_pen = min(agg_undersized_r / 20.0, 1.0)
        combined = max(o_pen, u_pen)
        agg_size_score = round((1.0 - combined) * 0.20, 4)
        agg_size_score = max(agg_size_score, 0.0)

    overall_score = round(
        agg_convention_score + agg_alignment_score + agg_workflow_score + agg_size_score,
        4,
    )

    return {
        "total_packs": total_packs,
        "total_branches_created": agg_branches_created,
        "convention_compliant_rate": convention_compliant_rate,
        "task_aligned_rate": task_aligned_rate,
        "pr_workflow_adherence_rate": pr_workflow_adherence_rate,
        "avg_commits_per_pr": avg_commits_per_pr,
        "oversized_pr_rate": oversized_pr_rate,
        "undersized_pr_rate": undersized_pr_rate,
        "branch_reuse_rate": branch_reuse_rate,
        "high_quality_packs": high_quality_packs,
        "low_quality_packs": low_quality_packs,
        "branch_naming_hygiene_score": overall_score,
    }
