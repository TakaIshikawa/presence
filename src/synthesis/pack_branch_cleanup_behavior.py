"""Pack branch cleanup behavior analyzer for git hygiene tracking.

Analyzes git branch lifecycle management in execution packs to measure branch
creation, cleanup success rates, orphaned branches, and naming consistency.
Tracks cleanup timing and correlation with pack outcomes.

Branch lifecycle metrics:
- Branch creation: Count of branches created during pack execution
- Cleanup success rate: Percentage of branches deleted after completion
- Orphaned branches: Branches not cleaned up after pack completion/failure
- Cleanup timing: Average time-to-cleanup after pack completion
- Naming consistency: Validation of branch naming patterns

Hygiene indicators:
- 100% cleanup: All branches deleted after completion
- Timely cleanup: Branch deleted within expected timeframe
- Consistent naming: Branch follows project naming conventions
- Low orphan rate: Minimal abandoned branches
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_branch_cleanup_behavior(records: object) -> dict[str, Any]:
    """Analyze git branch lifecycle and cleanup behavior in execution packs.

    Tracks branch creation, deletion, orphaned branches, cleanup timing,
    and naming consistency to identify packs with poor branch hygiene.

    Args:
        records: List of pack execution dictionaries with keys:
            - pack_id: Execution pack identifier
            - branch_created: Branch name created for this pack (or None)
            - branch_deleted: Whether branch was deleted (True/False)
            - pack_status: Pack outcome (completed/failed/skipped)
            - cleanup_timing_seconds: Time from completion to deletion (or None)
            - branch_name_valid: Whether branch follows naming conventions
            - git_events: Optional list of git operation events

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - packs_with_branches: Count of packs that created branches
            - branches_created: Total number of branches created
            - branches_deleted: Total number of branches deleted
            - orphaned_branches: Count of branches not cleaned up
            - cleanup_success_rate: Percentage of branches successfully deleted
            - avg_cleanup_timing_seconds: Average time-to-cleanup
            - timely_cleanup_count: Cleanups within expected timeframe (<300s)
            - naming_violations: Count of branches with invalid names
            - naming_consistency_rate: Percentage following naming conventions
            - completed_packs_cleaned: Completed packs with branch cleanup
            - failed_packs_cleaned: Failed packs with branch cleanup

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    packs_with_branches = 0
    branches_created = 0
    branches_deleted = 0
    orphaned_branches = 0
    timely_cleanup_count = 0
    naming_violations = 0

    cleanup_timings: list[float] = []
    completed_packs_cleaned = 0
    failed_packs_cleaned = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        # Extract branch lifecycle information
        branch_created = _string(record.get("branch_created"))
        branch_deleted = _bool_value(record.get("branch_deleted"))
        pack_status = _string(record.get("pack_status"))
        cleanup_timing = _float_value(record.get("cleanup_timing_seconds"))
        branch_name_valid = _bool_value(record.get("branch_name_valid"))

        # Track branch creation
        if branch_created:
            packs_with_branches += 1
            branches_created += 1

            # Track deletion
            if branch_deleted:
                branches_deleted += 1

                # Track cleanup timing
                if cleanup_timing is not None:
                    cleanup_timings.append(cleanup_timing)

                    # Timely cleanup is within 5 minutes (300 seconds)
                    if cleanup_timing <= 300.0:
                        timely_cleanup_count += 1

                # Track cleanup by pack status
                if pack_status == "completed":
                    completed_packs_cleaned += 1
                elif pack_status == "failed":
                    failed_packs_cleaned += 1
            else:
                # Branch created but not deleted = orphaned
                orphaned_branches += 1

            # Track naming consistency
            if branch_name_valid is False:
                naming_violations += 1

    # Calculate metrics
    cleanup_success_rate = _percentage(branches_deleted, branches_created)
    naming_consistency_rate = _percentage(
        branches_created - naming_violations,
        branches_created
    )
    avg_cleanup_timing = _average(cleanup_timings)

    return {
        "total_packs": total_packs,
        "packs_with_branches": packs_with_branches,
        "branches_created": branches_created,
        "branches_deleted": branches_deleted,
        "orphaned_branches": orphaned_branches,
        "cleanup_success_rate": cleanup_success_rate,
        "avg_cleanup_timing_seconds": avg_cleanup_timing,
        "timely_cleanup_count": timely_cleanup_count,
        "naming_violations": naming_violations,
        "naming_consistency_rate": naming_consistency_rate,
        "completed_packs_cleaned": completed_packs_cleaned,
        "failed_packs_cleaned": failed_packs_cleaned,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _bool_value(value: object) -> bool | None:
    """Extract boolean from value."""
    if isinstance(value, bool):
        return value
    return None


def _float_value(value: object) -> float | None:
    """Extract float from value."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
