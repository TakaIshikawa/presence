"""Pack branch merge readiness analyzer for deployment safety.

Analyzes pack branch readiness for merging to identify potential risks and
ensure quality standards are met before merging changes.

Merge readiness dimensions:
- Verification pass rate: Percentage of passing verification tests
- Test coverage delta: Change in test coverage from branch
- Type safety issues: Count of type errors or warnings
- Merge conflict risk: Likelihood of merge conflicts
- Unresolved TODOs: Count of incomplete work items
- Branch staleness: Days since divergence from main

Readiness indicators:
- Ready: All verifications pass, no type errors, no conflicts
- Needs work: Some failures or issues to address
- High risk: Multiple failures or significant staleness
- Blocked: Critical issues preventing merge
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_branch_merge_readiness(records: object) -> dict[str, Any]:
    """Analyze pack branch readiness for merging.

    Evaluates multiple merge readiness dimensions to determine if a pack
    branch is safe to merge into main.

    Args:
        records: List of pack execution dictionaries with keys:
            - pack_id: Execution pack identifier
            - verification_pass_rate: Percentage of passing verifications (0-100)
            - test_coverage_delta: Change in coverage percentage
            - type_safety_issues: Count of type errors/warnings
            - merge_conflict_risk_score: Risk score (0.0-1.0)
            - unresolved_todos_count: Count of incomplete TODOs
            - branch_staleness_days: Days since branch diverged
            - verification_results: Optional list of verification outcomes

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - ready_packs: Count of packs ready to merge
            - needs_work_packs: Count needing fixes before merge
            - high_risk_packs: Count with significant issues
            - blocked_packs: Count with critical blockers
            - avg_verification_pass_rate: Average pass rate across packs
            - avg_test_coverage_delta: Average coverage change
            - total_type_safety_issues: Total type errors across packs
            - avg_merge_conflict_risk: Average conflict risk score
            - total_unresolved_todos: Total incomplete TODOs
            - stale_branches: Count of branches >7 days old

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    ready_packs = 0
    needs_work_packs = 0
    high_risk_packs = 0
    blocked_packs = 0

    verification_pass_rates: list[float] = []
    test_coverage_deltas: list[float] = []
    merge_conflict_risks: list[float] = []

    total_type_safety_issues = 0
    total_unresolved_todos = 0
    stale_branches = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        # Extract metrics
        pass_rate = _float_value(record.get("verification_pass_rate"))
        coverage_delta = _float_value(record.get("test_coverage_delta"))
        type_issues = _int_value(record.get("type_safety_issues"))
        conflict_risk = _float_value(record.get("merge_conflict_risk_score"))
        unresolved_todos = _int_value(record.get("unresolved_todos_count"))
        staleness_days = _int_value(record.get("branch_staleness_days"))

        # Track aggregates
        if pass_rate is not None:
            verification_pass_rates.append(pass_rate)

        if coverage_delta is not None:
            test_coverage_deltas.append(coverage_delta)

        if conflict_risk is not None:
            merge_conflict_risks.append(conflict_risk)

        if type_issues is not None:
            total_type_safety_issues += type_issues

        if unresolved_todos is not None:
            total_unresolved_todos += unresolved_todos

        if staleness_days is not None and staleness_days > 7:
            stale_branches += 1

        # Determine readiness category
        category = _categorize_readiness(
            pass_rate,
            type_issues,
            conflict_risk,
            unresolved_todos,
            staleness_days
        )

        if category == "ready":
            ready_packs += 1
        elif category == "needs_work":
            needs_work_packs += 1
        elif category == "high_risk":
            high_risk_packs += 1
        elif category == "blocked":
            blocked_packs += 1

    # Calculate averages
    avg_pass_rate = _average(verification_pass_rates)
    avg_coverage_delta = _average(test_coverage_deltas)
    avg_conflict_risk = _average(merge_conflict_risks)

    return {
        "total_packs": total_packs,
        "ready_packs": ready_packs,
        "needs_work_packs": needs_work_packs,
        "high_risk_packs": high_risk_packs,
        "blocked_packs": blocked_packs,
        "avg_verification_pass_rate": avg_pass_rate,
        "avg_test_coverage_delta": avg_coverage_delta,
        "total_type_safety_issues": total_type_safety_issues,
        "avg_merge_conflict_risk": avg_conflict_risk,
        "total_unresolved_todos": total_unresolved_todos,
        "stale_branches": stale_branches,
    }


def _categorize_readiness(
    pass_rate: float | None,
    type_issues: int | None,
    conflict_risk: float | None,
    unresolved_todos: int | None,
    staleness_days: int | None,
) -> str:
    """Categorize merge readiness based on metrics.

    Categories:
    - blocked: Pass rate <50% or >5 type errors or conflict risk >0.7
    - high_risk: Pass rate <80% or >2 type errors or conflict risk >0.5 or >14 days stale
    - needs_work: Pass rate <100% or >0 type errors or conflict risk >0.2 or >0 todos
    - ready: All checks pass
    """
    # Check for blocking issues
    if pass_rate is not None and pass_rate < 50.0:
        return "blocked"
    if type_issues is not None and type_issues > 5:
        return "blocked"
    if conflict_risk is not None and conflict_risk > 0.7:
        return "blocked"

    # Check for high risk
    if pass_rate is not None and pass_rate < 80.0:
        return "high_risk"
    if type_issues is not None and type_issues > 2:
        return "high_risk"
    if conflict_risk is not None and conflict_risk > 0.5:
        return "high_risk"
    if staleness_days is not None and staleness_days > 14:
        return "high_risk"

    # Check for needs work
    if pass_rate is not None and pass_rate < 100.0:
        return "needs_work"
    if type_issues is not None and type_issues > 0:
        return "needs_work"
    if conflict_risk is not None and conflict_risk > 0.2:
        return "needs_work"
    if unresolved_todos is not None and unresolved_todos > 0:
        return "needs_work"

    # All checks pass
    return "ready"


def _int_value(value: object) -> int | None:
    """Extract integer from value."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _float_value(value: object) -> float | None:
    """Extract float from value."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _average(values: list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
