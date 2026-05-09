"""Pack verification incremental retry analyzer.

Analyzes verification retry patterns across execution packs to measure incremental
vs full-suite re-verification after fixes. Identifies opportunities for targeted
re-verification and measures efficiency of verification strategies.

Verification incrementality metrics:
- Incremental vs full-suite: Per-file vs full re-verification
- Full re-verification time cost: Total time on unnecessary full re-runs
- Targeted re-verification opportunities: Times when per-file would suffice
- Verification efficiency ratio: Changed files / verified files
- Unnecessary full verification runs: Full runs when targeted would work

Quality indicators:
- High incremental ratio: >70% use per-file verification
- Low full re-verification cost: <30% of verification time
- High efficiency ratio: >0.8 (verify only what changed)
- Few unnecessary full runs: <20% of verifications
- Fast average verification: <30s per verification
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_verification_incremental(records: object) -> dict[str, Any]:
    """Analyze incremental vs full-suite verification patterns across packs.

    Evaluates verification strategy efficiency and identifies opportunities
    for targeted re-verification.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_verifications: Total verification runs
            - incremental_verifications: Per-file verification runs
            - full_suite_verifications: Full re-verification runs
            - incremental_time_seconds: Time spent on incremental
            - full_suite_time_seconds: Time spent on full suite
            - changed_files_count: Number of files changed
            - verified_files_count: Number of files verified
            - unnecessary_full_runs: Full runs that could be incremental
            - targeted_opportunities: Missed incremental opportunities

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - total_verifications: Sum of all verification runs
            - incremental_verifications: Total per-file verifications
            - full_suite_verifications: Total full-suite verifications
            - incremental_ratio: Percentage of incremental verifications
            - full_suite_ratio: Percentage of full-suite verifications
            - incremental_time_seconds: Total time on incremental
            - full_suite_time_seconds: Total time on full suite
            - total_verification_time_seconds: Total verification time
            - full_suite_time_cost_ratio: Percentage of time on full suite
            - avg_verification_time_seconds: Average time per verification
            - changed_files_count: Total files changed
            - verified_files_count: Total files verified
            - verification_efficiency_ratio: Changed / verified ratio
            - unnecessary_full_runs: Total unnecessary full verifications
            - unnecessary_full_run_ratio: Percentage unnecessary
            - targeted_opportunities: Total missed incremental opportunities
            - targeted_opportunity_ratio: Percentage missed
            - sessions_using_incremental: Sessions with >0 incremental
            - incremental_adoption_rate: Percentage of sessions using it
            - incremental_efficiency_score: 0-1 overall efficiency score

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
    total_verifications = 0
    incremental_verifications = 0
    full_suite_verifications = 0

    incremental_time = 0.0
    full_suite_time = 0.0

    changed_files_count = 0
    verified_files_count = 0

    unnecessary_full_runs = 0
    targeted_opportunities = 0

    sessions_using_incremental = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        # Count verification types
        session_total = _int(record.get("total_verifications", 0))
        session_incremental = _int(record.get("incremental_verifications", 0))
        session_full_suite = _int(record.get("full_suite_verifications", 0))

        total_verifications += session_total
        incremental_verifications += session_incremental
        full_suite_verifications += session_full_suite

        # Track time
        inc_time = _float(record.get("incremental_time_seconds", 0))
        full_time = _float(record.get("full_suite_time_seconds", 0))
        incremental_time += inc_time
        full_suite_time += full_time

        # Track file counts
        changed = _int(record.get("changed_files_count", 0))
        verified = _int(record.get("verified_files_count", 0))
        changed_files_count += changed
        verified_files_count += verified

        # Track inefficiencies
        unnecessary = _int(record.get("unnecessary_full_runs", 0))
        opportunities = _int(record.get("targeted_opportunities", 0))
        unnecessary_full_runs += unnecessary
        targeted_opportunities += opportunities

        # Track adoption
        if session_incremental > 0:
            sessions_using_incremental += 1

    # Calculate aggregate metrics
    incremental_ratio = _percentage(incremental_verifications, total_verifications)
    full_suite_ratio = _percentage(full_suite_verifications, total_verifications)

    total_verification_time = incremental_time + full_suite_time
    full_suite_time_cost_ratio = _percentage(full_suite_time, total_verification_time)

    avg_verification_time = (
        total_verification_time / total_verifications
        if total_verifications > 0
        else 0.0
    )
    avg_verification_time = round(avg_verification_time, 2)

    verification_efficiency_ratio = (
        changed_files_count / verified_files_count
        if verified_files_count > 0
        else 0.0
    )
    verification_efficiency_ratio = round(min(1.0, verification_efficiency_ratio), 3)

    unnecessary_full_run_ratio = _percentage(
        unnecessary_full_runs, full_suite_verifications
    )
    targeted_opportunity_ratio = _percentage(
        targeted_opportunities, total_verifications
    )

    incremental_adoption_rate = _percentage(
        sessions_using_incremental, total_sessions
    )

    # Calculate efficiency score
    efficiency_score = _calculate_efficiency_score(
        incremental_ratio,
        full_suite_time_cost_ratio,
        verification_efficiency_ratio,
        unnecessary_full_run_ratio,
    )

    return {
        "total_sessions": total_sessions,
        "total_verifications": total_verifications,
        "incremental_verifications": incremental_verifications,
        "full_suite_verifications": full_suite_verifications,
        "incremental_ratio": incremental_ratio,
        "full_suite_ratio": full_suite_ratio,
        "incremental_time_seconds": round(incremental_time, 2),
        "full_suite_time_seconds": round(full_suite_time, 2),
        "total_verification_time_seconds": round(total_verification_time, 2),
        "full_suite_time_cost_ratio": full_suite_time_cost_ratio,
        "avg_verification_time_seconds": avg_verification_time,
        "changed_files_count": changed_files_count,
        "verified_files_count": verified_files_count,
        "verification_efficiency_ratio": verification_efficiency_ratio,
        "unnecessary_full_runs": unnecessary_full_runs,
        "unnecessary_full_run_ratio": unnecessary_full_run_ratio,
        "targeted_opportunities": targeted_opportunities,
        "targeted_opportunity_ratio": targeted_opportunity_ratio,
        "sessions_using_incremental": sessions_using_incremental,
        "incremental_adoption_rate": incremental_adoption_rate,
        "incremental_efficiency_score": efficiency_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "total_verifications": 0,
        "incremental_verifications": 0,
        "full_suite_verifications": 0,
        "incremental_ratio": 0.0,
        "full_suite_ratio": 0.0,
        "incremental_time_seconds": 0.0,
        "full_suite_time_seconds": 0.0,
        "total_verification_time_seconds": 0.0,
        "full_suite_time_cost_ratio": 0.0,
        "avg_verification_time_seconds": 0.0,
        "changed_files_count": 0,
        "verified_files_count": 0,
        "verification_efficiency_ratio": 0.0,
        "unnecessary_full_runs": 0,
        "unnecessary_full_run_ratio": 0.0,
        "targeted_opportunities": 0,
        "targeted_opportunity_ratio": 0.0,
        "sessions_using_incremental": 0,
        "incremental_adoption_rate": 0.0,
        "incremental_efficiency_score": 0.0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return 0
    return 0


def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _calculate_efficiency_score(
    incremental_ratio: float,
    full_suite_time_cost_ratio: float,
    efficiency_ratio: float,
    unnecessary_full_run_ratio: float,
) -> float:
    """Calculate overall incremental efficiency score (0-1).

    Score components:
    - 0.30: Incremental usage ratio (higher is better)
    - 0.25: Full suite time cost penalty (lower is better)
    - 0.25: Verification efficiency ratio (higher is better)
    - 0.20: Unnecessary full run penalty (lower is better)
    """
    # Incremental ratio component (0-0.30)
    # Target: >70% incremental
    if incremental_ratio >= 70.0:
        incremental_component = 0.30
    else:
        incremental_component = (incremental_ratio / 70.0) * 0.30

    # Full suite time cost penalty (0-0.25)
    # Target: <30% time on full suite
    if full_suite_time_cost_ratio <= 30.0:
        time_cost_component = 0.25
    else:
        penalty = min(full_suite_time_cost_ratio - 30.0, 70.0) / 70.0
        time_cost_component = 0.25 * (1.0 - penalty)

    # Efficiency ratio component (0-0.25)
    # Already 0-1, scale to 0-0.25
    efficiency_component = efficiency_ratio * 0.25

    # Unnecessary full run penalty (0-0.20)
    # Target: <20% unnecessary
    if unnecessary_full_run_ratio <= 20.0:
        unnecessary_component = 0.20
    else:
        penalty = min(unnecessary_full_run_ratio - 20.0, 80.0) / 80.0
        unnecessary_component = 0.20 * (1.0 - penalty)

    score = (
        incremental_component +
        time_cost_component +
        efficiency_component +
        unnecessary_component
    )
    return round(max(0.0, min(1.0, score)), 3)
