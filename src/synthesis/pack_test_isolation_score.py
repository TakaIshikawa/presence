"""Pack test isolation score analyzer for test quality and independence.

Analyzes test isolation quality within execution packs. Evaluates whether tests
are independent, properly isolated, and free from shared mutable state or order
dependencies. Measures test independence and identifies potential test pollution.

Test isolation metrics:
- Shared fixture usage: Tests sharing mutable fixtures or global state
- Order dependency detection: Tests that must run in specific order
- Test independence score: 0.0 to 1.0 scale measuring isolation quality
- Global state modification: Tests modifying globals without cleanup
- Cross-test pollution: Data leaking between tests

Quality indicators:
- High independence score (>0.8): Well-isolated, order-independent tests
- Low shared state: Minimal shared fixtures, each test sets up own state
- No order dependencies: Tests can run in any order
- Proper cleanup: Tests restore state after modifications
- No pollution: Tests don't affect each other's data
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_test_isolation_score(records: object) -> dict[str, Any]:
    """Analyze test isolation quality within execution packs.

    Evaluates test independence, shared state usage, order dependencies,
    and calculates overall test isolation quality score.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_tests: Total number of tests in pack
            - shared_fixtures: Number of tests sharing mutable fixtures
            - order_dependent_tests: Tests requiring specific execution order
            - global_state_modifications: Tests modifying global state
            - tests_without_cleanup: Tests not cleaning up after themselves
            - cross_test_pollution_cases: Detected data pollution cases
            - test_reruns_needed: Tests that fail on first run but pass on rerun
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_total_tests: Average number of tests per pack
            - avg_test_isolation_score: Average isolation score (0.0-1.0)
            - high_isolation_packs: Count of packs with score >0.8
            - low_isolation_packs: Count of packs with score <0.5
            - avg_shared_fixtures_ratio: Percentage of tests with shared fixtures
            - avg_order_dependency_ratio: Percentage of order-dependent tests
            - avg_cleanup_issue_ratio: Percentage of tests without cleanup
            - total_pollution_cases: Total cross-test pollution detected
            - packs_with_pollution: Count of packs with pollution issues
            - avg_rerun_ratio: Percentage of tests needing reruns

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_tests_counts: list[int | float] = []
    isolation_scores: list[float] = []

    high_isolation_packs = 0  # > 0.8 score
    low_isolation_packs = 0   # < 0.5 score

    shared_fixtures_ratios: list[float] = []
    order_dependency_ratios: list[float] = []
    cleanup_issue_ratios: list[float] = []
    rerun_ratios: list[float] = []

    total_pollution_cases = 0
    packs_with_pollution = 0

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        total_tests = _extract_int(record.get("total_tests"))
        shared_fixtures = _extract_int(record.get("shared_fixtures"))
        order_dependent = _extract_int(record.get("order_dependent_tests"))
        global_modifications = _extract_int(record.get("global_state_modifications"))
        no_cleanup = _extract_int(record.get("tests_without_cleanup"))
        pollution_cases = _extract_int(record.get("cross_test_pollution_cases"))
        reruns_needed = _extract_int(record.get("test_reruns_needed"))

        total_packs += 1

        # Track total tests
        if total_tests is not None:
            total_tests_counts.append(total_tests)

            # Calculate isolation score
            if total_tests > 0:
                score = _calculate_isolation_score(
                    total_tests,
                    shared_fixtures,
                    order_dependent,
                    global_modifications,
                    no_cleanup,
                    pollution_cases,
                    reruns_needed,
                )
                isolation_scores.append(score)

                # Classify isolation quality
                if score > 0.8:
                    high_isolation_packs += 1
                elif score < 0.5:
                    low_isolation_packs += 1

                # Calculate ratios
                if shared_fixtures is not None:
                    shared_fixtures_ratios.append(
                        _percentage(shared_fixtures, total_tests)
                    )

                if order_dependent is not None:
                    order_dependency_ratios.append(
                        _percentage(order_dependent, total_tests)
                    )

                if no_cleanup is not None:
                    cleanup_issue_ratios.append(_percentage(no_cleanup, total_tests))

                if reruns_needed is not None:
                    rerun_ratios.append(_percentage(reruns_needed, total_tests))

        # Track pollution
        if pollution_cases is not None:
            total_pollution_cases += pollution_cases
            if pollution_cases > 0:
                packs_with_pollution += 1

    # Calculate metrics
    avg_total_tests = _average(total_tests_counts)
    avg_isolation_score = _average(isolation_scores)
    avg_shared_fixtures = _average(shared_fixtures_ratios)
    avg_order_dependency = _average(order_dependency_ratios)
    avg_cleanup_issues = _average(cleanup_issue_ratios)
    avg_rerun = _average(rerun_ratios)

    return {
        "total_packs": total_packs,
        "avg_total_tests": avg_total_tests,
        "avg_test_isolation_score": avg_isolation_score,
        "high_isolation_packs": high_isolation_packs,
        "low_isolation_packs": low_isolation_packs,
        "avg_shared_fixtures_ratio": avg_shared_fixtures,
        "avg_order_dependency_ratio": avg_order_dependency,
        "avg_cleanup_issue_ratio": avg_cleanup_issues,
        "total_pollution_cases": total_pollution_cases,
        "packs_with_pollution": packs_with_pollution,
        "avg_rerun_ratio": avg_rerun,
    }


def _calculate_isolation_score(
    total_tests: int,
    shared_fixtures: int | None,
    order_dependent: int | None,
    global_modifications: int | None,
    no_cleanup: int | None,
    pollution_cases: int | None,
    reruns_needed: int | None,
) -> float:
    """Calculate test isolation score from 0.0 (poor) to 1.0 (perfect).

    Score is based on multiple factors:
    - Shared fixtures penalty: -0.7 per 100% of tests (proportional)
    - Order dependency penalty: -1.0 per 100% of tests (severe)
    - Global state penalty: -0.4 per 100% of tests
    - Cleanup issue penalty: -0.5 per 100% of tests
    - Pollution penalty: -0.1 per case
    - Rerun penalty: -0.2 per 100% of tests

    Perfect isolation (1.0) means:
    - No shared mutable fixtures
    - No order dependencies
    - No global state modifications
    - All tests clean up properly
    - No cross-test pollution
    - No tests needing reruns
    """
    if total_tests <= 0:
        return 0.0

    score = 1.0

    # Penalty for shared fixtures (proportional to ratio)
    if shared_fixtures is not None and shared_fixtures > 0:
        ratio = shared_fixtures / total_tests
        score -= min(ratio * 0.7, 1.0)

    # Penalty for order dependencies (severe penalty)
    if order_dependent is not None and order_dependent > 0:
        ratio = order_dependent / total_tests
        score -= min(ratio * 1.0, 1.0)

    # Penalty for global state modifications
    if global_modifications is not None and global_modifications > 0:
        ratio = global_modifications / total_tests
        score -= min(ratio * 0.4, 1.0)

    # Penalty for cleanup issues
    if no_cleanup is not None and no_cleanup > 0:
        ratio = no_cleanup / total_tests
        score -= min(ratio * 0.5, 1.0)

    # Penalty for pollution cases
    if pollution_cases is not None and pollution_cases > 0:
        score -= min(pollution_cases * 0.1, 1.0)

    # Penalty for reruns needed
    if reruns_needed is not None and reruns_needed > 0:
        ratio = reruns_needed / total_tests
        score -= min(ratio * 0.2, 1.0)

    # Clamp to [0.0, 1.0]
    return round(max(0.0, min(1.0, score)), 3)


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_int(value: object) -> int | None:
    """Extract integer from value if available."""
    if isinstance(value, int) and not isinstance(value, bool):
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
