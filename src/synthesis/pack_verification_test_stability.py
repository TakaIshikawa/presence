"""Pack verification test stability analyzer for workflow reports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence


@dataclass(frozen=True)
class PackVerificationRun:
    pack_id: str
    run_timestamp: str
    test_results: dict[str, str]  # test_name -> status (pass/fail)
    execution_time_seconds: float


@dataclass(frozen=True)
class FlakyTest:
    test_name: str
    pass_count: int
    fail_count: int
    flip_rate: float


@dataclass(frozen=True)
class PackStabilityMetrics:
    total_packs: int
    total_runs: int
    stable_packs: int
    flaky_packs: int
    total_tests_tracked: int
    stable_tests: int
    flaky_tests: int
    average_stability_score: float
    average_timing_variance: float


@dataclass(frozen=True)
class PackStabilityExample:
    pack_id: str
    stability_score: float
    flaky_tests: list[str]
    timing_variance: float


@dataclass(frozen=True)
class PackVerificationTestStability:
    metrics: PackStabilityMetrics
    flaky_tests_by_pack: dict[str, list[FlakyTest]]
    examples: tuple[PackStabilityExample, ...]
    insights: tuple[str, ...]


def analyze_pack_verification_test_stability(
    verification_runs: Sequence[PackVerificationRun],
) -> PackVerificationTestStability:
    """Detect flaky or unstable test patterns in pack verification results."""
    _validate_verification_runs(verification_runs)

    if not verification_runs:
        metrics = PackStabilityMetrics(
            total_packs=0,
            total_runs=0,
            stable_packs=0,
            flaky_packs=0,
            total_tests_tracked=0,
            stable_tests=0,
            flaky_tests=0,
            average_stability_score=0.0,
            average_timing_variance=0.0,
        )
        return PackVerificationTestStability(
            metrics=metrics,
            flaky_tests_by_pack={},
            examples=(),
            insights=("No verification runs provided.",),
        )

    # Group runs by pack
    runs_by_pack: dict[str, list[PackVerificationRun]] = {}
    for run in verification_runs:
        if run.pack_id not in runs_by_pack:
            runs_by_pack[run.pack_id] = []
        runs_by_pack[run.pack_id].append(run)

    flaky_tests_by_pack: dict[str, list[FlakyTest]] = {}
    stability_scores: list[float] = []
    timing_variances: list[float] = []
    examples: list[PackStabilityExample] = []
    stable_packs = 0
    flaky_packs = 0
    total_tests_tracked = 0
    stable_tests_count = 0
    flaky_tests_count = 0

    for pack_id, pack_runs in runs_by_pack.items():
        if len(pack_runs) < 2:
            # Need at least 2 runs to detect instability
            continue

        # Track test outcomes across runs
        test_outcomes: dict[str, list[str]] = {}
        for run in pack_runs:
            for test_name, status in run.test_results.items():
                if test_name not in test_outcomes:
                    test_outcomes[test_name] = []
                test_outcomes[test_name].append(status)

        # Identify flaky tests
        flaky_tests: list[FlakyTest] = []
        for test_name, outcomes in test_outcomes.items():
            total_tests_tracked += 1
            pass_count = outcomes.count("pass")
            fail_count = outcomes.count("fail")

            # Flaky if has both passes and fails
            if pass_count > 0 and fail_count > 0:
                flip_rate = _percentage(fail_count, len(outcomes))
                flaky_tests.append(
                    FlakyTest(
                        test_name=test_name,
                        pass_count=pass_count,
                        fail_count=fail_count,
                        flip_rate=flip_rate,
                    )
                )
                flaky_tests_count += 1
            else:
                stable_tests_count += 1

        if flaky_tests:
            flaky_tests_by_pack[pack_id] = flaky_tests
            flaky_packs += 1
        else:
            stable_packs += 1

        # Calculate stability score (0-1)
        if test_outcomes:
            stability_score = _percentage(stable_tests_count, total_tests_tracked) / 100.0
        else:
            stability_score = 1.0
        stability_scores.append(stability_score)

        # Calculate timing variance
        execution_times = [run.execution_time_seconds for run in pack_runs]
        timing_variance = _timing_variance(execution_times)
        timing_variances.append(timing_variance)

        # Add example if flaky
        if flaky_tests and len(examples) < 5:
            examples.append(
                PackStabilityExample(
                    pack_id=pack_id,
                    stability_score=round(stability_score, 2),
                    flaky_tests=[ft.test_name for ft in flaky_tests[:3]],  # Truncate
                    timing_variance=timing_variance,
                )
            )

    metrics = PackStabilityMetrics(
        total_packs=len(runs_by_pack),
        total_runs=len(verification_runs),
        stable_packs=stable_packs,
        flaky_packs=flaky_packs,
        total_tests_tracked=total_tests_tracked,
        stable_tests=stable_tests_count,
        flaky_tests=flaky_tests_count,
        average_stability_score=round(sum(stability_scores) / len(stability_scores), 2)
        if stability_scores
        else 0.0,
        average_timing_variance=round(sum(timing_variances) / len(timing_variances), 2)
        if timing_variances
        else 0.0,
    )

    return PackVerificationTestStability(
        metrics=metrics,
        flaky_tests_by_pack=flaky_tests_by_pack,
        examples=tuple(examples),
        insights=_generate_insights(metrics, flaky_tests_by_pack),
    )


def _validate_verification_runs(runs: Sequence[PackVerificationRun]) -> None:
    """Validate verification run structure."""
    if not isinstance(runs, (list, tuple)):
        raise ValueError("verification_runs must be a list or tuple")

    for run in runs:
        if not isinstance(run, PackVerificationRun):
            raise ValueError("verification_runs must contain PackVerificationRun instances")
        if not isinstance(run.pack_id, str):
            raise ValueError("pack_id must be a string")
        if not isinstance(run.run_timestamp, str):
            raise ValueError("run_timestamp must be a string")
        if not isinstance(run.test_results, dict):
            raise ValueError("test_results must be a dict")
        if not isinstance(run.execution_time_seconds, (int, float)):
            raise ValueError("execution_time_seconds must be a number")
        if run.execution_time_seconds < 0:
            raise ValueError("execution_time_seconds must be non-negative")

        for test_name, status in run.test_results.items():
            if not isinstance(test_name, str):
                raise ValueError("test_results keys must be strings")
            if not isinstance(status, str):
                raise ValueError("test_results values must be strings")
            if status not in {"pass", "fail"}:
                raise ValueError("test_results values must be 'pass' or 'fail'")


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage with 2 decimal precision."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _timing_variance(execution_times: list[float]) -> float:
    """Calculate variance in execution times as percentage of mean."""
    if len(execution_times) < 2:
        return 0.0

    mean_time = sum(execution_times) / len(execution_times)
    if mean_time == 0:
        return 0.0

    variance = sum((t - mean_time) ** 2 for t in execution_times) / len(execution_times)
    std_dev = variance ** 0.5

    # Return as percentage of mean
    return round((std_dev / mean_time) * 100.0, 2)


def _generate_insights(
    metrics: PackStabilityMetrics,
    flaky_tests_by_pack: dict[str, list[FlakyTest]],
) -> tuple[str, ...]:
    """Generate human-readable insights from metrics."""
    if metrics.total_packs == 0:
        return ("No verification runs provided.",)

    insights = []

    insights.append(
        f"{metrics.stable_packs} of {metrics.total_packs} packs have stable test results "
        f"({_percentage(metrics.stable_packs, metrics.total_packs)}%)."
    )

    if metrics.flaky_packs > 0:
        insights.append(
            f"{metrics.flaky_packs} packs have flaky tests with inconsistent pass/fail patterns."
        )

    if metrics.flaky_tests > 0:
        insights.append(
            f"{metrics.flaky_tests} of {metrics.total_tests_tracked} tests are flaky "
            f"({_percentage(metrics.flaky_tests, metrics.total_tests_tracked)}%)."
        )

    if metrics.average_timing_variance > 20.0:
        insights.append(
            f"High timing variance ({metrics.average_timing_variance}%): "
            f"execution times vary significantly across runs."
        )

    if metrics.average_stability_score < 0.8 and metrics.total_packs >= 2:
        insights.append(
            f"Low average stability score ({metrics.average_stability_score}): "
            f"many tests fail inconsistently."
        )

    # Identify worst flaky test
    if flaky_tests_by_pack:
        worst_test: FlakyTest | None = None
        worst_pack: str | None = None
        for pack_id, flaky_tests in flaky_tests_by_pack.items():
            for test in flaky_tests:
                if worst_test is None or test.flip_rate > worst_test.flip_rate:
                    worst_test = test
                    worst_pack = pack_id

        if worst_test and worst_pack:
            insights.append(
                f"Most flaky test: '{worst_test.test_name}' in pack '{worst_pack}' "
                f"({worst_test.flip_rate}% failure rate)."
            )

    return tuple(insights)
