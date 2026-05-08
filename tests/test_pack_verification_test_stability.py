"""Tests for pack verification test stability analyzer."""

import pytest

from synthesis.pack_verification_test_stability import (
    PackVerificationRun,
    PackVerificationTestStability,
    analyze_pack_verification_test_stability,
)


def test_empty_runs_returns_zero_state():
    result = analyze_pack_verification_test_stability([])

    assert result.metrics.total_packs == 0
    assert result.metrics.total_runs == 0
    assert result.metrics.stable_packs == 0
    assert result.metrics.flaky_packs == 0
    assert "No verification runs" in result.insights[0]


def test_single_run_per_pack_cannot_detect_flakiness():
    runs = [
        PackVerificationRun("pack-1", "2024-01-01", {"test_a": "pass"}, 10.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    assert result.metrics.total_packs == 1
    assert result.metrics.total_runs == 1
    # Cannot detect flakiness with only 1 run
    assert result.metrics.flaky_packs == 0


def test_stable_tests_no_flakiness():
    runs = [
        PackVerificationRun("pack-1", "2024-01-01", {"test_a": "pass", "test_b": "pass"}, 10.0),
        PackVerificationRun("pack-1", "2024-01-02", {"test_a": "pass", "test_b": "pass"}, 11.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    assert result.metrics.total_packs == 1
    assert result.metrics.stable_packs == 1
    assert result.metrics.flaky_packs == 0
    assert result.metrics.stable_tests == 2
    assert result.metrics.flaky_tests == 0


def test_flaky_test_detected():
    runs = [
        PackVerificationRun("pack-1", "2024-01-01", {"test_a": "pass"}, 10.0),
        PackVerificationRun("pack-1", "2024-01-02", {"test_a": "fail"}, 10.5),
        PackVerificationRun("pack-1", "2024-01-03", {"test_a": "pass"}, 11.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    assert result.metrics.total_packs == 1
    assert result.metrics.flaky_packs == 1
    assert result.metrics.flaky_tests == 1
    assert "pack-1" in result.flaky_tests_by_pack
    assert len(result.flaky_tests_by_pack["pack-1"]) == 1
    assert result.flaky_tests_by_pack["pack-1"][0].test_name == "test_a"


def test_flip_rate_calculation():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test_a": "fail"}, 10.0),
        PackVerificationRun("pack-1", "t3", {"test_a": "fail"}, 10.0),
        PackVerificationRun("pack-1", "t4", {"test_a": "pass"}, 10.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    flaky_test = result.flaky_tests_by_pack["pack-1"][0]
    assert flaky_test.pass_count == 2
    assert flaky_test.fail_count == 2
    assert flaky_test.flip_rate == 50.0


def test_multiple_packs_mixed_stability():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test_a": "pass"}, 11.0),
        PackVerificationRun("pack-2", "t1", {"test_b": "pass"}, 5.0),
        PackVerificationRun("pack-2", "t2", {"test_b": "fail"}, 6.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    assert result.metrics.total_packs == 2
    assert result.metrics.stable_packs == 1
    assert result.metrics.flaky_packs == 1
    assert "pack-2" in result.flaky_tests_by_pack


def test_timing_variance_low():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test_a": "pass"}, 10.1),
        PackVerificationRun("pack-1", "t3", {"test_a": "pass"}, 9.9),
    ]

    result = analyze_pack_verification_test_stability(runs)

    # Variance should be very low
    assert result.metrics.average_timing_variance < 5.0


def test_timing_variance_high():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test_a": "pass"}, 20.0),
        PackVerificationRun("pack-1", "t3", {"test_a": "pass"}, 5.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    # Variance should be high
    assert result.metrics.average_timing_variance > 20.0


def test_stability_score_calculation():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass", "test_b": "pass", "test_c": "pass"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test_a": "fail", "test_b": "pass", "test_c": "pass"}, 10.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    # 2 stable + 1 flaky = 3 total, stability = 2/3 = 0.67
    assert 0.6 <= result.metrics.average_stability_score <= 0.7


def test_examples_capped_at_five():
    runs = []
    for i in range(10):
        runs.append(PackVerificationRun(f"pack-{i}", "t1", {"test": "pass"}, 10.0))
        runs.append(PackVerificationRun(f"pack-{i}", "t2", {"test": "fail"}, 10.0))

    result = analyze_pack_verification_test_stability(runs)

    assert len(result.examples) <= 5


def test_examples_truncate_long_test_lists():
    # Create tests that alternate between pass/fail across runs
    test_results_1 = {f"test_{i}": "pass" for i in range(20)}
    test_results_2 = {f"test_{i}": "fail" for i in range(20)}
    runs = [
        PackVerificationRun("pack-1", "t1", test_results_1, 10.0),
        PackVerificationRun("pack-1", "t2", test_results_2, 10.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    assert len(result.examples) == 1
    assert len(result.examples[0].flaky_tests) <= 3


def test_high_timing_variance_insight():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test": "pass"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test": "pass"}, 30.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    timing_insight = [i for i in result.insights if "timing variance" in i.lower()]
    assert len(timing_insight) > 0


def test_low_stability_score_insight():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass", "test_b": "pass"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test_a": "fail", "test_b": "fail"}, 10.0),
        PackVerificationRun("pack-2", "t1", {"test_c": "pass"}, 5.0),
        PackVerificationRun("pack-2", "t2", {"test_c": "fail"}, 5.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    stability_insight = [i for i in result.insights if "low average stability" in i.lower()]
    assert len(stability_insight) > 0


def test_worst_flaky_test_insight():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass", "test_b": "pass"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test_a": "fail", "test_b": "fail"}, 10.0),
        PackVerificationRun("pack-1", "t3", {"test_a": "fail", "test_b": "pass"}, 10.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    # test_a has 2 failures out of 3 = 66.67% flip rate
    worst_insight = [i for i in result.insights if "most flaky test" in i.lower()]
    assert len(worst_insight) > 0
    assert "test_a" in worst_insight[0]


def test_all_tests_stable_positive_insights():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test_a": "pass"}, 10.5),
        PackVerificationRun("pack-2", "t1", {"test_b": "pass"}, 5.0),
        PackVerificationRun("pack-2", "t2", {"test_b": "pass"}, 5.5),
    ]

    result = analyze_pack_verification_test_stability(runs)

    assert result.metrics.flaky_packs == 0
    assert result.metrics.stable_packs == 2
    assert not any("flaky" in insight.lower() for insight in result.insights)


@pytest.mark.parametrize(
    ("runs", "message"),
    [
        ("not_a_list", "list or tuple"),
        ([{"pack_id": "p1"}], "PackVerificationRun"),
    ],
)
def test_invalid_runs_raise_errors(runs, message):
    with pytest.raises(ValueError, match=message):
        analyze_pack_verification_test_stability(runs)


def test_invalid_test_status_raises_error():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "unknown"}, 10.0),
    ]

    with pytest.raises(ValueError, match="must be 'pass' or 'fail'"):
        analyze_pack_verification_test_stability(runs)


def test_negative_execution_time_raises_error():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass"}, -5.0),
    ]

    with pytest.raises(ValueError, match="non-negative"):
        analyze_pack_verification_test_stability(runs)


def test_multiple_tests_per_run():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass", "test_b": "pass", "test_c": "fail"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test_a": "fail", "test_b": "pass", "test_c": "fail"}, 11.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    assert result.metrics.total_tests_tracked == 3
    assert result.metrics.stable_tests == 2  # test_b and test_c
    assert result.metrics.flaky_tests == 1  # test_a


def test_empty_test_results():
    runs = [
        PackVerificationRun("pack-1", "t1", {}, 10.0),
        PackVerificationRun("pack-1", "t2", {}, 10.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    assert result.metrics.total_tests_tracked == 0
    assert result.metrics.stable_packs == 1  # No flakiness detected


def test_different_tests_across_runs():
    runs = [
        PackVerificationRun("pack-1", "t1", {"test_a": "pass"}, 10.0),
        PackVerificationRun("pack-1", "t2", {"test_b": "pass"}, 10.0),
    ]

    result = analyze_pack_verification_test_stability(runs)

    # Each test only appears once, so both are stable
    assert result.metrics.stable_tests == 2
    assert result.metrics.flaky_tests == 0
