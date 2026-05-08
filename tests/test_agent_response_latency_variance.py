"""Tests for agent response latency variance analyzer."""

import pytest

from engagement.agent_response_latency_variance import (
    analyze_agent_response_latency_variance,
)


def test_empty_input_returns_zeroed_metrics():
    report = analyze_agent_response_latency_variance([])

    assert report["total_responses"] == 0
    assert report["min_latency"] == 0.0
    assert report["max_latency"] == 0.0
    assert report["mean_latency"] == 0.0
    assert report["variance_ratio"] == 0.0
    assert report["degradation_detected"] is False
    assert report["examples"] == []


def test_single_response_no_variance():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 10.5}
    ])

    assert report["total_responses"] == 1
    assert report["min_latency"] == 10.5
    assert report["max_latency"] == 10.5
    assert report["mean_latency"] == 10.5
    assert report["variance_ratio"] == 1.0


def test_consistent_latency_no_variance():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 10.0},
        {"turn_index": 1, "latency": 10.5},
        {"turn_index": 2, "latency": 9.5},
        {"turn_index": 3, "latency": 10.0},
    ])

    assert report["total_responses"] == 4
    assert report["variance_ratio"] < 2.0
    assert report["degradation_detected"] is False


def test_high_variance_flagged():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 5.0},
        {"turn_index": 1, "latency": 20.0},
    ])

    assert report["variance_ratio"] == 4.0
    assert any(ex["reason"] == "high_variance" for ex in report["examples"])


def test_degradation_pattern_detected():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 5.0},
        {"turn_index": 1, "latency": 6.0},
        {"turn_index": 2, "latency": 15.0},
        {"turn_index": 3, "latency": 18.0},
    ])

    assert report["degradation_detected"] is True
    assert any(ex["reason"] == "degradation" for ex in report["examples"])


def test_no_degradation_for_consistent_performance():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 10.0},
        {"turn_index": 1, "latency": 11.0},
        {"turn_index": 2, "latency": 10.5},
        {"turn_index": 3, "latency": 10.0},
    ])

    assert report["degradation_detected"] is False


def test_slowest_response_included_in_examples():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 5.0},
        {"turn_index": 1, "latency": 25.0},
        {"turn_index": 2, "latency": 10.0},
    ])

    assert any(
        ex["reason"] == "slowest_response" and "turn 1" in ex["details"]
        for ex in report["examples"]
    )


def test_examples_capped_at_five():
    report = analyze_agent_response_latency_variance([
        {"turn_index": i, "latency": i * 10.0} for i in range(10)
    ])

    assert len(report["examples"]) <= 5


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_agent_response_latency_variance({"turn_index": 0, "latency": 10.0})


def test_none_input_returns_zeroed_metrics():
    report = analyze_agent_response_latency_variance(None)

    assert report["total_responses"] == 0


def test_non_dict_records_are_skipped():
    report = analyze_agent_response_latency_variance([
        "not a dict",
        {"turn_index": 0, "latency": 10.0},
    ])

    assert report["total_responses"] == 1


def test_missing_latency_field_skips_record():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0},
        {"turn_index": 1, "latency": 10.0},
    ])

    assert report["total_responses"] == 1


def test_alternative_field_names():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "response_time": 10.0},
        {"turn_index": 1, "duration": 15.0},
        {"turn_index": 2, "latency_seconds": 20.0},
    ])

    assert report["total_responses"] == 3
    assert report["min_latency"] == 10.0
    assert report["max_latency"] == 20.0


def test_negative_latency_skipped():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": -5.0},
        {"turn_index": 1, "latency": 10.0},
    ])

    assert report["total_responses"] == 1
    assert report["min_latency"] == 10.0


def test_string_latency_converted():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": "10.5"},
        {"turn_index": 1, "latency": "15.0"},
    ])

    assert report["total_responses"] == 2
    assert report["min_latency"] == 10.5


def test_boolean_latency_skipped():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": True},
        {"turn_index": 1, "latency": 10.0},
    ])

    assert report["total_responses"] == 1


def test_mean_latency_calculation():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 10.0},
        {"turn_index": 1, "latency": 20.0},
    ])

    assert report["mean_latency"] == 15.0


def test_variance_ratio_with_zero_min():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 0.0},
        {"turn_index": 1, "latency": 10.0},
    ])

    assert report["variance_ratio"] == 0.0


def test_three_times_variance_threshold():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 10.0},
        {"turn_index": 1, "latency": 31.0},
    ])

    assert report["variance_ratio"] > 3.0
    assert any(ex["reason"] == "high_variance" for ex in report["examples"])


def test_degradation_requires_2x_slowdown():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 10.0},
        {"turn_index": 1, "latency": 11.0},
        {"turn_index": 2, "latency": 19.0},
        {"turn_index": 3, "latency": 22.0},
    ])

    # late mean = (19+22)/2 = 20.5, early mean = (10+11)/2 = 10.5
    # 20.5 / 10.5 ≈ 1.95, which is < 2x
    assert report["degradation_detected"] is False


def test_degradation_with_clear_2x_slowdown():
    report = analyze_agent_response_latency_variance([
        {"turn_index": 0, "latency": 10.0},
        {"turn_index": 1, "latency": 10.0},
        {"turn_index": 2, "latency": 25.0},
        {"turn_index": 3, "latency": 25.0},
    ])

    # late mean = 25, early mean = 10, ratio = 2.5x
    assert report["degradation_detected"] is True


def test_fallback_turn_index():
    report = analyze_agent_response_latency_variance([
        {"latency": 10.0},
        {"latency": 20.0},
    ])

    assert report["total_responses"] == 2
