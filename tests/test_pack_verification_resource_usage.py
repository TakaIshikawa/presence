"""Tests for pack verification resource usage analyzer."""

import pytest

from synthesis.pack_verification_resource_usage import (
    analyze_pack_verification_resource_usage,
)


def test_empty_records_returns_zero_metrics():
    result = analyze_pack_verification_resource_usage([])

    assert result["total_packs"] == 0
    assert result["avg_verifications_per_pack"] == 0.0
    assert result["avg_verification_cost_seconds"] == 0.0


def test_none_records_treated_as_empty():
    result = analyze_pack_verification_resource_usage(None)

    assert result["total_packs"] == 0


def test_single_pack_low_cost():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 5,
            "avg_verification_time_seconds": 20.0,
            "total_verification_memory_mb": 100.0,
            "total_verification_cpu_seconds": 50.0,
            "successful_verifications": 5,
            "failed_verifications": 0,
        }
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["total_packs"] == 1
    assert result["avg_verifications_per_pack"] == 5.0
    assert result["avg_verification_cost_seconds"] == 20.0
    assert result["low_cost_packs"] == 1
    assert result["high_cost_packs"] == 0


def test_single_pack_high_cost():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 3,
            "avg_verification_time_seconds": 150.0,
        }
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["avg_verification_cost_seconds"] == 150.0
    assert result["low_cost_packs"] == 0
    assert result["high_cost_packs"] == 1


def test_efficiency_ratio_calculation():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "successful_verifications": 9,  # 90% efficiency
        }
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["avg_efficiency_ratio"] == 0.9
    assert result["high_efficiency_packs"] == 1


def test_low_efficiency():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "successful_verifications": 4,  # 40% efficiency
        }
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["avg_efficiency_ratio"] == 0.4
    assert result["low_efficiency_packs"] == 1


def test_success_rate_calculation():
    records = [
        {
            "pack_id": "pack-1",
            "successful_verifications": 8,
            "failed_verifications": 2,  # 80% success rate
        }
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["avg_success_rate"] == 80.0


def test_optimization_opportunities():
    records = [
        {
            "pack_id": "pack-1",
            "inefficient_verifications": 3,
        },
        {
            "pack_id": "pack-2",
            "inefficient_verifications": 5,
        },
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["total_optimization_opportunities"] == 8


def test_resource_usage_tracking():
    records = [
        {
            "pack_id": "pack-1",
            "total_verification_memory_mb": 200.0,
            "total_verification_cpu_seconds": 100.0,
        },
        {
            "pack_id": "pack-2",
            "total_verification_memory_mb": 300.0,
            "total_verification_cpu_seconds": 150.0,
        },
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["avg_verification_memory_mb"] == 250.0  # (200 + 300) / 2
    assert result["avg_verification_cpu_seconds"] == 125.0  # (100 + 150) / 2


def test_comprehensive_pack_analysis():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "total_verification_time_seconds": 250.0,
            "total_verification_memory_mb": 150.0,
            "total_verification_cpu_seconds": 80.0,
            "successful_verifications": 9,
            "failed_verifications": 1,
            "avg_verification_time_seconds": 25.0,
            "inefficient_verifications": 2,
        },
        {
            "pack_id": "pack-2",
            "total_verifications": 5,
            "total_verification_time_seconds": 600.0,
            "total_verification_memory_mb": 250.0,
            "total_verification_cpu_seconds": 120.0,
            "successful_verifications": 5,  # 5/5 = 1.0 efficiency >0.8
            "failed_verifications": 0,
            "avg_verification_time_seconds": 130.0,  # >120s for high cost
            "inefficient_verifications": 1,
        },
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["total_packs"] == 2
    assert result["avg_verifications_per_pack"] == 7.5  # (10 + 5) / 2
    assert result["avg_verification_cost_seconds"] == 77.5  # (25 + 130) / 2
    assert result["avg_verification_memory_mb"] == 200.0  # (150 + 250) / 2
    assert result["avg_verification_cpu_seconds"] == 100.0  # (80 + 120) / 2
    assert result["avg_efficiency_ratio"] == 0.95  # (0.9 + 1.0) / 2 = 0.95
    assert result["low_cost_packs"] == 1
    assert result["high_cost_packs"] == 1
    assert result["high_efficiency_packs"] == 2
    assert result["total_optimization_opportunities"] == 3
    assert result["avg_success_rate"] == 95.0  # (90 + 100) / 2


def test_missing_optional_fields():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 5,
            # Missing other fields
        }
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["total_packs"] == 1
    assert result["avg_verifications_per_pack"] == 5.0
    assert result["avg_verification_cost_seconds"] == 0.0
    assert result["avg_efficiency_ratio"] == 0.0


def test_non_mapping_records_skipped():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 5,
            "avg_verification_time_seconds": 30.0,
        },
        "not a dict",
        None,
        123,
        {
            "pack_id": "pack-2",
            "total_verifications": 3,
            "avg_verification_time_seconds": 25.0,
        },
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["total_packs"] == 2


def test_invalid_records_raises_value_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_verification_resource_usage("not a list")


def test_zero_verifications():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 0,
            "successful_verifications": 0,
        }
    ]

    result = analyze_pack_verification_resource_usage(records)

    # Zero verifications should not cause division errors
    assert result["total_packs"] == 1
    assert result["avg_efficiency_ratio"] == 0.0


def test_cost_thresholds():
    records = [
        {
            "pack_id": "pack-1",
            "avg_verification_time_seconds": 30.0,  # Exactly at threshold
        },
        {
            "pack_id": "pack-2",
            "avg_verification_time_seconds": 120.0,  # Exactly at threshold
        },
    ]

    result = analyze_pack_verification_resource_usage(records)

    # 30 is not < 30, so not low cost
    # 120 is not > 120, so not high cost
    assert result["low_cost_packs"] == 0
    assert result["high_cost_packs"] == 0


def test_efficiency_thresholds():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "successful_verifications": 8,  # 0.8 efficiency
        },
        {
            "pack_id": "pack-2",
            "total_verifications": 10,
            "successful_verifications": 5,  # 0.5 efficiency
        },
    ]

    result = analyze_pack_verification_resource_usage(records)

    # 0.8 is not > 0.8, so not high efficiency
    # 0.5 is not < 0.5, so not low efficiency
    assert result["high_efficiency_packs"] == 0
    assert result["low_efficiency_packs"] == 0


def test_boolean_values_rejected():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": True,  # Should be rejected
            "successful_verifications": False,  # Should be rejected
        }
    ]

    result = analyze_pack_verification_resource_usage(records)

    # Booleans should be treated as None
    assert result["avg_verifications_per_pack"] == 0.0
    assert result["avg_efficiency_ratio"] == 0.0


def test_float_values_accepted():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 5.5,
            "avg_verification_time_seconds": 25.7,
            "total_verification_memory_mb": 150.3,
        }
    ]

    result = analyze_pack_verification_resource_usage(records)

    assert result["total_packs"] == 1
    assert result["avg_verifications_per_pack"] == 5.5
    assert result["avg_verification_cost_seconds"] == 25.7
    assert result["avg_verification_memory_mb"] == 150.3
