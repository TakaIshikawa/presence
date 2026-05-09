"""Tests for pack verification parallelization efficiency analyzer."""

import pytest

from synthesis.pack_verification_parallelization import (
    analyze_pack_verification_parallelization,
)


def test_empty_records_returns_zero_metrics():
    result = analyze_pack_verification_parallelization([])

    assert result["total_packs"] == 0
    assert result["avg_total_verifications"] == 0.0
    assert result["avg_parallelization_ratio"] == 0.0
    assert result["high_parallelization_packs"] == 0
    assert result["low_parallelization_packs"] == 0


def test_none_records_treated_as_empty():
    result = analyze_pack_verification_parallelization(None)

    assert result["total_packs"] == 0


def test_high_parallelization_pack():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "parallel_verifications": 8,
            "sequential_verifications": 2,
            "verification_batches": 2,
            "avg_batch_size": 4.0,
            "total_verification_time_seconds": 100.0,
            "parallel_time_seconds": 80.0,
            "max_concurrent_verifications": 4,
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    assert result["total_packs"] == 1
    assert result["avg_total_verifications"] == 10.0
    assert result["avg_parallelization_ratio"] == 80.0  # 8/10 * 100
    assert result["avg_sequential_ratio"] == 20.0  # 2/10 * 100
    assert result["high_parallelization_packs"] == 1
    assert result["low_parallelization_packs"] == 0


def test_low_parallelization_pack():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "parallel_verifications": 2,
            "sequential_verifications": 8,
            "total_verification_time_seconds": 100.0,
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    assert result["total_packs"] == 1
    assert result["avg_parallelization_ratio"] == 20.0
    assert result["avg_sequential_ratio"] == 80.0
    assert result["high_parallelization_packs"] == 0
    assert result["low_parallelization_packs"] == 1


def test_moderate_parallelization_pack():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "parallel_verifications": 5,
            "sequential_verifications": 5,
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    assert result["avg_parallelization_ratio"] == 50.0
    assert result["high_parallelization_packs"] == 0
    assert result["low_parallelization_packs"] == 0


def test_batch_size_tracking():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 12,
            "verification_batches": 3,
            "avg_batch_size": 4.0,
        },
        {
            "pack_id": "pack-2",
            "total_verifications": 6,
            "verification_batches": 2,
            "avg_batch_size": 3.0,
        },
    ]

    result = analyze_pack_verification_parallelization(records)

    assert result["total_packs"] == 2
    assert result["avg_batch_size"] == 3.5  # (4.0 + 3.0) / 2


def test_verification_time_tracking():
    records = [
        {
            "pack_id": "pack-1",
            "total_verification_time_seconds": 100.0,
            "parallel_time_seconds": 60.0,
        },
        {
            "pack_id": "pack-2",
            "total_verification_time_seconds": 200.0,
            "parallel_time_seconds": 120.0,
        },
    ]

    result = analyze_pack_verification_parallelization(records)

    assert result["avg_verification_time"] == 150.0  # (100 + 200) / 2
    assert result["avg_parallel_time_ratio"] == 60.0  # Both are 60%


def test_time_saved_estimate():
    records = [
        {
            "pack_id": "pack-1",
            "total_verification_time_seconds": 100.0,
            "parallel_time_seconds": 25.0,  # 25s parallel with 4 concurrent
            "max_concurrent_verifications": 4,
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    # Sequential estimate: 25s * 4 = 100s
    # Time saved: 100s - 25s = 75s
    assert result["total_time_saved_estimate"] == 75.0


def test_concurrent_efficiency_calculation():
    records = [
        {
            "pack_id": "pack-1",
            "parallel_verifications": 8,
            "max_concurrent_verifications": 4,
        },
        {
            "pack_id": "pack-2",
            "parallel_verifications": 6,
            "max_concurrent_verifications": 3,
        },
    ]

    result = analyze_pack_verification_parallelization(records)

    # Pack 1: min(8/4, 1.0) = 1.0 = 100%
    # Pack 2: min(6/3, 1.0) = 1.0 = 100%
    # Average: 100%
    assert result["avg_concurrent_efficiency"] == 100.0


def test_concurrent_efficiency_less_than_max():
    records = [
        {
            "pack_id": "pack-1",
            "parallel_verifications": 2,
            "max_concurrent_verifications": 4,
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    # Efficiency: 2/4 = 0.5 = 50%
    assert result["avg_concurrent_efficiency"] == 50.0


def test_multiple_packs_average_metrics():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "parallel_verifications": 8,
            "sequential_verifications": 2,
        },
        {
            "pack_id": "pack-2",
            "total_verifications": 20,
            "parallel_verifications": 10,
            "sequential_verifications": 10,
        },
        {
            "pack_id": "pack-3",
            "total_verifications": 5,
            "parallel_verifications": 1,
            "sequential_verifications": 4,
        },
    ]

    result = analyze_pack_verification_parallelization(records)

    assert result["total_packs"] == 3
    assert result["avg_total_verifications"] == round((10 + 20 + 5) / 3, 2)
    # Parallelization: 80%, 50%, 20% -> avg 50%
    assert result["avg_parallelization_ratio"] == 50.0
    assert result["high_parallelization_packs"] == 1  # pack-1
    assert result["low_parallelization_packs"] == 1  # pack-3


def test_missing_optional_fields():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            # Missing other fields
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    assert result["total_packs"] == 1
    assert result["avg_total_verifications"] == 10.0
    assert result["avg_parallelization_ratio"] == 0.0  # No data
    assert result["avg_batch_size"] == 0.0  # No data


def test_zero_total_verifications():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 0,
            "parallel_verifications": 0,
            "sequential_verifications": 0,
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    assert result["total_packs"] == 1
    assert result["avg_total_verifications"] == 0.0
    assert result["avg_parallelization_ratio"] == 0.0


def test_percentage_calculation_with_zero_denominator():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 0,
            "parallel_verifications": 0,
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    # Should handle zero denominator gracefully
    assert result["avg_parallelization_ratio"] == 0.0


def test_non_mapping_records_skipped():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "parallel_verifications": 8,
        },
        "not a dict",
        None,
        123,
        {
            "pack_id": "pack-2",
            "total_verifications": 5,
            "parallel_verifications": 3,
        },
    ]

    result = analyze_pack_verification_parallelization(records)

    # Only 2 valid records
    assert result["total_packs"] == 2


def test_invalid_records_raises_value_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_verification_parallelization("not a list")


def test_parallelization_ratio_exactly_70_percent():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "parallel_verifications": 7,
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    # 70% is not > 70%, so not high parallelization
    assert result["avg_parallelization_ratio"] == 70.0
    assert result["high_parallelization_packs"] == 0


def test_parallelization_ratio_exactly_30_percent():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10,
            "parallel_verifications": 3,
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    # 30% is not < 30%, so not low parallelization
    assert result["avg_parallelization_ratio"] == 30.0
    assert result["low_parallelization_packs"] == 0


def test_float_values_accepted():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 10.5,
            "parallel_verifications": 7.2,
            "avg_batch_size": 3.7,
            "total_verification_time_seconds": 105.5,
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    assert result["total_packs"] == 1
    assert result["avg_total_verifications"] == 10.5
    assert result["avg_batch_size"] == 3.7
    assert result["avg_verification_time"] == 105.5


def test_boolean_values_rejected():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": True,  # Should be rejected
            "parallel_verifications": False,  # Should be rejected
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    # Booleans should be treated as None
    assert result["avg_total_verifications"] == 0.0
    assert result["avg_parallelization_ratio"] == 0.0


def test_comprehensive_pack_analysis():
    records = [
        {
            "pack_id": "pack-1",
            "total_verifications": 20,
            "parallel_verifications": 16,
            "sequential_verifications": 4,
            "verification_batches": 4,
            "avg_batch_size": 4.0,
            "total_verification_time_seconds": 150.0,
            "parallel_time_seconds": 120.0,
            "max_concurrent_verifications": 8,
            "task_title": "High parallelization task",
        },
        {
            "pack_id": "pack-2",
            "total_verifications": 10,
            "parallel_verifications": 2,
            "sequential_verifications": 8,
            "verification_batches": 2,
            "avg_batch_size": 1.0,
            "total_verification_time_seconds": 200.0,
            "parallel_time_seconds": 20.0,
            "max_concurrent_verifications": 2,
            "task_title": "Low parallelization task",
        },
    ]

    result = analyze_pack_verification_parallelization(records)

    assert result["total_packs"] == 2
    assert result["avg_total_verifications"] == 15.0  # (20 + 10) / 2
    assert result["avg_parallelization_ratio"] == 50.0  # (80 + 20) / 2
    assert result["avg_sequential_ratio"] == 50.0  # (20 + 80) / 2
    assert result["high_parallelization_packs"] == 1
    assert result["low_parallelization_packs"] == 1
    assert result["avg_batch_size"] == 2.5  # (4.0 + 1.0) / 2
    assert result["avg_verification_time"] == 175.0  # (150 + 200) / 2
    assert result["avg_parallel_time_ratio"] == 45.0  # (120/150*100 + 20/200*100) / 2 = (80 + 10) / 2
    # Time saved: pack-1 = (120*8-120) = 840, pack-2 = (20*2-20) = 20
    assert result["total_time_saved_estimate"] == 860.0


def test_no_time_saved_without_max_concurrent():
    records = [
        {
            "pack_id": "pack-1",
            "total_verification_time_seconds": 100.0,
            "parallel_time_seconds": 50.0,
            # Missing max_concurrent_verifications
        }
    ]

    result = analyze_pack_verification_parallelization(records)

    # Should not calculate time saved without max_concurrent
    assert result["total_time_saved_estimate"] == 0.0
