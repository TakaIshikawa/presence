"""Tests for pack task scope granularity analyzer."""

import pytest

from synthesis.pack_task_scope_analyzer import analyze_pack_task_scope_analyzer


def test_empty_records_returns_zero_metrics():
    result = analyze_pack_task_scope_analyzer([])

    assert result["total_packs"] == 0
    assert result["avg_tasks_per_pack"] == 0.0
    assert result["small_scope_ratio"] == 0.0
    assert result["medium_scope_ratio"] == 0.0
    assert result["large_scope_ratio"] == 0.0


def test_none_records_treated_as_empty():
    result = analyze_pack_task_scope_analyzer(None)

    assert result["total_packs"] == 0


def test_single_pack_all_small_scope():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 5,
            "small_scope_tasks": 5,
            "medium_scope_tasks": 0,
            "large_scope_tasks": 0,
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["total_packs"] == 1
    assert result["avg_tasks_per_pack"] == 5.0
    assert result["small_scope_ratio"] == 100.0
    assert result["medium_scope_ratio"] == 0.0
    assert result["large_scope_ratio"] == 0.0


def test_multiple_packs_mixed_scopes():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 10,
            "small_scope_tasks": 6,
            "medium_scope_tasks": 3,
            "large_scope_tasks": 1,
        },
        {
            "pack_id": "pack-2",
            "total_tasks": 5,
            "small_scope_tasks": 1,
            "medium_scope_tasks": 2,
            "large_scope_tasks": 2,
        },
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["total_packs"] == 2
    assert result["avg_tasks_per_pack"] == 7.5  # (10 + 5) / 2
    # Total: 7 small, 5 medium, 3 large out of 15 tasks
    assert result["small_scope_ratio"] == round((7 / 15) * 100, 2)
    assert result["medium_scope_ratio"] == round((5 / 15) * 100, 2)
    assert result["large_scope_ratio"] == round((3 / 15) * 100, 2)


def test_scope_variance_high_homogeneity():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 10,
            "scope_variance": 0.2,  # Low variance = high homogeneity
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["avg_scope_variance"] == 0.2
    assert result["high_homogeneity_packs"] == 1
    assert result["low_homogeneity_packs"] == 0


def test_scope_variance_low_homogeneity():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 10,
            "scope_variance": 0.8,  # High variance = low homogeneity
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["avg_scope_variance"] == 0.8
    assert result["high_homogeneity_packs"] == 0
    assert result["low_homogeneity_packs"] == 1


def test_estimation_accuracy_perfect():
    records = [
        {
            "pack_id": "pack-1",
            "expected_files_count": 5,
            "actual_files_changed": 5,
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["avg_estimation_accuracy"] == 100.0
    assert result["high_accuracy_packs"] == 1
    assert result["low_accuracy_packs"] == 0


def test_estimation_accuracy_high():
    records = [
        {
            "pack_id": "pack-1",
            "expected_files_count": 10,
            "actual_files_changed": 9,  # 90% accuracy
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["avg_estimation_accuracy"] == 90.0
    assert result["high_accuracy_packs"] == 1


def test_estimation_accuracy_low():
    records = [
        {
            "pack_id": "pack-1",
            "expected_files_count": 10,
            "actual_files_changed": 4,  # 40% accuracy
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    # Accuracy = (1 - |10-4|/10) * 100 = 40%
    assert result["avg_estimation_accuracy"] == 40.0
    assert result["low_accuracy_packs"] == 1


def test_estimation_accuracy_over_estimate():
    records = [
        {
            "pack_id": "pack-1",
            "expected_files_count": 5,
            "actual_files_changed": 10,  # Over-estimated, actual was higher
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    # Accuracy = (1 - |5-10|/10) * 100 = 50%
    assert result["avg_estimation_accuracy"] == 50.0


def test_balanced_distribution():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 10,
            "small_scope_tasks": 3,
            "medium_scope_tasks": 4,
            "large_scope_tasks": 3,
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    # No category exceeds 70%, so it's balanced
    assert result["balanced_distribution_packs"] == 1


def test_unbalanced_distribution():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 10,
            "small_scope_tasks": 8,  # 80% - dominates
            "medium_scope_tasks": 1,
            "large_scope_tasks": 1,
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    # Small scope exceeds 70%, so it's unbalanced
    assert result["balanced_distribution_packs"] == 0


def test_comprehensive_pack_analysis():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 12,
            "small_scope_tasks": 4,
            "medium_scope_tasks": 5,
            "large_scope_tasks": 3,
            "expected_files_count": 10,
            "actual_files_changed": 9,
            "scope_variance": 0.25,
        },
        {
            "pack_id": "pack-2",
            "total_tasks": 8,
            "small_scope_tasks": 2,
            "medium_scope_tasks": 3,
            "large_scope_tasks": 3,
            "expected_files_count": 8,
            "actual_files_changed": 8,
            "scope_variance": 0.15,
        },
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["total_packs"] == 2
    assert result["avg_tasks_per_pack"] == 10.0  # (12 + 8) / 2
    assert result["small_scope_ratio"] == 30.0  # (4+2)/(12+8) * 100
    assert result["medium_scope_ratio"] == 40.0  # (5+3)/(12+8) * 100
    assert result["large_scope_ratio"] == 30.0  # (3+3)/(12+8) * 100
    assert result["avg_scope_variance"] == 0.2  # (0.25 + 0.15) / 2
    assert result["high_homogeneity_packs"] == 2
    assert result["avg_estimation_accuracy"] == 95.0  # (90 + 100) / 2
    assert result["high_accuracy_packs"] == 2
    assert result["balanced_distribution_packs"] == 2


def test_missing_optional_fields():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 10,
            # Missing scope breakdown and other fields
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["total_packs"] == 1
    assert result["avg_tasks_per_pack"] == 10.0
    assert result["small_scope_ratio"] == 0.0
    assert result["avg_scope_variance"] == 0.0
    assert result["avg_estimation_accuracy"] == 0.0


def test_non_mapping_records_skipped():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 5,
            "small_scope_tasks": 3,
            "medium_scope_tasks": 2,
            "large_scope_tasks": 0,
        },
        "not a dict",
        None,
        123,
        {
            "pack_id": "pack-2",
            "total_tasks": 3,
            "small_scope_tasks": 1,
            "medium_scope_tasks": 1,
            "large_scope_tasks": 1,
        },
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["total_packs"] == 2


def test_invalid_records_raises_value_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_task_scope_analyzer("not a list")


def test_zero_total_tasks():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 0,
            "small_scope_tasks": 0,
            "medium_scope_tasks": 0,
            "large_scope_tasks": 0,
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["total_packs"] == 1
    assert result["avg_tasks_per_pack"] == 0.0
    assert result["balanced_distribution_packs"] == 0  # Zero tasks can't be balanced


def test_estimation_accuracy_both_zero():
    records = [
        {
            "pack_id": "pack-1",
            "expected_files_count": 0,
            "actual_files_changed": 0,
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    # Both zero = 100% accuracy
    assert result["avg_estimation_accuracy"] == 100.0


def test_float_values_accepted():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 10.5,  # Will be converted to int
            "scope_variance": 0.35,  # Float variance is kept as float
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    assert result["total_packs"] == 1
    assert result["avg_tasks_per_pack"] == 10.0  # Float task count converted to int
    assert result["avg_scope_variance"] == 0.35


def test_boolean_values_rejected():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": True,  # Should be rejected
            "small_scope_tasks": False,  # Should be rejected
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    # Booleans should be treated as None
    assert result["avg_tasks_per_pack"] == 0.0


def test_scope_variance_exactly_thresholds():
    records = [
        {
            "pack_id": "pack-1",
            "scope_variance": 0.3,  # Exactly at threshold
        },
        {
            "pack_id": "pack-2",
            "scope_variance": 0.7,  # Exactly at threshold
        },
    ]

    result = analyze_pack_task_scope_analyzer(records)

    # 0.3 is not < 0.3, so not high homogeneity
    # 0.7 is not > 0.7, so not low homogeneity
    assert result["high_homogeneity_packs"] == 0
    assert result["low_homogeneity_packs"] == 0


def test_estimation_accuracy_exactly_thresholds():
    records = [
        {
            "pack_id": "pack-1",
            "expected_files_count": 10,
            "actual_files_changed": 8,  # 80% accuracy
        },
        {
            "pack_id": "pack-2",
            "expected_files_count": 10,
            "actual_files_changed": 5,  # 50% accuracy
        },
    ]

    result = analyze_pack_task_scope_analyzer(records)

    # 80% is not > 80%, so not high accuracy
    # 50% is not < 50%, so not low accuracy
    assert result["high_accuracy_packs"] == 0
    assert result["low_accuracy_packs"] == 0


def test_balanced_distribution_exactly_70_percent():
    records = [
        {
            "pack_id": "pack-1",
            "total_tasks": 10,
            "small_scope_tasks": 7,  # Exactly 70%
            "medium_scope_tasks": 2,
            "large_scope_tasks": 1,
        }
    ]

    result = analyze_pack_task_scope_analyzer(records)

    # 70% is not > 70%, so it's still considered balanced
    assert result["balanced_distribution_packs"] == 1
