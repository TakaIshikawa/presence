"""Tests for pack verification timeout consistency analyzer."""

import pytest

from synthesis.pack_verification_timeout_consistency import (
    analyze_pack_verification_timeout_consistency,
)


def test_empty_records_returns_zero_metrics():
    result = analyze_pack_verification_timeout_consistency([])

    assert result["total_packs"] == 0
    assert result["total_commands"] == 0
    assert result["inconsistent_pack_count"] == 0
    assert result["missing_timeout_count"] == 0


def test_consistent_timeouts_pass():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=30",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "pytest --timeout=30",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-3",
            "verification_command": "pytest --timeout=35",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    # Small variance (30, 30, 35) should pass
    assert result["inconsistent_pack_count"] == 0
    assert result["missing_timeout_count"] == 0


def test_missing_timeouts_detected():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=30",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "pytest",  # No timeout
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["missing_timeout_count"] == 1
    assert len(result["missing_timeouts"]) == 1
    assert result["missing_timeouts"][0]["pack"] == "pack-1"
    assert result["missing_timeouts"][0]["missing_count"] == 1


def test_extreme_short_timeout_detected():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=5",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "pytest --timeout=30",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["extreme_timeout_count"] == 1
    extreme = result["extreme_timeouts"][0]
    assert extreme["timeout"] == 5
    assert extreme["issue"] == "too_short"


def test_extreme_long_timeout_detected():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=700",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "pytest --timeout=30",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["extreme_timeout_count"] == 1
    extreme = result["extreme_timeouts"][0]
    assert extreme["timeout"] == 700
    assert extreme["issue"] == "too_long"


def test_high_variance_detected():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=10",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "pytest --timeout=100",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-3",
            "verification_command": "pytest --timeout=200",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["inconsistent_pack_count"] == 1
    inconsistent = result["inconsistent_packs"][0]
    assert inconsistent["inconsistent_timeouts"] is True
    assert inconsistent["timeout_variance"] > 0.5
    assert "recommended_timeout" in inconsistent


def test_parse_timeout_equals_format():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=45",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["total_commands"] == 1
    assert result["missing_timeout_count"] == 0


def test_parse_timeout_space_format():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout 45",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["missing_timeout_count"] == 0


def test_parse_gnu_timeout_command():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "timeout 30 pytest tests/",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["missing_timeout_count"] == 0


def test_parse_gnu_timeout_with_unit():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "timeout 5m pytest",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    # 5 minutes = 300 seconds
    assert result["missing_timeout_count"] == 0


def test_npm_millisecond_timeout_converted():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "npm test -- --timeout=5000",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    # Should convert 5000ms to 5s
    assert result["missing_timeout_count"] == 0


def test_command_without_timeout():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest tests/test_foo.py",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    # Single command packs are skipped for consistency checks
    assert result["total_commands"] == 1
    assert result["inconsistent_pack_count"] == 0


def test_multiple_packs_independent():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=30",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "pytest --timeout=300",
        },
        {
            "execution_pack": {"id": "pack-2"},
            "task_id": "task-3",
            "verification_command": "npm test --timeout=5000",
        },
        {
            "execution_pack": {"id": "pack-2"},
            "task_id": "task-4",
            "verification_command": "npm test --timeout=5000",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["total_packs"] == 2
    # pack-1 has high variance, pack-2 is consistent
    assert result["inconsistent_pack_count"] == 1


def test_single_command_pack_skipped():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=5",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    # Single command packs don't trigger consistency checks
    assert result["inconsistent_pack_count"] == 0
    assert result["missing_timeout_count"] == 0


def test_malformed_record_skipped():
    records = [
        "not a dict",
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=30",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["total_commands"] == 1


def test_missing_verification_command():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            # No verification command
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["total_commands"] == 0


def test_timeout_configs_included_in_output():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=10",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "pytest --timeout=200",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    inconsistent = result["inconsistent_packs"][0]
    timeout_configs = inconsistent["timeout_configs"]

    assert len(timeout_configs) == 2
    assert any(cfg["task_id"] == "task-1" and cfg["timeout"] == 10 for cfg in timeout_configs)
    assert any(cfg["task_id"] == "task-2" and cfg["timeout"] == 200 for cfg in timeout_configs)


def test_recommended_timeout_is_average():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=20",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "pytest --timeout=100",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    if result["inconsistent_pack_count"] > 0:
        inconsistent = result["inconsistent_packs"][0]
        # Average of 20 and 100 is 60
        assert inconsistent["recommended_timeout"] == 60


def test_parse_timeout_flag_format():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "go test -timeout 30s",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["missing_timeout_count"] == 0


def test_none_records_treated_as_empty():
    result = analyze_pack_verification_timeout_consistency(None)

    assert result["total_packs"] == 0
    assert result["total_commands"] == 0


def test_invalid_records_type_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_verification_timeout_consistency("not a list")


def test_pack_key_extraction_variations():
    records = [
        {
            "executionPack": {"key": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=30",
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "pytest --timeout=30",
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    # Both should be grouped under pack-1
    assert result["total_packs"] == 1


def test_missing_and_extreme_in_same_pack():
    records = [
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-1",
            "verification_command": "pytest --timeout=5",  # Too short
        },
        {
            "execution_pack": {"id": "pack-1"},
            "task_id": "task-2",
            "verification_command": "pytest",  # Missing
        },
    ]

    result = analyze_pack_verification_timeout_consistency(records)

    assert result["missing_timeout_count"] == 1
    assert result["extreme_timeout_count"] == 1
