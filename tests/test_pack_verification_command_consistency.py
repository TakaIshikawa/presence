"""Tests for pack verification command consistency analysis."""

import pytest

from synthesis.pack_verification_command_consistency import analyze_pack_verification_command_consistency


def test_matching_commands_are_consistent():
    report = analyze_pack_verification_command_consistency(
        [
            {
                "pack_key": "pack-a",
                "testCommand": "uv run pytest tests/test_a.py",
                "verificationCommand": "uv run pytest tests/test_a.py",
            }
        ]
    )

    assert report["pack_count"] == 1
    assert report["inconsistent_pack_count"] == 0


def test_missing_task_commands_are_reported():
    report = analyze_pack_verification_command_consistency(
        [{"pack_key": "pack-a", "verificationCommand": "uv run pytest tests/test_a.py"}]
    )

    assert report["missing_task_command_count"] == 1
    assert report["examples"][0]["reason"] == "missing_task_command"


def test_pack_command_covering_multiple_task_tests_is_consistent():
    report = analyze_pack_verification_command_consistency(
        [
            {
                "executionPack": {"key": "pack-a"},
                "testCommand": "uv run pytest tests/test_a.py",
                "verificationCommand": "uv run pytest tests/test_a.py tests/test_b.py",
            },
            {"executionPack": {"key": "pack-a"}, "testCommand": "uv run pytest tests/test_b.py"},
        ]
    )

    assert report["inconsistent_pack_count"] == 0


def test_inconsistent_pack_command_is_reported():
    report = analyze_pack_verification_command_consistency(
        [
            {
                "execution_pack": {"key": "pack-a"},
                "testCommand": "uv run pytest tests/test_a.py",
                "verificationCommand": "uv run pytest tests/test_other.py",
            }
        ]
    )

    assert report["inconsistent_pack_count"] == 1
    assert report["packs"][0]["missing_test_paths"] == ["tests/test_a.py"]


def test_unpackaged_records_use_fallback_group():
    report = analyze_pack_verification_command_consistency(
        [{"testCommand": "uv run pytest tests/test_a.py", "verificationCommand": "uv run pytest tests/test_a.py"}]
    )

    assert report["packs"][0]["pack_key"] == "unpackaged"


def test_invalid_input_raises():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_verification_command_consistency({"pack_key": "pack-a"})
