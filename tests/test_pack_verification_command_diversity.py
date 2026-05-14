"""Tests for pack verification command diversity analyzer."""

import pytest

from synthesis.pack_verification_command_diversity import (
    _calculate_timing_bucket,
    _identify_verification,
    _percentage,
    analyze_pack_verification_command_diversity,
)


def test_empty_input_returns_zeroed_metrics():
    result = analyze_pack_verification_command_diversity([])

    assert result["total_packs"] == 0
    assert result["total_verification_commands"] == 0
    assert result["unique_verification_tools"] == 0
    assert result["verification_density"] == 0.0


def test_none_input_treated_as_empty_list():
    result = analyze_pack_verification_command_diversity(None)

    assert result["total_packs"] == 0


def test_invalid_input_type_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_verification_command_diversity("not a list")


def test_verification_commands_are_counted_by_category_and_timing():
    result = analyze_pack_verification_command_diversity([
        {
            "pack_id": "pack1",
            "sessions": [
                {
                    "total_turns": 9,
                    "tool_calls": [
                        {"tool_name": "Bash", "command": "pytest tests/", "turn_index": 1},
                        {"tool_name": "Bash", "command": "mypy src/", "turn_index": 4},
                        {"tool_name": "Bash", "command": "ruff check .", "turn_index": 7},
                    ],
                }
            ],
        }
    ])

    assert result["total_packs"] == 1
    assert result["total_verification_commands"] == 3
    assert result["unique_verification_tools"] == 3
    assert result["verification_by_category"] == {"test": 1, "type": 1, "lint": 1}
    assert result["early_verification_rate"] == 33.33
    assert result["mid_verification_rate"] == 33.33
    assert result["late_verification_rate"] == 33.33
    assert result["verification_coverage_score"] > 0


def test_missing_type_and_build_checks_are_flagged_for_edited_files():
    result = analyze_pack_verification_command_diversity([
        {
            "pack_id": "pack1",
            "sessions": [
                {
                    "edited_files": ["src/app.py"],
                    "tool_calls": [{"tool_name": "Edit", "file_path": "src/app.py"}],
                }
            ],
        }
    ])

    assert result["missing_type_check_count"] == 1
    assert result["missing_build_check_count"] == 1


def test_targeted_and_broad_test_scope_are_tracked():
    result = analyze_pack_verification_command_diversity([
        {
            "pack_id": "pack1",
            "sessions": [
                {
                    "tool_calls": [
                        {"tool_name": "Bash", "command": "pytest tests/test_one.py"},
                        {"tool_name": "Bash", "command": "pytest tests/"},
                    ],
                }
            ],
        }
    ])

    assert result["targeted_test_count"] == 1
    assert result["broad_test_count"] == 1
    assert result["test_scope_breadth"] == 0.5


def test_helper_functions_classify_commands_and_percentages():
    assert _identify_verification("python -m pytest tests/") == {
        "tool": "pytest",
        "category": "test",
    }
    assert _identify_verification("echo ok") is None
    assert _calculate_timing_bucket(5, 10) == "mid"
    assert _percentage(1, 4) == 25.0
