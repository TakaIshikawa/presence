"""Tests for session command sequence analyzer."""

import pytest

from synthesis.session_command_sequence import (
    analyze_session_command_sequence,
)


def test_empty_input_returns_zeroed_metrics():
    report = analyze_session_command_sequence([])

    assert report["total_commands"] == 0
    assert report["test_before_build_count"] == 0
    assert report["verify_before_install_count"] == 0
    assert report["redundant_install_count"] == 0
    assert report["issue_percentage"] == 0.0
    assert report["examples"] == []


def test_correct_sequence_build_then_test_has_no_issues():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "npm run build"},
        {"turn_index": 1, "command": "npm test"},
    ])

    assert report["test_before_build_count"] == 0
    assert report["examples"] == []


def test_test_before_build_flags_issue():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "pytest tests/"},
        {"turn_index": 1, "command": "npm run build"},
    ])

    assert report["test_before_build_count"] == 1
    assert report["issue_percentage"] == 50.0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "test_before_build"


def test_correct_sequence_install_then_verify_has_no_issues():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "npm install"},
        {"turn_index": 1, "command": "npm test"},
    ])

    assert report["verify_before_install_count"] == 0


def test_verify_before_install_flags_issue():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "pytest"},
        {"turn_index": 1, "command": "npm install"},
    ])

    assert report["verify_before_install_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "verify_before_install"


def test_single_install_command_has_no_redundancy():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "npm install"},
    ])

    assert report["redundant_install_count"] == 0


def test_two_installs_close_together_has_no_redundancy():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "npm install"},
        {"turn_index": 1, "command": "npm install"},
    ])

    assert report["redundant_install_count"] == 0


def test_three_installs_close_together_flags_redundancy():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "npm install"},
        {"turn_index": 1, "command": "npm install"},
        {"turn_index": 2, "command": "npm install"},
    ])

    assert report["redundant_install_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "redundant_install"


def test_installs_separated_by_gap_do_not_flag_redundancy():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "npm install"},
        {"turn_index": 5, "command": "npm install"},
    ])

    assert report["redundant_install_count"] == 0


def test_multiple_test_before_build_instances():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "pytest"},
        {"turn_index": 1, "command": "jest"},
        {"turn_index": 2, "command": "npm run build"},
    ])

    assert report["test_before_build_count"] == 2


def test_test_after_build_does_not_flag_issue():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "npm run build"},
        {"turn_index": 1, "command": "pytest"},
        {"turn_index": 2, "command": "jest"},
    ])

    assert report["test_before_build_count"] == 0


def test_mixed_issues_in_single_session():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "pytest"},  # verify before install
        {"turn_index": 1, "command": "npm install"},
        {"turn_index": 2, "command": "npm install"},
        {"turn_index": 3, "command": "npm install"},  # redundant install
        {"turn_index": 4, "command": "jest"},  # test before build
        {"turn_index": 5, "command": "npm run build"},
    ])

    assert report["verify_before_install_count"] == 1
    assert report["redundant_install_count"] == 1
    assert report["test_before_build_count"] == 2  # Both pytest and jest
    assert report["issue_percentage"] == 66.67
    assert len(report["examples"]) >= 3


def test_examples_capped_at_five():
    records = []
    for i in range(10):
        records.append({"turn_index": i, "command": "pytest"})
    records.append({"turn_index": 10, "command": "npm run build"})

    report = analyze_session_command_sequence(records)

    assert report["test_before_build_count"] == 10
    assert len(report["examples"]) == 5


def test_command_normalization_case_insensitive():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "PYTEST tests/"},
        {"turn_index": 1, "command": "npm run BUILD"},
    ])

    assert report["test_before_build_count"] == 1


def test_command_normalization_whitespace():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "pytest   tests/"},
        {"turn_index": 1, "command": "npm  run  build"},
    ])

    assert report["test_before_build_count"] == 1


def test_various_test_commands_recognized():
    test_commands = [
        "pytest",
        "jest",
        "vitest",
        "npm test",
        "yarn test",
        "pnpm test",
        "go test",
        "cargo test",
    ]

    for cmd in test_commands:
        report = analyze_session_command_sequence([
            {"turn_index": 0, "command": cmd},
            {"turn_index": 1, "command": "npm run build"},
        ])
        assert report["test_before_build_count"] == 1, f"Failed for command: {cmd}"


def test_various_build_commands_recognized():
    build_commands = [
        "npm run build",
        "tsc",
        "webpack",
        "vite build",
        "yarn build",
        "pnpm build",
        "cargo build",
        "make",
    ]

    for cmd in build_commands:
        report = analyze_session_command_sequence([
            {"turn_index": 0, "command": "pytest"},
            {"turn_index": 1, "command": cmd},
        ])
        # After this build, subsequent tests should not flag
        report2 = analyze_session_command_sequence([
            {"turn_index": 0, "command": cmd},
            {"turn_index": 1, "command": "pytest"},
        ])
        assert report2["test_before_build_count"] == 0, f"Failed for command: {cmd}"


def test_various_install_commands_recognized():
    install_commands = [
        "npm install",
        "npm i",
        "yarn install",
        "yarn add lodash",
        "pnpm install",
        "pnpm add react",
        "pip install requests",
        "poetry install",
        "uv sync",
    ]

    for cmd in install_commands:
        report = analyze_session_command_sequence([
            {"turn_index": 0, "command": "pytest"},
            {"turn_index": 1, "command": cmd},
        ])
        assert report["verify_before_install_count"] == 1, f"Failed for command: {cmd}"


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_command_sequence({"turn_index": 0, "command": "pytest"})


def test_missing_turn_index_raises_error():
    with pytest.raises(ValueError, match="each record must have a turn_index"):
        analyze_session_command_sequence([
            {"command": "pytest"}
        ])


def test_missing_command_raises_error():
    with pytest.raises(ValueError, match="each record must have a command"):
        analyze_session_command_sequence([
            {"turn_index": 0}
        ])


def test_empty_command_raises_error():
    with pytest.raises(ValueError, match="command must not be empty"):
        analyze_session_command_sequence([
            {"turn_index": 0, "command": "  "}
        ])


def test_boolean_turn_index_raises_error():
    with pytest.raises(ValueError, match="turn_index must be an integer"):
        analyze_session_command_sequence([
            {"turn_index": True, "command": "pytest"}
        ])


def test_negative_turn_index_raises_error():
    with pytest.raises(ValueError, match="turn_index must be non-negative"):
        analyze_session_command_sequence([
            {"turn_index": -1, "command": "pytest"}
        ])


def test_unordered_records_raise_error():
    with pytest.raises(ValueError, match="records must be ordered by turn_index"):
        analyze_session_command_sequence([
            {"turn_index": 1, "command": "pytest"},
            {"turn_index": 0, "command": "npm test"},
        ])


def test_non_dict_record_raises_error():
    with pytest.raises(ValueError, match="each record must be a dict"):
        analyze_session_command_sequence([
            "not a dict"
        ])


def test_non_string_command_raises_error():
    with pytest.raises(ValueError, match="command must be a string"):
        analyze_session_command_sequence([
            {"turn_index": 0, "command": 123}
        ])


def test_non_verification_commands_do_not_affect_verify_before_install():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "git status"},
        {"turn_index": 1, "command": "npm install"},
        {"turn_index": 2, "command": "pytest"},
    ])

    assert report["verify_before_install_count"] == 0


def test_non_test_commands_do_not_affect_test_before_build():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "git status"},
        {"turn_index": 1, "command": "npm run build"},
    ])

    assert report["test_before_build_count"] == 0


def test_redundant_install_reset_after_gap():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "npm install"},
        {"turn_index": 1, "command": "npm install"},
        {"turn_index": 2, "command": "npm install"},  # Flags redundancy
        {"turn_index": 10, "command": "npm install"},  # Gap resets
        {"turn_index": 11, "command": "npm install"},
        {"turn_index": 12, "command": "npm install"},  # Flags redundancy again
    ])

    assert report["redundant_install_count"] == 2


def test_issue_percentage_calculation():
    report = analyze_session_command_sequence([
        {"turn_index": 0, "command": "pytest"},  # Issue
        {"turn_index": 1, "command": "npm install"},
        {"turn_index": 2, "command": "npm run build"},
        {"turn_index": 3, "command": "npm test"},  # No issue
    ])

    assert report["total_commands"] == 4
    assert report["verify_before_install_count"] == 1
    assert report["test_before_build_count"] == 1
    assert report["issue_percentage"] == 50.0
