"""Tests for pack verification test selection analyzer."""

import pytest

from synthesis.pack_verification_test_selection import (
    analyze_pack_verification_test_selection,
)


def test_empty_input_returns_zeroed_metrics():
    report = analyze_pack_verification_test_selection([])

    assert report["pack_count"] == 0
    assert report["wrong_module_count"] == 0
    assert report["missing_companion_count"] == 0
    assert report["overly_broad_count"] == 0
    assert report["issue_percentage"] == 0.0
    assert report["examples"] == []


def test_aligned_verification_has_no_issues():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_foo.py"},
        }
    ])

    assert report["pack_count"] == 1
    assert report["wrong_module_count"] == 0
    assert report["missing_companion_count"] == 0
    assert report["overly_broad_count"] == 0
    assert report["examples"] == []


def test_verification_testing_wrong_module_flags_issue():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_bar.py"},
        }
    ])

    assert report["wrong_module_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "wrong_module"


def test_missing_companion_test_coverage_flags_issue():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py", "src/bar.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_foo.py"},
        }
    ])

    assert report["missing_companion_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "missing_companion"
    assert "bar.py" in report["examples"][0]["details"]


def test_overly_broad_pattern_for_small_change_flags_issue():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py"],
            "executionPack": {"verificationCommand": "pytest tests/"},
        }
    ])

    assert report["overly_broad_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "overly_broad"


def test_broad_pattern_for_many_files_does_not_flag():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/a.py", "src/b.py", "src/c.py", "src/d.py"],
            "executionPack": {"verificationCommand": "pytest tests/"},
        }
    ])

    assert report["overly_broad_count"] == 0


def test_test_files_in_expected_files_are_ignored():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py", "tests/test_foo.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_foo.py"},
        }
    ])

    assert report["wrong_module_count"] == 0
    assert report["missing_companion_count"] == 0


def test_multiple_packs_analyzed_independently():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_foo.py"},
        },
        {
            "pack_key": "pack-b",
            "expectedFiles": ["src/bar.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_wrong.py"},
        }
    ])

    assert report["pack_count"] == 2
    assert report["wrong_module_count"] == 1


def test_pack_with_multiple_tasks_aggregates_expected_files():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_foo.py tests/test_bar.py"},
        },
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/bar.py"],
        }
    ])

    assert report["pack_count"] == 1
    assert report["wrong_module_count"] == 0
    assert report["missing_companion_count"] == 0


def test_examples_capped_at_five():
    records = []
    for i in range(10):
        records.append({
            "pack_key": f"pack-{i}",
            "expectedFiles": ["src/foo.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_wrong.py"},
        })

    report = analyze_pack_verification_test_selection(records)

    assert report["wrong_module_count"] == 10
    assert len(report["examples"]) == 5


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_verification_test_selection({"pack_key": "pack-a"})


def test_none_input_returns_zeroed_metrics():
    report = analyze_pack_verification_test_selection(None)

    assert report["pack_count"] == 0
    assert report["examples"] == []


def test_non_dict_records_are_skipped():
    report = analyze_pack_verification_test_selection([
        "not a dict",
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_foo.py"},
        }
    ])

    assert report["pack_count"] == 1


def test_unpackaged_records_use_fallback_key():
    report = analyze_pack_verification_test_selection([
        {
            "expectedFiles": ["src/foo.py"],
            "testCommand": "pytest tests/test_foo.py",
        }
    ])

    assert report["pack_count"] == 1


def test_python_file_module_extraction():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/synthesis/analyzer.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_synthesis_analyzer.py"},
        }
    ])

    # Should not flag wrong module since test path contains synthesis/analyzer
    assert report["wrong_module_count"] == 0


def test_typescript_test_file_patterns():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/components/Button.ts"],
            "executionPack": {"verificationCommand": "jest tests/components/Button.test.ts"},
        }
    ])

    assert report["wrong_module_count"] == 0


def test_javascript_spec_file_patterns():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/utils/helper.js"],
            "executionPack": {"verificationCommand": "vitest tests/utils/helper.spec.js"},
        }
    ])

    assert report["wrong_module_count"] == 0


def test_npm_test_considered_broad():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.js"],
            "executionPack": {"verificationCommand": "npm test"},
        }
    ])

    assert report["overly_broad_count"] == 1


def test_cargo_test_considered_broad():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/lib.rs"],
            "executionPack": {"verificationCommand": "cargo test"},
        }
    ])

    assert report["overly_broad_count"] == 1


def test_go_test_all_considered_broad():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["pkg/handler.go"],
            "executionPack": {"verificationCommand": "go test ./..."},
        }
    ])

    assert report["overly_broad_count"] == 1


def test_issue_percentage_calculation():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_wrong.py"},
        },
        {
            "pack_key": "pack-b",
            "expectedFiles": ["src/bar.py"],
            "executionPack": {"verificationCommand": "pytest tests/test_bar.py"},
        }
    ])

    assert report["pack_count"] == 2
    assert report["wrong_module_count"] == 1
    assert report["issue_percentage"] == 50.0


def test_verification_command_from_pack_nested_structure():
    report = analyze_pack_verification_test_selection([
        {
            "executionPack": {
                "key": "pack-a",
                "verificationCommand": "pytest tests/test_foo.py"
            },
            "expectedFiles": ["src/foo.py"],
        }
    ])

    assert report["pack_count"] == 1
    assert report["missing_companion_count"] == 0


def test_verification_command_from_task_level():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "testCommand": "pytest tests/test_foo.py",
            "expectedFiles": ["src/foo.py"],
        }
    ])

    # Task-level verification should be used if pack-level missing
    assert report["pack_count"] == 1


def test_empty_expected_files_does_not_crash():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": [],
            "executionPack": {"verificationCommand": "pytest tests/"},
        }
    ])

    assert report["pack_count"] == 1
    assert report["wrong_module_count"] == 0


def test_missing_expected_files_field_does_not_crash():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "executionPack": {"verificationCommand": "pytest tests/"},
        }
    ])

    assert report["pack_count"] == 1


def test_missing_verification_command_does_not_crash():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py"],
        }
    ])

    assert report["pack_count"] == 1
    # No verification command means no issues can be detected
    assert report["wrong_module_count"] == 0


def test_whitespace_handling_in_paths():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["  src/foo.py  "],
            "executionPack": {"verificationCommand": "pytest  tests/test_foo.py  "},
        }
    ])

    assert report["wrong_module_count"] == 0


def test_mixed_issues_in_single_pack():
    report = analyze_pack_verification_test_selection([
        {
            "pack_key": "pack-a",
            "expectedFiles": ["src/foo.py"],
            "executionPack": {"verificationCommand": "pytest tests/"},
        }
    ])

    # Should flag overly_broad, but not missing_companion since we're using broad pattern
    assert report["overly_broad_count"] == 1
