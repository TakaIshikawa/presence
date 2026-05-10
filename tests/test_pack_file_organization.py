"""Tests for pack file organization and companion test file discipline analyzer."""

import pytest

from synthesis.pack_file_organization import analyze_pack_file_organization


# --- Input validation ---


def test_none_input_returns_empty_result():
    result = analyze_pack_file_organization(None)

    assert result["total_sessions"] == 0
    assert result["test_companion_discipline"] == 1.0
    assert result["file_organization_correctness"] == 1.0
    assert result["naming_consistency"] == 1.0


def test_empty_list_returns_empty_result():
    result = analyze_pack_file_organization([])

    assert result["total_sessions"] == 0
    assert result["source_files_with_tests_count"] == 0


def test_non_list_input_raises():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_pack_file_organization({"expected_files": []})


def test_non_mapping_records_are_skipped():
    result = analyze_pack_file_organization(["not_a_dict", 42, None])

    assert result["total_sessions"] == 0


# --- Test companion discipline ---


def test_source_with_companion_test_scores_full():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "src/synthesis/pack_file_organization.py",
                "tests/test_pack_file_organization.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["source_files_with_tests_count"] == 1
    assert result["source_files_without_tests_count"] == 0
    assert result["test_companion_discipline"] == 1.0


def test_source_without_companion_test_scores_zero():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "src/synthesis/lonely_module.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["source_files_with_tests_count"] == 0
    assert result["source_files_without_tests_count"] == 1
    assert result["test_companion_discipline"] == 0.0


def test_mixed_pairing_partial_score():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "src/synthesis/alpha.py",
                "tests/test_alpha.py",
                "src/synthesis/beta.py",
                # beta has no test
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["source_files_with_tests_count"] == 1
    assert result["source_files_without_tests_count"] == 1
    assert result["test_companion_discipline"] == 0.5


def test_actual_changed_files_included_in_analysis():
    records = [
        {
            "session_id": "s1",
            "expected_files": ["src/synthesis/foo.py"],
            "actual_changed_files": ["tests/test_foo.py"],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["source_files_with_tests_count"] == 1
    assert result["test_companion_discipline"] == 1.0


def test_duplicate_files_across_expected_and_actual_deduplicated():
    records = [
        {
            "session_id": "s1",
            "expected_files": ["src/synthesis/dup.py", "tests/test_dup.py"],
            "actual_changed_files": ["src/synthesis/dup.py", "tests/test_dup.py"],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["source_files_with_tests_count"] == 1
    assert result["source_files_without_tests_count"] == 0


# --- Orphaned test files ---


def test_orphaned_test_detected():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "tests/test_nonexistent_module.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["orphaned_test_files_count"] == 1
    assert "tests/test_nonexistent_module.py" in result["orphaned_test_files"]


def test_no_orphans_when_source_present():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "src/synthesis/widget.py",
                "tests/test_widget.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["orphaned_test_files_count"] == 0


# --- File location / placement correctness ---


def test_correctly_placed_files_score_full():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "src/synthesis/good.py",
                "tests/test_good.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["file_organization_correctness"] == 1.0
    assert result["misplaced_files_count"] == 0


def test_test_file_outside_tests_dir_is_misplaced():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "src/synthesis/module.py",
                "src/test_module.py",  # test file in src/ directory
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["misplaced_files_count"] == 1
    assert "src/test_module.py" in result["misplaced_files"]
    assert result["file_organization_correctness"] < 1.0


def test_source_in_unknown_directory_is_misplaced():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "random_dir/module.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["misplaced_files_count"] == 1
    assert result["file_organization_correctness"] == 0.0


def test_root_level_scripts_are_acceptable():
    records = [
        {
            "session_id": "s1",
            "expected_files": ["run.py"],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["misplaced_files_count"] == 0
    assert result["file_organization_correctness"] == 1.0


# --- Naming conventions ---


def test_correct_naming_convention_scores_full():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "tests/test_foo.py",
                "tests/test_bar.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["naming_convention_violations"] == 0
    assert result["naming_consistency"] == 1.0


def test_test_file_without_test_prefix_is_violation():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "tests/foo_tests.py",  # wrong naming
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["naming_convention_violations"] == 1
    assert result["naming_consistency"] == 0.0


def test_mixed_naming_conventions():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "tests/test_good.py",
                "tests/bad_spec.py",  # wrong naming
                "tests/test_also_good.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["naming_convention_violations"] == 1
    assert result["naming_consistency"] == pytest.approx(0.667, abs=0.01)


# --- Test-to-source ratio ---


def test_test_to_source_ratio_calculated():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "src/synthesis/a.py",
                "src/synthesis/b.py",
                "tests/test_a.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["test_to_source_ratio"] == 0.5


def test_test_to_source_ratio_with_no_sources():
    records = [
        {
            "session_id": "s1",
            "expected_files": ["tests/test_orphan.py"],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["test_to_source_ratio"] == 0.0


# --- Session-level tracking ---


def test_sessions_with_full_pairing_tracked():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "src/synthesis/a.py",
                "tests/test_a.py",
            ],
        },
        {
            "session_id": "s2",
            "expected_files": [
                "src/synthesis/b.py",
                # no test
            ],
        },
    ]
    result = analyze_pack_file_organization(records)

    assert result["sessions_with_full_pairing"] == 1
    assert result["total_sessions"] == 2


def test_sessions_with_no_tests_tracked():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "src/synthesis/lonely.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["sessions_with_no_tests"] == 1


# --- Multi-session aggregation ---


def test_multi_session_deduplicates_files():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "src/synthesis/shared.py",
                "tests/test_shared.py",
            ],
        },
        {
            "session_id": "s2",
            "expected_files": [
                "src/synthesis/shared.py",  # duplicate
                "tests/test_shared.py",  # duplicate
                "src/synthesis/extra.py",
            ],
        },
    ]
    result = analyze_pack_file_organization(records)

    # shared.py counted once, extra.py counted once
    assert result["source_files_with_tests_count"] == 1
    assert result["source_files_without_tests_count"] == 1


# --- Edge cases ---


def test_non_python_files_ignored_for_source_test_pairing():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "README.md",
                "schema.sql",
                "src/synthesis/real.py",
                "tests/test_real.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["source_files_with_tests_count"] == 1
    assert result["source_files_without_tests_count"] == 0
    assert result["file_organization_correctness"] == 1.0


def test_config_files_excluded_from_source_count():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "conftest.py",
                "__init__.py",
                "setup.py",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["source_files_with_tests_count"] == 0
    assert result["source_files_without_tests_count"] == 0


def test_whitespace_in_file_paths_stripped():
    records = [
        {
            "session_id": "s1",
            "expected_files": [
                "  src/synthesis/spaced.py  ",
                " tests/test_spaced.py ",
            ],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["source_files_with_tests_count"] == 1
    assert result["test_companion_discipline"] == 1.0


def test_empty_string_files_ignored():
    records = [
        {
            "session_id": "s1",
            "expected_files": ["", "  ", "src/synthesis/real.py"],
        }
    ]
    result = analyze_pack_file_organization(records)

    assert result["source_files_without_tests_count"] == 1


def test_orphaned_tests_limited_to_ten():
    orphans = [f"tests/test_orphan_{i}.py" for i in range(15)]
    records = [{"session_id": "s1", "expected_files": orphans}]
    result = analyze_pack_file_organization(records)

    assert result["orphaned_test_files_count"] == 15
    assert len(result["orphaned_test_files"]) == 10
