"""Tests for session file creation ratio analyzer."""

import pytest

from synthesis.session_file_creation_ratio import (
    analyze_session_file_creation_ratio,
    _categorize_file_type,
    _calculate_creation_ratio,
)


class TestAnalyzeSessionFileCreationRatio:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_file_creation_ratio([])

        assert result["total_file_operations"] == 0
        assert result["write_new_file_count"] == 0
        assert result["edit_count"] == 0
        assert result["write_existing_file_count"] == 0
        assert result["creation_to_modification_ratio"] == 0.0
        assert result["has_high_creation_ratio"] is False
        assert result["avg_created_file_size"] == 0.0
        assert result["avg_modified_file_size"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_file_creation_ratio(None)
        assert result["total_file_operations"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_file_creation_ratio("not a list")

    def test_invalid_threshold_raises_error(self):
        """Verify invalid threshold raises ValueError."""
        with pytest.raises(ValueError, match="high_ratio_threshold must be"):
            analyze_session_file_creation_ratio([], high_ratio_threshold=-0.1)

    def test_single_new_file_creation(self):
        """Verify single new file creation."""
        result = analyze_session_file_creation_ratio([
            {
                "tool_name": "Write",
                "file_path": "src/new.py",
                "is_new_file": True,
                "file_size": 100,
                "turn_index": 0,
            }
        ])

        assert result["write_new_file_count"] == 1
        assert result["edit_count"] == 0
        assert result["creation_to_modification_ratio"] == float("inf")
        assert result["has_high_creation_ratio"] is True
        assert result["avg_created_file_size"] == 100.0

    def test_single_edit_operation(self):
        """Verify single edit operation."""
        result = analyze_session_file_creation_ratio([
            {
                "tool_name": "Edit",
                "file_path": "src/existing.py",
                "is_new_file": False,
                "file_size": 200,
                "turn_index": 0,
            }
        ])

        assert result["write_new_file_count"] == 0
        assert result["edit_count"] == 1
        assert result["creation_to_modification_ratio"] == 0.0
        assert result["has_high_creation_ratio"] is False
        assert result["avg_modified_file_size"] == 200.0

    def test_balanced_creation_and_modification(self):
        """Verify balanced creation and modification (ratio = 1.0)."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "src/new1.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Edit", "file_path": "src/existing.py", "is_new_file": False, "turn_index": 1},
        ])

        assert result["write_new_file_count"] == 1
        assert result["edit_count"] == 1
        assert result["creation_to_modification_ratio"] == 1.0
        assert result["has_high_creation_ratio"] is True  # 1.0 > 0.5 default threshold

    def test_low_creation_ratio(self):
        """Verify low creation ratio (more edits than creations)."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "src/new.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Edit", "file_path": "src/file1.py", "is_new_file": False, "turn_index": 1},
            {"tool_name": "Edit", "file_path": "src/file2.py", "is_new_file": False, "turn_index": 2},
            {"tool_name": "Edit", "file_path": "src/file3.py", "is_new_file": False, "turn_index": 3},
        ])

        # 1 creation / 3 edits = 0.333
        assert result["write_new_file_count"] == 1
        assert result["edit_count"] == 3
        assert result["creation_to_modification_ratio"] == 0.333
        assert result["has_high_creation_ratio"] is False

    def test_high_creation_ratio(self):
        """Verify high creation ratio (more creations than edits)."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "src/new1.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Write", "file_path": "src/new2.py", "is_new_file": True, "turn_index": 1},
            {"tool_name": "Write", "file_path": "src/new3.py", "is_new_file": True, "turn_index": 2},
            {"tool_name": "Edit", "file_path": "src/existing.py", "is_new_file": False, "turn_index": 3},
        ])

        # 3 creations / 1 edit = 3.0
        assert result["write_new_file_count"] == 3
        assert result["edit_count"] == 1
        assert result["creation_to_modification_ratio"] == 3.0
        assert result["has_high_creation_ratio"] is True

    def test_custom_threshold(self):
        """Verify custom threshold is respected."""
        result = analyze_session_file_creation_ratio(
            [
                {"tool_name": "Write", "file_path": "new1.py", "is_new_file": True, "turn_index": 0},
                {"tool_name": "Write", "file_path": "new2.py", "is_new_file": True, "turn_index": 1},
                {"tool_name": "Edit", "file_path": "existing.py", "is_new_file": False, "turn_index": 2},
            ],
            high_ratio_threshold=2.5,
        )

        # Ratio is 2.0, threshold is 2.5
        assert result["creation_to_modification_ratio"] == 2.0
        assert result["has_high_creation_ratio"] is False

    def test_write_to_existing_file_tracked_separately(self):
        """Verify Write operations to existing files are tracked separately."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "new.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Write", "file_path": "existing.py", "is_new_file": False, "turn_index": 1},
            {"tool_name": "Edit", "file_path": "another.py", "is_new_file": False, "turn_index": 2},
        ])

        assert result["write_new_file_count"] == 1
        assert result["write_existing_file_count"] == 1
        assert result["edit_count"] == 1
        assert result["total_file_operations"] == 3

    def test_file_type_distribution_all_types(self):
        """Verify file type distribution categorizes correctly."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "tests/test_foo.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Write", "file_path": "src/main.py", "is_new_file": True, "turn_index": 1},
            {"tool_name": "Write", "file_path": "config.json", "is_new_file": True, "turn_index": 2},
            {"tool_name": "Write", "file_path": "README.md", "is_new_file": True, "turn_index": 3},
            {"tool_name": "Write", "file_path": "data.csv", "is_new_file": True, "turn_index": 4},
        ])

        dist = result["file_type_distribution"]
        assert dist["test_files"] == 1
        assert dist["source_files"] == 1
        assert dist["config_files"] == 1
        assert dist["doc_files"] == 1
        assert dist["other_files"] == 1

    def test_file_type_percentages(self):
        """Verify file type percentages calculated correctly."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "tests/test_a.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Write", "file_path": "tests/test_b.py", "is_new_file": True, "turn_index": 1},
            {"tool_name": "Write", "file_path": "src/main.py", "is_new_file": True, "turn_index": 2},
            {"tool_name": "Write", "file_path": "README.md", "is_new_file": True, "turn_index": 3},
        ])

        dist = result["file_type_distribution"]
        assert dist["test_percentage"] == 50.0  # 2/4
        assert dist["source_percentage"] == 25.0  # 1/4
        assert dist["doc_percentage"] == 25.0  # 1/4
        assert dist["config_percentage"] == 0.0

    def test_average_file_size_calculation(self):
        """Verify average file size for created vs modified files."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "new1.py", "is_new_file": True, "file_size": 100, "turn_index": 0},
            {"tool_name": "Write", "file_path": "new2.py", "is_new_file": True, "file_size": 200, "turn_index": 1},
            {"tool_name": "Edit", "file_path": "existing1.py", "is_new_file": False, "file_size": 500, "turn_index": 2},
            {"tool_name": "Edit", "file_path": "existing2.py", "is_new_file": False, "file_size": 700, "turn_index": 3},
        ])

        # Created: (100 + 200) / 2 = 150
        # Modified: (500 + 700) / 2 = 600
        assert result["avg_created_file_size"] == 150.0
        assert result["avg_modified_file_size"] == 600.0

    def test_file_size_without_data(self):
        """Verify average file size is 0 when no size data available."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "new.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Edit", "file_path": "existing.py", "is_new_file": False, "turn_index": 1},
        ])

        assert result["avg_created_file_size"] == 0.0
        assert result["avg_modified_file_size"] == 0.0

    def test_mixed_tools_counted_correctly(self):
        """Verify mixed tool calls are counted correctly."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "new.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Read", "file_path": "src/file.py", "turn_index": 1},
            {"tool_name": "Edit", "file_path": "existing.py", "is_new_file": False, "turn_index": 2},
            {"tool_name": "Bash", "command": "ls", "turn_index": 3},
        ])

        # Only Write and Edit should be counted
        assert result["total_file_operations"] == 2
        assert result["write_new_file_count"] == 1
        assert result["edit_count"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "WRITE", "file_path": "new.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "edit", "file_path": "existing.py", "is_new_file": False, "turn_index": 1},
        ])

        assert result["write_new_file_count"] == 1
        assert result["edit_count"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_file_creation_ratio([
            "not a dict",
            {"tool_name": "Write", "file_path": "new.py", "is_new_file": True, "turn_index": 0},
        ])

        assert result["write_new_file_count"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_file_creation_ratio([
            {"file_path": "new.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Write", "file_path": "new2.py", "is_new_file": True, "turn_index": 1},
        ])

        assert result["write_new_file_count"] == 1

    def test_whitespace_handling_in_tool_names(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "  Write  ", "file_path": "new.py", "is_new_file": True, "turn_index": 0},
        ])

        assert result["write_new_file_count"] == 1

    def test_boolean_file_size_ignored(self):
        """Verify boolean values for file_size are ignored."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "new.py", "is_new_file": True, "file_size": True, "turn_index": 0},
        ])

        assert result["avg_created_file_size"] == 0.0

    def test_zero_threshold_edge_case(self):
        """Verify zero threshold allows any creation ratio to be high."""
        result = analyze_session_file_creation_ratio(
            [
                {"tool_name": "Write", "file_path": "new.py", "is_new_file": True, "turn_index": 0},
                {"tool_name": "Edit", "file_path": "existing.py", "is_new_file": False, "turn_index": 1},
                {"tool_name": "Edit", "file_path": "existing2.py", "is_new_file": False, "turn_index": 2},
            ],
            high_ratio_threshold=0.0,
        )

        # Ratio is 0.5 (1/2), which is > 0.0
        assert result["has_high_creation_ratio"] is True


class TestCategorizeFileType:
    """Test file type categorization helper."""

    def test_test_files_various_patterns(self):
        """Verify test file patterns are recognized."""
        assert _categorize_file_type("tests/test_foo.py") == "test"
        assert _categorize_file_type("test_bar.py") == "test"
        assert _categorize_file_type("src/foo_test.py") == "test"
        assert _categorize_file_type("src/foo.test.js") == "test"
        assert _categorize_file_type("test/integration.py") == "test"
        assert _categorize_file_type("foo.spec.ts") == "test"

    def test_source_files_various_languages(self):
        """Verify source file extensions are recognized."""
        assert _categorize_file_type("src/main.py") == "source"
        assert _categorize_file_type("app.js") == "source"
        assert _categorize_file_type("component.tsx") == "source"
        assert _categorize_file_type("util.ts") == "source"
        assert _categorize_file_type("Main.java") == "source"
        assert _categorize_file_type("main.go") == "source"
        assert _categorize_file_type("lib.rs") == "source"

    def test_config_files_various_formats(self):
        """Verify config file patterns are recognized."""
        assert _categorize_file_type("config.json") == "config"
        assert _categorize_file_type("settings.yaml") == "config"
        assert _categorize_file_type("setup.toml") == "config"
        assert _categorize_file_type("app.ini") == "config"
        assert _categorize_file_type(".eslintrc") == "config"
        assert _categorize_file_type("webpack.config.js") == "config"

    def test_doc_files_various_formats(self):
        """Verify documentation file patterns are recognized."""
        assert _categorize_file_type("README.md") == "docs"
        assert _categorize_file_type("guide.rst") == "docs"
        assert _categorize_file_type("notes.txt") == "docs"
        assert _categorize_file_type("docs/api.md") == "docs"

    def test_other_files(self):
        """Verify unrecognized files are categorized as other."""
        assert _categorize_file_type("data.csv") == "other"
        assert _categorize_file_type("image.png") == "other"
        assert _categorize_file_type("archive.zip") == "other"

    def test_empty_path_returns_other(self):
        """Verify empty path returns other."""
        assert _categorize_file_type("") == "other"

    def test_case_insensitive_matching(self):
        """Verify categorization is case-insensitive."""
        assert _categorize_file_type("TEST_FOO.PY") == "test"
        assert _categorize_file_type("README.MD") == "docs"
        assert _categorize_file_type("CONFIG.JSON") == "config"


class TestCalculateCreationRatio:
    """Test creation ratio calculation helper."""

    def test_equal_creation_and_modification(self):
        """Verify equal counts result in ratio of 1.0."""
        assert _calculate_creation_ratio(5, 5) == 1.0

    def test_more_creations_than_modifications(self):
        """Verify more creations result in ratio > 1.0."""
        assert _calculate_creation_ratio(10, 5) == 2.0

    def test_fewer_creations_than_modifications(self):
        """Verify fewer creations result in ratio < 1.0."""
        assert _calculate_creation_ratio(3, 10) == 0.3

    def test_no_modifications_with_creations(self):
        """Verify infinity when there are creations but no modifications."""
        assert _calculate_creation_ratio(5, 0) == float("inf")

    def test_no_creations_no_modifications(self):
        """Verify zero when there are no operations."""
        assert _calculate_creation_ratio(0, 0) == 0.0

    def test_result_rounded_to_three_decimals(self):
        """Verify result is rounded to 3 decimal places."""
        assert _calculate_creation_ratio(1, 3) == 0.333


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_efficient_workflow_prefer_edits(self):
        """Simulate efficient workflow that prefers editing existing files."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "tests/test_new.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Edit", "file_path": "src/main.py", "is_new_file": False, "turn_index": 1},
            {"tool_name": "Edit", "file_path": "src/utils.py", "is_new_file": False, "turn_index": 2},
            {"tool_name": "Edit", "file_path": "src/helpers.py", "is_new_file": False, "turn_index": 3},
            {"tool_name": "Edit", "file_path": "tests/test_existing.py", "is_new_file": False, "turn_index": 4},
        ])

        # 1 creation / 4 edits = 0.25 (low ratio, good)
        assert result["creation_to_modification_ratio"] == 0.25
        assert result["has_high_creation_ratio"] is False

    def test_over_engineering_many_new_files(self):
        """Simulate over-engineering with many new files."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "src/new1.py", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Write", "file_path": "src/new2.py", "is_new_file": True, "turn_index": 1},
            {"tool_name": "Write", "file_path": "src/new3.py", "is_new_file": True, "turn_index": 2},
            {"tool_name": "Write", "file_path": "src/new4.py", "is_new_file": True, "turn_index": 3},
            {"tool_name": "Edit", "file_path": "src/existing.py", "is_new_file": False, "turn_index": 4},
        ])

        # 4 creations / 1 edit = 4.0 (high ratio, potential over-engineering)
        assert result["creation_to_modification_ratio"] == 4.0
        assert result["has_high_creation_ratio"] is True

    def test_config_heavy_session(self):
        """Simulate session creating many config files."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "config.json", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Write", "file_path": ".eslintrc", "is_new_file": True, "turn_index": 1},
            {"tool_name": "Write", "file_path": "tsconfig.json", "is_new_file": True, "turn_index": 2},
            {"tool_name": "Write", "file_path": "src/main.py", "is_new_file": True, "turn_index": 3},
        ])

        dist = result["file_type_distribution"]
        assert dist["config_files"] == 3
        assert dist["source_files"] == 1
        assert dist["config_percentage"] == 75.0

    def test_test_driven_development_pattern(self):
        """Simulate TDD pattern with test file creation."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "tests/test_feature.py", "is_new_file": True, "file_size": 500, "turn_index": 0},
            {"tool_name": "Write", "file_path": "src/feature.py", "is_new_file": True, "file_size": 300, "turn_index": 1},
            {"tool_name": "Edit", "file_path": "src/feature.py", "is_new_file": False, "file_size": 350, "turn_index": 2},
            {"tool_name": "Edit", "file_path": "tests/test_feature.py", "is_new_file": False, "file_size": 550, "turn_index": 3},
        ])

        # 2 creations / 2 edits = 1.0
        assert result["creation_to_modification_ratio"] == 1.0
        dist = result["file_type_distribution"]
        assert dist["test_files"] == 1
        assert dist["source_files"] == 1

    def test_small_new_files_pattern(self):
        """Simulate pattern of creating many small new files."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "src/helper1.py", "is_new_file": True, "file_size": 50, "turn_index": 0},
            {"tool_name": "Write", "file_path": "src/helper2.py", "is_new_file": True, "file_size": 60, "turn_index": 1},
            {"tool_name": "Write", "file_path": "src/helper3.py", "is_new_file": True, "file_size": 40, "turn_index": 2},
            {"tool_name": "Edit", "file_path": "src/main.py", "is_new_file": False, "file_size": 500, "turn_index": 3},
        ])

        # Average created file size: (50 + 60 + 40) / 3 = 50
        # Average modified file size: 500
        assert result["avg_created_file_size"] == 50.0
        assert result["avg_modified_file_size"] == 500.0
        assert result["creation_to_modification_ratio"] == 3.0

    def test_documentation_heavy_session(self):
        """Simulate session creating many documentation files."""
        result = analyze_session_file_creation_ratio([
            {"tool_name": "Write", "file_path": "README.md", "is_new_file": True, "turn_index": 0},
            {"tool_name": "Write", "file_path": "docs/guide.md", "is_new_file": True, "turn_index": 1},
            {"tool_name": "Write", "file_path": "docs/api.md", "is_new_file": True, "turn_index": 2},
            {"tool_name": "Edit", "file_path": "src/main.py", "is_new_file": False, "turn_index": 3},
        ])

        dist = result["file_type_distribution"]
        assert dist["doc_files"] == 3
        assert dist["doc_percentage"] == 100.0
        assert result["creation_to_modification_ratio"] == 3.0
