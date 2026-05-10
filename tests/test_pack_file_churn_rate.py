"""Tests for pack file churn rate analyzer."""

import pytest

from synthesis.pack_file_churn_rate import (
    analyze_pack_file_churn_rate,
    _normalize_path,
    _percentage,
)


class TestAnalyzePackFileChurnRate:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_file_churn_rate([])

        assert result["total_files"] == 0
        assert result["total_modifications"] == 0
        assert result["churn_rate"] == 0.0
        assert result["avg_modifications_per_file"] == 0.0
        assert result["single_touch_rate"] == 0.0
        assert result["hotspot_files"] == []
        assert result["max_modifications"] == 0
        assert "warning" not in result

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_file_churn_rate(None)
        assert result["total_files"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_file_churn_rate("not a list")

    def test_no_churn_single_modifications(self):
        """Verify no churn when all files modified exactly once."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/a.py", "task_id": "task1"},
            {"file_path": "src/b.py", "task_id": "task2"},
            {"file_path": "src/c.py", "task_id": "task3"},
        ])

        assert result["total_files"] == 3
        assert result["total_modifications"] == 3
        assert result["churn_rate"] == 0.0
        assert result["avg_modifications_per_file"] == 1.0
        assert result["single_touch_rate"] == 100.0
        assert result["hotspot_files"] == []
        assert result["max_modifications"] == 1
        assert "warning" not in result

    def test_moderate_churn_some_repeated_modifications(self):
        """Verify moderate churn calculation with some repeated files."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/a.py", "task_id": "task1"},
            {"file_path": "src/a.py", "task_id": "task2"},  # a.py modified twice
            {"file_path": "src/b.py", "task_id": "task3"},
            {"file_path": "src/c.py", "task_id": "task4"},
        ])

        # 3 unique files, 1 modified twice (33.33% churn)
        assert result["total_files"] == 3
        assert result["total_modifications"] == 4
        assert result["churn_rate"] == 33.33
        assert result["avg_modifications_per_file"] == 1.33
        assert result["single_touch_rate"] == 66.67
        assert result["max_modifications"] == 2
        # Should have warning since churn_rate > 25%
        assert "warning" in result

    def test_high_churn_many_repeated_modifications(self):
        """Verify high churn calculation with many repeated files."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/a.py", "task_id": "task1"},
            {"file_path": "src/a.py", "task_id": "task2"},
            {"file_path": "src/a.py", "task_id": "task3"},
            {"file_path": "src/b.py", "task_id": "task4"},
            {"file_path": "src/b.py", "task_id": "task5"},
            {"file_path": "src/c.py", "task_id": "task6"},
        ])

        # 3 unique files, 2 modified multiple times (66.67% churn)
        assert result["total_files"] == 3
        assert result["total_modifications"] == 6
        assert result["churn_rate"] == 66.67
        assert result["avg_modifications_per_file"] == 2.0
        assert result["single_touch_rate"] == 33.33
        assert result["max_modifications"] == 3
        assert "warning" in result

    def test_hotspot_files_identified(self):
        """Verify hotspot files (>3 modifications) are identified."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/hotspot.py", "task_id": "task1"},
            {"file_path": "src/hotspot.py", "task_id": "task2"},
            {"file_path": "src/hotspot.py", "task_id": "task3"},
            {"file_path": "src/hotspot.py", "task_id": "task4"},
            {"file_path": "src/hotspot.py", "task_id": "task5"},  # 5 modifications
            {"file_path": "src/normal.py", "task_id": "task6"},
            {"file_path": "src/normal.py", "task_id": "task7"},  # 2 modifications
        ])

        hotspots = result["hotspot_files"]
        assert len(hotspots) == 1
        assert hotspots[0]["file_path"] == "src/hotspot.py"
        assert hotspots[0]["modification_count"] == 5
        assert result["max_modifications"] == 5

    def test_multiple_hotspot_files_sorted_by_count(self):
        """Verify multiple hotspot files are sorted by modification count."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/hot1.py", "task_id": f"task{i}"}
            for i in range(6)  # 6 modifications
        ] + [
            {"file_path": "src/hot2.py", "task_id": f"task{i}"}
            for i in range(4)  # 4 modifications
        ])

        hotspots = result["hotspot_files"]
        assert len(hotspots) == 2
        # Should be sorted descending by count
        assert hotspots[0]["file_path"] == "src/hot1.py"
        assert hotspots[0]["modification_count"] == 6
        assert hotspots[1]["file_path"] == "src/hot2.py"
        assert hotspots[1]["modification_count"] == 4

    def test_exactly_three_modifications_not_hotspot(self):
        """Verify files with exactly 3 modifications are not hotspots."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/boundary.py", "task_id": "task1"},
            {"file_path": "src/boundary.py", "task_id": "task2"},
            {"file_path": "src/boundary.py", "task_id": "task3"},
        ])

        # 3 modifications is not > 3, so not a hotspot
        assert result["hotspot_files"] == []
        assert result["max_modifications"] == 3

    def test_exactly_four_modifications_is_hotspot(self):
        """Verify files with exactly 4 modifications are hotspots."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/boundary.py", "task_id": "task1"},
            {"file_path": "src/boundary.py", "task_id": "task2"},
            {"file_path": "src/boundary.py", "task_id": "task3"},
            {"file_path": "src/boundary.py", "task_id": "task4"},
        ])

        # 4 modifications is > 3, so it's a hotspot
        assert len(result["hotspot_files"]) == 1
        assert result["hotspot_files"][0]["modification_count"] == 4

    def test_single_file_modified_once(self):
        """Verify single file modified once has zero churn."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/single.py", "task_id": "task1"},
        ])

        assert result["total_files"] == 1
        assert result["total_modifications"] == 1
        assert result["churn_rate"] == 0.0
        assert result["avg_modifications_per_file"] == 1.0
        assert result["single_touch_rate"] == 100.0
        assert result["hotspot_files"] == []
        assert "warning" not in result

    def test_single_file_modified_multiple_times(self):
        """Verify single file modified multiple times has 100% churn."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/churned.py", "task_id": "task1"},
            {"file_path": "src/churned.py", "task_id": "task2"},
            {"file_path": "src/churned.py", "task_id": "task3"},
        ])

        assert result["total_files"] == 1
        assert result["total_modifications"] == 3
        assert result["churn_rate"] == 100.0
        assert result["avg_modifications_per_file"] == 3.0
        assert result["single_touch_rate"] == 0.0
        assert result["max_modifications"] == 3
        assert "warning" in result

    def test_file_paths_normalized(self):
        """Verify file paths are normalized for consistent tracking."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "./src/foo.py", "task_id": "task1"},
            {"file_path": "src/foo.py", "task_id": "task2"},
            {"file_path": "src\\foo.py", "task_id": "task3"},
        ])

        # All three should be normalized to same path
        assert result["total_files"] == 1
        assert result["total_modifications"] == 3
        assert result["churn_rate"] == 100.0

    def test_empty_file_path_skipped(self):
        """Verify empty file paths are skipped."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "", "task_id": "task1"},
            {"file_path": "   ", "task_id": "task2"},
            {"file_path": "src/valid.py", "task_id": "task3"},
        ])

        assert result["total_files"] == 1
        assert result["total_modifications"] == 1

    def test_missing_file_path_skipped(self):
        """Verify records without file_path are skipped."""
        result = analyze_pack_file_churn_rate([
            {"task_id": "task1"},  # No file_path
            {"file_path": "src/valid.py", "task_id": "task2"},
        ])

        assert result["total_files"] == 1
        assert result["total_modifications"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_file_churn_rate([
            "not a dict",
            123,
            None,
            {"file_path": "src/valid.py", "task_id": "task1"},
        ])

        assert result["total_files"] == 1
        assert result["total_modifications"] == 1

    def test_task_id_optional(self):
        """Verify task_id is optional for tracking."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/a.py"},
            {"file_path": "src/a.py"},
        ])

        # Should still track modifications without task_id
        assert result["total_files"] == 1
        assert result["total_modifications"] == 2
        assert result["churn_rate"] == 100.0

    def test_warning_threshold_at_25_percent(self):
        """Verify warning appears exactly when churn_rate > 25%."""
        # Exactly 25% should not have warning
        result1 = analyze_pack_file_churn_rate([
            {"file_path": "src/churned.py", "task_id": "task1"},
            {"file_path": "src/churned.py", "task_id": "task2"},  # Churned
            {"file_path": "src/stable1.py", "task_id": "task3"},
            {"file_path": "src/stable2.py", "task_id": "task4"},
            {"file_path": "src/stable3.py", "task_id": "task5"},
        ])
        # 1 of 4 files churned = 25%
        assert result1["churn_rate"] == 25.0
        assert "warning" not in result1

        # Slightly above 25% should have warning
        result2 = analyze_pack_file_churn_rate([
            {"file_path": "src/churned.py", "task_id": "task1"},
            {"file_path": "src/churned.py", "task_id": "task2"},  # Churned
            {"file_path": "src/stable1.py", "task_id": "task3"},
            {"file_path": "src/stable2.py", "task_id": "task4"},
        ])
        # 1 of 3 files churned = 33.33%
        assert result2["churn_rate"] == 33.33
        assert "warning" in result2


class TestNormalizePath:
    """Test path normalization helper."""

    def test_no_change_for_clean_path(self):
        """Verify clean paths are not modified."""
        assert _normalize_path("src/foo.py") == "src/foo.py"

    def test_leading_dot_slash_removed(self):
        """Verify leading ./ is removed."""
        assert _normalize_path("./src/foo.py") == "src/foo.py"

    def test_backslashes_converted(self):
        """Verify backslashes are converted to forward slashes."""
        assert _normalize_path("src\\foo.py") == "src/foo.py"
        assert _normalize_path("src\\dir\\foo.py") == "src/dir/foo.py"

    def test_combined_normalization(self):
        """Verify combined path normalization."""
        assert _normalize_path(".\\src\\foo.py") == "src/foo.py"


class TestPercentage:
    """Test percentage calculation helper."""

    def test_perfect_percentage(self):
        """Verify 100% calculation."""
        assert _percentage(5, 5) == 100.0

    def test_partial_percentage(self):
        """Verify partial percentage calculation."""
        assert _percentage(1, 3) == 33.33

    def test_zero_numerator(self):
        """Verify zero numerator returns 0%."""
        assert _percentage(0, 5) == 0.0

    def test_zero_denominator(self):
        """Verify zero denominator returns 0%."""
        assert _percentage(5, 0) == 0.0

    def test_both_zero(self):
        """Verify both zero returns 0%."""
        assert _percentage(0, 0) == 0.0


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_well_scoped_pack_low_churn(self):
        """Simulate well-scoped pack with minimal churn."""
        result = analyze_pack_file_churn_rate([
            {"file_path": "src/analyzer.py", "task_id": "task1"},
            {"file_path": "tests/test_analyzer.py", "task_id": "task1"},
            {"file_path": "src/helper.py", "task_id": "task2"},
            {"file_path": "tests/test_helper.py", "task_id": "task2"},
        ])

        assert result["total_files"] == 4
        assert result["churn_rate"] == 0.0
        assert result["single_touch_rate"] == 100.0
        assert "warning" not in result

    def test_poorly_scoped_pack_high_churn(self):
        """Simulate poorly scoped pack requiring many iterations."""
        result = analyze_pack_file_churn_rate([
            # Initial implementation
            {"file_path": "src/feature.py", "task_id": "task1"},
            {"file_path": "tests/test_feature.py", "task_id": "task1"},
            # Fix bugs
            {"file_path": "src/feature.py", "task_id": "task2"},
            {"file_path": "tests/test_feature.py", "task_id": "task2"},
            # More fixes
            {"file_path": "src/feature.py", "task_id": "task3"},
            {"file_path": "tests/test_feature.py", "task_id": "task3"},
        ])

        assert result["total_files"] == 2
        assert result["total_modifications"] == 6
        assert result["churn_rate"] == 100.0
        assert result["avg_modifications_per_file"] == 3.0
        assert result["single_touch_rate"] == 0.0
        assert "warning" in result

    def test_mixed_stability_realistic_pack(self):
        """Simulate realistic pack with mixed file stability."""
        result = analyze_pack_file_churn_rate([
            # Stable files - modified once
            {"file_path": "src/utils.py", "task_id": "task1"},
            {"file_path": "src/constants.py", "task_id": "task2"},
            {"file_path": "tests/test_utils.py", "task_id": "task1"},
            # Churned file - iterative development
            {"file_path": "src/core.py", "task_id": "task1"},
            {"file_path": "src/core.py", "task_id": "task2"},
            {"file_path": "src/core.py", "task_id": "task3"},
            # Hotspot file - many iterations
            {"file_path": "src/config.py", "task_id": "task1"},
            {"file_path": "src/config.py", "task_id": "task2"},
            {"file_path": "src/config.py", "task_id": "task3"},
            {"file_path": "src/config.py", "task_id": "task4"},
            {"file_path": "src/config.py", "task_id": "task5"},
        ])

        # 5 unique files, 2 modified multiple times (40% churn)
        assert result["total_files"] == 5
        assert result["total_modifications"] == 11
        assert result["churn_rate"] == 40.0
        assert result["single_touch_rate"] == 60.0
        # config.py is hotspot (5 modifications > 3)
        assert len(result["hotspot_files"]) == 1
        assert result["hotspot_files"][0]["file_path"] == "src/config.py"
        assert result["max_modifications"] == 5
        assert "warning" in result

    def test_refactoring_scenario_high_churn(self):
        """Simulate refactoring causing high churn across files."""
        # Simulate refactoring where same files get touched repeatedly
        files = ["src/a.py", "src/b.py", "src/c.py", "src/d.py"]
        events = []
        for iteration in range(1, 4):  # 3 refactoring iterations
            for file in files:
                events.append({"file_path": file, "task_id": f"refactor_{iteration}"})

        result = analyze_pack_file_churn_rate(events)

        # All 4 files modified 3 times each
        assert result["total_files"] == 4
        assert result["total_modifications"] == 12
        assert result["churn_rate"] == 100.0
        assert result["avg_modifications_per_file"] == 3.0
        assert result["single_touch_rate"] == 0.0
        assert result["hotspot_files"] == []  # 3 modifications is not > 3
        assert "warning" in result
