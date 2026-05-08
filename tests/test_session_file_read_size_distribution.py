"""Tests for session file read size distribution analyzer."""

import pytest

from synthesis.session_file_read_size_distribution import (
    analyze_session_file_read_size_distribution,
    BUCKET_SMALL,
    BUCKET_MEDIUM,
    BUCKET_LARGE,
    TARGETED_THRESHOLD_HIGH,
    TARGETED_THRESHOLD_MODERATE,
    _categorize_bucket,
    _percentage,
    _average,
    _classify_efficiency_pattern,
)


class TestAnalyzeSessionFileReadSizeDistribution:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_file_read_size_distribution([])

        assert result["total_reads"] == 0
        assert result["bucket_counts"] == {
            "under_50": 0,
            "50_to_200": 0,
            "200_to_500": 0,
            "over_500": 0,
        }
        assert result["targeted_read_count"] == 0
        assert result["targeted_read_percentage"] == 0.0
        assert result["avg_lines_per_read"] == 0.0
        assert result["efficiency_pattern"] == "empty"
        assert result["examples"] == {
            "under_50": [],
            "50_to_200": [],
            "200_to_500": [],
            "over_500": [],
        }

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_file_read_size_distribution(None)
        assert result["total_reads"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_file_read_size_distribution("not a list")

    def test_single_small_read(self):
        """Verify single small read is categorized correctly."""
        result = analyze_session_file_read_size_distribution([
            {
                "file_path": "src/foo.py",
                "lines_read": 30,
                "turn_index": 0,
            }
        ])

        assert result["total_reads"] == 1
        assert result["bucket_counts"]["under_50"] == 1
        assert result["avg_lines_per_read"] == 30.0
        assert result["efficiency_pattern"] == "highly_targeted"

    def test_single_medium_read(self):
        """Verify single medium read is categorized correctly."""
        result = analyze_session_file_read_size_distribution([
            {
                "file_path": "src/foo.py",
                "lines_read": 150,
                "turn_index": 0,
            }
        ])

        assert result["total_reads"] == 1
        assert result["bucket_counts"]["50_to_200"] == 1
        assert result["avg_lines_per_read"] == 150.0

    def test_single_large_read(self):
        """Verify single large read is categorized correctly."""
        result = analyze_session_file_read_size_distribution([
            {
                "file_path": "src/foo.py",
                "lines_read": 1000,
                "turn_index": 0,
            }
        ])

        assert result["total_reads"] == 1
        assert result["bucket_counts"]["over_500"] == 1
        assert result["avg_lines_per_read"] == 1000.0
        assert result["efficiency_pattern"] == "full_file_heavy"

    def test_boundary_cases_for_buckets(self):
        """Verify boundary values are categorized correctly."""
        result = analyze_session_file_read_size_distribution([
            {"file_path": "a.py", "lines_read": 49, "turn_index": 0},  # under_50
            {"file_path": "b.py", "lines_read": 50, "turn_index": 1},  # 50_to_200
            {"file_path": "c.py", "lines_read": 199, "turn_index": 2},  # 50_to_200
            {"file_path": "d.py", "lines_read": 200, "turn_index": 3},  # 200_to_500
            {"file_path": "e.py", "lines_read": 499, "turn_index": 4},  # 200_to_500
            {"file_path": "f.py", "lines_read": 500, "turn_index": 5},  # over_500
        ])

        assert result["bucket_counts"]["under_50"] == 1
        assert result["bucket_counts"]["50_to_200"] == 2
        assert result["bucket_counts"]["200_to_500"] == 2
        assert result["bucket_counts"]["over_500"] == 1

    def test_targeted_read_with_offset(self):
        """Verify reads with offset parameter are counted as targeted."""
        result = analyze_session_file_read_size_distribution([
            {
                "file_path": "src/foo.py",
                "lines_read": 30,
                "offset": 100,
                "turn_index": 0,
            }
        ])

        assert result["targeted_read_count"] == 1
        assert result["targeted_read_percentage"] == 100.0

    def test_targeted_read_with_limit(self):
        """Verify reads with limit parameter are counted as targeted."""
        result = analyze_session_file_read_size_distribution([
            {
                "file_path": "src/foo.py",
                "lines_read": 50,
                "limit": 50,
                "turn_index": 0,
            }
        ])

        assert result["targeted_read_count"] == 1
        assert result["targeted_read_percentage"] == 100.0

    def test_targeted_read_with_both_offset_and_limit(self):
        """Verify reads with both offset and limit are counted as targeted."""
        result = analyze_session_file_read_size_distribution([
            {
                "file_path": "src/foo.py",
                "lines_read": 30,
                "offset": 100,
                "limit": 30,
                "turn_index": 0,
            }
        ])

        assert result["targeted_read_count"] == 1

    def test_full_file_read_without_offset_or_limit(self):
        """Verify reads without offset/limit are not counted as targeted."""
        result = analyze_session_file_read_size_distribution([
            {
                "file_path": "src/foo.py",
                "lines_read": 500,
                "turn_index": 0,
            }
        ])

        assert result["targeted_read_count"] == 0
        assert result["targeted_read_percentage"] == 0.0

    def test_mixed_targeted_and_full_reads(self):
        """Verify mix of targeted and full reads."""
        result = analyze_session_file_read_size_distribution([
            {"file_path": "a.py", "lines_read": 30, "offset": 10, "turn_index": 0},
            {"file_path": "b.py", "lines_read": 500, "turn_index": 1},
            {"file_path": "c.py", "lines_read": 40, "limit": 40, "turn_index": 2},
            {"file_path": "d.py", "lines_read": 1000, "turn_index": 3},
        ])

        assert result["total_reads"] == 4
        assert result["targeted_read_count"] == 2
        assert result["targeted_read_percentage"] == 50.0

    def test_highly_targeted_efficiency_pattern(self):
        """Verify highly targeted pattern for >75% small reads."""
        # 8 small reads, 2 medium reads = 80% small
        records = [
            {"file_path": f"file{i}.py", "lines_read": 30, "turn_index": i}
            for i in range(8)
        ]
        records.extend([
            {"file_path": "file8.py", "lines_read": 100, "turn_index": 8},
            {"file_path": "file9.py", "lines_read": 150, "turn_index": 9},
        ])

        result = analyze_session_file_read_size_distribution(records)
        assert result["efficiency_pattern"] == "highly_targeted"

    def test_moderately_targeted_efficiency_pattern(self):
        """Verify moderately targeted pattern for 50-75% small+medium reads."""
        # 6 small, 2 medium, 2 large = 80% small+medium
        records = [
            {"file_path": f"file{i}.py", "lines_read": 30, "turn_index": i}
            for i in range(6)
        ]
        records.extend([
            {"file_path": "file6.py", "lines_read": 100, "turn_index": 6},
            {"file_path": "file7.py", "lines_read": 150, "turn_index": 7},
            {"file_path": "file8.py", "lines_read": 1000, "turn_index": 8},
            {"file_path": "file9.py", "lines_read": 1000, "turn_index": 9},
        ])

        result = analyze_session_file_read_size_distribution(records)
        assert result["efficiency_pattern"] == "moderately_targeted"

    def test_full_file_heavy_efficiency_pattern(self):
        """Verify full_file_heavy pattern for >50% large reads."""
        # 2 small, 6 large = 60% large
        records = [
            {"file_path": "file0.py", "lines_read": 30, "turn_index": 0},
            {"file_path": "file1.py", "lines_read": 40, "turn_index": 1},
        ]
        records.extend([
            {"file_path": f"file{i}.py", "lines_read": 1000, "turn_index": i}
            for i in range(2, 8)
        ])

        result = analyze_session_file_read_size_distribution(records)
        assert result["efficiency_pattern"] == "full_file_heavy"

    def test_mixed_efficiency_pattern(self):
        """Verify mixed pattern for balanced distribution."""
        records = [
            {"file_path": "file0.py", "lines_read": 30, "turn_index": 0},   # under_50
            {"file_path": "file1.py", "lines_read": 100, "turn_index": 1},  # 50_to_200
            {"file_path": "file2.py", "lines_read": 300, "turn_index": 2},  # 200_to_500
            {"file_path": "file3.py", "lines_read": 800, "turn_index": 3},  # over_500
        ]

        result = analyze_session_file_read_size_distribution(records)
        assert result["efficiency_pattern"] == "mixed"

    def test_average_lines_per_read_calculation(self):
        """Verify average lines calculation."""
        result = analyze_session_file_read_size_distribution([
            {"file_path": "a.py", "lines_read": 100, "turn_index": 0},
            {"file_path": "b.py", "lines_read": 200, "turn_index": 1},
            {"file_path": "c.py", "lines_read": 300, "turn_index": 2},
        ])

        assert result["avg_lines_per_read"] == 200.0

    def test_examples_limited_to_three_per_bucket(self):
        """Verify examples are limited to 3 per bucket."""
        records = [
            {"file_path": f"file{i}.py", "lines_read": 30, "turn_index": i}
            for i in range(10)
        ]

        result = analyze_session_file_read_size_distribution(records)
        assert len(result["examples"]["under_50"]) == 3

    def test_examples_contain_expected_fields(self):
        """Verify example structure."""
        result = analyze_session_file_read_size_distribution([
            {
                "file_path": "src/foo.py",
                "lines_read": 30,
                "turn_index": 5,
            }
        ])

        example = result["examples"]["under_50"][0]
        assert example["file_path"] == "src/foo.py"
        assert example["lines_read"] == 30
        assert example["turn_index"] == 5

    def test_malformed_record_without_mapping_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_file_read_size_distribution([
            "not a dict",
            {"file_path": "foo.py", "lines_read": 30, "turn_index": 0},
        ])

        assert result["total_reads"] == 1

    def test_record_with_missing_lines_read_skipped(self):
        """Verify records without lines_read are skipped."""
        result = analyze_session_file_read_size_distribution([
            {"file_path": "foo.py", "turn_index": 0},
            {"file_path": "bar.py", "lines_read": 30, "turn_index": 1},
        ])

        assert result["total_reads"] == 1

    def test_record_with_negative_lines_read_skipped(self):
        """Verify records with negative lines_read are skipped."""
        result = analyze_session_file_read_size_distribution([
            {"file_path": "foo.py", "lines_read": -10, "turn_index": 0},
            {"file_path": "bar.py", "lines_read": 30, "turn_index": 1},
        ])

        assert result["total_reads"] == 1

    def test_record_with_zero_lines_read_counted(self):
        """Verify records with zero lines_read are counted."""
        result = analyze_session_file_read_size_distribution([
            {"file_path": "empty.py", "lines_read": 0, "turn_index": 0},
        ])

        assert result["total_reads"] == 1
        assert result["bucket_counts"]["under_50"] == 1

    def test_lines_read_as_string_converted(self):
        """Verify lines_read as string is converted to int."""
        result = analyze_session_file_read_size_distribution([
            {"file_path": "foo.py", "lines_read": "100", "turn_index": 0},
        ])

        assert result["total_reads"] == 1
        assert result["bucket_counts"]["50_to_200"] == 1

    def test_lines_read_as_float_converted(self):
        """Verify lines_read as float is converted to int."""
        result = analyze_session_file_read_size_distribution([
            {"file_path": "foo.py", "lines_read": 100.7, "turn_index": 0},
        ])

        assert result["total_reads"] == 1
        assert result["avg_lines_per_read"] == 100.0

    def test_missing_file_path_uses_default(self):
        """Verify missing file_path uses 'unknown' in examples."""
        result = analyze_session_file_read_size_distribution([
            {"lines_read": 30, "turn_index": 0},
        ])

        example = result["examples"]["under_50"][0]
        assert example["file_path"] == "unknown"

    def test_missing_turn_index_uses_record_index(self):
        """Verify missing turn_index uses record index."""
        result = analyze_session_file_read_size_distribution([
            {"file_path": "foo.py", "lines_read": 30},
        ])

        example = result["examples"]["under_50"][0]
        assert example["turn_index"] == 0


class TestCategorizeBucket:
    """Test bucket categorization helper."""

    def test_zero_lines_under_50(self):
        """Verify 0 lines is categorized as under_50."""
        assert _categorize_bucket(0) == "under_50"

    def test_49_lines_under_50(self):
        """Verify 49 lines is categorized as under_50."""
        assert _categorize_bucket(49) == "under_50"

    def test_50_lines_is_50_to_200(self):
        """Verify 50 lines is categorized as 50_to_200."""
        assert _categorize_bucket(50) == "50_to_200"

    def test_199_lines_is_50_to_200(self):
        """Verify 199 lines is categorized as 50_to_200."""
        assert _categorize_bucket(199) == "50_to_200"

    def test_200_lines_is_200_to_500(self):
        """Verify 200 lines is categorized as 200_to_500."""
        assert _categorize_bucket(200) == "200_to_500"

    def test_499_lines_is_200_to_500(self):
        """Verify 499 lines is categorized as 200_to_500."""
        assert _categorize_bucket(499) == "200_to_500"

    def test_500_lines_is_over_500(self):
        """Verify 500 lines is categorized as over_500."""
        assert _categorize_bucket(500) == "over_500"

    def test_1000_lines_is_over_500(self):
        """Verify 1000 lines is categorized as over_500."""
        assert _categorize_bucket(1000) == "over_500"


class TestPercentage:
    """Test percentage calculation helper."""

    def test_zero_denominator_returns_zero(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(10, 0) == 0.0

    def test_negative_denominator_returns_zero(self):
        """Verify negative denominator returns 0.0."""
        assert _percentage(10, -5) == 0.0

    def test_zero_numerator_returns_zero(self):
        """Verify zero numerator returns 0.0."""
        assert _percentage(0, 10) == 0.0

    def test_equal_numerator_and_denominator_returns_100(self):
        """Verify equal values return 100.0."""
        assert _percentage(10, 10) == 100.0

    def test_half_returns_50(self):
        """Verify half returns 50.0."""
        assert _percentage(5, 10) == 50.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _percentage(1, 3) == 33.33


class TestAverage:
    """Test average calculation helper."""

    def test_zero_count_returns_zero(self):
        """Verify zero count returns 0.0."""
        assert _average(100, 0) == 0.0

    def test_negative_count_returns_zero(self):
        """Verify negative count returns 0.0."""
        assert _average(100, -5) == 0.0

    def test_simple_average(self):
        """Verify simple average calculation."""
        assert _average(100, 4) == 25.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _average(10, 3) == 3.33


class TestClassifyEfficiencyPattern:
    """Test efficiency pattern classification."""

    def test_empty_buckets_returns_empty(self):
        """Verify empty buckets return 'empty' pattern."""
        buckets = {
            "under_50": 0,
            "50_to_200": 0,
            "200_to_500": 0,
            "over_500": 0,
        }
        assert _classify_efficiency_pattern(buckets, 0) == "empty"

    def test_highly_targeted_pattern(self):
        """Verify highly targeted pattern for >75% small reads."""
        buckets = {
            "under_50": 8,
            "50_to_200": 1,
            "200_to_500": 0,
            "over_500": 1,
        }
        assert _classify_efficiency_pattern(buckets, 10) == "highly_targeted"

    def test_moderately_targeted_pattern(self):
        """Verify moderately targeted pattern."""
        buckets = {
            "under_50": 5,
            "50_to_200": 3,
            "200_to_500": 1,
            "over_500": 1,
        }
        assert _classify_efficiency_pattern(buckets, 10) == "moderately_targeted"

    def test_full_file_heavy_pattern(self):
        """Verify full_file_heavy pattern for >50% large reads."""
        buckets = {
            "under_50": 2,
            "50_to_200": 2,
            "200_to_500": 0,
            "over_500": 6,
        }
        assert _classify_efficiency_pattern(buckets, 10) == "full_file_heavy"

    def test_mixed_pattern(self):
        """Verify mixed pattern for balanced distribution."""
        buckets = {
            "under_50": 3,
            "50_to_200": 3,
            "200_to_500": 2,
            "over_500": 2,
        }
        assert _classify_efficiency_pattern(buckets, 10) == "mixed"

    def test_boundary_at_75_percent_small(self):
        """Verify boundary at exactly 75% small reads."""
        # Exactly 75% should be highly_targeted (> threshold uses >, not >=)
        # So we test just above threshold
        buckets = {
            "under_50": 76,
            "50_to_200": 24,
            "200_to_500": 0,
            "over_500": 0,
        }
        assert _classify_efficiency_pattern(buckets, 100) == "highly_targeted"

    def test_boundary_just_below_75_percent_small(self):
        """Verify just below 75% small reads is moderately_targeted."""
        buckets = {
            "under_50": 74,
            "50_to_200": 10,
            "200_to_500": 10,
            "over_500": 6,
        }
        pattern = _classify_efficiency_pattern(buckets, 100)
        # 74% small + 10% medium = 84% small+medium > 75%
        assert pattern == "moderately_targeted"


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_optimization_mode_baseline_session(self):
        """Simulate baseline mode with many full file re-reads."""
        records = [
            {"file_path": "src/main.py", "lines_read": 500, "turn_index": 0},
            {"file_path": "src/main.py", "lines_read": 500, "turn_index": 1},
            {"file_path": "src/utils.py", "lines_read": 300, "turn_index": 2},
            {"file_path": "src/main.py", "lines_read": 500, "turn_index": 3},
            {"file_path": "tests/test.py", "lines_read": 400, "turn_index": 4},
        ]

        result = analyze_session_file_read_size_distribution(records)
        assert result["efficiency_pattern"] == "full_file_heavy"
        assert result["targeted_read_count"] == 0
        assert result["avg_lines_per_read"] == 440.0

    def test_optimization_mode_optimized_session(self):
        """Simulate optimized mode with targeted reads."""
        records = [
            {"file_path": "src/main.py", "lines_read": 30, "offset": 470, "limit": 30, "turn_index": 0},
            {"file_path": "src/main.py", "lines_read": 40, "offset": 460, "limit": 40, "turn_index": 1},
            {"file_path": "src/utils.py", "lines_read": 20, "offset": 280, "limit": 20, "turn_index": 2},
            {"file_path": "src/main.py", "lines_read": 30, "offset": 450, "limit": 30, "turn_index": 3},
            {"file_path": "tests/test.py", "lines_read": 25, "offset": 375, "limit": 25, "turn_index": 4},
        ]

        result = analyze_session_file_read_size_distribution(records)
        assert result["efficiency_pattern"] == "highly_targeted"
        assert result["targeted_read_count"] == 5
        assert result["targeted_read_percentage"] == 100.0
        assert result["avg_lines_per_read"] == 29.0

    def test_mixed_optimization_session(self):
        """Simulate session with mix of targeted and full reads."""
        records = [
            {"file_path": "src/main.py", "lines_read": 500, "turn_index": 0},  # Initial read
            {"file_path": "src/main.py", "lines_read": 30, "offset": 470, "limit": 30, "turn_index": 1},
            {"file_path": "src/utils.py", "lines_read": 300, "turn_index": 2},  # Initial read
            {"file_path": "src/utils.py", "lines_read": 20, "offset": 280, "limit": 20, "turn_index": 3},
            {"file_path": "src/main.py", "lines_read": 30, "offset": 450, "limit": 30, "turn_index": 4},
        ]

        result = analyze_session_file_read_size_distribution(records)
        assert result["targeted_read_count"] == 3
        assert result["targeted_read_percentage"] == 60.0
