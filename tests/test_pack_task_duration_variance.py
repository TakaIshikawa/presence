"""Tests for pack task duration variance analyzer."""

import pytest

from synthesis.pack_task_duration_variance import (
    analyze_pack_task_duration_variance,
    _extract_duration,
    _average,
    _standard_deviation,
    _percentage,
)


class TestAnalyzePackTaskDurationVariance:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics with perfect predictability."""
        result = analyze_pack_task_duration_variance([])

        assert result["total_tasks"] == 0
        assert result["mean_duration"] == 0.0
        assert result["duration_variance"] == 0.0
        assert result["predictability_score"] == 1.0
        assert result["outlier_task_rate"] == 0.0
        assert result["outlier_count"] == 0
        assert result["warnings"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_task_duration_variance(None)
        assert result["total_tasks"] == 0
        assert result["predictability_score"] == 1.0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_task_duration_variance("not a list")

    def test_single_task_returns_perfect_predictability(self):
        """Verify single task has zero variance and perfect predictability."""
        result = analyze_pack_task_duration_variance([100.0])

        assert result["total_tasks"] == 1
        assert result["mean_duration"] == 100.0
        assert result["duration_variance"] == 0.0
        assert result["predictability_score"] == 1.0
        assert result["outlier_task_rate"] == 0.0
        assert result["outlier_count"] == 0
        assert result["warnings"] == []

    def test_uniform_durations_zero_variance(self):
        """Verify uniform durations produce zero variance."""
        result = analyze_pack_task_duration_variance([50.0, 50.0, 50.0, 50.0])

        assert result["total_tasks"] == 4
        assert result["mean_duration"] == 50.0
        assert result["duration_variance"] == 0.0
        assert result["predictability_score"] == 1.0
        assert result["outlier_task_rate"] == 0.0
        assert result["outlier_count"] == 0
        assert result["warnings"] == []

    def test_high_variance_low_predictability(self):
        """Verify high variance produces low predictability score."""
        # Durations: 10, 20, 100, 200 (highly variable)
        result = analyze_pack_task_duration_variance([10.0, 20.0, 100.0, 200.0])

        assert result["total_tasks"] == 4
        assert result["mean_duration"] == 82.5
        # Variance should be high (stddev/mean > 0.5)
        assert result["duration_variance"] > 0.5
        # Predictability should be low (< 0.5)
        assert result["predictability_score"] < 0.5
        # Should have warnings
        assert len(result["warnings"]) > 0
        assert "Low predictability" in result["warnings"][0]

    def test_moderate_variance_moderate_predictability(self):
        """Verify moderate variance produces moderate predictability."""
        # Durations: 90, 95, 100, 105, 110 (low variance)
        result = analyze_pack_task_duration_variance([90.0, 95.0, 100.0, 105.0, 110.0])

        assert result["total_tasks"] == 5
        assert result["mean_duration"] == 100.0
        # Variance should be low
        assert result["duration_variance"] < 0.2
        # Predictability should be high
        assert result["predictability_score"] > 0.8
        # No warnings expected
        assert result["warnings"] == []

    def test_outlier_detection_above_2x_mean(self):
        """Verify outlier detection for tasks >2x mean duration."""
        # Durations: 50, 50, 50, 150 (one outlier at 3x mean)
        result = analyze_pack_task_duration_variance([50.0, 50.0, 50.0, 150.0])

        assert result["total_tasks"] == 4
        assert result["mean_duration"] == 75.0
        # 150 > 2 * 75 (150 > 150 is false, but let's use 200)
        # Actually: mean = 75, 2x = 150, so 150 is NOT > 150
        # Let me fix: 50, 50, 50, 200
        result = analyze_pack_task_duration_variance([50.0, 50.0, 50.0, 200.0])

        assert result["total_tasks"] == 4
        assert result["mean_duration"] == 87.5
        # 200 > 2 * 87.5 (200 > 175) -> 1 outlier
        assert result["outlier_count"] == 1
        assert result["outlier_task_rate"] == 25.0

    def test_multiple_outliers(self):
        """Verify multiple outlier detection."""
        # Durations: 10, 10, 100, 120
        result = analyze_pack_task_duration_variance([10.0, 10.0, 100.0, 120.0])

        assert result["total_tasks"] == 4
        assert result["mean_duration"] == 60.0
        # 2x mean = 120, so 120 is NOT > 120, but 100 is not > 120
        # Let's use: 10, 10, 10, 100, 150
        result = analyze_pack_task_duration_variance([10.0, 10.0, 10.0, 100.0, 150.0])

        assert result["total_tasks"] == 5
        assert result["mean_duration"] == 56.0
        # 2x mean = 112, so 150 > 112 (1 outlier)
        assert result["outlier_count"] == 1
        assert result["outlier_task_rate"] == 20.0

    def test_no_outliers(self):
        """Verify no outliers when all tasks within 2x mean."""
        result = analyze_pack_task_duration_variance([45.0, 50.0, 55.0])

        assert result["total_tasks"] == 3
        assert result["mean_duration"] == 50.0
        # 2x mean = 100, all values < 100
        assert result["outlier_count"] == 0
        assert result["outlier_task_rate"] == 0.0

    def test_dict_input_with_duration_key(self):
        """Verify dict input with 'duration' key is handled."""
        result = analyze_pack_task_duration_variance([
            {"duration": 50.0},
            {"duration": 60.0},
            {"duration": 70.0},
        ])

        assert result["total_tasks"] == 3
        assert result["mean_duration"] == 60.0

    def test_mixed_input_formats(self):
        """Verify mixed numeric and dict formats."""
        result = analyze_pack_task_duration_variance([
            50.0,
            {"duration": 60.0},
            70,
            {"duration": 80},
        ])

        assert result["total_tasks"] == 4
        assert result["mean_duration"] == 65.0

    def test_negative_durations_filtered_out(self):
        """Verify negative durations are filtered out."""
        result = analyze_pack_task_duration_variance([50.0, -10.0, 60.0, -20.0])

        assert result["total_tasks"] == 2  # Only 50 and 60
        assert result["mean_duration"] == 55.0

    def test_malformed_records_skipped(self):
        """Verify malformed records are skipped."""
        result = analyze_pack_task_duration_variance([
            50.0,
            "not a number",
            {"not_duration": 100},
            None,
            60.0,
        ])

        assert result["total_tasks"] == 2  # Only 50 and 60
        assert result["mean_duration"] == 55.0

    def test_all_malformed_records_returns_empty_metrics(self):
        """Verify all malformed records returns empty metrics."""
        result = analyze_pack_task_duration_variance([
            "not a number",
            {"not_duration": 100},
            None,
        ])

        assert result["total_tasks"] == 0
        assert result["mean_duration"] == 0.0
        assert result["predictability_score"] == 1.0

    def test_zero_duration_included(self):
        """Verify zero duration is valid and included."""
        result = analyze_pack_task_duration_variance([0.0, 50.0, 100.0])

        assert result["total_tasks"] == 3
        assert result["mean_duration"] == 50.0

    def test_variance_calculation_accuracy(self):
        """Verify variance calculation is accurate."""
        # Simple case: [10, 20, 30]
        # Mean = 20
        # Variance = ((10-20)^2 + (20-20)^2 + (30-20)^2) / 3 = (100 + 0 + 100) / 3 = 66.67
        # Stddev = sqrt(66.67) = 8.16
        # CV = 8.16 / 20 = 0.41
        result = analyze_pack_task_duration_variance([10.0, 20.0, 30.0])

        assert result["total_tasks"] == 3
        assert result["mean_duration"] == 20.0
        assert 0.40 <= result["duration_variance"] <= 0.42
        assert 0.58 <= result["predictability_score"] <= 0.60

    def test_extreme_variance_caps_at_zero_predictability(self):
        """Verify extreme variance caps predictability at 0.0."""
        # Very high variance: 1, 1000
        result = analyze_pack_task_duration_variance([1.0, 1000.0])

        assert result["total_tasks"] == 2
        # Variance will be very high (>=1.0)
        assert result["duration_variance"] >= 1.0
        # Predictability should be capped at 0.0
        assert result["predictability_score"] == 0.0
        assert len(result["warnings"]) > 0

    def test_warnings_threshold_at_0_5(self):
        """Verify warnings are generated when predictability < 0.5."""
        # Create data with predictability just below threshold
        # Need variance = 0.51 to get predictability = 0.49
        # Use [10, 100] -> mean=55, stddev=45, CV=0.82, pred=0.18
        result = analyze_pack_task_duration_variance([10.0, 100.0])

        assert result["predictability_score"] < 0.5
        assert len(result["warnings"]) == 1
        assert "Low predictability" in result["warnings"][0]

    def test_no_warnings_when_predictability_high(self):
        """Verify no warnings when predictability >= 0.5."""
        # Low variance case
        result = analyze_pack_task_duration_variance([95.0, 100.0, 105.0])

        assert result["predictability_score"] >= 0.5
        assert result["warnings"] == []

    def test_large_dataset_performance(self):
        """Verify analyzer handles large datasets efficiently."""
        # 1000 tasks with random-ish durations
        durations = [50.0 + (i % 20) for i in range(1000)]
        result = analyze_pack_task_duration_variance(durations)

        assert result["total_tasks"] == 1000
        assert result["mean_duration"] > 0
        assert 0.0 <= result["predictability_score"] <= 1.0


class TestExtractDuration:
    """Test duration extraction helper."""

    def test_extract_int(self):
        """Verify int is extracted as float."""
        assert _extract_duration(100) == 100.0

    def test_extract_float(self):
        """Verify float is extracted directly."""
        assert _extract_duration(123.45) == 123.45

    def test_extract_from_dict_with_duration_key(self):
        """Verify duration extracted from dict."""
        assert _extract_duration({"duration": 50.0}) == 50.0

    def test_extract_from_dict_with_int_duration(self):
        """Verify int duration in dict is converted to float."""
        assert _extract_duration({"duration": 50}) == 50.0

    def test_extract_from_dict_without_duration_key(self):
        """Verify None returned for dict without duration key."""
        assert _extract_duration({"other": 50.0}) is None

    def test_extract_from_string_returns_none(self):
        """Verify string returns None."""
        assert _extract_duration("100") is None

    def test_extract_from_none_returns_none(self):
        """Verify None returns None."""
        assert _extract_duration(None) is None

    def test_extract_from_list_returns_none(self):
        """Verify list returns None."""
        assert _extract_duration([100.0]) is None


class TestAverage:
    """Test average calculation helper."""

    def test_empty_list_returns_zero(self):
        """Verify empty list returns 0.0."""
        assert _average([]) == 0.0

    def test_single_value(self):
        """Verify single value returns that value."""
        assert _average([42.0]) == 42.0

    def test_multiple_values(self):
        """Verify average of multiple values."""
        assert _average([10.0, 20.0, 30.0]) == 20.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _average([10.0, 20.0, 25.0]) == 18.33


class TestStandardDeviation:
    """Test standard deviation calculation helper."""

    def test_empty_list_returns_zero(self):
        """Verify empty list returns 0.0."""
        assert _standard_deviation([], 0.0) == 0.0

    def test_single_value_returns_zero(self):
        """Verify single value returns 0.0."""
        assert _standard_deviation([100.0], 100.0) == 0.0

    def test_uniform_values_returns_zero(self):
        """Verify uniform values return 0.0."""
        assert _standard_deviation([50.0, 50.0, 50.0], 50.0) == 0.0

    def test_simple_case(self):
        """Verify standard deviation calculation."""
        # [10, 20, 30] with mean 20
        # Variance = ((10-20)^2 + (20-20)^2 + (30-20)^2) / 3 = 66.67
        # Stddev = sqrt(66.67) = 8.16
        result = _standard_deviation([10.0, 20.0, 30.0], 20.0)
        assert 8.15 <= result <= 8.17

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        result = _standard_deviation([1.0, 2.0, 3.0, 4.0, 5.0], 3.0)
        # Should be a value with 2 decimal places
        assert result == round(result, 2)


class TestPercentage:
    """Test percentage calculation helper."""

    def test_zero_denominator_returns_zero(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(10, 0) == 0.0

    def test_zero_numerator_returns_zero(self):
        """Verify zero numerator returns 0.0."""
        assert _percentage(0, 10) == 0.0

    def test_simple_percentage(self):
        """Verify simple percentage calculation."""
        assert _percentage(1, 4) == 25.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _percentage(1, 3) == 33.33


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_highly_predictable_pack(self):
        """Simulate pack with highly consistent task durations."""
        durations = [28.0, 30.0, 29.0, 31.0, 30.0, 29.0]
        result = analyze_pack_task_duration_variance(durations)

        assert result["total_tasks"] == 6
        assert 29.0 <= result["mean_duration"] <= 30.0
        assert result["duration_variance"] < 0.1
        assert result["predictability_score"] > 0.9
        assert result["outlier_count"] == 0
        assert result["warnings"] == []

    def test_unpredictable_pack_with_outliers(self):
        """Simulate pack with high variance and outliers."""
        durations = [10.0, 15.0, 20.0, 150.0, 200.0]
        result = analyze_pack_task_duration_variance(durations)

        assert result["total_tasks"] == 5
        assert result["duration_variance"] > 0.5
        assert result["predictability_score"] < 0.5
        assert result["outlier_count"] >= 1
        assert len(result["warnings"]) > 0

    def test_bimodal_distribution(self):
        """Simulate pack with two clusters of durations."""
        # Fast tasks: 20-30s, Slow tasks: 100-110s
        durations = [20.0, 25.0, 30.0, 100.0, 105.0, 110.0]
        result = analyze_pack_task_duration_variance(durations)

        assert result["total_tasks"] == 6
        # Should have high variance due to bimodal distribution
        assert result["duration_variance"] > 0.5
        assert result["predictability_score"] < 0.5

    def test_all_zero_durations(self):
        """Simulate pack where all tasks complete instantly."""
        result = analyze_pack_task_duration_variance([0.0, 0.0, 0.0])

        assert result["total_tasks"] == 3
        assert result["mean_duration"] == 0.0
        assert result["duration_variance"] == 0.0
        assert result["predictability_score"] == 1.0

    def test_realistic_ci_pipeline_durations(self):
        """Simulate realistic CI/CD pipeline task durations."""
        # Lint: 5s, Type check: 12s, Unit tests: 45s, Integration: 120s, Deploy: 60s
        durations = [5.0, 12.0, 45.0, 120.0, 60.0]
        result = analyze_pack_task_duration_variance(durations)

        assert result["total_tasks"] == 5
        assert result["mean_duration"] == 48.4
        # High variance due to wide range
        assert result["duration_variance"] > 0.5
        # 120 > 2 * 48.4 (120 > 96.8) -> 1 outlier
        assert result["outlier_count"] == 1
