"""Tests for pack verification timing distribution analyzer."""

import pytest

from synthesis.pack_verification_timing_distribution import (
    analyze_pack_verification_timing_distribution,
    _percentage,
    _average,
    _calculate_correlation_from_pairs,
)


class TestAnalyzePackVerificationTimingDistribution:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_verification_timing_distribution([])

        assert result["total_packs"] == 0
        assert result["avg_verification_per_task"] == 0.0
        assert result["avg_verification_delay"] == 0.0
        assert result["immediate_verification_rate"] == 0.0
        assert result["batched_verification_rate"] == 0.0
        assert result["error_triggered_rate"] == 0.0
        assert result["delayed_verification_rate"] == 0.0
        assert result["timing_strategy_distribution"] == []
        assert result["avg_fix_iterations"] == 0.0
        assert result["verification_delay_correlation"] == 0.0
        assert result["high_immediate_packs"] == 0
        assert result["delayed_packs"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_verification_timing_distribution(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_verification_timing_distribution("not a list")

    def test_immediate_verification_detected(self):
        """Verify immediate verification (within 1 minute) is detected."""
        result = analyze_pack_verification_timing_distribution([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "events": [
                            {"event_type": "edit", "timestamp": 100},
                            {"event_type": "verification", "timestamp": 130, "details": {}},
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_verification_per_task"] == 1.0
        assert result["avg_verification_delay"] == 30.0  # 130 - 100
        assert result["immediate_verification_rate"] == 100.0
        assert result["high_immediate_packs"] == 1

    def test_batched_verification_detected(self):
        """Verify batched verification (multiple edits before verification) is detected."""
        result = analyze_pack_verification_timing_distribution([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "events": [
                            {"event_type": "edit", "timestamp": 100},
                            {"event_type": "edit", "timestamp": 150},
                            {"event_type": "edit", "timestamp": 200},
                            {"event_type": "verification", "timestamp": 280, "details": {}},
                        ]
                    }
                ]
            }
        ])

        # Delay from last edit: 280 - 200 = 80 seconds (within 1-5 min range)
        # Multiple edits in window = batched
        assert result["batched_verification_rate"] == 100.0

    def test_delayed_verification_detected(self):
        """Verify delayed verification (>5 minutes) is detected."""
        result = analyze_pack_verification_timing_distribution([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "events": [
                            {"event_type": "edit", "timestamp": 100},
                            {"event_type": "verification", "timestamp": 500, "details": {}},
                        ]
                    }
                ]
            }
        ])

        # Delay: 500 - 100 = 400 seconds (>5 minutes)
        assert result["avg_verification_delay"] == 400.0
        assert result["delayed_verification_rate"] == 100.0
        assert result["delayed_packs"] == 1

    def test_error_triggered_verification_detected(self):
        """Verify error-triggered verification is detected."""
        result = analyze_pack_verification_timing_distribution([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "events": [
                            {"event_type": "edit", "timestamp": 100},
                            {"event_type": "error", "timestamp": 150},
                            {"event_type": "verification", "timestamp": 160, "details": {}},
                        ]
                    }
                ]
            }
        ])

        # Verification within 60s of error = error-triggered
        assert result["error_triggered_rate"] == 100.0

    def test_mixed_timing_patterns(self):
        """Verify mixed timing patterns are tracked correctly."""
        result = analyze_pack_verification_timing_distribution([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "events": [
                            {"event_type": "edit", "timestamp": 100},
                            {"event_type": "verification", "timestamp": 130, "details": {}},  # Immediate
                        ]
                    },
                    {
                        "task_id": "task2",
                        "events": [
                            {"event_type": "edit", "timestamp": 200},
                            {"event_type": "verification", "timestamp": 700, "details": {}},  # Delayed
                        ]
                    }
                ]
            }
        ])

        # 2 verifications: 1 immediate, 1 delayed
        assert result["immediate_verification_rate"] == 50.0
        assert result["delayed_verification_rate"] == 50.0

    def test_fix_iterations_tracked(self):
        """Verify fix iterations are tracked correctly."""
        result = analyze_pack_verification_timing_distribution([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "events": [
                            {"event_type": "edit", "timestamp": 100},
                            {"event_type": "error", "timestamp": 110},
                            {"event_type": "edit", "timestamp": 120},
                            {"event_type": "error", "timestamp": 130},
                            {"event_type": "edit", "timestamp": 140},
                            {"event_type": "verification", "timestamp": 150, "details": {}},
                        ]
                    }
                ]
            }
        ])

        # 2 errors = 2 fix iterations
        assert result["avg_fix_iterations"] == 2.0

    def test_verification_delay_correlation_negative(self):
        """Verify negative correlation (faster verification = fewer iterations)."""
        result = analyze_pack_verification_timing_distribution([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "events": [
                            {"event_type": "edit", "timestamp": 100},
                            {"event_type": "verification", "timestamp": 120, "details": {}},
                            {"event_type": "error", "timestamp": 130},
                        ]
                    },
                    {
                        "task_id": "task2",
                        "events": [
                            {"event_type": "edit", "timestamp": 200},
                            {"event_type": "verification", "timestamp": 600, "details": {}},
                            {"event_type": "error", "timestamp": 610},
                            {"event_type": "error", "timestamp": 620},
                            {"event_type": "error", "timestamp": 630},
                        ]
                    }
                ]
            }
        ])

        # Task1: delay=20, iterations=1
        # Task2: delay=400, iterations=3
        # Should show positive correlation (longer delay = more iterations)
        assert result["verification_delay_correlation"] > 0

    def test_multiple_packs_aggregation(self):
        """Verify metrics are correctly aggregated across multiple packs."""
        result = analyze_pack_verification_timing_distribution([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "events": [
                            {"event_type": "edit", "timestamp": 100},
                            {"event_type": "verification", "timestamp": 130, "details": {}},
                        ]
                    }
                ]
            },
            {
                "pack_id": "pack2",
                "tasks": [
                    {
                        "task_id": "task1",
                        "events": [
                            {"event_type": "edit", "timestamp": 200},
                            {"event_type": "verification", "timestamp": 700, "details": {}},
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 2
        # Pack1: immediate, Pack2: delayed
        assert result["high_immediate_packs"] == 1
        assert result["delayed_packs"] == 1

    def test_timing_strategy_distribution(self):
        """Verify timing strategy distribution is calculated correctly."""
        result = analyze_pack_verification_timing_distribution([
            {
                "pack_id": "pack1",
                "tasks": [
                    {
                        "task_id": "task1",
                        "events": [
                            {"event_type": "edit", "timestamp": 100},
                            {"event_type": "verification", "timestamp": 130, "details": {}},
                        ]
                    },
                    {
                        "task_id": "task2",
                        "events": [
                            {"event_type": "edit", "timestamp": 200},
                            {"event_type": "verification", "timestamp": 230, "details": {}},
                        ]
                    },
                    {
                        "task_id": "task3",
                        "events": [
                            {"event_type": "edit", "timestamp": 300},
                            {"event_type": "verification", "timestamp": 800, "details": {}},
                        ]
                    }
                ]
            }
        ])

        distribution = result["timing_strategy_distribution"]
        assert len(distribution) > 0

        # Find immediate strategy
        immediate = next((d for d in distribution if d["strategy"] == "immediate"), None)
        assert immediate is not None
        assert immediate["count"] == 2
        assert immediate["percentage"] == pytest.approx(66.67, abs=0.01)


class TestHelperFunctions:
    """Test helper functions."""

    def test_percentage_calculation(self):
        """Verify percentage calculation."""
        assert _percentage(50, 100) == 50.0
        assert _percentage(1, 3) == 33.33
        assert _percentage(0, 100) == 0.0

    def test_percentage_zero_denominator(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(50, 0) == 0.0

    def test_average_calculation(self):
        """Verify average calculation."""
        assert _average([1.0, 2.0, 3.0]) == 2.0
        assert _average([10.0, 20.0]) == 15.0
        assert _average([100.0]) == 100.0

    def test_average_empty_list(self):
        """Verify empty list returns 0.0."""
        assert _average([]) == 0.0

    def test_calculate_correlation_positive(self):
        """Verify positive correlation detection."""
        # As delay increases, iterations increase
        pairs = [(10.0, 1), (20.0, 2), (30.0, 3), (40.0, 4)]
        corr = _calculate_correlation_from_pairs(pairs)
        assert corr > 0.9  # Strong positive correlation

    def test_calculate_correlation_negative(self):
        """Verify negative correlation detection."""
        # As delay increases, iterations decrease
        pairs = [(10.0, 4), (20.0, 3), (30.0, 2), (40.0, 1)]
        corr = _calculate_correlation_from_pairs(pairs)
        assert corr < -0.9  # Strong negative correlation

    def test_calculate_correlation_zero(self):
        """Verify zero correlation detection."""
        # No relationship
        pairs = [(10.0, 2), (20.0, 2), (30.0, 2), (40.0, 2)]
        corr = _calculate_correlation_from_pairs(pairs)
        assert abs(corr) < 0.1  # Near zero

    def test_calculate_correlation_insufficient_data(self):
        """Verify insufficient data returns 0.0."""
        assert _calculate_correlation_from_pairs([]) == 0.0
        assert _calculate_correlation_from_pairs([(10.0, 1)]) == 0.0
