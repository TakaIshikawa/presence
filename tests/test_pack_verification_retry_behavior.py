"""Tests for pack verification retry behavior analyzer."""

import pytest

from synthesis.pack_verification_retry_behavior import (
    analyze_pack_verification_retry_behavior,
)


class TestAnalyzePackVerificationRetryBehavior:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_verification_retry_behavior([])

        assert result["total_packs"] == 0
        assert result["avg_verification_attempts"] == 0.0
        assert result["avg_first_attempt_success_rate"] == 0.0
        assert result["avg_retry_count"] == 0.0
        assert result["avg_retry_success_rate"] == 0.0
        assert result["avg_retries_per_verification"] == 0.0
        assert result["avg_excessive_retry_rate"] == 0.0
        assert result["avg_retry_time_seconds"] == 0.0
        assert result["high_first_attempt_packs"] == 0
        assert result["low_first_attempt_packs"] == 0
        assert result["packs_with_excessive_retries"] == 0
        assert result["efficient_retry_packs"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_verification_retry_behavior(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_verification_retry_behavior("not a list")

    def test_all_first_attempt_success(self):
        """Verify pack where all verifications pass on first attempt."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "total_verification_attempts": 10,
                "first_attempt_successes": 10,
                "first_attempt_failures": 0,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_first_attempt_success_rate"] == 100.0
        assert result["high_first_attempt_packs"] == 1

    def test_high_first_attempt_success_rate(self):
        """Verify detection of high first-attempt success packs."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "first_attempt_successes": 18,
                "first_attempt_failures": 2,
            }
        ])

        # 18 / 20 = 90%
        assert result["avg_first_attempt_success_rate"] == 90.0
        assert result["high_first_attempt_packs"] == 1

    def test_low_first_attempt_success_rate(self):
        """Verify detection of low first-attempt success packs."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "first_attempt_successes": 10,
                "first_attempt_failures": 10,
            }
        ])

        # 10 / 20 = 50%
        assert result["avg_first_attempt_success_rate"] == 50.0
        assert result["low_first_attempt_packs"] == 1

    def test_retry_success_rate(self):
        """Verify calculation of retry success rate."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "successful_retries": 14,
                "failed_retries": 6,
            }
        ])

        # 14 / 20 = 70%
        assert result["avg_retry_success_rate"] == 70.0

    def test_retries_per_verification_tracking(self):
        """Verify tracking of average retries per verification."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "avg_retries_per_verification": 1.2,
            }
        ])

        assert result["avg_retries_per_verification"] == 1.2
        assert result["efficient_retry_packs"] == 1

    def test_inefficient_retry_packs(self):
        """Verify detection of packs with high retry rates."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "avg_retries_per_verification": 2.5,
            }
        ])

        assert result["avg_retries_per_verification"] == 2.5
        assert result["efficient_retry_packs"] == 0

    def test_excessive_retry_detection(self):
        """Verify detection of excessive retry cases."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "total_verification_attempts": 20,
                "excessive_retry_count": 3,
            }
        ])

        # 3 / 20 = 15%
        assert result["avg_excessive_retry_rate"] == 15.0
        assert result["packs_with_excessive_retries"] == 1

    def test_no_excessive_retries(self):
        """Verify packs with no excessive retries."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "total_verification_attempts": 20,
                "excessive_retry_count": 0,
            }
        ])

        assert result["avg_excessive_retry_rate"] == 0.0
        assert result["packs_with_excessive_retries"] == 0

    def test_retry_time_tracking(self):
        """Verify tracking of time spent on retries."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "total_retry_time_seconds": 120.5,
            }
        ])

        assert result["avg_retry_time_seconds"] == 120.5

    def test_multiple_packs_averaged(self):
        """Verify metrics averaged across multiple packs."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "first_attempt_successes": 8,
                "first_attempt_failures": 2,
                "avg_retries_per_verification": 1.0,
            },
            {
                "pack_id": "pack2",
                "first_attempt_successes": 18,
                "first_attempt_failures": 2,
                "avg_retries_per_verification": 1.2,
            },
        ])

        assert result["total_packs"] == 2
        # (80% + 90%) / 2 = 85%
        assert result["avg_first_attempt_success_rate"] == 85.0
        # (1.0 + 1.2) / 2 = 1.1
        assert result["avg_retries_per_verification"] == 1.1

    def test_boundary_first_attempt_classification(self):
        """Verify boundary cases for first-attempt classification."""
        result = analyze_pack_verification_retry_behavior([
            # Exactly 85% (should not be high)
            {
                "pack_id": "p1",
                "first_attempt_successes": 17,
                "first_attempt_failures": 3,
            },
            # Just above 85% (should be high)
            {
                "pack_id": "p2",
                "first_attempt_successes": 18,
                "first_attempt_failures": 2,
            },
            # Exactly 60% (should not be low)
            {
                "pack_id": "p3",
                "first_attempt_successes": 12,
                "first_attempt_failures": 8,
            },
            # Just below 60% (should be low)
            {
                "pack_id": "p4",
                "first_attempt_successes": 11,
                "first_attempt_failures": 9,
            },
        ])

        # >85% means strictly greater
        assert result["high_first_attempt_packs"] == 1
        # <60% means strictly less
        assert result["low_first_attempt_packs"] == 1

    def test_boundary_efficient_retry_classification(self):
        """Verify boundary cases for efficient retry classification."""
        result = analyze_pack_verification_retry_behavior([
            # Exactly 1.5 (should not be efficient)
            {
                "pack_id": "p1",
                "avg_retries_per_verification": 1.5,
            },
            # Just below 1.5 (should be efficient)
            {
                "pack_id": "p2",
                "avg_retries_per_verification": 1.4,
            },
        ])

        # <1.5 means strictly less
        assert result["efficient_retry_packs"] == 1

    def test_comprehensive_pack_all_fields(self):
        """Verify comprehensive pack with all fields populated."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "comprehensive",
                "pack_title": "Test Pack",
                "total_verification_attempts": 50,
                "first_attempt_successes": 42,
                "first_attempt_failures": 8,
                "total_retries": 15,
                "successful_retries": 12,
                "failed_retries": 3,
                "excessive_retry_count": 2,
                "avg_retries_per_verification": 1.3,
                "total_retry_time_seconds": 95.5,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_verification_attempts"] == 50.0
        # 42 / 50 = 84%
        assert result["avg_first_attempt_success_rate"] == 84.0
        assert result["avg_retry_count"] == 15.0
        # 12 / 15 = 80%
        assert result["avg_retry_success_rate"] == 80.0
        assert result["avg_retries_per_verification"] == 1.3
        # 2 / 50 = 4%
        assert result["avg_excessive_retry_rate"] == 4.0
        assert result["avg_retry_time_seconds"] == 95.5
        assert result["high_first_attempt_packs"] == 0  # 84% is not >85%
        assert result["packs_with_excessive_retries"] == 1
        assert result["efficient_retry_packs"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_verification_retry_behavior([
            "not a dict",
            {
                "pack_id": "pack1",
                "total_verification_attempts": 10,
            },
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "total_verification_attempts": True,
                "first_attempt_successes": False,
            }
        ])

        assert result["avg_verification_attempts"] == 0.0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "total_verification_attempts": 20,
                # Missing most fields
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_verification_attempts"] == 20.0

    def test_zero_attempts_no_division_error(self):
        """Verify zero attempts doesn't cause division errors."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "total_verification_attempts": 0,
                "excessive_retry_count": 0,
            }
        ])

        assert result["avg_excessive_retry_rate"] == 0.0

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_pack_verification_retry_behavior([
            {
                "pack_id": "pack1",
                "total_verification_attempts": 20.5,
                "avg_retries_per_verification": 1.35,
            }
        ])

        assert result["avg_verification_attempts"] == 20.5
        assert result["avg_retries_per_verification"] == 1.35
