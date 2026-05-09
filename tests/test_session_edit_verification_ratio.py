"""Tests for session Edit-to-verification ratio analyzer."""

import pytest

from synthesis.session_edit_verification_ratio import (
    analyze_session_edit_verification_ratio,
)


class TestAnalyzeSessionEditVerificationRatio:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_edit_verification_ratio([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_edits"] == 0
        assert result["avg_edit_calls"] == 0.0
        assert result["avg_verification_activities"] == 0.0
        assert result["avg_edit_to_verification_ratio"] == 0.0
        assert result["avg_verifications_per_edit"] == 0.0
        assert result["avg_verify_after_edit_rate"] == 0.0
        assert result["avg_full_read_after_edit_rate"] == 0.0
        assert result["avg_targeted_read_after_edit_rate"] == 0.0
        assert result["high_discipline_sessions"] == 0
        assert result["low_discipline_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_edit_verification_ratio(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_edit_verification_ratio("not a list")

    def test_session_with_no_edits(self):
        """Verify session with zero Edit calls handled gracefully."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "session1",
                "total_edit_calls": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_edits"] == 0

    def test_high_discipline_low_ratio(self):
        """Verify high discipline with low edit-to-verification ratio."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "session1",
                "total_edit_calls": 10,
                "total_verification_activities": 20,
                "verify_after_edit_count": 9,
                "targeted_read_after_edit": 8,
            }
        ])

        assert result["sessions_with_edits"] == 1
        assert result["avg_edit_calls"] == 10.0
        assert result["avg_verification_activities"] == 20.0
        # 10 / 20 = 0.5 ratio
        assert result["avg_edit_to_verification_ratio"] == 0.5
        # 20 / 10 = 2.0 verifications per edit
        assert result["avg_verifications_per_edit"] == 2.0
        # 9 / 10 = 90%
        assert result["avg_verify_after_edit_rate"] == 90.0
        # 8 / 10 = 80%
        assert result["avg_targeted_read_after_edit_rate"] == 80.0
        assert result["high_discipline_sessions"] == 1
        assert result["low_discipline_sessions"] == 0

    def test_low_discipline_high_ratio(self):
        """Verify low discipline with high edit-to-verification ratio."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "session1",
                "total_edit_calls": 20,
                "total_verification_activities": 4,
                "verify_after_edit_count": 2,
            }
        ])

        # 20 / 4 = 5.0 ratio (poor discipline)
        assert result["avg_edit_to_verification_ratio"] == 5.0
        # 4 / 20 = 0.2 verifications per edit
        assert result["avg_verifications_per_edit"] == 0.2
        # 2 / 20 = 10%
        assert result["avg_verify_after_edit_rate"] == 10.0
        assert result["high_discipline_sessions"] == 0
        assert result["low_discipline_sessions"] == 1

    def test_balanced_verification_pattern(self):
        """Verify balanced edit-verification pattern."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "session1",
                "total_edit_calls": 20,
                "total_verification_activities": 10,
                "verify_after_edit_count": 15,
                "full_read_after_edit": 5,
                "targeted_read_after_edit": 12,
            }
        ])

        # 20 / 10 = 2.0 ratio (boundary)
        assert result["avg_edit_to_verification_ratio"] == 2.0
        # 10 / 20 = 0.5 verifications per edit
        assert result["avg_verifications_per_edit"] == 0.5
        # 15 / 20 = 75%
        assert result["avg_verify_after_edit_rate"] == 75.0
        # 5 / 20 = 25%
        assert result["avg_full_read_after_edit_rate"] == 25.0
        # 12 / 20 = 60%
        assert result["avg_targeted_read_after_edit_rate"] == 60.0

    def test_targeted_read_preferred_over_full_read(self):
        """Verify preference for targeted reads over full reads."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "session1",
                "total_edit_calls": 20,
                "full_read_after_edit": 2,
                "targeted_read_after_edit": 18,
            }
        ])

        # 2 / 20 = 10% full read
        assert result["avg_full_read_after_edit_rate"] == 10.0
        # 18 / 20 = 90% targeted read
        assert result["avg_targeted_read_after_edit_rate"] == 90.0

    def test_full_read_heavy_pattern(self):
        """Verify pattern relying heavily on full reads."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "session1",
                "total_edit_calls": 10,
                "full_read_after_edit": 9,
                "targeted_read_after_edit": 1,
            }
        ])

        # 9 / 10 = 90% full read
        assert result["avg_full_read_after_edit_rate"] == 90.0
        # 1 / 10 = 10% targeted read
        assert result["avg_targeted_read_after_edit_rate"] == 10.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "session1",
                "total_edit_calls": 10,
                "total_verification_activities": 20,
            },
            {
                "session_id": "session2",
                "total_edit_calls": 20,
                "total_verification_activities": 10,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_edits"] == 2
        # (10 + 20) / 2 = 15
        assert result["avg_edit_calls"] == 15.0
        # (20 + 10) / 2 = 15
        assert result["avg_verification_activities"] == 15.0
        # (10/20 + 20/10) / 2 = (0.5 + 2.0) / 2 = 1.25
        assert result["avg_edit_to_verification_ratio"] == 1.25

    def test_zero_verifications(self):
        """Verify handling of sessions with edits but no verifications."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "session1",
                "total_edit_calls": 10,
                "total_verification_activities": 0,
            }
        ])

        assert result["avg_edit_calls"] == 10.0
        assert result["avg_verification_activities"] == 0.0
        # No verifications, so no ratio calculated
        assert result["avg_edit_to_verification_ratio"] == 0.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_edit_verification_ratio([
            "not a dict",
            {
                "session_id": "session1",
                "total_edit_calls": 10,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for integer fields."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "session1",
                "total_edit_calls": True,
                "total_verification_activities": False,
            }
        ])

        assert result["sessions_with_edits"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "session1",
                "total_edit_calls": 10,
                "total_verification_activities": 5,
                # Missing verify_after, full_read, targeted_read
            }
        ])

        assert result["avg_edit_calls"] == 10.0
        # 10 / 5 = 2.0
        assert result["avg_edit_to_verification_ratio"] == 2.0
        # Missing fields result in 0.0 averages
        assert result["avg_verify_after_edit_rate"] == 0.0

    def test_boundary_discipline_classification(self):
        """Verify boundary cases for discipline classification."""
        result = analyze_session_edit_verification_ratio([
            # Exactly 2.0 (should not be high)
            {
                "session_id": "s1",
                "total_edit_calls": 10,
                "total_verification_activities": 5,
            },
            # Just below 2.0 (should be high)
            {
                "session_id": "s2",
                "total_edit_calls": 10,
                "total_verification_activities": 6,
            },
            # Exactly 4.0 (should not be low)
            {
                "session_id": "s3",
                "total_edit_calls": 20,
                "total_verification_activities": 5,
            },
            # Just above 4.0 (should be low)
            {
                "session_id": "s4",
                "total_edit_calls": 21,
                "total_verification_activities": 5,
            },
        ])

        # <2.0 means strictly less
        assert result["high_discipline_sessions"] == 1
        # >4.0 means strictly greater
        assert result["low_discipline_sessions"] == 1

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_edit_verification_ratio([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_edit_calls": 50,
                "total_verification_activities": 75,
                "verify_after_edit_count": 40,
                "full_read_after_edit": 15,
                "targeted_read_after_edit": 30,
            }
        ])

        assert result["sessions_with_edits"] == 1
        assert result["avg_edit_calls"] == 50.0
        assert result["avg_verification_activities"] == 75.0
        # 50 / 75 = 0.67 (approx)
        assert 0.66 <= result["avg_edit_to_verification_ratio"] <= 0.68
        # 75 / 50 = 1.5
        assert result["avg_verifications_per_edit"] == 1.5
        # 40 / 50 = 80%
        assert result["avg_verify_after_edit_rate"] == 80.0
        # 15 / 50 = 30%
        assert result["avg_full_read_after_edit_rate"] == 30.0
        # 30 / 50 = 60%
        assert result["avg_targeted_read_after_edit_rate"] == 60.0
        assert result["high_discipline_sessions"] == 1
