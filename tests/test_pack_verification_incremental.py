"""Tests for pack verification incremental retry analyzer."""

import pytest

from synthesis.pack_verification_incremental import analyze_pack_verification_incremental


class TestAnalyzePackVerificationIncremental:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_verification_incremental([])
        assert result["total_sessions"] == 0
        assert result["total_verifications"] == 0
        assert result["incremental_efficiency_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_verification_incremental(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_verification_incremental("not a list")

    def test_single_session_incremental_only(self):
        """Verify session with only incremental verifications."""
        result = analyze_pack_verification_incremental([
            {
                "session_id": "session1",
                "total_verifications": 5,
                "incremental_verifications": 5,
                "full_suite_verifications": 0,
                "incremental_time_seconds": 50.0,
                "full_suite_time_seconds": 0.0,
                "changed_files_count": 3,
                "verified_files_count": 3,
                "unnecessary_full_runs": 0,
            }
        ])

        assert result["total_verifications"] == 5
        assert result["incremental_ratio"] == 100.0
        assert result["verification_efficiency_ratio"] == 1.0
        assert result["sessions_using_incremental"] == 1

    def test_single_session_full_suite_only(self):
        """Verify session with only full-suite verifications."""
        result = analyze_pack_verification_incremental([
            {
                "session_id": "session1",
                "total_verifications": 3,
                "incremental_verifications": 0,
                "full_suite_verifications": 3,
                "incremental_time_seconds": 0.0,
                "full_suite_time_seconds": 90.0,
                "changed_files_count": 2,
                "verified_files_count": 10,
                "unnecessary_full_runs": 2,
            }
        ])

        assert result["full_suite_ratio"] == 100.0
        assert result["full_suite_time_cost_ratio"] == 100.0
        assert result["verification_efficiency_ratio"] == 0.2
        assert result["sessions_using_incremental"] == 0

    def test_mixed_verification_strategy(self):
        """Verify mixed incremental and full-suite verifications."""
        result = analyze_pack_verification_incremental([
            {
                "session_id": "session1",
                "total_verifications": 10,
                "incremental_verifications": 7,
                "full_suite_verifications": 3,
                "incremental_time_seconds": 35.0,
                "full_suite_time_seconds": 45.0,
                "changed_files_count": 5,
                "verified_files_count": 6,
                "unnecessary_full_runs": 1,
                "targeted_opportunities": 2,
            }
        ])

        assert result["incremental_ratio"] == 70.0
        assert result["full_suite_ratio"] == 30.0
        # 45/80 = 56.25%
        assert result["full_suite_time_cost_ratio"] == 56.25
        # 5/6 = 0.833
        assert result["verification_efficiency_ratio"] == 0.833
        # 1/3 = 33.33%
        assert result["unnecessary_full_run_ratio"] == 33.33

    def test_multiple_sessions_aggregation(self):
        """Verify aggregation across multiple sessions."""
        result = analyze_pack_verification_incremental([
            {
                "session_id": "session1",
                "total_verifications": 5,
                "incremental_verifications": 4,
                "full_suite_verifications": 1,
                "incremental_time_seconds": 20.0,
                "full_suite_time_seconds": 30.0,
                "changed_files_count": 3,
                "verified_files_count": 4,
            },
            {
                "session_id": "session2",
                "total_verifications": 7,
                "incremental_verifications": 5,
                "full_suite_verifications": 2,
                "incremental_time_seconds": 30.0,
                "full_suite_time_seconds": 40.0,
                "changed_files_count": 4,
                "verified_files_count": 5,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["total_verifications"] == 12
        assert result["incremental_verifications"] == 9
        assert result["full_suite_verifications"] == 3
        # 9/12 = 75%
        assert result["incremental_ratio"] == 75.0
        assert result["incremental_time_seconds"] == 50.0
        assert result["full_suite_time_seconds"] == 70.0
        assert result["changed_files_count"] == 7
        assert result["verified_files_count"] == 9

    def test_efficiency_score_perfect_incremental(self):
        """Verify efficiency score with perfect incremental usage."""
        result = analyze_pack_verification_incremental([
            {
                "session_id": "session1",
                "total_verifications": 10,
                "incremental_verifications": 10,
                "full_suite_verifications": 0,
                "incremental_time_seconds": 100.0,
                "full_suite_time_seconds": 0.0,
                "changed_files_count": 8,
                "verified_files_count": 8,
                "unnecessary_full_runs": 0,
            }
        ])

        assert result["incremental_efficiency_score"] >= 0.9

    def test_efficiency_score_poor_full_suite(self):
        """Verify efficiency score with poor full-suite usage."""
        result = analyze_pack_verification_incremental([
            {
                "session_id": "session1",
                "total_verifications": 10,
                "incremental_verifications": 0,
                "full_suite_verifications": 10,
                "incremental_time_seconds": 0.0,
                "full_suite_time_seconds": 200.0,
                "changed_files_count": 2,
                "verified_files_count": 20,
                "unnecessary_full_runs": 8,
            }
        ])

        assert result["incremental_efficiency_score"] < 0.3

    def test_missing_optional_fields(self):
        """Verify missing optional fields are handled gracefully."""
        result = analyze_pack_verification_incremental([
            {"session_id": "session1"}
        ])

        assert result["total_sessions"] == 1
        assert result["total_verifications"] == 0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_verification_incremental([
            "not a dict",
            {"session_id": "session1", "total_verifications": 3},
        ])

        assert result["total_sessions"] == 1
