"""Tests for pack Write vs Edit tool discipline analyzer."""

import pytest

from synthesis.pack_write_tool_discipline import analyze_pack_write_tool_discipline


class TestAnalyzePackWriteToolDiscipline:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_write_tool_discipline([])

        assert result["total_sessions"] == 0
        assert result["total_write_count"] == 0
        assert result["total_edit_count"] == 0
        assert result["total_file_operations"] == 0
        assert result["write_edit_ratio"] == 0.0
        assert result["write_on_existing_count"] == 0
        assert result["write_on_existing_ratio"] == 0.0
        assert result["edit_string_match_failure_count"] == 0
        assert result["edit_match_failure_ratio"] == 0.0
        assert result["replace_all_usage_count"] == 0
        assert result["replace_all_usage_ratio"] == 0.0
        assert result["new_file_justification_count"] == 0
        assert result["new_file_justification_ratio"] == 0.0
        assert result["disciplined_sessions"] == 0
        assert result["tool_discipline_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_write_tool_discipline(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_write_tool_discipline("not a list")

    def test_single_session_edit_only(self):
        """Verify pack with single session using only Edit."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 0,
                "edit_count": 20,
                "write_on_existing_count": 0,
                "edit_string_match_failures": 1,
                "replace_all_count": 5,
                "new_file_created_count": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["total_write_count"] == 0
        assert result["total_edit_count"] == 20
        assert result["total_file_operations"] == 20
        assert result["write_edit_ratio"] == 0.0
        # No Write calls means disciplined
        assert result["disciplined_sessions"] == 1

    def test_single_session_write_only_new_files(self):
        """Verify pack with Write calls creating only new files."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 10,
                "edit_count": 0,
                "write_on_existing_count": 0,
                "edit_string_match_failures": 0,
                "replace_all_count": 0,
                "new_file_created_count": 10,
            }
        ])

        assert result["total_write_count"] == 10
        assert result["total_edit_count"] == 0
        assert result["write_edit_ratio"] == 100.0
        assert result["write_on_existing_ratio"] == 0.0
        assert result["new_file_justification_ratio"] == 100.0
        assert result["disciplined_sessions"] == 1

    def test_single_session_write_on_existing_violation(self):
        """Verify detection of Write-on-existing violations."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 10,
                "edit_count": 0,
                "write_on_existing_count": 8,
                "edit_string_match_failures": 0,
                "replace_all_count": 0,
                "new_file_created_count": 2,
            }
        ])

        # 8/10 = 80%
        assert result["write_on_existing_ratio"] == 80.0
        # 2/10 = 20%
        assert result["new_file_justification_ratio"] == 20.0
        # >10% write-on-existing means not disciplined
        assert result["disciplined_sessions"] == 0

    def test_multi_session_aggregation(self):
        """Verify aggregation across multiple sessions."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 5,
                "edit_count": 15,
                "write_on_existing_count": 1,
                "edit_string_match_failures": 2,
                "replace_all_count": 4,
                "new_file_created_count": 4,
            },
            {
                "session_id": "session2",
                "write_count": 3,
                "edit_count": 17,
                "write_on_existing_count": 0,
                "edit_string_match_failures": 1,
                "replace_all_count": 6,
                "new_file_created_count": 3,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["total_write_count"] == 8
        assert result["total_edit_count"] == 32
        assert result["total_file_operations"] == 40
        # 8/40 = 20%
        assert result["write_edit_ratio"] == 20.0
        # 1/8 = 12.5%
        assert result["write_on_existing_ratio"] == 12.5
        # 3/32 = 9.375%
        assert result["edit_match_failure_ratio"] == 9.38
        # 10/32 = 31.25%
        assert result["replace_all_usage_ratio"] == 31.25
        # 7/8 = 87.5%
        assert result["new_file_justification_ratio"] == 87.5

    def test_edit_match_failure_tracking(self):
        """Verify Edit string match failure tracking."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 0,
                "edit_count": 100,
                "edit_string_match_failures": 10,
            }
        ])

        # 10/100 = 10%
        assert result["edit_match_failure_ratio"] == 10.0

    def test_replace_all_usage_tracking(self):
        """Verify replace_all usage tracking."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 0,
                "edit_count": 50,
                "replace_all_count": 15,
            }
        ])

        # 15/50 = 30%
        assert result["replace_all_usage_ratio"] == 30.0

    def test_disciplined_sessions_count(self):
        """Verify disciplined session counting."""
        result = analyze_pack_write_tool_discipline([
            # Session 1: 5% write-on-existing (disciplined)
            {
                "session_id": "session1",
                "write_count": 20,
                "edit_count": 80,
                "write_on_existing_count": 1,
                "new_file_created_count": 19,
            },
            # Session 2: 50% write-on-existing (not disciplined)
            {
                "session_id": "session2",
                "write_count": 10,
                "edit_count": 10,
                "write_on_existing_count": 5,
                "new_file_created_count": 5,
            },
            # Session 3: 0% write-on-existing (disciplined)
            {
                "session_id": "session3",
                "write_count": 15,
                "edit_count": 25,
                "write_on_existing_count": 0,
                "new_file_created_count": 15,
            },
        ])

        assert result["disciplined_sessions"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_write_tool_discipline([
            "not a dict",
            {
                "session_id": "session1",
                "write_count": 10,
                "edit_count": 20,
            },
        ])

        assert result["total_sessions"] == 1
        assert result["total_write_count"] == 10

    def test_missing_fields_handled_gracefully(self):
        """Verify missing fields are handled with defaults."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                # All other fields missing
            }
        ])

        assert result["total_sessions"] == 1
        assert result["total_write_count"] == 0
        assert result["total_edit_count"] == 0

    def test_optimal_pattern_high_discipline_score(self):
        """Verify optimal tool usage pattern scores highly."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 5,
                "edit_count": 95,
                "write_on_existing_count": 0,
                "edit_string_match_failures": 2,
                "replace_all_count": 25,
                "new_file_created_count": 5,
            }
        ])

        # 0% write-on-existing (perfect)
        assert result["write_on_existing_ratio"] == 0.0
        # 100% new file justification (perfect)
        assert result["new_file_justification_ratio"] == 100.0
        # 2.11% edit failures (excellent)
        assert result["edit_match_failure_ratio"] == 2.11
        # 26.32% replace-all usage (good)
        assert result["replace_all_usage_ratio"] == 26.32
        # High overall score
        assert result["tool_discipline_score"] > 0.9

    def test_anti_pattern_excessive_write_on_existing(self):
        """Verify anti-pattern of excessive Write-on-existing."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 50,
                "edit_count": 10,
                "write_on_existing_count": 45,
                "edit_string_match_failures": 0,
                "replace_all_count": 0,
                "new_file_created_count": 5,
            }
        ])

        # 90% write-on-existing (very poor)
        assert result["write_on_existing_ratio"] == 90.0
        # 10% new file justification (poor)
        assert result["new_file_justification_ratio"] == 10.0
        # Low overall score
        assert result["tool_discipline_score"] < 0.25

    def test_anti_pattern_high_edit_failures(self):
        """Verify anti-pattern of high Edit match failures."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 0,
                "edit_count": 100,
                "edit_string_match_failures": 30,
                "replace_all_count": 0,
            }
        ])

        # 30% edit failures (very poor)
        assert result["edit_match_failure_ratio"] == 30.0
        # Penalized for high failure rate
        assert result["tool_discipline_score"] < 0.5

    def test_discipline_score_components(self):
        """Verify discipline score calculation components."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 10,
                "edit_count": 90,
                "write_on_existing_count": 1,
                "edit_string_match_failures": 3,
                "replace_all_count": 20,
                "new_file_created_count": 9,
            }
        ])

        # 10% write-on-existing (at threshold)
        assert result["write_on_existing_ratio"] == 10.0
        # 90% new file justification (at threshold)
        assert result["new_file_justification_ratio"] == 90.0
        # 3.33% edit failures (good)
        assert result["edit_match_failure_ratio"] == 3.33
        # 22.22% replace-all usage (good)
        assert result["replace_all_usage_ratio"] == 22.22
        # High overall score
        assert result["tool_discipline_score"] >= 0.9

    def test_pack_with_no_operations(self):
        """Verify pack with no file operations."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 0,
                "edit_count": 0,
            }
        ])

        assert result["total_file_operations"] == 0
        assert result["write_edit_ratio"] == 0.0
        # No Write calls means disciplined
        assert result["disciplined_sessions"] == 1

    def test_mixed_discipline_sessions(self):
        """Verify mixed disciplined and undisciplined sessions."""
        result = analyze_pack_write_tool_discipline([
            # Disciplined session
            {
                "session_id": "session1",
                "write_count": 5,
                "edit_count": 45,
                "write_on_existing_count": 0,
                "edit_string_match_failures": 1,
                "replace_all_count": 10,
                "new_file_created_count": 5,
            },
            # Undisciplined session
            {
                "session_id": "session2",
                "write_count": 20,
                "edit_count": 10,
                "write_on_existing_count": 15,
                "edit_string_match_failures": 3,
                "replace_all_count": 1,
                "new_file_created_count": 5,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["disciplined_sessions"] == 1
        # Aggregate metrics affected by undisciplined session
        # 15/25 = 60%
        assert result["write_on_existing_ratio"] == 60.0

    def test_zero_write_count_sessions(self):
        """Verify sessions with zero Write calls are disciplined."""
        result = analyze_pack_write_tool_discipline([
            {
                "session_id": "session1",
                "write_count": 0,
                "edit_count": 50,
                "edit_string_match_failures": 5,
            },
            {
                "session_id": "session2",
                "write_count": 0,
                "edit_count": 30,
                "edit_string_match_failures": 2,
            },
        ])

        # Both sessions have no Write calls
        assert result["disciplined_sessions"] == 2

    def test_comprehensive_pack_scenario(self):
        """Verify comprehensive pack with varied patterns."""
        result = analyze_pack_write_tool_discipline([
            # Session 1: Excellent discipline
            {
                "session_id": "session1",
                "write_count": 3,
                "edit_count": 47,
                "write_on_existing_count": 0,
                "edit_string_match_failures": 1,
                "replace_all_count": 12,
                "new_file_created_count": 3,
            },
            # Session 2: Good discipline
            {
                "session_id": "session2",
                "write_count": 7,
                "edit_count": 43,
                "write_on_existing_count": 1,
                "edit_string_match_failures": 2,
                "replace_all_count": 10,
                "new_file_created_count": 6,
            },
            # Session 3: Poor discipline
            {
                "session_id": "session3",
                "write_count": 25,
                "edit_count": 25,
                "write_on_existing_count": 20,
                "edit_string_match_failures": 5,
                "replace_all_count": 3,
                "new_file_created_count": 5,
            },
        ])

        assert result["total_sessions"] == 3
        assert result["total_write_count"] == 35
        assert result["total_edit_count"] == 115
        assert result["total_file_operations"] == 150
        # 35/150 = 23.33%
        assert result["write_edit_ratio"] == 23.33
        # 21/35 = 60%
        assert result["write_on_existing_ratio"] == 60.0
        # 8/115 = 6.96%
        assert result["edit_match_failure_ratio"] == 6.96
        # 25/115 = 21.74%
        assert result["replace_all_usage_ratio"] == 21.74
        # 14/35 = 40%
        assert result["new_file_justification_ratio"] == 40.0
        # 1 disciplined session (session 1 only; session 2 has 14.29% write-on-existing)
        assert result["disciplined_sessions"] == 1
        # Moderate score due to mixed patterns
        assert 0.3 < result["tool_discipline_score"] < 0.6
