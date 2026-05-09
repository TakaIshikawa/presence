"""Tests for pack Glob vs Grep search strategy efficiency analyzer."""

import pytest

from synthesis.pack_glob_grep_efficiency import analyze_pack_glob_grep_efficiency


class TestAnalyzePackGlobGrepEfficiency:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_glob_grep_efficiency([])

        assert result["total_sessions"] == 0
        assert result["total_glob_count"] == 0
        assert result["total_grep_count"] == 0
        assert result["total_searches"] == 0
        assert result["glob_grep_ratio"] == 0.0
        assert result["glob_first_ratio"] == 0.0
        assert result["avg_grep_to_read_latency"] == 0.0
        assert result["max_grep_to_read_latency"] == 0
        assert result["total_unnecessary_grep_count"] == 0
        assert result["unnecessary_grep_ratio"] == 0.0
        assert result["searches_leading_to_action"] == 0
        assert result["search_to_action_efficiency"] == 0.0
        assert result["optimal_pattern_sessions"] == 0
        assert result["search_strategy_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_glob_grep_efficiency(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_glob_grep_efficiency("not a list")

    def test_single_session_glob_only(self):
        """Verify pack with single session using only Glob."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 10,
                "grep_count": 0,
                "glob_first_searches": 8,
                "total_search_workflows": 10,
                "grep_to_read_turns": [],
                "unnecessary_grep_count": 0,
                "searches_leading_to_edits": 6,
                "total_searches": 10,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["total_glob_count"] == 10
        assert result["total_grep_count"] == 0
        assert result["total_searches"] == 10
        assert result["glob_grep_ratio"] == 100.0
        assert result["glob_first_ratio"] == 80.0
        assert result["optimal_pattern_sessions"] == 1

    def test_single_session_grep_only(self):
        """Verify pack with single session using only Grep."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 0,
                "grep_count": 15,
                "glob_first_searches": 0,
                "total_search_workflows": 15,
                "grep_to_read_turns": [1, 2, 1, 3, 2],
                "unnecessary_grep_count": 5,
                "searches_leading_to_edits": 10,
                "total_searches": 15,
            }
        ])

        assert result["total_glob_count"] == 0
        assert result["total_grep_count"] == 15
        assert result["glob_grep_ratio"] == 0.0
        assert result["glob_first_ratio"] == 0.0
        assert result["optimal_pattern_sessions"] == 0

    def test_multi_session_aggregation(self):
        """Verify aggregation across multiple sessions."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 10,
                "grep_count": 5,
                "glob_first_searches": 8,
                "total_search_workflows": 10,
                "grep_to_read_turns": [1, 2],
                "unnecessary_grep_count": 2,
                "searches_leading_to_edits": 12,
                "total_searches": 15,
            },
            {
                "session_id": "session2",
                "glob_count": 8,
                "grep_count": 7,
                "glob_first_searches": 6,
                "total_search_workflows": 10,
                "grep_to_read_turns": [2, 3, 1],
                "unnecessary_grep_count": 3,
                "searches_leading_to_edits": 10,
                "total_searches": 15,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["total_glob_count"] == 18
        assert result["total_grep_count"] == 12
        assert result["total_searches"] == 30
        # 18/30 = 60%
        assert result["glob_grep_ratio"] == 60.0
        # 14/20 = 70%
        assert result["glob_first_ratio"] == 70.0
        # (1+2+2+3+1)/5 = 1.8
        assert result["avg_grep_to_read_latency"] == 1.8
        assert result["total_unnecessary_grep_count"] == 5

    def test_glob_first_ratio_calculation(self):
        """Verify Glob-first ratio calculation."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 10,
                "grep_count": 10,
                "glob_first_searches": 15,
                "total_search_workflows": 20,
                "total_searches": 20,
            }
        ])

        # 15/20 = 75%
        assert result["glob_first_ratio"] == 75.0

    def test_grep_to_read_latency_tracking(self):
        """Verify Grep-to-Read latency tracking."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 5,
                "grep_count": 5,
                "grep_to_read_turns": [1, 2, 3, 4, 5],
                "total_searches": 10,
            }
        ])

        # (1+2+3+4+5)/5 = 3.0
        assert result["avg_grep_to_read_latency"] == 3.0
        assert result["max_grep_to_read_latency"] == 5

    def test_grep_to_read_latency_across_sessions(self):
        """Verify latency aggregation across sessions."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 5,
                "grep_count": 5,
                "grep_to_read_turns": [1, 2],
                "total_searches": 10,
            },
            {
                "session_id": "session2",
                "glob_count": 5,
                "grep_count": 5,
                "grep_to_read_turns": [3, 4],
                "total_searches": 10,
            },
        ])

        # (1+2+3+4)/4 = 2.5
        assert result["avg_grep_to_read_latency"] == 2.5
        assert result["max_grep_to_read_latency"] == 4

    def test_unnecessary_grep_tracking(self):
        """Verify unnecessary Grep usage tracking."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 10,
                "grep_count": 20,
                "unnecessary_grep_count": 8,
                "total_searches": 30,
            }
        ])

        # 8/20 = 40%
        assert result["unnecessary_grep_ratio"] == 40.0

    def test_search_to_action_efficiency(self):
        """Verify search-to-action efficiency calculation."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 10,
                "grep_count": 10,
                "searches_leading_to_edits": 15,
                "total_searches": 20,
            }
        ])

        # 15/20 = 75%
        assert result["search_to_action_efficiency"] == 75.0

    def test_optimal_pattern_sessions_count(self):
        """Verify optimal pattern session counting."""
        result = analyze_pack_glob_grep_efficiency([
            # Session 1: 80% Glob-first (optimal)
            {
                "session_id": "session1",
                "glob_count": 10,
                "grep_count": 5,
                "glob_first_searches": 8,
                "total_search_workflows": 10,
                "total_searches": 15,
            },
            # Session 2: 60% Glob-first (not optimal)
            {
                "session_id": "session2",
                "glob_count": 10,
                "grep_count": 5,
                "glob_first_searches": 6,
                "total_search_workflows": 10,
                "total_searches": 15,
            },
            # Session 3: 90% Glob-first (optimal)
            {
                "session_id": "session3",
                "glob_count": 10,
                "grep_count": 5,
                "glob_first_searches": 9,
                "total_search_workflows": 10,
                "total_searches": 15,
            },
        ])

        assert result["optimal_pattern_sessions"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_glob_grep_efficiency([
            "not a dict",
            {
                "session_id": "session1",
                "glob_count": 10,
                "grep_count": 5,
                "total_searches": 15,
            },
        ])

        assert result["total_sessions"] == 1
        assert result["total_glob_count"] == 10

    def test_missing_fields_handled_gracefully(self):
        """Verify missing fields are handled with defaults."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                # All other fields missing
            }
        ])

        assert result["total_sessions"] == 1
        assert result["total_glob_count"] == 0
        assert result["total_grep_count"] == 0

    def test_empty_grep_to_read_turns_list(self):
        """Verify empty latency list is handled."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 10,
                "grep_count": 0,
                "grep_to_read_turns": [],
                "total_searches": 10,
            }
        ])

        assert result["avg_grep_to_read_latency"] == 0.0
        assert result["max_grep_to_read_latency"] == 0

    def test_zero_latency_values_ignored(self):
        """Verify zero latency values are filtered out."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 5,
                "grep_count": 5,
                "grep_to_read_turns": [0, 1, 0, 2, 0, 3],
                "total_searches": 10,
            }
        ])

        # Only non-zero values: (1+2+3)/3 = 2.0
        assert result["avg_grep_to_read_latency"] == 2.0

    def test_optimal_pattern_high_strategy_score(self):
        """Verify optimal search pattern scores highly."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 20,
                "grep_count": 5,
                "glob_first_searches": 18,
                "total_search_workflows": 20,
                "grep_to_read_turns": [1, 1, 2, 1],
                "unnecessary_grep_count": 1,
                "searches_leading_to_edits": 20,
                "total_searches": 25,
            }
        ])

        # 90% Glob-first (excellent)
        assert result["glob_first_ratio"] == 90.0
        # 1.25 avg latency (excellent)
        assert result["avg_grep_to_read_latency"] == 1.25
        # 20% unnecessary (acceptable)
        assert result["unnecessary_grep_ratio"] == 20.0
        # 80% search-to-action (excellent)
        assert result["search_to_action_efficiency"] == 80.0
        # High overall score
        assert result["search_strategy_score"] > 0.8

    def test_anti_pattern_low_glob_usage(self):
        """Verify anti-pattern of low Glob usage."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 2,
                "grep_count": 18,
                "glob_first_searches": 2,
                "total_search_workflows": 20,
                "grep_to_read_turns": [3, 4, 5, 3, 4],
                "unnecessary_grep_count": 10,
                "searches_leading_to_edits": 8,
                "total_searches": 20,
            }
        ])

        # 10% Glob-first (poor)
        assert result["glob_first_ratio"] == 10.0
        # High latency
        assert result["avg_grep_to_read_latency"] == 3.8
        # 55.56% unnecessary
        assert result["unnecessary_grep_ratio"] == 55.56
        # 40% search-to-action
        assert result["search_to_action_efficiency"] == 40.0
        # Low overall score
        assert result["search_strategy_score"] < 0.4

    def test_anti_pattern_high_latency(self):
        """Verify anti-pattern of high Grep-to-Read latency."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 10,
                "grep_count": 10,
                "glob_first_searches": 15,
                "total_search_workflows": 20,
                "grep_to_read_turns": [5, 6, 7, 8, 9],
                "unnecessary_grep_count": 2,
                "searches_leading_to_edits": 15,
                "total_searches": 20,
            }
        ])

        # Average latency: 7.0 (poor)
        assert result["avg_grep_to_read_latency"] == 7.0
        assert result["max_grep_to_read_latency"] == 9
        # Penalized for high latency
        assert result["search_strategy_score"] < 0.8

    def test_strategy_score_components(self):
        """Verify strategy score calculation components."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 15,
                "grep_count": 5,
                "glob_first_searches": 16,
                "total_search_workflows": 20,
                "grep_to_read_turns": [1, 1, 2],
                "unnecessary_grep_count": 1,
                "searches_leading_to_edits": 16,
                "total_searches": 20,
            }
        ])

        # 80% Glob-first (good)
        assert result["glob_first_ratio"] == 80.0
        # 1.33 avg latency (excellent)
        assert result["avg_grep_to_read_latency"] == 1.33
        # 20% unnecessary (acceptable)
        assert result["unnecessary_grep_ratio"] == 20.0
        # 80% search-to-action (excellent)
        assert result["search_to_action_efficiency"] == 80.0
        # High overall score
        assert result["search_strategy_score"] > 0.85

    def test_pack_with_no_searches(self):
        """Verify pack with no search operations."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 0,
                "grep_count": 0,
                "total_searches": 0,
            }
        ])

        assert result["total_searches"] == 0
        assert result["glob_grep_ratio"] == 0.0
        # Score includes latency component when no searches
        assert result["search_strategy_score"] == 0.3

    def test_mixed_pattern_sessions(self):
        """Verify mixed optimal and suboptimal sessions."""
        result = analyze_pack_glob_grep_efficiency([
            # Optimal session
            {
                "session_id": "session1",
                "glob_count": 15,
                "grep_count": 5,
                "glob_first_searches": 16,
                "total_search_workflows": 20,
                "grep_to_read_turns": [1, 1],
                "unnecessary_grep_count": 1,
                "searches_leading_to_edits": 18,
                "total_searches": 20,
            },
            # Suboptimal session
            {
                "session_id": "session2",
                "glob_count": 5,
                "grep_count": 15,
                "glob_first_searches": 8,
                "total_search_workflows": 20,
                "grep_to_read_turns": [3, 4, 5],
                "unnecessary_grep_count": 8,
                "searches_leading_to_edits": 10,
                "total_searches": 20,
            },
        ])

        assert result["total_sessions"] == 2
        # (16+8)/(20+20) = 60%
        assert result["glob_first_ratio"] == 60.0
        # 1 optimal out of 2
        assert result["optimal_pattern_sessions"] == 1
        # Moderate overall score
        assert 0.4 < result["search_strategy_score"] <= 0.81

    def test_non_list_grep_to_read_turns_ignored(self):
        """Verify non-list grep_to_read_turns is ignored."""
        result = analyze_pack_glob_grep_efficiency([
            {
                "session_id": "session1",
                "glob_count": 10,
                "grep_count": 5,
                "grep_to_read_turns": "not a list",
                "total_searches": 15,
            }
        ])

        assert result["avg_grep_to_read_latency"] == 0.0
        assert result["max_grep_to_read_latency"] == 0

    def test_comprehensive_pack_scenario(self):
        """Verify comprehensive pack with varied patterns."""
        result = analyze_pack_glob_grep_efficiency([
            # Session 1: Excellent pattern
            {
                "session_id": "session1",
                "glob_count": 20,
                "grep_count": 3,
                "glob_first_searches": 18,
                "total_search_workflows": 20,
                "grep_to_read_turns": [1, 1, 2],
                "unnecessary_grep_count": 1,
                "searches_leading_to_edits": 20,
                "total_searches": 23,
            },
            # Session 2: Good pattern
            {
                "session_id": "session2",
                "glob_count": 12,
                "grep_count": 8,
                "glob_first_searches": 14,
                "total_search_workflows": 18,
                "grep_to_read_turns": [1, 2, 2, 1],
                "unnecessary_grep_count": 2,
                "searches_leading_to_edits": 16,
                "total_searches": 20,
            },
            # Session 3: Poor pattern
            {
                "session_id": "session3",
                "glob_count": 3,
                "grep_count": 17,
                "glob_first_searches": 5,
                "total_search_workflows": 18,
                "grep_to_read_turns": [3, 4, 5, 3],
                "unnecessary_grep_count": 10,
                "searches_leading_to_edits": 12,
                "total_searches": 20,
            },
        ])

        assert result["total_sessions"] == 3
        assert result["total_glob_count"] == 35
        assert result["total_grep_count"] == 28
        assert result["total_searches"] == 63
        # 35/63 = 55.56%
        assert result["glob_grep_ratio"] == 55.56
        # (18+14+5)/(20+18+18) = 66.07%
        assert result["glob_first_ratio"] == 66.07
        # (1+1+2+1+2+2+1+3+4+5+3)/11 = 2.27
        assert result["avg_grep_to_read_latency"] == 2.27
        assert result["max_grep_to_read_latency"] == 5
        # 13 total unnecessary
        assert result["total_unnecessary_grep_count"] == 13
        # 13/28 = 46.43%
        assert result["unnecessary_grep_ratio"] == 46.43
        # (20+16+12)/63 = 76.19%
        assert result["search_to_action_efficiency"] == 76.19
        # 2 optimal sessions (session1 and session2)
        assert result["optimal_pattern_sessions"] == 2
        # Moderate-to-good score
        assert 0.6 < result["search_strategy_score"] < 0.9
