"""Tests for session webfetch reliability analyzer."""

import pytest

from synthesis.session_webfetch_reliability import (
    analyze_session_webfetch_reliability,
)


class TestAnalyzeSessionWebfetchReliability:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_webfetch_reliability([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_web_calls"] == 0
        assert result["avg_total_web_calls"] == 0.0
        assert result["avg_webfetch_calls"] == 0.0
        assert result["avg_websearch_calls"] == 0.0
        assert result["avg_success_rate"] == 0.0
        assert result["avg_redirect_success_rate"] == 0.0
        assert result["avg_auth_failure_rate"] == 0.0
        assert result["avg_cache_hit_rate"] == 0.0
        assert result["avg_sources_inclusion_rate"] == 0.0
        assert result["avg_web_failure_impact"] == 0.0
        assert result["high_reliability_sessions"] == 0
        assert result["low_reliability_sessions"] == 0
        assert result["sessions_missing_sources"] == 0
        assert result["sessions_with_auth_issues"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_webfetch_reliability(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_webfetch_reliability("not a list")

    def test_session_with_no_web_calls(self):
        """Verify session with zero web tool calls handled gracefully."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 0,
                "total_websearch_calls": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_web_calls"] == 0

    def test_successful_webfetch_only(self):
        """Verify session with only successful WebFetch calls."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10,
                "total_websearch_calls": 0,
                "successful_fetches": 10,
                "failed_fetches": 0,
            }
        ])

        assert result["sessions_with_web_calls"] == 1
        assert result["avg_total_web_calls"] == 10.0
        assert result["avg_webfetch_calls"] == 10.0
        assert result["avg_websearch_calls"] == 0.0
        assert result["avg_success_rate"] == 100.0
        assert result["high_reliability_sessions"] == 1

    def test_websearch_only(self):
        """Verify session with only WebSearch calls."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 0,
                "total_websearch_calls": 5,
                "successful_fetches": 5,
                "failed_fetches": 0,
            }
        ])

        assert result["avg_webfetch_calls"] == 0.0
        assert result["avg_websearch_calls"] == 5.0
        assert result["avg_total_web_calls"] == 5.0

    def test_mixed_webfetch_and_websearch(self):
        """Verify session with both WebFetch and WebSearch calls."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 8,
                "total_websearch_calls": 4,
                "successful_fetches": 10,
                "failed_fetches": 2,
            }
        ])

        assert result["avg_total_web_calls"] == 12.0
        assert result["avg_webfetch_calls"] == 8.0
        assert result["avg_websearch_calls"] == 4.0
        # 10 / 12 = 83.33%
        assert 83.0 <= result["avg_success_rate"] <= 84.0

    def test_high_success_rate(self):
        """Verify detection of high reliability sessions."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 20,
                "successful_fetches": 19,
                "failed_fetches": 1,
            }
        ])

        # 19 / 20 = 95%
        assert result["avg_success_rate"] == 95.0
        assert result["high_reliability_sessions"] == 1
        assert result["low_reliability_sessions"] == 0

    def test_low_success_rate(self):
        """Verify detection of low reliability sessions."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 20,
                "successful_fetches": 12,
                "failed_fetches": 8,
            }
        ])

        # 12 / 20 = 60%
        assert result["avg_success_rate"] == 60.0
        assert result["high_reliability_sessions"] == 0
        assert result["low_reliability_sessions"] == 1

    def test_redirect_handling(self):
        """Verify redirect handling success rate calculation."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10,
                "redirect_handled": 8,
                "redirect_failed": 2,
            }
        ])

        # 8 / 10 = 80%
        assert result["avg_redirect_success_rate"] == 80.0

    def test_all_redirects_handled(self):
        """Verify session with all redirects handled correctly."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10,
                "redirect_handled": 5,
                "redirect_failed": 0,
            }
        ])

        assert result["avg_redirect_success_rate"] == 100.0

    def test_authentication_failures(self):
        """Verify detection of authentication failures."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 20,
                "successful_fetches": 15,
                "failed_fetches": 5,
                "auth_failures": 3,
            }
        ])

        # 3 / 20 = 15%
        assert result["avg_auth_failure_rate"] == 15.0
        assert result["sessions_with_auth_issues"] == 1

    def test_no_authentication_failures(self):
        """Verify sessions without auth failures."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 20,
                "successful_fetches": 18,
                "failed_fetches": 2,
                "auth_failures": 0,
            }
        ])

        assert result["avg_auth_failure_rate"] == 0.0
        assert result["sessions_with_auth_issues"] == 0

    def test_cache_hit_rate(self):
        """Verify cache hit rate calculation."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 20,
                "cache_hits": 12,
                "cache_misses": 8,
            }
        ])

        # 12 / 20 = 60%
        assert result["avg_cache_hit_rate"] == 60.0

    def test_no_cache_hits(self):
        """Verify sessions with no cache utilization."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10,
                "cache_hits": 0,
                "cache_misses": 10,
            }
        ])

        assert result["avg_cache_hit_rate"] == 0.0

    def test_all_cache_hits(self):
        """Verify sessions with full cache utilization."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10,
                "cache_hits": 10,
                "cache_misses": 0,
            }
        ])

        assert result["avg_cache_hit_rate"] == 100.0

    def test_websearch_with_sources(self):
        """Verify detection of WebSearch with Sources section."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_websearch_calls": 10,
                "websearch_with_sources": 9,
                "websearch_without_sources": 1,
            }
        ])

        # 9 / 10 = 90%
        assert result["avg_sources_inclusion_rate"] == 90.0
        assert result["sessions_missing_sources"] == 0

    def test_websearch_missing_sources(self):
        """Verify detection of WebSearch missing Sources section."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_websearch_calls": 10,
                "websearch_with_sources": 5,
                "websearch_without_sources": 5,
            }
        ])

        # 5 / 10 = 50%
        assert result["avg_sources_inclusion_rate"] == 50.0
        # >20% missing means >20% without sources
        # 50% with sources means 50% without, which is >20%
        assert result["sessions_missing_sources"] == 1

    def test_all_websearch_have_sources(self):
        """Verify sessions where all WebSearch include Sources."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_websearch_calls": 8,
                "websearch_with_sources": 8,
                "websearch_without_sources": 0,
            }
        ])

        assert result["avg_sources_inclusion_rate"] == 100.0
        assert result["sessions_missing_sources"] == 0

    def test_web_failure_impact_on_tasks(self):
        """Verify correlation between web failures and task incompletion."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10,
                "total_tasks": 5,
                "tasks_incomplete_due_to_web_failure": 2,
            }
        ])

        # 2 / 5 = 40%
        assert result["avg_web_failure_impact"] == 40.0

    def test_no_web_failure_impact(self):
        """Verify sessions where web failures don't impact tasks."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10,
                "total_tasks": 5,
                "tasks_incomplete_due_to_web_failure": 0,
            }
        ])

        assert result["avg_web_failure_impact"] == 0.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10,
                "successful_fetches": 9,
                "failed_fetches": 1,
            },
            {
                "session_id": "session2",
                "total_webfetch_calls": 20,
                "successful_fetches": 18,
                "failed_fetches": 2,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_web_calls"] == 2
        # (10 + 20) / 2 = 15
        assert result["avg_total_web_calls"] == 15.0
        # (90% + 90%) / 2 = 90%
        assert result["avg_success_rate"] == 90.0

    def test_boundary_reliability_classification(self):
        """Verify boundary cases for reliability classification."""
        result = analyze_session_webfetch_reliability([
            # Exactly 90% (should not be high)
            {
                "session_id": "s1",
                "total_webfetch_calls": 10,
                "successful_fetches": 9,
                "failed_fetches": 1,
            },
            # Just above 90% (should be high)
            {
                "session_id": "s2",
                "total_webfetch_calls": 100,
                "successful_fetches": 91,
                "failed_fetches": 9,
            },
            # Exactly 70% (should not be low)
            {
                "session_id": "s3",
                "total_webfetch_calls": 10,
                "successful_fetches": 7,
                "failed_fetches": 3,
            },
            # Just below 70% (should be low)
            {
                "session_id": "s4",
                "total_webfetch_calls": 100,
                "successful_fetches": 69,
                "failed_fetches": 31,
            },
        ])

        # >90% means strictly greater
        assert result["high_reliability_sessions"] == 1
        # <70% means strictly less
        assert result["low_reliability_sessions"] == 1

    def test_boundary_missing_sources_classification(self):
        """Verify boundary cases for missing sources classification."""
        result = analyze_session_webfetch_reliability([
            # Exactly 80% with sources (20% missing, should not trigger)
            {
                "session_id": "s1",
                "total_websearch_calls": 10,
                "websearch_with_sources": 8,
                "websearch_without_sources": 2,
            },
            # 79% with sources (21% missing, should trigger)
            {
                "session_id": "s2",
                "total_websearch_calls": 100,
                "websearch_with_sources": 79,
                "websearch_without_sources": 21,
            },
        ])

        # >20% missing means strictly greater than 20%
        assert result["sessions_missing_sources"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_webfetch_reliability([
            "not a dict",
            {
                "session_id": "session1",
                "total_webfetch_calls": 5,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": True,
                "successful_fetches": False,
            }
        ])

        assert result["sessions_with_web_calls"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10,
                # Missing most fields
            }
        ])

        assert result["sessions_with_web_calls"] == 1
        assert result["avg_total_web_calls"] == 10.0
        # Missing fields result in 0.0 averages
        assert result["avg_success_rate"] == 0.0

    def test_zero_web_calls_no_division_error(self):
        """Verify zero web calls doesn't cause division errors."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 0,
                "total_websearch_calls": 0,
            }
        ])

        assert result["sessions_with_web_calls"] == 0
        assert result["avg_total_web_calls"] == 0.0

    def test_zero_attempts_no_division_error(self):
        """Verify zero fetch attempts doesn't cause division errors."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 0,
                "successful_fetches": 0,
                "failed_fetches": 0,
            }
        ])

        assert result["avg_success_rate"] == 0.0

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_webfetch_calls": 30,
                "total_websearch_calls": 10,
                "successful_fetches": 36,
                "failed_fetches": 4,
                "redirect_handled": 8,
                "redirect_failed": 2,
                "auth_failures": 1,
                "cache_hits": 15,
                "cache_misses": 25,
                "websearch_with_sources": 9,
                "websearch_without_sources": 1,
                "tasks_incomplete_due_to_web_failure": 1,
                "total_tasks": 8,
            }
        ])

        assert result["sessions_with_web_calls"] == 1
        assert result["avg_total_web_calls"] == 40.0
        assert result["avg_webfetch_calls"] == 30.0
        assert result["avg_websearch_calls"] == 10.0
        # 36 / 40 = 90%
        assert result["avg_success_rate"] == 90.0
        # 8 / 10 = 80%
        assert result["avg_redirect_success_rate"] == 80.0
        # 1 / 40 = 2.5%
        assert result["avg_auth_failure_rate"] == 2.5
        # 15 / 40 = 37.5%
        assert result["avg_cache_hit_rate"] == 37.5
        # 9 / 10 = 90%
        assert result["avg_sources_inclusion_rate"] == 90.0
        # 1 / 8 = 12.5%
        assert result["avg_web_failure_impact"] == 12.5
        assert result["high_reliability_sessions"] == 0  # Exactly 90%, not >90%
        assert result["low_reliability_sessions"] == 0
        assert result["sessions_missing_sources"] == 0
        assert result["sessions_with_auth_issues"] == 1

    def test_mixed_session_quality(self):
        """Verify mixed session quality across multiple sessions."""
        result = analyze_session_webfetch_reliability([
            # High reliability
            {
                "session_id": "s1",
                "total_webfetch_calls": 10,
                "successful_fetches": 10,
                "failed_fetches": 0,
            },
            # Medium reliability (not classified)
            {
                "session_id": "s2",
                "total_webfetch_calls": 10,
                "successful_fetches": 8,
                "failed_fetches": 2,
            },
            # Low reliability
            {
                "session_id": "s3",
                "total_webfetch_calls": 10,
                "successful_fetches": 6,
                "failed_fetches": 4,
            },
        ])

        assert result["total_sessions"] == 3
        assert result["sessions_with_web_calls"] == 3
        # (100% + 80% + 60%) / 3 = 80%
        assert result["avg_success_rate"] == 80.0
        assert result["high_reliability_sessions"] == 1
        assert result["low_reliability_sessions"] == 1

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10.5,
                "successful_fetches": 9.5,
                "failed_fetches": 1.0,
            }
        ])

        assert result["avg_total_web_calls"] == 10.5
        # 9.5 / 10.5 = 90.48%
        assert 90.0 <= result["avg_success_rate"] <= 91.0

    def test_all_redirects_failed(self):
        """Verify session where all redirects failed."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 5,
                "redirect_handled": 0,
                "redirect_failed": 5,
            }
        ])

        assert result["avg_redirect_success_rate"] == 0.0

    def test_zero_tasks_no_division_error(self):
        """Verify zero total tasks doesn't cause division errors."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 10,
                "total_tasks": 0,
                "tasks_incomplete_due_to_web_failure": 5,
            }
        ])

        # Should not crash, web failure impact should not be calculated
        assert result["avg_web_failure_impact"] == 0.0

    def test_high_auth_failure_rate(self):
        """Verify detection of high auth failure rate."""
        result = analyze_session_webfetch_reliability([
            {
                "session_id": "session1",
                "total_webfetch_calls": 20,
                "successful_fetches": 5,
                "failed_fetches": 15,
                "auth_failures": 12,
            }
        ])

        # 12 / 20 = 60%
        assert result["avg_auth_failure_rate"] == 60.0
        assert result["sessions_with_auth_issues"] == 1
