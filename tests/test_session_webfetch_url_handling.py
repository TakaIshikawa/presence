"""Tests for session_webfetch_url_handling analyzer."""

from __future__ import annotations

import pytest

from synthesis.session_webfetch_url_handling import analyze_session_webfetch_url_handling


class TestAnalyzeSessionWebfetchUrlHandling:
    """Tests for analyze_session_webfetch_url_handling."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_session_webfetch_url_handling([])
        assert result["total_sessions"] == 0
        assert result["total_webfetch_calls"] == 0
        assert result["webfetch_url_handling_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_session_webfetch_url_handling(None)
        assert result["total_sessions"] == 0
        assert result["total_webfetch_calls"] == 0
        assert result["webfetch_url_handling_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_webfetch_url_handling("not a list")
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_webfetch_url_handling(42)

    def test_single_session_high_quality(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_webfetch_calls": 10,
                "user_provided_urls": 9,
                "generated_urls": 1,
                "redirect_detected": 3,
                "redirect_followed": 3,
                "redirect_missed": 0,
                "github_urls_total": 4,
                "github_urls_via_cli": 4,
                "avg_prompt_length": 35.0,
                "repeated_url_fetches": 2,
                "repeated_url_cache_hits": 2,
                "session_total_tool_calls": 50,
            }
        ]
        result = analyze_session_webfetch_url_handling(records)
        assert result["total_sessions"] == 1
        assert result["total_webfetch_calls"] == 10
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 0
        assert result["webfetch_url_handling_score"] > 0.7

    def test_single_session_low_quality(self) -> None:
        records = [
            {
                "session_id": "s2",
                "total_webfetch_calls": 10,
                "user_provided_urls": 1,
                "generated_urls": 9,
                "redirect_detected": 5,
                "redirect_followed": 1,
                "redirect_missed": 4,
                "github_urls_total": 6,
                "github_urls_via_cli": 0,
                "avg_prompt_length": 5.0,
                "repeated_url_fetches": 4,
                "repeated_url_cache_hits": 0,
                "session_total_tool_calls": 30,
            }
        ]
        result = analyze_session_webfetch_url_handling(records)
        assert result["total_sessions"] == 1
        assert result["low_quality_sessions"] == 1
        assert result["high_quality_sessions"] == 0
        assert result["webfetch_url_handling_score"] < 0.4

    def test_multiple_sessions_mixed_quality(self) -> None:
        high = {
            "session_id": "high",
            "total_webfetch_calls": 10,
            "user_provided_urls": 10,
            "generated_urls": 0,
            "redirect_detected": 2,
            "redirect_followed": 2,
            "redirect_missed": 0,
            "github_urls_total": 3,
            "github_urls_via_cli": 3,
            "avg_prompt_length": 30.0,
            "repeated_url_fetches": 1,
            "repeated_url_cache_hits": 1,
            "session_total_tool_calls": 40,
        }
        low = {
            "session_id": "low",
            "total_webfetch_calls": 8,
            "user_provided_urls": 0,
            "generated_urls": 8,
            "redirect_detected": 4,
            "redirect_followed": 0,
            "redirect_missed": 4,
            "github_urls_total": 5,
            "github_urls_via_cli": 0,
            "avg_prompt_length": 3.0,
            "repeated_url_fetches": 3,
            "repeated_url_cache_hits": 0,
            "session_total_tool_calls": 20,
        }
        result = analyze_session_webfetch_url_handling([high, low])
        assert result["total_sessions"] == 2
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 1
        # Overall score should be between the two extremes
        assert 0.2 < result["webfetch_url_handling_score"] < 0.9

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            42,
            None,
            {
                "session_id": "valid",
                "total_webfetch_calls": 5,
                "user_provided_urls": 5,
                "generated_urls": 0,
                "redirect_detected": 0,
                "redirect_followed": 0,
                "redirect_missed": 0,
                "github_urls_total": 0,
                "github_urls_via_cli": 0,
                "avg_prompt_length": 25.0,
                "repeated_url_fetches": 0,
                "repeated_url_cache_hits": 0,
                "session_total_tool_calls": 10,
            },
        ]
        result = analyze_session_webfetch_url_handling(records)
        assert result["total_sessions"] == 1

    def test_zero_webfetch_session(self) -> None:
        records = [
            {
                "session_id": "no_wf",
                "total_webfetch_calls": 0,
                "user_provided_urls": 0,
                "generated_urls": 0,
                "redirect_detected": 0,
                "redirect_followed": 0,
                "redirect_missed": 0,
                "github_urls_total": 0,
                "github_urls_via_cli": 0,
                "avg_prompt_length": 0,
                "repeated_url_fetches": 0,
                "repeated_url_cache_hits": 0,
                "session_total_tool_calls": 20,
            }
        ]
        result = analyze_session_webfetch_url_handling(records)
        assert result["total_sessions"] == 1
        assert result["total_webfetch_calls"] == 0
        # With no webfetch calls, URL rate percentages should be 0
        assert result["user_provided_url_rate"] == 0.0
        assert result["generated_url_rate"] == 0.0

    def test_result_keys_complete(self) -> None:
        result = analyze_session_webfetch_url_handling([])
        expected_keys = {
            "total_sessions",
            "total_webfetch_calls",
            "user_provided_url_rate",
            "generated_url_rate",
            "redirect_follow_rate",
            "redirect_missed_rate",
            "github_url_cli_fallback_rate",
            "avg_prompt_length",
            "repeated_url_cache_hit_rate",
            "high_quality_sessions",
            "low_quality_sessions",
            "webfetch_url_handling_score",
        }
        assert set(result.keys()) == expected_keys
