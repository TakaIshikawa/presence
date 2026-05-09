"""Tests for session WebFetch redirect handling analyzer."""

import pytest

from synthesis.session_webfetch_redirect_handling import (
    WebFetchCall,
    WebFetchMetrics,
    Finding,
    analyze_session_webfetch_redirect_handling,
)


class TestAnalyzeSessionWebFetchRedirectHandling:
    """Test main analyzer function."""

    def test_empty_fetches_returns_zero_metrics(self):
        """Verify empty fetches returns zero metrics."""
        metrics, findings = analyze_session_webfetch_redirect_handling([])
        assert metrics.total_fetches == 0
        assert metrics.redirect_followup_rate == 0.0
        assert len(findings) == 0

    def test_single_successful_fetch(self):
        """Verify successful fetch with no issues."""
        fetches = [
            WebFetchCall(
                turn_index=1,
                url="https://example.com/docs",
                prompt="Extract the API endpoint details",
                was_redirect=False,
                redirect_followed_up=False,
                is_authenticated_url=False,
                prompt_is_specific=True,
                was_retry=False,
                retry_after_transient_failure=False,
                within_cache_window=False,
            )
        ]
        metrics, findings = analyze_session_webfetch_redirect_handling(fetches)
        assert metrics.total_fetches == 1
        assert len(findings) == 0

    def test_redirect_followed_up(self):
        """Verify redirect follow-up is tracked."""
        fetches = [
            WebFetchCall(
                turn_index=2,
                url="https://example.com",
                prompt="Get content",
                was_redirect=True,
                redirect_followed_up=True,
                is_authenticated_url=False,
                prompt_is_specific=True,
                was_retry=False,
                retry_after_transient_failure=False,
                within_cache_window=False,
            )
        ]
        metrics, findings = analyze_session_webfetch_redirect_handling(fetches)
        assert metrics.redirect_count == 1
        assert metrics.redirect_followed_up_count == 1
        assert metrics.redirect_followup_rate == 100.0

    def test_missing_redirect_followup_critical(self):
        """Verify critical finding for missing redirect follow-up."""
        fetches = [
            WebFetchCall(
                turn_index=3,
                url="https://short.url/abc",
                prompt="Get page",
                was_redirect=True,
                redirect_followed_up=False,
                is_authenticated_url=False,
                prompt_is_specific=True,
                was_retry=False,
                retry_after_transient_failure=False,
                within_cache_window=False,
            )
        ]
        metrics, findings = analyze_session_webfetch_redirect_handling(fetches)
        redirect_findings = [f for f in findings if f.category == "redirect_handling"]
        assert len(redirect_findings) >= 1
        assert redirect_findings[0].severity == "critical"

    def test_authenticated_url_attempt_critical(self):
        """Verify critical finding for authenticated URL."""
        fetches = [
            WebFetchCall(
                turn_index=4,
                url="https://github.com/user/repo/issues/123",
                prompt="Get issue details",
                was_redirect=False,
                redirect_followed_up=False,
                is_authenticated_url=True,
                prompt_is_specific=True,
                was_retry=False,
                retry_after_transient_failure=False,
                within_cache_window=False,
            )
        ]
        metrics, findings = analyze_session_webfetch_redirect_handling(fetches)
        assert metrics.authenticated_url_attempts == 1
        auth_findings = [f for f in findings if f.category == "authenticated_url_avoidance"]
        assert auth_findings[0].severity == "critical"

    def test_vague_prompt_warning(self):
        """Verify warning for vague prompt."""
        fetches = [
            WebFetchCall(
                turn_index=5,
                url="https://example.com",
                prompt="Get info",
                was_redirect=False,
                redirect_followed_up=False,
                is_authenticated_url=False,
                prompt_is_specific=False,
                was_retry=False,
                retry_after_transient_failure=False,
                within_cache_window=False,
            )
        ]
        metrics, findings = analyze_session_webfetch_redirect_handling(fetches)
        prompt_findings = [f for f in findings if f.category == "prompt_specificity"]
        assert prompt_findings[0].severity == "warning"

    def test_prompt_specificity_rate(self):
        """Verify prompt specificity rate calculation."""
        fetches = [
            WebFetchCall(turn_index=1, url="a.com", prompt="specific query",
                         was_redirect=False, redirect_followed_up=False,
                         is_authenticated_url=False, prompt_is_specific=True,
                         was_retry=False, retry_after_transient_failure=False,
                         within_cache_window=False),
            WebFetchCall(turn_index=2, url="b.com", prompt="get data",
                         was_redirect=False, redirect_followed_up=False,
                         is_authenticated_url=False, prompt_is_specific=False,
                         was_retry=False, retry_after_transient_failure=False,
                         within_cache_window=False),
        ]
        metrics, findings = analyze_session_webfetch_redirect_handling(fetches)
        # 1 specific out of 2 = 50%
        assert metrics.prompt_specificity_rate == 50.0

    def test_cache_window_warning(self):
        """Verify warning for fetch within cache window."""
        fetches = [
            WebFetchCall(
                turn_index=6,
                url="https://example.com",
                prompt="Extract data",
                was_redirect=False,
                redirect_followed_up=False,
                is_authenticated_url=False,
                prompt_is_specific=True,
                was_retry=False,
                retry_after_transient_failure=False,
                within_cache_window=True,
            )
        ]
        metrics, findings = analyze_session_webfetch_redirect_handling(fetches)
        cache_findings = [f for f in findings if f.category == "cache_awareness"]
        assert cache_findings[0].severity == "warning"

    def test_unintelligent_retry_warning(self):
        """Verify warning for retry after permanent failure."""
        fetches = [
            WebFetchCall(
                turn_index=7,
                url="https://broken.com",
                prompt="Get content",
                was_redirect=False,
                redirect_followed_up=False,
                is_authenticated_url=False,
                prompt_is_specific=True,
                was_retry=True,
                retry_after_transient_failure=False,
                within_cache_window=False,
            )
        ]
        metrics, findings = analyze_session_webfetch_redirect_handling(fetches)
        retry_findings = [f for f in findings if f.category == "retry_discipline"]
        assert retry_findings[0].severity == "warning"

    def test_intelligent_retry_no_warning(self):
        """Verify no warning for intelligent retry."""
        fetches = [
            WebFetchCall(
                turn_index=8,
                url="https://timeout.com",
                prompt="Get data",
                was_redirect=False,
                redirect_followed_up=False,
                is_authenticated_url=False,
                prompt_is_specific=True,
                was_retry=True,
                retry_after_transient_failure=True,
                within_cache_window=False,
            )
        ]
        metrics, findings = analyze_session_webfetch_redirect_handling(fetches)
        retry_findings = [f for f in findings if f.category == "retry_discipline"]
        assert len(retry_findings) == 0

    def test_invalid_fetches_type_raises_error(self):
        """Verify invalid fetches type raises ValueError."""
        with pytest.raises(ValueError, match="must be a list or tuple"):
            analyze_session_webfetch_redirect_handling("not a list")

    def test_invalid_fetch_instance_raises_error(self):
        """Verify invalid fetch instance raises ValueError."""
        with pytest.raises(ValueError, match="WebFetchCall instance"):
            analyze_session_webfetch_redirect_handling([{"url": "test.com"}])
