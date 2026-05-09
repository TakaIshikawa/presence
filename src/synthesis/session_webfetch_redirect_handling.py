"""Session WebFetch redirect handling and retry discipline analyzer.

Analyzes WebFetch tool usage in session transcripts for redirect detection and
follow-up, authenticated URL avoidance, prompt specificity, cache awareness,
and retry discipline.

WebFetch discipline dimensions:
1. Redirect detection and follow-up:
   - Detecting redirect responses
   - Executing follow-up fetch with redirect URL

2. Authenticated URL avoidance:
   - Avoiding GitHub, Google Docs, Confluence, Jira
   - Using specialized tools instead

3. Prompt specificity:
   - Clear extraction instructions
   - Targeted content queries

4. Cache behavior awareness:
   - Understanding 15-minute cache
   - Avoiding redundant fetches

5. Retry discipline:
   - Retrying after transient failures
   - Not retrying permanent failures

Quality indicators:
- 100% redirect follow-up rate
- No authenticated URL attempts
- Specific prompts (>80% clarity)
- Cache-aware behavior
- Intelligent retry patterns
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class WebFetchCall:
    """Represents a WebFetch tool call."""

    turn_index: int
    url: str
    prompt: str
    was_redirect: bool
    redirect_followed_up: bool
    is_authenticated_url: bool  # GitHub, Docs, etc.
    prompt_is_specific: bool
    was_retry: bool
    retry_after_transient_failure: bool
    within_cache_window: bool  # Within 15 min of previous fetch


@dataclass(frozen=True)
class Finding:
    """Represents a WebFetch discipline finding."""

    severity: str
    category: str
    message: str
    turn_index: int
    example: str


@dataclass(frozen=True)
class WebFetchMetrics:
    """Aggregate metrics for WebFetch discipline."""

    total_fetches: int
    redirect_count: int
    redirect_followed_up_count: int
    redirect_followup_rate: float
    authenticated_url_attempts: int
    specific_prompt_count: int
    prompt_specificity_rate: float
    cache_aware_count: int
    retry_count: int
    intelligent_retry_count: int
    findings_count: int
    critical_findings: int
    warning_findings: int


def analyze_session_webfetch_redirect_handling(
    fetches: Sequence[WebFetchCall],
) -> tuple[WebFetchMetrics, Sequence[Finding]]:
    """Analyze WebFetch redirect handling and discipline.

    Args:
        fetches: Sequence of WebFetchCall instances

    Returns:
        Tuple of (metrics, findings)

    Raises:
        ValueError: If fetches is invalid
    """
    _validate_fetches(fetches)

    if not fetches:
        return (
            WebFetchMetrics(
                total_fetches=0,
                redirect_count=0,
                redirect_followed_up_count=0,
                redirect_followup_rate=0.0,
                authenticated_url_attempts=0,
                specific_prompt_count=0,
                prompt_specificity_rate=0.0,
                cache_aware_count=0,
                retry_count=0,
                intelligent_retry_count=0,
                findings_count=0,
                critical_findings=0,
                warning_findings=0,
            ),
            (),
        )

    findings: list[Finding] = []

    redirect_count = sum(1 for f in fetches if f.was_redirect)
    redirect_followed_up = sum(
        1 for f in fetches if f.was_redirect and f.redirect_followed_up
    )
    authenticated_attempts = sum(1 for f in fetches if f.is_authenticated_url)
    specific_prompt_count = sum(1 for f in fetches if f.prompt_is_specific)
    cache_aware_count = sum(1 for f in fetches if not f.within_cache_window)
    retry_count = sum(1 for f in fetches if f.was_retry)
    intelligent_retry_count = sum(
        1 for f in fetches if f.was_retry and f.retry_after_transient_failure
    )

    findings.extend(_detect_missing_redirect_followup(fetches))
    findings.extend(_detect_authenticated_url_attempts(fetches))
    findings.extend(_detect_vague_prompts(fetches))
    findings.extend(_detect_cache_unawareness(fetches))
    findings.extend(_detect_unintelligent_retries(fetches))

    critical_findings = sum(1 for f in findings if f.severity == "critical")
    warning_findings = sum(1 for f in findings if f.severity == "warning")

    total = len(fetches)
    metrics = WebFetchMetrics(
        total_fetches=total,
        redirect_count=redirect_count,
        redirect_followed_up_count=redirect_followed_up,
        redirect_followup_rate=_percentage(redirect_followed_up, redirect_count),
        authenticated_url_attempts=authenticated_attempts,
        specific_prompt_count=specific_prompt_count,
        prompt_specificity_rate=_percentage(specific_prompt_count, total),
        cache_aware_count=cache_aware_count,
        retry_count=retry_count,
        intelligent_retry_count=intelligent_retry_count,
        findings_count=len(findings),
        critical_findings=critical_findings,
        warning_findings=warning_findings,
    )

    return metrics, tuple(findings)


def _validate_fetches(fetches: Sequence[WebFetchCall]) -> None:
    """Validate fetches structure."""
    if not isinstance(fetches, (list, tuple)):
        raise ValueError("fetches must be a list or tuple")

    for i, fetch in enumerate(fetches):
        if not isinstance(fetch, WebFetchCall):
            raise ValueError(f"fetches[{i}] must be a WebFetchCall instance")


def _detect_missing_redirect_followup(
    fetches: Sequence[WebFetchCall],
) -> list[Finding]:
    """Detect redirects without follow-up fetch."""
    findings: list[Finding] = []

    for fetch in fetches:
        if fetch.was_redirect and not fetch.redirect_followed_up:
            findings.append(
                Finding(
                    severity="critical",
                    category="redirect_handling",
                    message=(
                        "WebFetch returned redirect but no follow-up fetch executed. "
                        "Make new WebFetch request with redirect URL."
                    ),
                    turn_index=fetch.turn_index,
                    example=fetch.url,
                )
            )

    return findings


def _detect_authenticated_url_attempts(
    fetches: Sequence[WebFetchCall],
) -> list[Finding]:
    """Detect attempts to fetch authenticated URLs."""
    findings: list[Finding] = []

    for fetch in fetches:
        if fetch.is_authenticated_url:
            findings.append(
                Finding(
                    severity="critical",
                    category="authenticated_url_avoidance",
                    message=(
                        "Attempted to fetch authenticated URL. "
                        "Use specialized tool (gh CLI, etc.) instead of WebFetch."
                    ),
                    turn_index=fetch.turn_index,
                    example=fetch.url,
                )
            )

    return findings


def _detect_vague_prompts(fetches: Sequence[WebFetchCall]) -> list[Finding]:
    """Detect vague or generic prompts."""
    findings: list[Finding] = []

    for fetch in fetches:
        if not fetch.prompt_is_specific:
            findings.append(
                Finding(
                    severity="warning",
                    category="prompt_specificity",
                    message=(
                        "WebFetch prompt is vague. "
                        "Provide specific extraction instructions for better results."
                    ),
                    turn_index=fetch.turn_index,
                    example=f"{fetch.url}: '{fetch.prompt[:50]}'",
                )
            )

    return findings


def _detect_cache_unawareness(
    fetches: Sequence[WebFetchCall],
) -> list[Finding]:
    """Detect fetches within cache window."""
    findings: list[Finding] = []

    for fetch in fetches:
        if fetch.within_cache_window:
            findings.append(
                Finding(
                    severity="warning",
                    category="cache_awareness",
                    message=(
                        "WebFetch within 15-minute cache window. "
                        "Result will be cached, avoid redundant fetches."
                    ),
                    turn_index=fetch.turn_index,
                    example=fetch.url,
                )
            )

    return findings


def _detect_unintelligent_retries(
    fetches: Sequence[WebFetchCall],
) -> list[Finding]:
    """Detect retries after permanent failures."""
    findings: list[Finding] = []

    for fetch in fetches:
        if fetch.was_retry and not fetch.retry_after_transient_failure:
            findings.append(
                Finding(
                    severity="warning",
                    category="retry_discipline",
                    message=(
                        "Retry after permanent failure. "
                        "Only retry transient failures (timeouts, network errors)."
                    ),
                    turn_index=fetch.turn_index,
                    example=fetch.url,
                )
            )

    return findings


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
