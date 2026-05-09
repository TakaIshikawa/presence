"""Session webfetch reliability analyzer.

Analyzes WebFetch and WebSearch tool usage patterns and success rates in sessions.
Tracks fetch attempts, success/failure rates, redirect handling, authentication failures,
cache opportunities, and correlation with task completion.

Webfetch reliability metrics:
- Total webfetch/websearch calls: Number of web tool invocations
- Success rate: Percentage of successful fetches vs failures
- Redirect handling: How often redirects are followed correctly
- Authentication failure detection: Failed fetches due to auth issues
- Cache hit opportunities: Fetches that could have been cached
- Missing sources detection: WebSearch without Sources section in response
- Correlation with task incompletion: Web tool failures leading to incomplete tasks

Quality indicators:
- High success rate (>85%): Most web fetches succeed
- Low redirect failures (<10%): Redirects handled correctly
- Low auth failure rate (<5%): Proper authentication or fallback
- Low missing sources rate (<10%): Sources properly included after WebSearch
- Few incomplete tasks due to web failures: Web issues don't block completion
- High cache utilization (>30%): Effective reuse of fetched content
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_webfetch_reliability(records: object) -> dict[str, Any]:
    """Analyze WebFetch and WebSearch tool usage patterns and success rates.

    Tracks web tool reliability, redirect handling, and impact on task completion.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_webfetch_calls: Total WebFetch tool calls
            - total_websearch_calls: Total WebSearch tool calls
            - successful_fetches: Number of successful fetches
            - failed_fetches: Number of failed fetches
            - redirect_handled: Redirects followed correctly
            - redirect_failed: Redirects not handled properly
            - auth_failures: Fetches failed due to authentication
            - cache_hits: Fetches served from cache
            - cache_misses: Fetches that could have been cached but weren't
            - websearch_with_sources: WebSearch with Sources section
            - websearch_without_sources: WebSearch missing Sources section
            - tasks_incomplete_due_to_web_failure: Tasks failed due to web issues
            - total_tasks: Total tasks in session
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_web_calls: Sessions using web tools
            - avg_total_web_calls: Average web calls per session
            - avg_webfetch_calls: Average WebFetch calls per session
            - avg_websearch_calls: Average WebSearch calls per session
            - avg_success_rate: Average % successful fetches
            - avg_redirect_success_rate: Average % redirects handled
            - avg_auth_failure_rate: Average % auth failures
            - avg_cache_hit_rate: Average % cache hits
            - avg_sources_inclusion_rate: Average % WebSearch with Sources
            - avg_web_failure_impact: Average % tasks failed due to web issues
            - high_reliability_sessions: Count with >90% success rate
            - low_reliability_sessions: Count with <70% success rate
            - sessions_missing_sources: Count with >20% missing Sources
            - sessions_with_auth_issues: Count with any auth failures

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_web_calls = 0

    total_web_calls: list[int | float] = []
    webfetch_calls: list[int | float] = []
    websearch_calls: list[int | float] = []
    success_rates: list[float] = []
    redirect_success_rates: list[float] = []
    auth_failure_rates: list[float] = []
    cache_hit_rates: list[float] = []
    sources_inclusion_rates: list[float] = []
    web_failure_impacts: list[float] = []

    high_reliability_sessions = 0  # >90% success rate
    low_reliability_sessions = 0   # <70% success rate
    sessions_missing_sources = 0   # >20% missing Sources
    sessions_with_auth_issues = 0  # Any auth failures

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_webfetch = _extract_number(record.get("total_webfetch_calls"))
        total_websearch = _extract_number(record.get("total_websearch_calls"))
        successful = _extract_number(record.get("successful_fetches"))
        failed = _extract_number(record.get("failed_fetches"))
        redirect_handled = _extract_number(record.get("redirect_handled"))
        redirect_failed = _extract_number(record.get("redirect_failed"))
        auth_failures = _extract_number(record.get("auth_failures"))
        cache_hits = _extract_number(record.get("cache_hits"))
        cache_misses = _extract_number(record.get("cache_misses"))
        with_sources = _extract_number(record.get("websearch_with_sources"))
        without_sources = _extract_number(record.get("websearch_without_sources"))
        tasks_incomplete = _extract_number(record.get("tasks_incomplete_due_to_web_failure"))
        total_tasks = _extract_number(record.get("total_tasks"))

        # Calculate total web calls
        total_web = 0
        if total_webfetch is not None:
            total_web += total_webfetch
            webfetch_calls.append(total_webfetch)
        if total_websearch is not None:
            total_web += total_websearch
            websearch_calls.append(total_websearch)

        if total_web > 0:
            sessions_with_web_calls += 1
            total_web_calls.append(total_web)

            # Calculate success rate
            if successful is not None and failed is not None:
                total_attempts = successful + failed
                if total_attempts > 0:
                    success_rate = _percentage(successful, total_attempts)
                    success_rates.append(success_rate)

                    if success_rate > 90.0:
                        high_reliability_sessions += 1
                    elif success_rate < 70.0:
                        low_reliability_sessions += 1

            # Calculate redirect success rate
            if redirect_handled is not None and redirect_failed is not None:
                total_redirects = redirect_handled + redirect_failed
                if total_redirects > 0:
                    redirect_success_rates.append(_percentage(redirect_handled, total_redirects))

            # Calculate auth failure rate
            if auth_failures is not None and successful is not None and failed is not None:
                total_attempts = successful + failed
                if total_attempts > 0:
                    auth_failure_rate = _percentage(auth_failures, total_attempts)
                    auth_failure_rates.append(auth_failure_rate)

                    if auth_failures > 0:
                        sessions_with_auth_issues += 1

            # Calculate cache hit rate
            if cache_hits is not None and cache_misses is not None:
                total_cache_opportunities = cache_hits + cache_misses
                if total_cache_opportunities > 0:
                    cache_hit_rates.append(_percentage(cache_hits, total_cache_opportunities))

            # Calculate sources inclusion rate
            if with_sources is not None and without_sources is not None:
                total_websearch_count = with_sources + without_sources
                if total_websearch_count > 0:
                    sources_rate = _percentage(with_sources, total_websearch_count)
                    sources_inclusion_rates.append(sources_rate)

                    if (100.0 - sources_rate) > 20.0:  # >20% missing
                        sessions_missing_sources += 1

            # Calculate web failure impact
            if tasks_incomplete is not None and total_tasks is not None and total_tasks > 0:
                web_failure_impacts.append(_percentage(tasks_incomplete, total_tasks))

    # Calculate aggregate metrics
    avg_total_web = _average(total_web_calls)
    avg_webfetch = _average(webfetch_calls)
    avg_websearch = _average(websearch_calls)
    avg_success = _average(success_rates)
    avg_redirect_success = _average(redirect_success_rates)
    avg_auth_failure = _average(auth_failure_rates)
    avg_cache_hit = _average(cache_hit_rates)
    avg_sources = _average(sources_inclusion_rates)
    avg_failure_impact = _average(web_failure_impacts)

    return {
        "total_sessions": total_sessions,
        "sessions_with_web_calls": sessions_with_web_calls,
        "avg_total_web_calls": avg_total_web,
        "avg_webfetch_calls": avg_webfetch,
        "avg_websearch_calls": avg_websearch,
        "avg_success_rate": avg_success,
        "avg_redirect_success_rate": avg_redirect_success,
        "avg_auth_failure_rate": avg_auth_failure,
        "avg_cache_hit_rate": avg_cache_hit,
        "avg_sources_inclusion_rate": avg_sources,
        "avg_web_failure_impact": avg_failure_impact,
        "high_reliability_sessions": high_reliability_sessions,
        "low_reliability_sessions": low_reliability_sessions,
        "sessions_missing_sources": sessions_missing_sources,
        "sessions_with_auth_issues": sessions_with_auth_issues,
    }


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value (int or float) if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
