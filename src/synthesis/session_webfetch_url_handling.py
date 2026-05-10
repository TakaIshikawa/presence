"""Analyze session-level WebFetch tool usage, URL validation, and redirect following.

Dimensions
----------
1. URL source discipline – URLs from user messages vs generated/guessed.
2. Redirect handling – Whether redirects are followed correctly.
3. Prompt quality – Whether WebFetch prompts are specific enough.
4. Fallback behavior – Whether sessions fall back to gh CLI for GitHub URLs.
5. Cache utilization – Whether repeated fetches leverage caching.

Metrics
-------
- total_sessions
- total_webfetch_calls
- user_provided_url_rate
- generated_url_rate
- redirect_follow_rate
- redirect_missed_rate
- github_url_cli_fallback_rate
- avg_prompt_length
- repeated_url_cache_hit_rate
- high_quality_sessions
- low_quality_sessions
- webfetch_url_handling_score

Quality indicators
------------------
- high_quality_sessions: session score > 0.7
- low_quality_sessions: session score < 0.4
- webfetch_url_handling_score: weighted aggregate 0-1
"""

from __future__ import annotations

from typing import Any, Mapping


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _empty_result() -> dict[str, Any]:
    return {
        "total_sessions": 0,
        "total_webfetch_calls": 0,
        "user_provided_url_rate": 0.0,
        "generated_url_rate": 0.0,
        "redirect_follow_rate": 0.0,
        "redirect_missed_rate": 0.0,
        "github_url_cli_fallback_rate": 0.0,
        "avg_prompt_length": 0.0,
        "repeated_url_cache_hit_rate": 0.0,
        "high_quality_sessions": 0,
        "low_quality_sessions": 0,
        "webfetch_url_handling_score": 0.0,
    }


# ---------------------------------------------------------------------------
# Session score
# ---------------------------------------------------------------------------

def _session_score(record: Mapping[str, Any]) -> float:
    score = 0.0

    # 1. User-provided URL rate (0-0.30): >80% from user = full points
    total_calls = _int(record.get("total_webfetch_calls"))
    user_urls = _int(record.get("user_provided_urls"))
    if total_calls > 0:
        user_rate = user_urls / total_calls
        score += min(user_rate / 0.80, 1.0) * 0.30

    # 2. Redirect follow rate (0-0.25): >90% followed = full points
    redirect_detected = _int(record.get("redirect_detected"))
    redirect_followed = _int(record.get("redirect_followed"))
    if redirect_detected > 0:
        follow_rate = redirect_followed / redirect_detected
        score += min(follow_rate / 0.90, 1.0) * 0.25
    else:
        # No redirects encountered – give full credit
        score += 0.25

    # 3. GitHub CLI fallback (0-0.20): >70% GitHub URLs via CLI = full points
    github_total = _int(record.get("github_urls_total"))
    github_cli = _int(record.get("github_urls_via_cli"))
    if github_total > 0:
        cli_rate = github_cli / github_total
        score += min(cli_rate / 0.70, 1.0) * 0.20
    else:
        # No GitHub URLs – give full credit
        score += 0.20

    # 4. Prompt quality (0-0.15): avg_prompt_length > 20 chars = full points
    avg_prompt = _float(record.get("avg_prompt_length"))
    if avg_prompt > 0:
        score += min(avg_prompt / 20.0, 1.0) * 0.15
    # No prompts – no credit

    # 5. Cache utilization (0-0.10): >50% cache hits on repeated = full points
    repeated = _int(record.get("repeated_url_fetches"))
    cache_hits = _int(record.get("repeated_url_cache_hits"))
    if repeated > 0:
        cache_rate = cache_hits / repeated
        score += min(cache_rate / 0.50, 1.0) * 0.10
    else:
        # No repeated fetches – give full credit
        score += 0.10

    return round(min(score, 1.0), 4)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def analyze_session_webfetch_url_handling(records: object) -> dict[str, Any]:
    """Analyze WebFetch URL handling quality across sessions.

    Parameters
    ----------
    records:
        A list of session dictionaries. ``None`` returns an empty result.
        Any other non-list type raises ``ValueError``.

    Returns
    -------
    dict[str, Any]
        Aggregated metrics and an overall quality score (0-1).
    """
    if records is None:
        return _empty_result()

    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    total_webfetch_calls = 0
    total_user_urls = 0
    total_generated_urls = 0
    total_redirect_detected = 0
    total_redirect_followed = 0
    total_redirect_missed = 0
    total_github_urls = 0
    total_github_cli = 0
    all_prompt_lengths: list[float] = []
    total_repeated = 0
    total_cache_hits = 0

    session_scores: list[float] = []
    high_quality = 0
    low_quality = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        wf_calls = _int(record.get("total_webfetch_calls"))
        total_webfetch_calls += wf_calls

        total_user_urls += _int(record.get("user_provided_urls"))
        total_generated_urls += _int(record.get("generated_urls"))

        total_redirect_detected += _int(record.get("redirect_detected"))
        total_redirect_followed += _int(record.get("redirect_followed"))
        total_redirect_missed += _int(record.get("redirect_missed"))

        total_github_urls += _int(record.get("github_urls_total"))
        total_github_cli += _int(record.get("github_urls_via_cli"))

        prompt_len = _float(record.get("avg_prompt_length"))
        if wf_calls > 0:
            all_prompt_lengths.append(prompt_len)

        total_repeated += _int(record.get("repeated_url_fetches"))
        total_cache_hits += _int(record.get("repeated_url_cache_hits"))

        s = _session_score(record)
        session_scores.append(s)

        if s > 0.7:
            high_quality += 1
        elif s < 0.4:
            low_quality += 1

    if total_sessions == 0:
        return _empty_result()

    return {
        "total_sessions": total_sessions,
        "total_webfetch_calls": total_webfetch_calls,
        "user_provided_url_rate": _percentage(total_user_urls, total_webfetch_calls),
        "generated_url_rate": _percentage(total_generated_urls, total_webfetch_calls),
        "redirect_follow_rate": _percentage(total_redirect_followed, total_redirect_detected),
        "redirect_missed_rate": _percentage(total_redirect_missed, total_redirect_detected),
        "github_url_cli_fallback_rate": _percentage(total_github_cli, total_github_urls),
        "avg_prompt_length": _average(all_prompt_lengths),
        "repeated_url_cache_hit_rate": _percentage(total_cache_hits, total_repeated),
        "high_quality_sessions": high_quality,
        "low_quality_sessions": low_quality,
        "webfetch_url_handling_score": _average(session_scores),
    }
