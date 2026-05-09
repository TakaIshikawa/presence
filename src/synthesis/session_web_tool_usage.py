"""Session WebFetch and WebSearch usage analyzer for research patterns.

Analyzes Claude Code session transcripts for WebFetch and WebSearch tool usage patterns
to measure research effectiveness, citation discipline, and web tool integration quality.

Web tool metrics:
- WebFetch and WebSearch call counts: Frequency of web research
- Web tool ratio: Web calls as percentage of total tool calls
- Sources cited ratio: Fraction of web calls followed by citation in response
- Redirect handling: WebFetch redirects properly handled with follow-up requests
- Authentication errors: Failed authenticated URL attempts (Google Docs, etc.)

Quality indicators:
- Appropriate web tool usage: Used when information is beyond knowledge cutoff
- High citation rate: >80% of web calls followed by Sources section
- Good redirect handling: WebFetch redirects followed up with new requests
- Low authentication errors: Minimal attempts to access authenticated services
- Balanced web/local ratio: Web research complements local file operations
"""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_session_web_tool_usage(records: object) -> dict[str, Any]:
    """Analyze WebFetch and WebSearch tool usage in agent sessions.

    Evaluates web research patterns, citation discipline, redirect handling,
    and authentication error avoidance.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_name: Name of the tool (WebFetch, WebSearch, etc.)
            - tool_params: Optional dict with tool parameters (url, query)
            - tool_result: Optional string with tool result/error
            - assistant_response: Optional assistant text after tool call
            - total_tool_calls: Optional total tools called this turn

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - total_tool_calls: Total tool calls across all turns
            - web_fetch_count: Number of WebFetch calls
            - web_search_count: Number of WebSearch calls
            - total_web_calls: Sum of WebFetch and WebSearch calls
            - web_tool_ratio: Percentage of web calls vs total tool calls
            - web_calls_with_citations: Web calls followed by Sources section
            - sources_cited_ratio: Percentage of web calls with citations
            - redirect_handling_count: WebFetch redirects properly handled
            - redirect_handling_ratio: Percentage of redirects handled correctly
            - authentication_error_count: Failed authenticated URL attempts
            - authentication_error_ratio: Percentage of web calls with auth errors
            - web_research_effectiveness_score: 0-1 overall score

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    total_turns = 0
    total_tool_calls = 0
    web_fetch_count = 0
    web_search_count = 0

    web_calls_with_citations = 0
    redirect_handling_count = 0
    redirect_opportunities = 0
    authentication_error_count = 0

    # Track web calls and their responses for citation checking
    web_call_indices: list[int] = []
    redirect_indices: list[int] = []

    for i, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        total_turns += 1
        tool_name = _string(record.get("tool_name"))

        # Count tool calls
        turn_tool_count = _int(record.get("total_tool_calls", 0))
        if turn_tool_count > 0:
            total_tool_calls += turn_tool_count
        elif tool_name:  # Single tool call
            total_tool_calls += 1

        if not tool_name:
            continue

        # Track WebFetch and WebSearch
        tool_lower = tool_name.lower()
        if tool_lower == "webfetch":
            web_fetch_count += 1
            web_call_indices.append(i)

            # Check for redirect in tool result
            tool_result = _string(record.get("tool_result", ""))
            if _is_redirect(tool_result):
                redirect_indices.append(i)
                redirect_opportunities += 1
                # Check if next WebFetch follows the redirect
                if _check_redirect_handling(records, i):
                    redirect_handling_count += 1

            # Check for authentication errors
            if _is_auth_error(tool_result):
                authentication_error_count += 1

        elif tool_lower == "websearch":
            web_search_count += 1
            web_call_indices.append(i)

    # Check citations for web calls
    for web_idx in web_call_indices:
        if _has_citation_in_response(records, web_idx):
            web_calls_with_citations += 1

    # Calculate metrics
    total_web_calls = web_fetch_count + web_search_count
    web_tool_ratio = _percentage(total_web_calls, total_tool_calls)
    sources_cited_ratio = _percentage(web_calls_with_citations, total_web_calls)
    redirect_handling_ratio = _percentage(redirect_handling_count, redirect_opportunities)
    authentication_error_ratio = _percentage(authentication_error_count, total_web_calls)

    # Calculate effectiveness score
    effectiveness_score = _calculate_effectiveness_score(
        sources_cited_ratio,
        redirect_handling_ratio,
        authentication_error_ratio,
        web_tool_ratio,
        total_tool_calls,
    )

    return {
        "total_turns": total_turns,
        "total_tool_calls": total_tool_calls,
        "web_fetch_count": web_fetch_count,
        "web_search_count": web_search_count,
        "total_web_calls": total_web_calls,
        "web_tool_ratio": web_tool_ratio,
        "web_calls_with_citations": web_calls_with_citations,
        "sources_cited_ratio": sources_cited_ratio,
        "redirect_handling_count": redirect_handling_count,
        "redirect_handling_ratio": redirect_handling_ratio,
        "authentication_error_count": authentication_error_count,
        "authentication_error_ratio": authentication_error_ratio,
        "web_research_effectiveness_score": effectiveness_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "total_tool_calls": 0,
        "web_fetch_count": 0,
        "web_search_count": 0,
        "total_web_calls": 0,
        "web_tool_ratio": 0.0,
        "web_calls_with_citations": 0,
        "sources_cited_ratio": 0.0,
        "redirect_handling_count": 0,
        "redirect_handling_ratio": 0.0,
        "authentication_error_count": 0,
        "authentication_error_ratio": 0.0,
        "web_research_effectiveness_score": 0.0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _is_redirect(tool_result: str) -> bool:
    """Check if tool result indicates a redirect to a different host."""
    if not tool_result:
        return False
    # Look for redirect patterns in WebFetch tool results
    redirect_patterns = [
        r"redirect.*to.*different host",
        r"redirected to",
        r"redirect.*url",
    ]
    tool_lower = tool_result.lower()
    return any(re.search(pattern, tool_lower) for pattern in redirect_patterns)


def _is_auth_error(tool_result: str) -> bool:
    """Check if tool result indicates authentication/authorization error."""
    if not tool_result:
        return False
    auth_indicators = [
        "authenticated",
        "authorization",
        "login required",
        "access denied",
        "forbidden",
        "google docs",
        "confluence",
        "jira",
    ]
    tool_lower = tool_result.lower()
    return any(indicator in tool_lower for indicator in auth_indicators)


def _check_redirect_handling(records: list, redirect_idx: int) -> bool:
    """Check if a redirect at redirect_idx was followed by a new WebFetch request.

    Args:
        records: List of turn records
        redirect_idx: Index of the redirect

    Returns:
        True if the next 1-2 turns contain a WebFetch call (indicating follow-up)
    """
    # Check next 2 turns for WebFetch
    for i in range(redirect_idx + 1, min(redirect_idx + 3, len(records))):
        if i >= len(records):
            break
        record = records[i]
        if not isinstance(record, Mapping):
            continue
        tool_name = _string(record.get("tool_name"))
        if tool_name.lower() == "webfetch":
            return True
    return False


def _has_citation_in_response(records: list, web_call_idx: int) -> bool:
    """Check if assistant response after web call includes Sources section.

    Args:
        records: List of turn records
        web_call_idx: Index of the web tool call

    Returns:
        True if the current turn's response contains a Sources section
    """
    # Check only the current turn's response
    if web_call_idx >= len(records):
        return False

    record = records[web_call_idx]
    if not isinstance(record, Mapping):
        return False

    response = _string(record.get("assistant_response", ""))
    if not response:
        return False

    # Look for Sources section patterns
    return _contains_sources_section(response)


def _contains_sources_section(text: str) -> bool:
    """Check if text contains a Sources section with citations."""
    if not text:
        return False

    # Look for Sources header followed by markdown links
    sources_patterns = [
        r"(?i)sources?\s*:.*?\[.+?\]\(.+?\)",  # Sources: followed by markdown link
        r"(?i)##\s*sources?\s*\n.*?\[.+?\]\(.+?\)",  # ## Sources header
    ]

    return any(re.search(pattern, text, re.DOTALL) for pattern in sources_patterns)


def _calculate_effectiveness_score(
    citation_ratio: float,
    redirect_ratio: float,
    auth_error_ratio: float,
    web_tool_ratio: float,
    total_tools: int,
) -> float:
    """Calculate web research effectiveness score (0-1).

    Score components:
    - 0.4: Citation discipline (high citation ratio is good)
    - 0.3: Redirect handling (high handling ratio is good)
    - 0.2: Auth error avoidance (low error ratio is good)
    - 0.1: Appropriate usage (balanced web/total ratio)
    """
    if total_tools == 0:
        return 0.0

    # Citation component (0-0.4)
    # Target: >80% citation rate
    citation_component = min(0.4, (citation_ratio / 100.0) * 0.5)

    # Redirect handling component (0-0.3)
    # Perfect handling = 100%
    redirect_component = (redirect_ratio / 100.0) * 0.3

    # Auth error component (0-0.2)
    # Lower is better; 0% errors = full points
    auth_error_component = max(0.0, 0.2 - (auth_error_ratio / 100.0) * 0.2)

    # Usage balance component (0-0.1)
    # Optimal: 5-20% web tools (not too many, not too few)
    if 5 <= web_tool_ratio <= 20:
        usage_component = 0.1
    elif web_tool_ratio < 5:
        usage_component = (web_tool_ratio / 5.0) * 0.1
    else:
        # Penalize excessive web usage
        usage_component = max(0.0, 0.1 - (web_tool_ratio - 20) / 200.0)

    score = (
        citation_component +
        redirect_component +
        auth_error_component +
        usage_component
    )

    return round(max(0.0, min(1.0, score)), 3)
