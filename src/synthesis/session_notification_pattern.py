"""Session user notification and communication pattern analyzer.

Analyzes how Claude communicates progress and results to users in session transcripts.
Measures tool description clarity, result communication practices, emoji usage violations,
thinking-aloud patterns, and sources citation compliance.

Communication metrics:
- Tool description clarity: Fraction of Bash/Task calls with clear descriptions
- Result communication ratio: Tool results followed by user-facing summary
- Emoji usage count: Unauthorized emoji usage violations
- Thinking aloud ratio: Commentary before tool calls vs direct execution
- Sources citation compliance: WebSearch calls followed by Sources section

Quality indicators:
- High description clarity: >90% of Bash/Task calls have clear descriptions
- High result communication: >80% of tool results summarized for user
- Zero emoji usage: No emojis unless explicitly requested by user
- Balanced thinking aloud: 30-70% (some context, not excessive)
- Perfect citation: 100% of WebSearch calls include Sources section
"""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_session_notification_pattern(records: object) -> dict[str, Any]:
    """Analyze communication and notification patterns in agent sessions.

    Evaluates how effectively Claude communicates with users, tracking tool
    description quality, result summaries, emoji violations, and citation compliance.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_name: Name of the tool used
            - tool_description: Optional description for Bash/Task calls
            - tool_result: Optional tool result text
            - assistant_response: Assistant text after tool call
            - assistant_text_before_tools: Optional text before tool calls

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - bash_task_calls: Total Bash and Task tool calls
            - bash_task_with_description: Calls with clear descriptions
            - tool_description_clarity_score: Percentage with descriptions
            - tool_results_count: Total tool calls returning results
            - results_with_summary: Results followed by user-facing summary
            - result_communication_ratio: Percentage of results communicated
            - emoji_usage_count: Unauthorized emoji occurrences
            - turns_with_commentary: Turns with text before tool calls
            - thinking_aloud_ratio: Percentage of turns with pre-tool commentary
            - websearch_calls: Total WebSearch tool calls
            - websearch_with_sources: WebSearch followed by Sources section
            - sources_citation_compliance: Percentage of WebSearch with citations
            - communication_quality_score: 0-1 overall communication score

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
    bash_task_calls = 0
    bash_task_with_description = 0

    tool_results_count = 0
    results_with_summary = 0

    emoji_usage_count = 0

    turns_with_commentary = 0

    websearch_calls = 0
    websearch_with_sources = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1
        tool_name = _string(record.get("tool_name"))

        # Track Bash/Task description clarity
        if tool_name.lower() in ("bash", "task"):
            bash_task_calls += 1
            tool_desc = _string(record.get("tool_description", ""))
            if tool_desc and len(tool_desc) >= 5:  # Meaningful description
                bash_task_with_description += 1

        # Track tool result communication
        tool_result = _string(record.get("tool_result", ""))
        if tool_result:
            tool_results_count += 1
            assistant_response = _string(record.get("assistant_response", ""))
            if _has_result_summary(assistant_response):
                results_with_summary += 1

        # Track emoji usage violations
        assistant_response = _string(record.get("assistant_response", ""))
        emoji_count = _count_emojis(assistant_response)
        emoji_usage_count += emoji_count

        # Track thinking aloud (commentary before tools)
        text_before = _string(record.get("assistant_text_before_tools", ""))
        if text_before and len(text_before) > 10:  # Meaningful commentary
            turns_with_commentary += 1

        # Track WebSearch citation compliance
        if tool_name.lower() == "websearch":
            websearch_calls += 1
            if _contains_sources_section(assistant_response):
                websearch_with_sources += 1

    # Calculate metrics
    tool_description_clarity_score = _percentage(bash_task_with_description, bash_task_calls)
    result_communication_ratio = _percentage(results_with_summary, tool_results_count)
    thinking_aloud_ratio = _percentage(turns_with_commentary, total_turns)
    sources_citation_compliance = _percentage(websearch_with_sources, websearch_calls)

    # Calculate quality score
    quality_score = _calculate_quality_score(
        tool_description_clarity_score,
        result_communication_ratio,
        emoji_usage_count,
        thinking_aloud_ratio,
        sources_citation_compliance,
        total_turns,
    )

    return {
        "total_turns": total_turns,
        "bash_task_calls": bash_task_calls,
        "bash_task_with_description": bash_task_with_description,
        "tool_description_clarity_score": tool_description_clarity_score,
        "tool_results_count": tool_results_count,
        "results_with_summary": results_with_summary,
        "result_communication_ratio": result_communication_ratio,
        "emoji_usage_count": emoji_usage_count,
        "turns_with_commentary": turns_with_commentary,
        "thinking_aloud_ratio": thinking_aloud_ratio,
        "websearch_calls": websearch_calls,
        "websearch_with_sources": websearch_with_sources,
        "sources_citation_compliance": sources_citation_compliance,
        "communication_quality_score": quality_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "bash_task_calls": 0,
        "bash_task_with_description": 0,
        "tool_description_clarity_score": 0.0,
        "tool_results_count": 0,
        "results_with_summary": 0,
        "result_communication_ratio": 0.0,
        "emoji_usage_count": 0,
        "turns_with_commentary": 0,
        "thinking_aloud_ratio": 0.0,
        "websearch_calls": 0,
        "websearch_with_sources": 0,
        "sources_citation_compliance": 0.0,
        "communication_quality_score": 0.0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _has_result_summary(text: str) -> bool:
    """Check if text contains a user-facing summary of results.

    Looks for patterns like:
    - Sentences describing outcomes
    - Present tense explanations
    - References to findings
    """
    if not text or len(text) < 10:
        return False

    # Simple heuristic: contains multiple sentences or specific keywords
    summary_indicators = [
        r"\bfound\b",
        r"\bshows?\b",
        r"\bindicates?\b",
        r"\breturned\b",
        r"\bcompleted\b",
        r"\bsuccessfully\b",
        r"\bfailed\b",
        r"\berror\b",
        r"\bresult",
    ]

    # Check for summary indicators
    text_lower = text.lower()
    return any(re.search(pattern, text_lower) for pattern in summary_indicators)


def _count_emojis(text: str) -> int:
    """Count emoji characters in text.

    Uses Unicode emoji ranges to detect emojis.
    """
    if not text:
        return 0

    # Common emoji Unicode ranges
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # Emoticons
        "\U0001F300-\U0001F5FF"  # Symbols & Pictographs
        "\U0001F680-\U0001F6FF"  # Transport & Map
        "\U0001F1E0-\U0001F1FF"  # Flags
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251"  # Enclosed characters
        "]+",
        flags=re.UNICODE
    )

    matches = emoji_pattern.findall(text)
    return len(matches)


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


def _calculate_quality_score(
    description_clarity: float,
    result_communication: float,
    emoji_count: int,
    thinking_aloud: float,
    citation_compliance: float,
    total_turns: int,
) -> float:
    """Calculate communication quality score (0-1).

    Score components:
    - 0.3: Tool description clarity (>90% is optimal)
    - 0.25: Result communication (>80% is optimal)
    - 0.2: No emoji violations (0 is perfect)
    - 0.15: Citation compliance (100% is perfect)
    - 0.1: Balanced thinking aloud (30-70% is optimal)
    """
    if total_turns == 0:
        return 0.0

    # Description clarity component (0-0.3)
    if description_clarity >= 90:
        clarity_component = 0.3
    else:
        clarity_component = (description_clarity / 90.0) * 0.3

    # Result communication component (0-0.25)
    if result_communication >= 80:
        communication_component = 0.25
    else:
        communication_component = (result_communication / 80.0) * 0.25

    # Emoji component (0-0.2)
    # Penalize emoji usage: 0 emojis = full points
    emoji_penalty = min(emoji_count, 20)  # Cap at 20 for calculation
    emoji_component = max(0.0, 0.2 - (emoji_penalty / 20.0) * 0.2)

    # Citation compliance component (0-0.15)
    citation_component = (citation_compliance / 100.0) * 0.15

    # Thinking aloud component (0-0.1)
    # Optimal: 30-70% (balanced)
    if 30 <= thinking_aloud <= 70:
        thinking_component = 0.1
    elif thinking_aloud < 30:
        thinking_component = (thinking_aloud / 30.0) * 0.1
    else:
        # Penalize excessive thinking aloud
        thinking_component = max(0.0, 0.1 - (thinking_aloud - 70) / 300.0)

    score = (
        clarity_component +
        communication_component +
        emoji_component +
        citation_component +
        thinking_component
    )

    return round(max(0.0, min(1.0, score)), 3)
