"""Session user notification timing and session boundary discipline analyzer.

Analyzes how and when agents communicate results to users during sessions.
Evaluates notification completeness after Task/background Bash completion,
summary quality, session boundary discipline, progress updates, and error
communication.

Metrics tracked:
- task_completion_notifications: Messages after Task tool completion
- background_task_notifications: Messages after background Bash completion
- silent_task_consumptions: Task completes but no user message follows
- session_boundary_messages: Final message before session end
- error_explanations_count: Errors explained to user vs silently consumed

Quality scores (0-1):
- notification_completeness: Ratio of tool completions followed by user message
- summary_quality: Concise user-facing summaries vs assuming user sees output
- session_boundary_discipline: Final status message before session end
"""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_session_notification_timing(records: object) -> dict[str, Any]:
    """Analyze notification timing and session boundary discipline.

    Evaluates how effectively the agent communicates task results,
    provides progress updates, and maintains session boundary discipline.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_name: Name of the tool used (Task, Bash, etc.)
            - tool_params: Dict with tool parameters (run_in_background, etc.)
            - tool_result: Tool result text
            - assistant_response: Assistant text after tool call
            - is_error: Whether the tool result was an error
            - is_last_turn: Whether this is the last turn in the session

    Returns:
        Dict with metrics and scores.

    Raises:
        ValueError: If records is not a list.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    total_turns = 0
    task_completions = 0
    task_completion_notifications = 0

    background_task_completions = 0
    background_task_notifications = 0

    silent_task_consumptions = 0

    session_boundary_messages = 0
    sessions_with_boundary = 0
    sessions_checked_boundary = 0

    error_occurrences = 0
    error_explanations_count = 0

    summary_quality_scores: list[float] = []
    progress_updates = 0
    long_operations = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1
        tool_name = _string(record.get("tool_name"))
        tool_params = record.get("tool_params") or {}
        if not isinstance(tool_params, Mapping):
            tool_params = {}
        tool_result = _string(record.get("tool_result", ""))
        assistant_response = _string(record.get("assistant_response", ""))
        is_error = bool(record.get("is_error", False))
        is_last_turn = bool(record.get("is_last_turn", False))
        is_background = bool(tool_params.get("run_in_background", False))

        # Track Task tool completions and notifications
        if tool_name.lower() == "task":
            task_completions += 1
            if is_background:
                background_task_completions += 1
                if _has_notification(assistant_response):
                    background_task_notifications += 1
                else:
                    silent_task_consumptions += 1
            else:
                if _has_notification(assistant_response):
                    task_completion_notifications += 1
                else:
                    silent_task_consumptions += 1

        # Track background Bash completions
        elif tool_name.lower() == "bash" and is_background:
            background_task_completions += 1
            if _has_notification(assistant_response):
                background_task_notifications += 1
            else:
                silent_task_consumptions += 1

        # Track long operations for progress update detection
        elif tool_name.lower() == "bash" and not is_background:
            if _is_long_operation(tool_params):
                long_operations += 1
                if _has_progress_context(assistant_response):
                    progress_updates += 1

        # Track error communication
        if is_error or _result_contains_error(tool_result):
            error_occurrences += 1
            if _has_error_explanation(assistant_response):
                error_explanations_count += 1

        # Evaluate summary quality for tool completions
        if tool_result and tool_name.lower() in ("task", "bash"):
            quality = _evaluate_summary_quality(assistant_response, tool_result)
            summary_quality_scores.append(quality)

        # Track session boundary discipline
        if is_last_turn:
            sessions_checked_boundary += 1
            if _has_boundary_message(assistant_response):
                sessions_with_boundary += 1
                session_boundary_messages += 1

    # Calculate scores
    # Total completions: all Task calls + background Bash calls
    total_completions = task_completions + _count_background_bash(records)
    total_notified = task_completion_notifications + background_task_notifications

    notification_completeness = _score(total_notified, total_completions)
    summary_quality = _average_score(summary_quality_scores)
    session_boundary_discipline = _score(
        sessions_with_boundary, sessions_checked_boundary
    )

    return {
        "total_turns": total_turns,
        "task_completions": task_completions,
        "task_completion_notifications": task_completion_notifications,
        "background_task_completions": background_task_completions,
        "background_task_notifications": background_task_notifications,
        "silent_task_consumptions": silent_task_consumptions,
        "session_boundary_messages": session_boundary_messages,
        "error_occurrences": error_occurrences,
        "error_explanations_count": error_explanations_count,
        "long_operations": long_operations,
        "progress_updates": progress_updates,
        "notification_completeness": notification_completeness,
        "summary_quality": summary_quality,
        "session_boundary_discipline": session_boundary_discipline,
    }


def _empty_result() -> dict[str, Any]:
    """Return zeroed result structure."""
    return {
        "total_turns": 0,
        "task_completions": 0,
        "task_completion_notifications": 0,
        "background_task_completions": 0,
        "background_task_notifications": 0,
        "silent_task_consumptions": 0,
        "session_boundary_messages": 0,
        "error_occurrences": 0,
        "error_explanations_count": 0,
        "long_operations": 0,
        "progress_updates": 0,
        "notification_completeness": 0.0,
        "summary_quality": 0.0,
        "session_boundary_discipline": 0.0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _score(numerator: int | float, denominator: int | float) -> float:
    """Calculate 0-1 score from ratio."""
    if denominator <= 0:
        return 0.0
    return round(min(1.0, max(0.0, numerator / denominator)), 3)


def _average_score(scores: list[float]) -> float:
    """Calculate average of score list (0-1)."""
    if not scores:
        return 0.0
    return round(sum(scores) / len(scores), 3)


def _has_notification(text: str) -> bool:
    """Check if assistant response contains a user-facing notification.

    A notification is meaningful text that communicates results to the user,
    not just a tool call or empty response.
    """
    if not text or len(text) < 10:
        return False

    # Notification indicators: communicating outcomes
    indicators = [
        r"\bcompleted?\b",
        r"\bfinished\b",
        r"\bdone\b",
        r"\bresults?\b",
        r"\bfound\b",
        r"\bshows?\b",
        r"\bsuccessfully\b",
        r"\bfailed\b",
        r"\berror\b",
        r"\bhere(?:'s| is| are)\b",
        r"\boutput\b",
        r"\bsummary\b",
        r"\breturned?\b",
        r"\binstalled\b",
        r"\bpassing\b",
        r"\bfailing\b",
    ]

    text_lower = text.lower()
    return any(re.search(p, text_lower) for p in indicators)


def _has_progress_context(assistant_response: str) -> bool:
    """Check if there was a progress update for a long-running operation."""
    if not assistant_response:
        return False

    # Check for progress-related language
    progress_indicators = [
        r"\brunning\b",
        r"\bexecuting\b",
        r"\bwaiting\b",
        r"\bin progress\b",
        r"\bstarting\b",
        r"\bnow\b.*\b(build|test|install)",
        r"\blet me\b",
    ]

    text_lower = assistant_response.lower()
    return any(re.search(p, text_lower) for p in progress_indicators)


def _is_long_operation(tool_params: Mapping) -> bool:
    """Detect if a Bash command represents a long-running operation."""
    command = _string(tool_params.get("command", ""))
    if not command:
        return False

    long_patterns = [
        r"\bnpm\s+(install|ci|run\s+build|run\s+test)\b",
        r"\byarn\s+(install|build|test)\b",
        r"\bpip\s+install\b",
        r"\bpytest\b",
        r"\bmake\b",
        r"\bcargo\s+(build|test)\b",
        r"\bdocker\s+(build|pull)\b",
        r"\bgit\s+(clone|pull|push)\b",
    ]

    return any(re.search(p, command) for p in long_patterns)


def _result_contains_error(tool_result: str) -> bool:
    """Check if a tool result contains error signals."""
    if not tool_result:
        return False

    error_patterns = [
        r"\berror\b",
        r"\bfailed\b",
        r"\bexception\b",
        r"\btraceback\b",
        r"\bexit code [1-9]",
        r"\bFAILED\b",
        r"\bERROR\b",
    ]

    return any(re.search(p, tool_result, re.IGNORECASE) for p in error_patterns)


def _has_error_explanation(assistant_response: str) -> bool:
    """Check if assistant explains the error to the user."""
    if not assistant_response or len(assistant_response) < 15:
        return False

    explanation_patterns = [
        r"\berror\b.*\b(because|due to|caused by|means)\b",
        r"\bfailed\b.*\b(because|due to|caused by|need|missing)\b",
        r"\b(fix|resolve|address)\b.*\b(error|issue|problem)\b",
        r"\b(the|this)\s+(error|issue|problem)\b",
        r"\blet me\b.*\b(fix|investigate|look)\b",
        r"\bseems?\b.*\b(like|to be)\b",
        r"\b(need|requires?)\b.*\b(install|update|change|fix)\b",
    ]

    text_lower = assistant_response.lower()
    return any(re.search(p, text_lower) for p in explanation_patterns)


def _evaluate_summary_quality(assistant_response: str, tool_result: str) -> float:
    """Evaluate how well the assistant summarizes tool results (0-1).

    High quality: concise, user-facing summary with key takeaways.
    Low quality: no summary, or just echoing raw output.
    """
    if not assistant_response:
        return 0.0

    score = 0.0

    # Presence of any response is baseline
    if len(assistant_response) >= 10:
        score += 0.3

    # Check for actionable/informative language
    informative_patterns = [
        r"\bcompleted?\b",
        r"\bsuccessfully\b",
        r"\bfound\b",
        r"\bshows?\b",
        r"\bresult\b",
        r"\bpassed\b",
        r"\bfailed\b",
        r"\bneed\b",
    ]
    text_lower = assistant_response.lower()
    matches = sum(
        1 for p in informative_patterns if re.search(p, text_lower)
    )
    score += min(0.3, matches * 0.1)

    # Penalize if response is just echoing raw output (very long, same as result)
    if tool_result and len(assistant_response) > len(tool_result) * 2:
        score -= 0.1  # Possibly verbose echo

    # Reward concise but meaningful summaries
    if 20 <= len(assistant_response) <= 500:
        score += 0.2
    elif len(assistant_response) > 500:
        score += 0.1  # Still okay but verbose

    # Reward structured communication (bullet points, headers)
    if re.search(r"^[-*]\s", assistant_response, re.MULTILINE):
        score += 0.1
    if re.search(r"^#{1,3}\s", assistant_response, re.MULTILINE):
        score += 0.1

    return round(max(0.0, min(1.0, score)), 3)


def _has_boundary_message(assistant_response: str) -> bool:
    """Check if the final turn has a proper session boundary message.

    A boundary message communicates final status: what was done,
    what's the state, any next steps.
    """
    if not assistant_response or len(assistant_response) < 10:
        return False

    boundary_patterns = [
        r"\bcomplete[d]?\b",
        r"\bdone\b",
        r"\bfinished\b",
        r"\ball\s+(set|good|tasks?)\b",
        r"\bready\b",
        r"\bcommitted?\b",
        r"\bpushed?\b",
        r"\bsummary\b",
        r"\bnext\s+steps?\b",
        r"\blet me know\b",
        r"\banything\s+else\b",
    ]

    text_lower = assistant_response.lower()
    return any(re.search(p, text_lower) for p in boundary_patterns)


def _count_background_bash(records: list) -> int:
    """Count background Bash tool completions in records."""
    count = 0
    for record in records:
        if not isinstance(record, Mapping):
            continue
        tool_name = _string(record.get("tool_name"))
        tool_params = record.get("tool_params") or {}
        if not isinstance(tool_params, Mapping):
            tool_params = {}
        if tool_name.lower() == "bash" and bool(tool_params.get("run_in_background")):
            count += 1
    return count
