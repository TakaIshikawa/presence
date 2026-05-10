"""Session notification quality and user communication analyzer.

Evaluates the quality of agent-to-user communication within individual
sessions, focusing on clear endings, error explanation, progress
signaling, and actionable outcomes.

Metrics:
- total_sessions, sessions_analyzed
- sessions_with_clear_ending, clear_ending_rate
- sessions_with_error_explanation, error_explanation_rate
- long_sessions_with_progress, progress_signaling_rate
- sessions_with_actionable_outcomes, actionable_outcome_rate
- avg_communication_tool_ratio
- notification_quality_score: weighted composite 0-1
"""

from __future__ import annotations

import re
from typing import Any, Mapping


_ACTIONABLE_PATTERNS = re.compile(
    r"(?:"
    r"https?://\S+"
    r"|`[^`]+\.[a-z]+`"
    r"|\.(?:py|ts|js|rs|go|rb|java|sh|yml|yaml|json|toml)\b"
    r"|(?:run|execute|try|use)\s+`"
    r"|(?:created|updated|modified|added|deleted)\s+\S+\.\S+"
    r")",
    re.IGNORECASE,
)

_ERROR_KEYWORDS = re.compile(
    r"\b(?:error|exception|failed|failure|traceback|stack\s*trace|bug|issue|problem)\b",
    re.IGNORECASE,
)

_PROGRESS_PATTERNS = re.compile(
    r"\b(?:"
    r"(?:now|next|moving|proceeding|continuing|starting|working)\s+(?:on|to|with)"
    r"|step\s+\d+"
    r"|(?:first|second|third|then|finally|next)"
    r"|(?:done|completed|finished)\s+(?:with|the)"
    r"|let me (?:now|move|continue|start|proceed)"
    r")\b",
    re.IGNORECASE,
)

_CLEAR_ENDING_PATTERNS = re.compile(
    r"(?:"
    r"\b(?:all\s+(?:tests?\s+)?pass|successfully|completed|done|finished|ready)\b"
    r"|\b(?:changes?\s+(?:have been\s+)?committed|pushed|merged)\b"
    r"|\b(?:here(?:'s| is) (?:the|a) summary)\b"
    r"|\b(?:let me know|feel free to)\b"
    r")",
    re.IGNORECASE,
)


def analyze_session_notification_quality(records: object) -> dict[str, Any]:
    """Analyze communication quality across sessions.

    Args:
        records: List of session dicts with messages containing
            role, text, and tool_calls fields.

    Returns:
        Dict with notification quality metrics.

    Raises:
        ValueError: If records is not a list.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_analyzed = 0
    clear_endings = 0
    error_explanations = 0
    sessions_with_errors = 0
    long_sessions_with_progress = 0
    long_sessions = 0
    actionable_outcomes = 0
    comm_ratios: list[float] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue
        total_sessions += 1

        messages = record.get("messages")
        if not isinstance(messages, list) or not messages:
            continue
        sessions_analyzed += 1

        assistant_messages = [
            m for m in messages
            if isinstance(m, Mapping) and m.get("role") == "assistant"
        ]
        if not assistant_messages:
            continue

        # Count tool calls across all messages
        total_tool_calls = 0
        total_text_messages = 0
        has_errors = False

        for msg in messages:
            if not isinstance(msg, Mapping):
                continue
            tool_calls = msg.get("tool_calls")
            if isinstance(tool_calls, list):
                total_tool_calls += len(tool_calls)
            if msg.get("role") == "assistant":
                text = str(msg.get("text") or msg.get("content") or "")
                if text.strip():
                    total_text_messages += 1
            # Check for errors in any message
            text = str(msg.get("text") or msg.get("content") or "")
            if _ERROR_KEYWORDS.search(text):
                has_errors = True

        # 1. Clear ending
        last_assistant = assistant_messages[-1]
        last_text = str(last_assistant.get("text") or last_assistant.get("content") or "")
        if _has_clear_ending(last_text):
            clear_endings += 1

        # 2. Error communication
        if has_errors:
            sessions_with_errors += 1
            if _has_error_explanation(assistant_messages):
                error_explanations += 1

        # 3. Progress signaling for long sessions
        if total_tool_calls >= 10:
            long_sessions += 1
            if _has_progress_signaling(assistant_messages):
                long_sessions_with_progress += 1

        # 4. Actionable outcomes
        if _has_actionable_outcome(last_text):
            actionable_outcomes += 1

        # 5. Communication-to-tool ratio
        if total_tool_calls > 0:
            comm_ratios.append(total_text_messages / total_tool_calls)

    clear_ending_rate = _safe_rate(clear_endings, sessions_analyzed)
    error_explanation_rate = _safe_rate(error_explanations, sessions_with_errors)
    progress_signaling_rate = _safe_rate(long_sessions_with_progress, long_sessions)
    actionable_outcome_rate = _safe_rate(actionable_outcomes, sessions_analyzed)
    avg_comm_ratio = sum(comm_ratios) / len(comm_ratios) if comm_ratios else 0.0

    notification_quality_score = _compute_quality_score(
        clear_ending_rate=clear_ending_rate,
        error_explanation_rate=error_explanation_rate,
        progress_signaling_rate=progress_signaling_rate,
        actionable_outcome_rate=actionable_outcome_rate,
    )

    return {
        "total_sessions": total_sessions,
        "sessions_analyzed": sessions_analyzed,
        "sessions_with_clear_ending": clear_endings,
        "clear_ending_rate": round(clear_ending_rate, 4),
        "sessions_with_error_explanation": error_explanations,
        "error_explanation_rate": round(error_explanation_rate, 4),
        "long_sessions_with_progress": long_sessions_with_progress,
        "progress_signaling_rate": round(progress_signaling_rate, 4),
        "sessions_with_actionable_outcomes": actionable_outcomes,
        "actionable_outcome_rate": round(actionable_outcome_rate, 4),
        "avg_communication_tool_ratio": round(avg_comm_ratio, 4),
        "notification_quality_score": round(notification_quality_score, 4),
    }


def _has_clear_ending(text: str) -> bool:
    return bool(_CLEAR_ENDING_PATTERNS.search(text))


def _has_error_explanation(assistant_messages: list[Mapping[str, Any]]) -> bool:
    """Check if errors are explained in plain text by the assistant."""
    for msg in assistant_messages:
        text = str(msg.get("text") or msg.get("content") or "")
        if _ERROR_KEYWORDS.search(text) and len(text) > 50:
            return True
    return False


def _has_progress_signaling(assistant_messages: list[Mapping[str, Any]]) -> bool:
    """Check if assistant provides progress updates during the session."""
    progress_count = 0
    for msg in assistant_messages:
        text = str(msg.get("text") or msg.get("content") or "")
        if _PROGRESS_PATTERNS.search(text):
            progress_count += 1
    return progress_count >= 2


def _has_actionable_outcome(text: str) -> bool:
    """Check if final message includes actionable results."""
    return bool(_ACTIONABLE_PATTERNS.search(text))


def _safe_rate(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 1.0
    return numerator / denominator


def _compute_quality_score(
    *,
    clear_ending_rate: float,
    error_explanation_rate: float,
    progress_signaling_rate: float,
    actionable_outcome_rate: float,
) -> float:
    """Weighted composite quality score 0-1."""
    score = (
        0.30 * clear_ending_rate
        + 0.25 * error_explanation_rate
        + 0.20 * progress_signaling_rate
        + 0.25 * actionable_outcome_rate
    )
    return min(1.0, max(0.0, score))
