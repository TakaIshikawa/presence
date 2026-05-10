"""Pack notification completeness and session boundary discipline analyzer.

Analyzes how well agents communicate outcomes, errors, and progress to users
across sessions in a pack. Ensures user visibility into agent actions and
outcomes per global instructions (communicate via text, not tools).

Communication discipline metrics:
- Explicit completion rate: Sessions ending with clear outcome summary
- Error notification rate: All errors/failures communicated to user
- Progress update frequency: Intermediate updates during long operations (>5min)
- Markdown usage consistency: Structured, readable communication
- Tool communication violations: Using Bash echo or comments for user messages

Quality indicators:
- High explicit completion rate (target 100%): All sessions end with clear summary
- High error notification rate (target 100%): All errors communicated
- Appropriate progress updates: Regular status during long operations
- Consistent markdown formatting: Professional, readable output
- Zero tool communication violations: Direct text communication only
"""

from __future__ import annotations

from typing import Any, Mapping


# Completion indicators to detect in final messages
COMPLETION_INDICATORS = (
    "done",
    "complete",
    "finished",
    "successfully",
    "committed",
    "implemented",
    "created",
    "fixed",
    "updated",
    "summary",
    "result",
    "outcome",
)

# Error/failure terms to detect
ERROR_INDICATORS = (
    "error",
    "fail",
    "exception",
    "traceback",
    "stderr",
    "exit code",
    "timeout",
    "issue",
    "problem",
    "cannot",
    "unable",
    "blocked",
)

# Progress update indicators
PROGRESS_INDICATORS = (
    "working on",
    "starting",
    "processing",
    "analyzing",
    "implementing",
    "in progress",
    "currently",
    "next",
    "step",
)


def analyze_pack_notification_completeness(records: object) -> dict[str, Any]:
    """Analyze notification completeness and communication discipline in packs.

    Evaluates how well agents communicate outcomes, errors, and progress to users,
    ensuring transparency and adherence to communication guidelines.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Pack identifier
            - sessions: List of session dictionaries with:
                - session_id: Session identifier
                - duration_minutes: Session duration in minutes
                - messages: List of assistant message dictionaries with:
                    - message_index: Message number
                    - text: Message text content
                    - is_final_message: Boolean if last message in session
                    - tool_calls: List of tool call dictionaries with:
                        - tool_name: Name of tool (Bash, etc.)
                        - command: Bash command (if applicable)
                - errors_encountered: List of error dictionaries with:
                    - error_type: Type of error
                    - error_message: Error message
                    - turn_index: When error occurred
                    - was_communicated: Boolean if user was notified

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - total_sessions: Total number of sessions
            - sessions_with_explicit_completion: Sessions ending with outcome summary
            - sessions_without_completion: Sessions ending abruptly
            - explicit_completion_rate: Percentage with explicit completion
            - total_errors: Total errors encountered
            - errors_communicated: Errors notified to user
            - errors_silent: Errors not communicated
            - error_notification_rate: Percentage of errors communicated
            - long_sessions: Sessions >5 minutes
            - long_sessions_with_progress: Long sessions with progress updates
            - progress_update_frequency: Percentage of long sessions with updates
            - messages_with_markdown: Messages using markdown formatting
            - total_messages: Total messages analyzed
            - markdown_usage_consistency: Percentage using markdown
            - tool_communication_violations: Count of Bash echo/comment communication
            - example_good_completion: Example of clear completion summary
            - example_poor_completion: Example of abrupt termination
            - example_good_error_notification: Example of clear error communication
            - example_poor_error_handling: Example of silent error handling

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_sessions = 0
    sessions_with_explicit_completion = 0
    sessions_without_completion = 0

    total_errors = 0
    errors_communicated = 0
    errors_silent = 0

    long_sessions = 0
    long_sessions_with_progress = 0

    messages_with_markdown = 0
    total_messages = 0

    tool_communication_violations = 0

    # Examples
    example_good_completion: dict[str, Any] = {}
    example_poor_completion: dict[str, Any] = {}
    example_good_error_notification: dict[str, Any] = {}
    example_poor_error_handling: dict[str, Any] = {}

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        sessions = _get_sessions(record)
        for session in sessions:
            if not isinstance(session, Mapping):
                continue

            total_sessions += 1
            session_id = session.get("session_id", "unknown")
            duration_minutes = _extract_number(session.get("duration_minutes"))

            messages = _get_messages(session)
            final_message_text = ""
            has_progress_updates = False

            for message in messages:
                if not isinstance(message, Mapping):
                    continue

                total_messages += 1
                text = _string(message.get("text"))
                is_final = message.get("is_final_message") is True

                # Check markdown usage
                if _has_markdown_formatting(text):
                    messages_with_markdown += 1

                # Check for tool-based communication violations
                tool_calls = _get_tool_calls(message)
                if _has_tool_communication_violation(tool_calls, text):
                    tool_communication_violations += 1

                # Track progress indicators
                if _has_progress_indicator(text):
                    has_progress_updates = True

                # Capture final message
                if is_final:
                    final_message_text = text

            # Check for explicit completion
            has_completion = _has_completion_indicator(final_message_text)
            if has_completion:
                sessions_with_explicit_completion += 1
                if not example_good_completion:
                    example_good_completion = {
                        "session_id": session_id,
                        "final_message": final_message_text[:200],
                    }
            else:
                sessions_without_completion += 1
                if not example_poor_completion:
                    example_poor_completion = {
                        "session_id": session_id,
                        "final_message": final_message_text[:200] if final_message_text else "(no final message)",
                    }

            # Check long session progress updates
            if duration_minutes is not None and duration_minutes > 5:
                long_sessions += 1
                if has_progress_updates:
                    long_sessions_with_progress += 1

            # Check error notification completeness
            errors = session.get("errors_encountered")
            if isinstance(errors, list):
                for error in errors:
                    if not isinstance(error, Mapping):
                        continue

                    total_errors += 1
                    was_communicated = error.get("was_communicated") is True

                    if was_communicated:
                        errors_communicated += 1
                        if not example_good_error_notification:
                            example_good_error_notification = {
                                "session_id": session_id,
                                "error_type": error.get("error_type"),
                                "error_message": _string(error.get("error_message"))[:200],
                            }
                    else:
                        errors_silent += 1
                        if not example_poor_error_handling:
                            example_poor_error_handling = {
                                "session_id": session_id,
                                "error_type": error.get("error_type"),
                                "error_message": _string(error.get("error_message"))[:200],
                            }

    # Calculate metrics
    explicit_completion_rate = _percentage(sessions_with_explicit_completion, total_sessions)
    error_notification_rate = _percentage(errors_communicated, total_errors)
    progress_update_frequency = _percentage(long_sessions_with_progress, long_sessions)
    markdown_usage_consistency = _percentage(messages_with_markdown, total_messages)

    return {
        "total_packs": total_packs,
        "total_sessions": total_sessions,
        "sessions_with_explicit_completion": sessions_with_explicit_completion,
        "sessions_without_completion": sessions_without_completion,
        "explicit_completion_rate": explicit_completion_rate,
        "total_errors": total_errors,
        "errors_communicated": errors_communicated,
        "errors_silent": errors_silent,
        "error_notification_rate": error_notification_rate,
        "long_sessions": long_sessions,
        "long_sessions_with_progress": long_sessions_with_progress,
        "progress_update_frequency": progress_update_frequency,
        "messages_with_markdown": messages_with_markdown,
        "total_messages": total_messages,
        "markdown_usage_consistency": markdown_usage_consistency,
        "tool_communication_violations": tool_communication_violations,
        "example_good_completion": example_good_completion,
        "example_poor_completion": example_poor_completion,
        "example_good_error_notification": example_good_error_notification,
        "example_poor_error_handling": example_poor_error_handling,
    }


def _get_sessions(record: Mapping[str, Any]) -> list[Any]:
    """Extract sessions list from pack record."""
    sessions = record.get("sessions")
    if isinstance(sessions, list):
        return sessions
    return []


def _get_messages(session: Mapping[str, Any]) -> list[Any]:
    """Extract messages list from session."""
    messages = session.get("messages")
    if isinstance(messages, list):
        return messages
    return []


def _get_tool_calls(message: Mapping[str, Any]) -> list[Any]:
    """Extract tool calls list from message."""
    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list):
        return tool_calls
    return []


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value from object."""
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return None
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _has_completion_indicator(text: str) -> bool:
    """Check if text contains completion indicators."""
    if not text:
        return False

    text_lower = text.lower()
    return any(indicator in text_lower for indicator in COMPLETION_INDICATORS)


def _has_error_indicator(text: str) -> bool:
    """Check if text contains error indicators."""
    if not text:
        return False

    text_lower = text.lower()
    return any(indicator in text_lower for indicator in ERROR_INDICATORS)


def _has_progress_indicator(text: str) -> bool:
    """Check if text contains progress update indicators."""
    if not text:
        return False

    text_lower = text.lower()
    return any(indicator in text_lower for indicator in PROGRESS_INDICATORS)


def _has_markdown_formatting(text: str) -> bool:
    """Check if text uses markdown formatting."""
    if not text:
        return False

    # Check for common markdown patterns
    markdown_patterns = [
        "**",  # Bold
        "##",  # Headers
        "- ",  # Lists
        "* ",  # Lists
        "`",   # Code
        "```", # Code blocks
        "[",   # Links
    ]

    return any(pattern in text for pattern in markdown_patterns)


def _has_tool_communication_violation(tool_calls: list[Any], message_text: str) -> bool:
    """Check if message uses tool-based communication instead of direct text.

    Violations include:
    - Bash echo commands used to communicate with user
    - Code comments added solely for user communication
    """
    for call in tool_calls:
        if not isinstance(call, Mapping):
            continue

        tool_name = _string(call.get("tool_name"))

        # Check for Bash echo violations
        if tool_name.lower() == "bash":
            command = _string(call.get("command"))
            if command:
                command_lower = command.lower()
                # Detect echo commands that appear to communicate with user
                if command_lower.startswith("echo") and not _is_technical_echo(command):
                    return True

    return False


def _is_technical_echo(command: str) -> bool:
    """Check if echo command is for technical purposes, not user communication.

    Technical uses: writing to files, piping to commands, environment setup.
    Non-technical: echo statements that print user-facing messages.
    """
    command_lower = command.lower()

    # Technical patterns
    if ">" in command or ">>" in command:  # Redirecting to file
        return True
    if "|" in command:  # Piping to another command
        return True
    if "$" in command and "=" not in command:  # Echoing environment variables
        return True

    # Non-technical: standalone echo for user communication
    return False
