"""Pack user notification completeness and session boundary discipline analyzer.

Analyzes whether Claude Code sessions communicate important outcomes to users,
detecting silent failures, missing outcome reporting, and unnecessary file creation.

Notification metrics:
- Session completion: % sessions ending with summary/outcome text
- Error escalation: % errors communicated to user vs silently handled
- Clarification usage: % sessions using AskUserQuestion when appropriate
- Outcome reporting: % sessions reporting URLs, results, metrics
- File creation discipline: % sessions avoiding unnecessary .md creation

Anti-patterns detected:
- Silent failures: tool error not followed by user message
- Missing PR URLs: gh pr create without reporting URL
- Proactive documentation: .md file creation without user request
- Missing outcome messages: session ends without final user communication
- Assumption-driven execution: not asking for clarification when needed

Quality indicators:
- High notification completeness: >90% sessions end with outcome message
- High error escalation: >95% errors communicated to user
- Good clarification usage: >20% sessions ask questions when needed
- High outcome reporting: >80% sessions report results (URLs, hashes, metrics)
- High creation discipline: <5% sessions create unnecessary .md files
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_notification_completeness(records: object) -> dict[str, Any]:
    """Analyze user notification completeness and session boundary discipline.

    Evaluates whether sessions properly communicate outcomes to users.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Pack identifier
            - sessions: List of session dictionaries with:
                - session_id: Session identifier
                - messages: List of message dictionaries with:
                    - message_index: Message number
                    - role: Message role (assistant/user)
                    - text_content: Message text content
                    - tool_calls: List of tool calls
                    - tool_results: List of tool results

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - total_sessions: Total number of sessions
            - sessions_with_final_message: Sessions ending with user-facing text
            - notification_completeness_score: % sessions with final outcome
            - total_tool_errors: Total tool errors encountered
            - errors_escalated_to_user: Errors followed by user message
            - error_escalation_rate: % errors communicated to user
            - silent_failures: Errors not communicated to user
            - total_sessions_with_pr_creation: Sessions creating PRs
            - pr_creation_with_url_reported: PR creations with URL in output
            - missing_pr_urls: PR creations without URL reporting
            - total_md_file_creations: Total .md file Write/create operations
            - proactive_md_creations: .md creations without user request
            - proactive_md_creation_rate: % .md files created proactively
            - sessions_using_askuser: Sessions using AskUserQuestion
            - clarification_usage_rate: % sessions asking for clarification
            - example_good_notification: Example of good outcome reporting
            - example_silent_failure: Example of silent error
            - example_missing_pr_url: Example of PR without URL
            - example_proactive_md: Example of proactive .md creation

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_sessions = 0
    sessions_with_final_message = 0

    # Error escalation tracking
    total_tool_errors = 0
    errors_escalated_to_user = 0
    silent_failures = 0

    # PR URL tracking
    total_sessions_with_pr_creation = 0
    pr_creation_with_url_reported = 0
    missing_pr_urls = 0

    # Markdown file creation tracking
    total_md_file_creations = 0
    proactive_md_creations = 0

    # AskUserQuestion usage
    sessions_using_askuser = 0

    # Examples
    example_good_notification: dict[str, Any] = {}
    example_silent_failure: dict[str, Any] = {}
    example_missing_pr_url: dict[str, Any] = {}
    example_proactive_md: dict[str, Any] = {}

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        sessions = _get_sessions(record)
        for session in sessions:
            if not isinstance(session, Mapping):
                continue

            total_sessions += 1

            messages = _get_messages(session)
            if not messages:
                continue

            # Session-level flags
            session_has_pr_creation = False
            session_has_pr_url_report = False
            session_has_md_creation = False
            session_has_explicit_md_request = False
            session_uses_askuser = False

            # Track last assistant message
            last_assistant_message_text = ""
            last_assistant_message_idx = -1

            # Track errors and escalation
            error_indices: list[int] = []
            escalated_error_indices: set[int] = set()

            for msg_idx, message in enumerate(messages):
                if not isinstance(message, Mapping):
                    continue

                role = _string(message.get("role", ""))
                text_content = _string(message.get("text_content", ""))

                # Track user requests for .md creation
                if role == "user" and text_content:
                    if _mentions_md_creation_request(text_content):
                        session_has_explicit_md_request = True

                # Track assistant messages
                if role == "assistant":
                    last_assistant_message_text = text_content
                    last_assistant_message_idx = msg_idx

                    # Check for AskUserQuestion usage
                    tool_calls = _get_tool_calls(message)
                    for tc in tool_calls:
                        if isinstance(tc, Mapping):
                            tool_name = _string(tc.get("tool_name"))
                            if tool_name == "AskUserQuestion":
                                session_uses_askuser = True

                            # Check for PR creation
                            if tool_name == "Bash":
                                command = _string(tc.get("command", ""))
                                if "gh pr create" in command or "git push" in command.lower():
                                    session_has_pr_creation = True

                            # Check for .md file creation
                            if tool_name in ("Write", "Edit"):
                                file_path = _string(tc.get("file_path", ""))
                                if file_path.endswith(".md"):
                                    session_has_md_creation = True

                    # Check tool results for errors
                    tool_results = _get_tool_results(message)
                    for tr in tool_results:
                        if isinstance(tr, Mapping):
                            if _is_tool_error(tr):
                                total_tool_errors += 1
                                error_indices.append(msg_idx)

                    # Check if text mentions PR URL
                    if text_content and ("github.com" in text_content.lower() or "pr #" in text_content.lower()):
                        session_has_pr_url_report = True

            # Determine if session has final notification
            if last_assistant_message_text and last_assistant_message_idx == len(messages) - 1:
                # Last message is from assistant with text content
                sessions_with_final_message += 1

                # Capture good example
                if not example_good_notification and (
                    "complete" in last_assistant_message_text.lower()
                    or "done" in last_assistant_message_text.lower()
                    or "github.com" in last_assistant_message_text.lower()
                ):
                    example_good_notification = {
                        "session_id": session.get("session_id", "unknown"),
                        "final_message": last_assistant_message_text[:200],
                    }

            # Check for error escalation
            for error_idx in error_indices:
                # Look for user-facing message after error
                escalated = False
                for msg_idx in range(error_idx + 1, len(messages)):
                    msg = messages[msg_idx]
                    if isinstance(msg, Mapping):
                        role = _string(msg.get("role", ""))
                        text = _string(msg.get("text_content", ""))
                        if role == "assistant" and text:
                            # Found assistant text after error - likely escalated
                            escalated = True
                            errors_escalated_to_user += 1
                            escalated_error_indices.add(error_idx)
                            break

                if not escalated:
                    silent_failures += 1
                    if not example_silent_failure:
                        example_silent_failure = {
                            "session_id": session.get("session_id", "unknown"),
                            "error_index": error_idx,
                        }

            # PR URL tracking
            if session_has_pr_creation:
                total_sessions_with_pr_creation += 1
                if session_has_pr_url_report:
                    pr_creation_with_url_reported += 1
                else:
                    missing_pr_urls += 1
                    if not example_missing_pr_url:
                        example_missing_pr_url = {
                            "session_id": session.get("session_id", "unknown"),
                        }

            # Markdown file creation tracking
            if session_has_md_creation:
                total_md_file_creations += 1
                if not session_has_explicit_md_request:
                    proactive_md_creations += 1
                    if not example_proactive_md:
                        example_proactive_md = {
                            "session_id": session.get("session_id", "unknown"),
                        }

            # AskUser tracking
            if session_uses_askuser:
                sessions_using_askuser += 1

    # Calculate metrics
    notification_completeness_score = _percentage(sessions_with_final_message, total_sessions)
    error_escalation_rate = _percentage(errors_escalated_to_user, total_tool_errors)
    clarification_usage_rate = _percentage(sessions_using_askuser, total_sessions)
    proactive_md_creation_rate = _percentage(proactive_md_creations, total_md_file_creations)

    return {
        "total_packs": total_packs,
        "total_sessions": total_sessions,
        "sessions_with_final_message": sessions_with_final_message,
        "notification_completeness_score": notification_completeness_score,
        "total_tool_errors": total_tool_errors,
        "errors_escalated_to_user": errors_escalated_to_user,
        "error_escalation_rate": error_escalation_rate,
        "silent_failures": silent_failures,
        "total_sessions_with_pr_creation": total_sessions_with_pr_creation,
        "pr_creation_with_url_reported": pr_creation_with_url_reported,
        "missing_pr_urls": missing_pr_urls,
        "total_md_file_creations": total_md_file_creations,
        "proactive_md_creations": proactive_md_creations,
        "proactive_md_creation_rate": proactive_md_creation_rate,
        "sessions_using_askuser": sessions_using_askuser,
        "clarification_usage_rate": clarification_usage_rate,
        "example_good_notification": example_good_notification,
        "example_silent_failure": example_silent_failure,
        "example_missing_pr_url": example_missing_pr_url,
        "example_proactive_md": example_proactive_md,
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


def _get_tool_results(message: Mapping[str, Any]) -> list[Any]:
    """Extract tool results list from message."""
    tool_results = message.get("tool_results")
    if isinstance(tool_results, list):
        return tool_results
    return []


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _is_tool_error(tool_result: Mapping[str, Any]) -> bool:
    """Check if tool result indicates an error.

    Heuristic: looks for error indicators in result.
    """
    error_field = tool_result.get("error")
    if error_field:
        return True

    # Check for exit code != 0 (for Bash)
    exit_code = tool_result.get("exit_code")
    if exit_code is not None and exit_code != 0:
        return True

    # Check for error text in output
    output = _string(tool_result.get("output", ""))
    if output and any(indicator in output.lower() for indicator in ["error:", "exception:", "failed:", "traceback"]):
        return True

    return False


def _mentions_md_creation_request(text: str) -> bool:
    """Check if user text explicitly requests .md file creation.

    Examples: "create a README", "write documentation", "add a CHANGELOG"
    """
    text_lower = text.lower()

    explicit_requests = [
        "create a readme",
        "create readme",
        "write a readme",
        "add a readme",
        "make a readme",
        "create documentation",
        "write documentation",
        "add documentation",
        "create a changelog",
        "write a changelog",
        "create .md",
        "create markdown",
    ]

    return any(req in text_lower for req in explicit_requests)


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
