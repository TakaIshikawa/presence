"""Pack tool call error density analyzer for quality assessment.

Analyzes execution pack logs to measure tool call error rates and patterns.
Identifies problematic error densities, clustering, retry patterns, and error cascades
that indicate poor task specification or environmental issues.

Error metrics:
- Error density: Failed tool calls / total tool calls (%)
- Error rate by tool type: Per-tool failure rates
- Error clustering: Whether errors concentrate in specific sessions
- Retry patterns: Repeated tool calls after failures
- Fatal vs recoverable errors: Error severity distribution

Quality indicators:
- High error density (>15%): Poor task specification or environment
- Error cascades: One failure triggering multiple downstream failures
- Repeated errors without resolution: Same file/operation failing repeatedly
- Error clustering: Errors concentrated vs distributed across sessions
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_pack_tool_call_error_density(records: object) -> dict[str, Any]:
    """Analyze tool call error rates and patterns in execution packs.

    Evaluates error density, clustering, retry patterns, and error cascades
    to identify quality issues in task specification or execution environment.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Pack identifier
            - sessions: List of session dictionaries with:
                - session_id: Session identifier
                - tool_calls: List of tool call dictionaries with:
                    - tool_name: Name of tool (Read, Write, Edit, Bash, etc.)
                    - success: Boolean indicating success/failure
                    - error_type: Optional error category (fatal, recoverable)
                    - file_path: Optional file being operated on
                    - turn_index: Turn number for sequencing
                - total_tool_calls: Optional total count
                - failed_tool_calls: Optional failed count

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - total_tool_calls: Total tool calls across all packs
            - failed_tool_calls: Total failed tool calls
            - error_density: Percentage of failed tool calls (0-100)
            - high_error_density_packs: Count of packs with >15% error rate
            - error_rate_by_tool: Dict of tool name -> error rate
            - tool_with_highest_error_rate: Tool name with highest failure rate
            - error_clustering_score: 0-1 score (1 = highly clustered)
            - clustered_error_packs: Count of packs with clustered errors
            - retry_attempts: Total number of retry attempts
            - successful_retries: Retries that eventually succeeded
            - failed_retries: Retries that never resolved
            - retry_resolution_rate: Percentage of retries that succeeded
            - error_cascade_count: Number of error cascades detected
            - fatal_error_count: Count of fatal errors
            - recoverable_error_count: Count of recoverable errors

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_tool_calls = 0
    failed_tool_calls = 0
    high_error_density_packs = 0

    # Track errors by tool type
    tool_call_counts: Counter[str] = Counter()
    tool_error_counts: Counter[str] = Counter()

    # Track clustering metrics
    clustering_scores: list[float] = []
    clustered_error_packs = 0

    # Track retry patterns
    retry_attempts = 0
    successful_retries = 0
    failed_retries = 0

    # Track error cascades and severity
    error_cascade_count = 0
    fatal_error_count = 0
    recoverable_error_count = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        sessions = _get_sessions(record)
        if not sessions:
            continue

        # Aggregate tool calls across all sessions in pack
        pack_total_calls = 0
        pack_failed_calls = 0
        pack_tool_counts: Counter[str] = Counter()
        pack_tool_errors: Counter[str] = Counter()
        session_error_counts: list[int] = []

        for session in sessions:
            if not isinstance(session, Mapping):
                continue

            tool_calls = _get_tool_calls(session)
            session_errors = 0

            for tool_call in tool_calls:
                if not isinstance(tool_call, Mapping):
                    continue

                tool_name = _string(tool_call.get("tool_name", "unknown"))
                success = _bool(tool_call.get("success", True))
                error_type = _string(tool_call.get("error_type", ""))

                pack_total_calls += 1
                pack_tool_counts[tool_name] += 1

                if not success:
                    pack_failed_calls += 1
                    pack_tool_errors[tool_name] += 1
                    session_errors += 1

                    # Track error severity
                    if error_type.lower() == "fatal":
                        fatal_error_count += 1
                    elif error_type.lower() == "recoverable":
                        recoverable_error_count += 1

            session_error_counts.append(session_errors)

            # Detect retry patterns in this session
            retries = _detect_retries(session)
            retry_attempts += retries["attempts"]
            successful_retries += retries["successful"]
            failed_retries += retries["failed"]

            # Detect error cascades in this session
            cascades = _detect_error_cascades(session)
            error_cascade_count += cascades

        # Aggregate pack metrics
        total_tool_calls += pack_total_calls
        failed_tool_calls += pack_failed_calls
        tool_call_counts.update(pack_tool_counts)
        tool_error_counts.update(pack_tool_errors)

        # Calculate pack error density
        pack_error_density = _percentage(pack_failed_calls, pack_total_calls)
        if pack_error_density > 15.0:
            high_error_density_packs += 1

        # Calculate error clustering for this pack
        if len(session_error_counts) > 1 and pack_failed_calls > 0:
            clustering = _calculate_clustering_score(session_error_counts)
            clustering_scores.append(clustering)
            if clustering > 0.7:
                clustered_error_packs += 1

    # Calculate overall error density
    error_density = _percentage(failed_tool_calls, total_tool_calls)

    # Calculate error rates by tool type
    error_rate_by_tool = {}
    for tool_name in tool_call_counts:
        calls = tool_call_counts[tool_name]
        errors = tool_error_counts[tool_name]
        error_rate_by_tool[tool_name] = _percentage(errors, calls)

    # Find tool with highest error rate
    tool_with_highest_error_rate = ""
    if error_rate_by_tool:
        tool_with_highest_error_rate = max(
            error_rate_by_tool.items(),
            key=lambda x: x[1]
        )[0]

    # Calculate average clustering score
    avg_clustering_score = _average(clustering_scores)

    # Calculate retry resolution rate
    retry_resolution_rate = _percentage(successful_retries, retry_attempts)

    return {
        "total_packs": total_packs,
        "total_tool_calls": total_tool_calls,
        "failed_tool_calls": failed_tool_calls,
        "error_density": error_density,
        "high_error_density_packs": high_error_density_packs,
        "error_rate_by_tool": error_rate_by_tool,
        "tool_with_highest_error_rate": tool_with_highest_error_rate,
        "error_clustering_score": avg_clustering_score,
        "clustered_error_packs": clustered_error_packs,
        "retry_attempts": retry_attempts,
        "successful_retries": successful_retries,
        "failed_retries": failed_retries,
        "retry_resolution_rate": retry_resolution_rate,
        "error_cascade_count": error_cascade_count,
        "fatal_error_count": fatal_error_count,
        "recoverable_error_count": recoverable_error_count,
    }


def _bool(value: object) -> bool:
    """Convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1")
    return bool(value)


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _get_sessions(record: Mapping[str, Any]) -> list[Any]:
    """Extract sessions list from pack record."""
    sessions = record.get("sessions")
    if isinstance(sessions, list):
        return sessions
    return []


def _get_tool_calls(session: Mapping[str, Any]) -> list[Any]:
    """Extract tool calls list from session."""
    tool_calls = session.get("tool_calls")
    if isinstance(tool_calls, list):
        return tool_calls
    return []


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 3)


def _calculate_clustering_score(session_error_counts: list[int]) -> float:
    """Calculate error clustering score (0-1).

    Measures whether errors are concentrated in specific sessions (high score)
    or distributed evenly across sessions (low score).

    Uses coefficient of variation: std_dev / mean
    Higher CV indicates more clustering.
    """
    if not session_error_counts or sum(session_error_counts) == 0:
        return 0.0

    # Filter to sessions with errors
    error_sessions = [count for count in session_error_counts if count > 0]
    if len(error_sessions) <= 1:
        return 1.0  # All errors in one session = maximum clustering

    # Calculate mean and standard deviation
    mean = sum(error_sessions) / len(error_sessions)
    if mean == 0:
        return 0.0

    variance = sum((x - mean) ** 2 for x in error_sessions) / len(error_sessions)
    std_dev = variance ** 0.5

    # Coefficient of variation normalized to 0-1
    cv = std_dev / mean
    # Cap at 1.0 for normalization
    return min(1.0, cv)


def _detect_retries(session: Mapping[str, Any]) -> dict[str, int]:
    """Detect retry patterns in a session.

    Returns:
        Dict with:
            - attempts: Number of retry attempts
            - successful: Number of successful retries
            - failed: Number of failed retries
    """
    tool_calls = _get_tool_calls(session)
    if len(tool_calls) < 2:
        return {"attempts": 0, "successful": 0, "failed": 0}

    # Track failures by (tool_name, file_path) to detect retries
    failures: dict[tuple[str, str], list[int]] = {}
    successes: dict[tuple[str, str], list[int]] = {}

    for i, tool_call in enumerate(tool_calls):
        if not isinstance(tool_call, Mapping):
            continue

        tool_name = _string(tool_call.get("tool_name", ""))
        file_path = _string(tool_call.get("file_path", ""))
        success = _bool(tool_call.get("success", True))
        turn_index = tool_call.get("turn_index", i)

        key = (tool_name, file_path)

        if success:
            if key not in successes:
                successes[key] = []
            successes[key].append(turn_index)
        else:
            if key not in failures:
                failures[key] = []
            failures[key].append(turn_index)

    # Count retries: operations that failed then succeeded or failed again
    retry_attempts = 0
    successful_retries = 0
    failed_retries = 0

    for key in failures:
        failure_turns = failures[key]
        success_turns = successes.get(key, [])

        # Check if eventually succeeded after failure
        if success_turns and failure_turns:
            # If any success came after a failure, count this as a retry attempt
            for success_turn in success_turns:
                if any(fail_turn < success_turn for fail_turn in failure_turns):
                    retry_attempts += 1
                    successful_retries += 1
                    break

        # Check if there were multiple failures (retry attempts that failed)
        if len(failure_turns) > 1:
            # Each additional failure after the first is a retry attempt
            retry_attempts += len(failure_turns) - 1

        # Count failed retries (multiple failures, no success)
        if len(failure_turns) > 1 and not success_turns:
            failed_retries += 1

    return {
        "attempts": retry_attempts,
        "successful": successful_retries,
        "failed": failed_retries,
    }


def _detect_error_cascades(session: Mapping[str, Any]) -> int:
    """Detect error cascades where one failure triggers multiple downstream failures.

    An error cascade is identified by:
    - Multiple consecutive tool call failures (3+ in a row)
    - Failures occurring within a short time window

    Returns:
        Count of error cascades detected
    """
    tool_calls = _get_tool_calls(session)
    if len(tool_calls) < 3:
        return 0

    cascade_count = 0
    consecutive_failures = 0

    for tool_call in tool_calls:
        if not isinstance(tool_call, Mapping):
            continue

        success = _bool(tool_call.get("success", True))

        if not success:
            consecutive_failures += 1
            # Cascade detected: 3+ consecutive failures
            if consecutive_failures >= 3:
                cascade_count = 1  # Mark that a cascade occurred
        else:
            consecutive_failures = 0

    return cascade_count
