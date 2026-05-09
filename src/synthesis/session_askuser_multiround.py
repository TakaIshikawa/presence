"""Session AskUserQuestion multi-turn conversation analyzer.

Analyzes Claude Code session transcripts for multi-turn AskUserQuestion patterns,
measuring how many times questions are asked, time between question rounds, and
correlation between question frequency and session outcomes.

Multi-turn metrics:
- AskUserQuestion invocations per session: Total question rounds
- Average questions per invocation: Questions bundled in each call
- Multi-round session percentage: Sessions with 2+ question rounds
- Time between successive rounds: Interval between AskUserQuestion calls
- Question count correlation with success: Relationship to task completion

Quality indicators:
- Low invocation count: Agent resolves needs in 1-2 question rounds
- Short intervals between rounds: Quick clarification cycles
- High questions per invocation: Batches related questions efficiently
- Positive success correlation: Questions lead to task completion
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_askuser_multiround(records: object) -> dict[str, Any]:
    """Analyze multi-turn AskUserQuestion conversation patterns in agent sessions.

    Evaluates question round frequency, batching efficiency, timing between rounds,
    and correlation with session success outcomes.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_name: Name of the tool (AskUserQuestion, etc.)
            - timestamp: Optional ISO timestamp of the turn
            - questions: Optional list of question dicts with:
                - question: Question text
            - session_completed: Optional boolean if session completed successfully
            - session_failed: Optional boolean if session failed

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - askuser_invocations: Total AskUserQuestion tool calls
            - total_questions_asked: Sum of all individual questions
            - avg_questions_per_invocation: Average questions per call
            - sessions_with_multiple_rounds: Boolean if 2+ invocations
            - multi_round_session_percentage: Percentage (0-100)
            - time_between_rounds_seconds: List of intervals between calls
            - avg_time_between_rounds_seconds: Average interval
            - min_time_between_rounds_seconds: Minimum interval
            - max_time_between_rounds_seconds: Maximum interval
            - session_completed: Whether session completed successfully
            - session_failed: Whether session failed
            - correlation_score: -1 to 1 score (more questions = better/worse)

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
    askuser_invocations = 0
    total_questions_asked = 0
    questions_per_invocation: list[int] = []

    # Track timestamps for time between rounds
    askuser_timestamps: list[float] = []

    # Track session outcome
    session_completed = False
    session_failed = False

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1
        tool_name = _string(record.get("tool_name"))

        if tool_name.lower() == "askuserquestion":
            askuser_invocations += 1

            # Extract questions list
            questions = record.get("questions")
            if isinstance(questions, list):
                question_count = len(questions)
                total_questions_asked += question_count
                questions_per_invocation.append(question_count)

            # Track timestamp for interval calculation
            timestamp = _float(record.get("timestamp"))
            if timestamp > 0:
                askuser_timestamps.append(timestamp)

        # Track session outcome (last record wins)
        if "session_completed" in record:
            session_completed = _bool(record.get("session_completed", False))
        if "session_failed" in record:
            session_failed = _bool(record.get("session_failed", False))

    # Calculate aggregate metrics
    avg_questions_per_invocation = _average(questions_per_invocation)

    # Multi-round detection
    sessions_with_multiple_rounds = askuser_invocations >= 2
    multi_round_percentage = 100.0 if sessions_with_multiple_rounds else 0.0

    # Calculate time between rounds
    time_intervals = _calculate_time_intervals(askuser_timestamps)
    avg_time_between_rounds = _average(time_intervals)
    min_time_between_rounds = min(time_intervals) if time_intervals else 0.0
    max_time_between_rounds = max(time_intervals) if time_intervals else 0.0

    # Calculate correlation score
    correlation_score = _calculate_correlation_score(
        askuser_invocations,
        session_completed,
        session_failed,
    )

    return {
        "total_turns": total_turns,
        "askuser_invocations": askuser_invocations,
        "total_questions_asked": total_questions_asked,
        "avg_questions_per_invocation": avg_questions_per_invocation,
        "sessions_with_multiple_rounds": sessions_with_multiple_rounds,
        "multi_round_session_percentage": multi_round_percentage,
        "time_between_rounds_seconds": time_intervals,
        "avg_time_between_rounds_seconds": avg_time_between_rounds,
        "min_time_between_rounds_seconds": min_time_between_rounds,
        "max_time_between_rounds_seconds": max_time_between_rounds,
        "session_completed": session_completed,
        "session_failed": session_failed,
        "correlation_score": correlation_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "askuser_invocations": 0,
        "total_questions_asked": 0,
        "avg_questions_per_invocation": 0.0,
        "sessions_with_multiple_rounds": False,
        "multi_round_session_percentage": 0.0,
        "time_between_rounds_seconds": [],
        "avg_time_between_rounds_seconds": 0.0,
        "min_time_between_rounds_seconds": 0.0,
        "max_time_between_rounds_seconds": 0.0,
        "session_completed": False,
        "session_failed": False,
        "correlation_score": 0.0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _bool(value: object) -> bool:
    """Convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1")
    return bool(value)


def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_time_intervals(timestamps: list[float]) -> list[float]:
    """Calculate time intervals between consecutive timestamps.

    Args:
        timestamps: List of Unix timestamps (seconds since epoch)

    Returns:
        List of intervals in seconds between consecutive timestamps
    """
    if len(timestamps) < 2:
        return []

    # Sort timestamps to ensure chronological order
    sorted_timestamps = sorted(timestamps)

    intervals = []
    for i in range(1, len(sorted_timestamps)):
        interval = sorted_timestamps[i] - sorted_timestamps[i - 1]
        intervals.append(round(interval, 2))

    return intervals


def _calculate_correlation_score(
    invocation_count: int,
    completed: bool,
    failed: bool,
) -> float:
    """Calculate correlation score between question count and session outcome.

    Score interpretation:
    - Positive score: More questions correlate with success
    - Negative score: More questions correlate with failure
    - Near zero: No clear correlation

    Args:
        invocation_count: Number of AskUserQuestion invocations
        completed: Whether session completed successfully
        failed: Whether session failed

    Returns:
        Correlation score from -1.0 to 1.0
    """
    if invocation_count == 0:
        # No questions asked
        if completed:
            return 0.5  # Completed without questions (autonomous)
        elif failed:
            return -0.5  # Failed without asking (should have asked?)
        else:
            return 0.0  # No outcome info

    # Questions were asked
    if completed:
        # Success after questions - correlation depends on question count
        # 1-2 questions = good (0.8-0.9)
        # 3-5 questions = moderate (0.5-0.7)
        # 6+ questions = weak correlation (0.2-0.4)
        if invocation_count <= 2:
            return 0.9
        elif invocation_count <= 5:
            return 0.6
        else:
            return 0.3
    elif failed:
        # Failure after questions - negative correlation
        # More questions = worse correlation
        if invocation_count <= 2:
            return -0.4
        elif invocation_count <= 5:
            return -0.7
        else:
            return -0.9
    else:
        # No outcome info - neutral
        return 0.0
