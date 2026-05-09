"""Session verification trigger patterns analyzer for verification timing.

Analyzes when and why verification is triggered during sessions to identify
optimal verification strategies and measure verification effectiveness.

Verification trigger types:
- Post-edit: Verification after code changes
- Post-error: Verification after build/test failures
- Explicit request: User explicitly requests verification
- Periodic: Regular verification checks
- Context switch: Verification when changing tasks

Verification metrics:
- Frequency: How often verification occurs
- Trigger contexts: Distribution of trigger types
- Time between verifications: Average interval
- Success by trigger type: Effectiveness of each trigger
- Verification tool distribution: /verify vs manual commands
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_verification_trigger_patterns(records: object) -> dict[str, Any]:
    """Analyze verification trigger patterns in a session.

    Evaluates when and why verification occurs, measuring frequency,
    trigger contexts, timing patterns, and success rates by trigger type.

    Args:
        records: List of verification event dictionaries with keys:
            - turn_index: Turn when verification occurred
            - trigger_type: Type of trigger (post_edit/post_error/explicit/etc)
            - tool_used: Verification tool (verify command, pytest, npm, etc)
            - success: Boolean indicating verification success
            - time_since_last_verification: Optional time interval

    Returns:
        Dict with:
            - total_verifications: Total verification events
            - verification_frequency: Verifications per turn (if turns provided)
            - trigger_contexts: Distribution by trigger type
            - verification_tool_distribution: Tools used for verification
            - avg_time_between_verifications: Average interval in turns
            - verification_success_by_trigger_type: Success rate per trigger
            - post_edit_verifications: Count of post-edit verifications
            - post_error_verifications: Count of post-error verifications
            - explicit_request_verifications: Count of explicit verifications

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of verification event dictionaries")

    total_verifications = 0

    # Track trigger types
    trigger_counts: dict[str, int] = {}
    trigger_success: dict[str, list[bool]] = {}

    # Track tools used
    tool_counts: dict[str, int] = {}

    # Track timing
    time_intervals: list[int] = []
    last_turn: int | None = None

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_verifications += 1

        # Track trigger type
        trigger_type = _string(record.get("trigger_type", "unknown"))
        trigger_counts[trigger_type] = trigger_counts.get(trigger_type, 0) + 1

        # Track success by trigger type
        success = record.get("success")
        if isinstance(success, bool):
            if trigger_type not in trigger_success:
                trigger_success[trigger_type] = []
            trigger_success[trigger_type].append(success)

        # Track tool used
        tool_used = _string(record.get("tool_used", "unknown"))
        if tool_used:
            tool_counts[tool_used] = tool_counts.get(tool_used, 0) + 1

        # Track timing
        turn_index = record.get("turn_index")
        if isinstance(turn_index, int) and last_turn is not None:
            interval = turn_index - last_turn
            if interval > 0:
                time_intervals.append(interval)
        if isinstance(turn_index, int):
            last_turn = turn_index

        # Also check for explicit time_since_last_verification
        time_since = record.get("time_since_last_verification")
        if isinstance(time_since, int) and time_since > 0:
            time_intervals.append(time_since)

    # Calculate metrics
    avg_time_between = _average(time_intervals)

    # Format trigger contexts
    total_triggers = sum(trigger_counts.values())
    trigger_distribution = [
        {
            "trigger_type": trigger,
            "count": count,
            "percentage": _percentage(count, total_triggers),
        }
        for trigger, count in sorted(trigger_counts.items(), key=lambda x: x[1], reverse=True)
    ]

    # Format tool distribution
    total_tools = sum(tool_counts.values())
    tool_distribution = [
        {
            "tool": tool,
            "count": count,
            "percentage": _percentage(count, total_tools),
        }
        for tool, count in sorted(tool_counts.items(), key=lambda x: x[1], reverse=True)
    ]

    # Calculate success rates by trigger type
    success_by_trigger = [
        {
            "trigger_type": trigger,
            "total": len(successes),
            "success_rate": _percentage(sum(successes), len(successes)),
        }
        for trigger, successes in sorted(trigger_success.items())
        if successes
    ]

    # Extract specific trigger counts
    post_edit = trigger_counts.get("post_edit", 0)
    post_error = trigger_counts.get("post_error", 0)
    explicit = trigger_counts.get("explicit_request", 0) + trigger_counts.get("explicit", 0)

    return {
        "total_verifications": total_verifications,
        "verification_frequency": _average([total_verifications]) if total_verifications > 0 else 0.0,
        "trigger_contexts": trigger_distribution,
        "verification_tool_distribution": tool_distribution,
        "avg_time_between_verifications": avg_time_between,
        "verification_success_by_trigger_type": success_by_trigger,
        "post_edit_verifications": post_edit,
        "post_error_verifications": post_error,
        "explicit_request_verifications": explicit,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
