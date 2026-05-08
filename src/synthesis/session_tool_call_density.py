"""Session tool call density analyzer for workflow hygiene reports."""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_tool_call_density(records: object) -> dict[str, Any]:
    """Detect sessions with unusual tool call patterns relative to file changes."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return {
            "total_sessions": 0,
            "under_verification_count": 0,
            "over_verification_count": 0,
            "long_gap_count": 0,
            "examples": [],
        }

    total_sessions = 0
    under_verification_count = 0
    over_verification_count = 0
    long_gap_count = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        tool_call_count = _tool_call_count(record)
        file_change_count = _file_change_count(record)
        tool_call_timestamps = _tool_call_timestamps(record)

        if file_change_count == 0:
            continue

        # Calculate density ratio
        density_ratio = tool_call_count / file_change_count if file_change_count > 0 else 0

        # Check for under-verification (<2 tool calls per file change)
        if density_ratio < 2.0 and tool_call_count > 0:
            under_verification_count += 1
            _append_example(
                examples,
                _session_id(record, index),
                "under_verification",
                f"{tool_call_count} tool calls for {file_change_count} file changes ({density_ratio:.1f} ratio)"
            )

        # Check for over-verification (>10 tool calls per file change)
        elif density_ratio > 10.0:
            over_verification_count += 1
            _append_example(
                examples,
                _session_id(record, index),
                "over_verification",
                f"{tool_call_count} tool calls for {file_change_count} file changes ({density_ratio:.1f} ratio)"
            )

        # Check for long gaps without tool activity (>5 minutes = 300 seconds)
        if tool_call_timestamps and len(tool_call_timestamps) >= 2:
            gaps = [
                tool_call_timestamps[i] - tool_call_timestamps[i-1]
                for i in range(1, len(tool_call_timestamps))
            ]
            max_gap = max(gaps) if gaps else 0
            if max_gap > 300:  # 5 minutes
                long_gap_count += 1
                _append_example(
                    examples,
                    _session_id(record, index),
                    "long_gap",
                    f"gap of {max_gap:.0f}s ({max_gap/60:.1f}min) without tool activity"
                )

    return {
        "total_sessions": total_sessions,
        "under_verification_count": under_verification_count,
        "over_verification_count": over_verification_count,
        "long_gap_count": long_gap_count,
        "issue_percentage": _percentage(
            under_verification_count + over_verification_count + long_gap_count,
            total_sessions
        ),
        "examples": examples[:5],
    }


def _tool_call_count(record: Mapping[str, Any]) -> int:
    """Extract tool call count from record."""
    for key in ("toolCallCount", "tool_call_count", "tool_calls"):
        value = record.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, list):
            return len(value)
    return 0


def _file_change_count(record: Mapping[str, Any]) -> int:
    """Extract file change count from record."""
    for key in ("fileChangeCount", "file_change_count", "changed_files", "files_changed"):
        value = record.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
        if isinstance(value, list):
            return len(value)
    return 0


def _tool_call_timestamps(record: Mapping[str, Any]) -> list[float]:
    """Extract tool call timestamps from record."""
    for key in ("toolCallTimestamps", "tool_call_timestamps", "timestamps"):
        value = record.get(key)
        if isinstance(value, list):
            timestamps = []
            for item in value:
                if isinstance(item, (int, float)) and not isinstance(item, bool):
                    timestamps.append(float(item))
            return sorted(timestamps)
    return []


def _session_id(record: Mapping[str, Any], fallback: int) -> str:
    """Extract session ID from record."""
    for key in ("sessionId", "session_id", "id"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return str(fallback)


def _append_example(
    examples: list[dict[str, Any]],
    session_id: str,
    reason: str,
    details: str
) -> None:
    """Add example if under limit."""
    if len(examples) < 5:
        examples.append({
            "session_id": session_id,
            "reason": reason,
            "details": details,
        })


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
