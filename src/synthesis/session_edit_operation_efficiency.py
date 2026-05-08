"""Session edit operation efficiency analyzer for workflow optimization.

Analyzes edit operation efficiency by tracking the ratio of Edit tool usage vs
Write tool usage for existing files. Detects inefficient patterns where agents
use Write to overwrite entire files instead of targeted Edit operations.

Efficiency metrics:
- Edit count: Number of targeted edit operations
- Write count: Number of full file write operations (excluding new files)
- Edit efficiency score: Ratio of edits to total modifications
- New file count: Files created (excluded from efficiency calculation)
"""

from __future__ import annotations

from typing import Any


def analyze_session_edit_operation_efficiency(records: object) -> dict[str, Any]:
    """Analyze edit operation efficiency in a session.

    Measures the ratio of Edit tool usage (targeted) vs Write tool usage
    (full file overwrites) for existing files.

    Args:
        records: List of file operation dictionaries with keys:
            - operation: "edit" or "write"
            - file_path: Path to the file
            - is_new_file: Boolean indicating if file is being created
            - turn_index: Turn number when operation occurred

    Returns:
        Dict with:
            - total_operations: Total file operations (excluding new files)
            - edit_count: Number of Edit operations
            - write_count: Number of Write operations (excluding new files)
            - new_file_count: Number of new files created
            - edit_efficiency_score: Ratio of edits to total (0.0-1.0)
            - efficiency_rating: Classification (high, medium, low, perfect)

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of file operation dictionaries")

    edit_count = 0
    write_count = 0
    new_file_count = 0

    for record in records:
        if not isinstance(record, dict):
            continue

        operation = _string(record.get("operation")).lower()
        is_new_file = record.get("is_new_file") is True

        if is_new_file:
            new_file_count += 1
            continue

        if operation == "edit":
            edit_count += 1
        elif operation == "write":
            write_count += 1

    total_operations = edit_count + write_count
    edit_efficiency_score = _calculate_efficiency_score(edit_count, total_operations)
    efficiency_rating = _rate_efficiency(edit_efficiency_score, total_operations)

    return {
        "total_operations": total_operations,
        "edit_count": edit_count,
        "write_count": write_count,
        "new_file_count": new_file_count,
        "edit_efficiency_score": edit_efficiency_score,
        "efficiency_rating": efficiency_rating,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _calculate_efficiency_score(edit_count: int, total: int) -> float:
    """Calculate edit efficiency score (0.0-1.0).

    Score = edit_count / total_operations
    """
    if total <= 0:
        return 0.0
    return round(edit_count / total, 3)


def _rate_efficiency(score: float, total: int) -> str:
    """Rate efficiency based on score.

    Ratings:
    - perfect: All edits (score = 1.0)
    - high: >= 0.75
    - medium: 0.5-0.75
    - low: < 0.5
    - empty: No operations
    """
    if total == 0:
        return "empty"
    if score == 1.0:
        return "perfect"
    elif score >= 0.75:
        return "high"
    elif score >= 0.5:
        return "medium"
    else:
        return "low"
