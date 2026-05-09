"""Pack Write vs Edit tool discipline analyzer.

Analyzes Write and Edit tool usage discipline across execution pack sessions to
measure adherence to 'prefer Edit for existing files' guidelines. Identifies
violations, tracks string match failures, and measures new file justification.

Tool discipline metrics:
- Write vs Edit counts: Total usage across pack sessions
- Write-on-existing ratio: Fraction of Write calls targeting existing files
- Edit string match failures: Failed old_string matches requiring retry
- Replace-all usage: Edit calls using replace_all parameter
- New file justification: Write calls creating genuinely new files

Quality indicators:
- Low write-on-existing ratio: <10% of Write calls overwrite existing files
- Low edit match failures: <5% of Edit calls fail on first attempt
- High replace-all usage: >20% of Edit calls use replace_all (good for refactoring)
- High new file justification: >90% of Write calls create genuinely new files
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_write_tool_discipline(records: object) -> dict[str, Any]:
    """Analyze Write and Edit tool usage discipline across pack sessions.

    Evaluates adherence to tool usage guidelines, identifies violations,
    and measures editing precision across all sessions in an execution pack.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - write_count: Number of Write tool calls
            - edit_count: Number of Edit tool calls
            - write_on_existing_count: Write calls targeting existing files
            - edit_string_match_failures: Edit calls with old_string mismatch
            - replace_all_count: Edit calls using replace_all parameter
            - new_file_created_count: Write calls creating genuinely new files
            - total_file_operations: Total Write + Edit operations

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - total_write_count: Sum of Write calls across all sessions
            - total_edit_count: Sum of Edit calls across all sessions
            - total_file_operations: Total Write + Edit operations
            - write_edit_ratio: Percentage of Write vs total operations
            - write_on_existing_count: Write calls targeting existing files
            - write_on_existing_ratio: Percentage of Write calls on existing files
            - edit_string_match_failure_count: Total failed Edit matches
            - edit_match_failure_ratio: Percentage of Edit calls that fail
            - replace_all_usage_count: Total Edit calls with replace_all
            - replace_all_usage_ratio: Percentage of Edit calls using replace_all
            - new_file_justification_count: Write calls creating new files
            - new_file_justification_ratio: Percentage of justified Write calls
            - disciplined_sessions: Sessions with <10% write-on-existing ratio
            - tool_discipline_score: 0-1 overall discipline score

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    total_write_count = 0
    total_edit_count = 0

    write_on_existing_count = 0
    edit_string_match_failure_count = 0
    replace_all_usage_count = 0
    new_file_justification_count = 0

    disciplined_sessions = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        # Count Write and Edit calls
        write_count = _int(record.get("write_count", 0))
        edit_count = _int(record.get("edit_count", 0))
        total_write_count += write_count
        total_edit_count += edit_count

        # Track Write-on-existing violations
        write_on_existing = _int(record.get("write_on_existing_count", 0))
        write_on_existing_count += write_on_existing

        # Track Edit string match failures
        match_failures = _int(record.get("edit_string_match_failures", 0))
        edit_string_match_failure_count += match_failures

        # Track replace_all usage
        replace_all = _int(record.get("replace_all_count", 0))
        replace_all_usage_count += replace_all

        # Track new file justification
        new_files = _int(record.get("new_file_created_count", 0))
        new_file_justification_count += new_files

        # Check if session is disciplined
        if write_count > 0:
            session_write_on_existing_ratio = (write_on_existing / write_count) * 100.0
            if session_write_on_existing_ratio < 10.0:
                disciplined_sessions += 1
        elif write_count == 0:
            # No Write calls means disciplined
            disciplined_sessions += 1

    # Calculate aggregate metrics
    total_file_operations = total_write_count + total_edit_count
    write_edit_ratio = _percentage(total_write_count, total_file_operations)
    write_on_existing_ratio = _percentage(write_on_existing_count, total_write_count)
    edit_match_failure_ratio = _percentage(edit_string_match_failure_count, total_edit_count)
    replace_all_usage_ratio = _percentage(replace_all_usage_count, total_edit_count)
    new_file_justification_ratio = _percentage(new_file_justification_count, total_write_count)

    # Calculate discipline score
    discipline_score = _calculate_discipline_score(
        write_on_existing_ratio,
        edit_match_failure_ratio,
        replace_all_usage_ratio,
        new_file_justification_ratio,
    )

    return {
        "total_sessions": total_sessions,
        "total_write_count": total_write_count,
        "total_edit_count": total_edit_count,
        "total_file_operations": total_file_operations,
        "write_edit_ratio": write_edit_ratio,
        "write_on_existing_count": write_on_existing_count,
        "write_on_existing_ratio": write_on_existing_ratio,
        "edit_string_match_failure_count": edit_string_match_failure_count,
        "edit_match_failure_ratio": edit_match_failure_ratio,
        "replace_all_usage_count": replace_all_usage_count,
        "replace_all_usage_ratio": replace_all_usage_ratio,
        "new_file_justification_count": new_file_justification_count,
        "new_file_justification_ratio": new_file_justification_ratio,
        "disciplined_sessions": disciplined_sessions,
        "tool_discipline_score": discipline_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "total_write_count": 0,
        "total_edit_count": 0,
        "total_file_operations": 0,
        "write_edit_ratio": 0.0,
        "write_on_existing_count": 0,
        "write_on_existing_ratio": 0.0,
        "edit_string_match_failure_count": 0,
        "edit_match_failure_ratio": 0.0,
        "replace_all_usage_count": 0,
        "replace_all_usage_ratio": 0.0,
        "new_file_justification_count": 0,
        "new_file_justification_ratio": 0.0,
        "disciplined_sessions": 0,
        "tool_discipline_score": 0.0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _calculate_discipline_score(
    write_on_existing_ratio: float,
    edit_match_failure_ratio: float,
    replace_all_ratio: float,
    new_file_justification_ratio: float,
) -> float:
    """Calculate tool discipline score (0-1).

    Score components:
    - 0.4: Low write-on-existing ratio (<10% is optimal)
    - 0.3: High new file justification (>90% is optimal)
    - 0.2: Low edit match failure ratio (<5% is optimal)
    - 0.1: Good replace-all usage (>20% is good)
    """
    # Write-on-existing component (0-0.4)
    # Target: <10% write-on-existing
    if write_on_existing_ratio <= 10:
        write_component = 0.4
    else:
        # Penalize higher ratios
        write_component = max(0.0, 0.4 - (write_on_existing_ratio - 10) / 100.0)

    # New file justification component (0-0.3)
    # Target: >90% justified
    if new_file_justification_ratio >= 90:
        justification_component = 0.3
    else:
        justification_component = (new_file_justification_ratio / 90.0) * 0.3

    # Edit match failure component (0-0.2)
    # Target: <5% failures
    if edit_match_failure_ratio <= 5:
        failure_component = 0.2
    else:
        # Penalize higher failure rates
        failure_component = max(0.0, 0.2 - (edit_match_failure_ratio - 5) / 50.0)

    # Replace-all usage component (0-0.1)
    # Target: >20% usage (indicates refactoring discipline)
    if replace_all_ratio >= 20:
        replace_component = 0.1
    else:
        replace_component = (replace_all_ratio / 20.0) * 0.1

    score = (
        write_component +
        justification_component +
        failure_component +
        replace_component
    )

    return round(max(0.0, min(1.0, score)), 3)
