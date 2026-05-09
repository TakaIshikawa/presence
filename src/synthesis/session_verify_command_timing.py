"""Session verify command timing analyzer for verification patterns.

Analyzes when verify commands are used relative to edit operations. Measures
verify command frequency, average edits between verify calls, verify-to-edit
ratio, and identifies patterns of over-verification (verify after every edit)
vs strategic verification (verify after complex multi-file changes).

Verification timing metrics:
- Verify command frequency: How often verify is called
- Average edits between verifies: Mean edit count between verify calls
- Verify-to-edit ratio: Balance of verification to editing
- Over-verification pattern: Verify after single edits
- Strategic verification: Verify after multi-file complex changes

Optimization indicators:
- Strategic verification: Verify after complex changes, not every edit
- Low verify-to-edit ratio: Efficient verification timing
- High edits between verifies: Batching edits before verification
- Targeted verification: Using verify when truly needed
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_verify_command_timing(records: object) -> dict[str, Any]:
    """Analyze verify command timing relative to edit operations.

    Tracks verify calls, measures timing relative to edits, and identifies
    patterns of over-verification vs strategic verification.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Verify, Edit, etc.)
            - turn_index: Turn number when tool was invoked
            - files_affected: Optional number of files affected by edit
            - verification_type: Optional type (unit, integration, build)

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls analyzed
            - verify_call_count: Number of verify command calls
            - edit_call_count: Number of Edit tool calls
            - avg_edits_between_verifies: Average edits between verify calls
            - verify_to_edit_ratio: Percentage verify/(verify+edit)
            - single_edit_verifies: Count of verifies after single edit
            - multi_edit_verifies: Count of verifies after multiple edits
            - over_verification_rate: Percentage of single-edit verifies
            - strategic_verification_rate: Percentage of multi-edit verifies
            - verify_frequency_per_10_edits: Verify calls per 10 edits

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    verify_call_count = 0
    edit_call_count = 0

    # Track edit count between verifies
    current_edit_count = 0
    edits_between_verifies: list[int] = []

    # Track verification patterns
    single_edit_verifies = 0   # Verify after 1 edit
    multi_edit_verifies = 0     # Verify after 2+ edits

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        total_tool_calls += 1
        tool_lower = tool_name.lower()

        if tool_lower == "edit":
            edit_call_count += 1
            current_edit_count += 1

        elif tool_lower == "verify":
            verify_call_count += 1

            # Record edit count before this verify
            if current_edit_count > 0:
                edits_between_verifies.append(current_edit_count)

                # Classify verification pattern
                if current_edit_count == 1:
                    single_edit_verifies += 1
                else:
                    multi_edit_verifies += 1

                # Reset counter
                current_edit_count = 0

    # Calculate metrics
    avg_edits_between = _average(edits_between_verifies)
    verify_edit_total = verify_call_count + edit_call_count
    verify_to_edit_ratio = _percentage(verify_call_count, verify_edit_total)

    # Verification pattern rates
    total_verifies = single_edit_verifies + multi_edit_verifies
    over_verification_rate = _percentage(single_edit_verifies, total_verifies)
    strategic_verification_rate = _percentage(multi_edit_verifies, total_verifies)

    # Verify frequency per 10 edits
    if edit_call_count > 0:
        verify_frequency_per_10 = (verify_call_count / edit_call_count) * 10
    else:
        verify_frequency_per_10 = 0.0

    return {
        "total_tool_calls": total_tool_calls,
        "verify_call_count": verify_call_count,
        "edit_call_count": edit_call_count,
        "avg_edits_between_verifies": avg_edits_between,
        "verify_to_edit_ratio": verify_to_edit_ratio,
        "single_edit_verifies": single_edit_verifies,
        "multi_edit_verifies": multi_edit_verifies,
        "over_verification_rate": over_verification_rate,
        "strategic_verification_rate": strategic_verification_rate,
        "verify_frequency_per_10_edits": round(verify_frequency_per_10, 2),
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
