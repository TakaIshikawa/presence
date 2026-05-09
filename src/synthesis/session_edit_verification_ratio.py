"""Session Edit-to-verification ratio analyzer for quality assurance patterns.

Analyzes the ratio of Edit tool calls to verification activities (Read, Bash with
verify/test/build commands, or /verify skill invocations) in Claude Code sessions.
Measures code quality discipline and verification thoroughness.

Edit-verification metrics:
- Total Edit calls: Number of Edit tool invocations
- Total verification activities: Read, verify commands, skill invocations
- Edit-to-verification ratio: Edits per verification activity
- Average verifications per edit: Inverse ratio
- Verify-after-edit rate: % of Edits followed by verification within 3 calls
- Full-read-after-edit rate: % using full Read after Edit
- Targeted-read-after-edit rate: % using offset/limit Read after Edit

Quality indicators:
- Low edit-to-verification ratio (<2.0): Good verification discipline
- High verify-after-edit rate (>70%): Consistent post-edit verification
- High targeted-read rate (>60%): Efficient verification with targeted reads
- Balanced verification mix: Not relying solely on full reads
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_edit_verification_ratio(records: object) -> dict[str, Any]:
    """Analyze Edit-to-verification ratio in Claude Code sessions.

    Evaluates verification discipline by measuring how frequently edits are
    followed by verification activities and the nature of those verifications.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_edit_calls: Number of Edit tool invocations
            - total_verification_activities: Total verification actions
            - verify_after_edit_count: Edits followed by verify within 3 calls
            - full_read_after_edit: Edits followed by full Read
            - targeted_read_after_edit: Edits followed by offset/limit Read
            - bash_verify_count: Bash calls with verify/test/build
            - verify_skill_count: /verify skill invocations
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_edits: Count of sessions with Edit calls
            - avg_edit_calls: Average Edit invocations per session
            - avg_verification_activities: Average verification actions
            - avg_edit_to_verification_ratio: Average edits per verification
            - avg_verifications_per_edit: Average verifications per edit
            - avg_verify_after_edit_rate: Average % verify within 3 calls
            - avg_full_read_after_edit_rate: Average % full Read after Edit
            - avg_targeted_read_after_edit_rate: Average % targeted Read after Edit
            - high_discipline_sessions: Count with <2.0 edit-verify ratio
            - low_discipline_sessions: Count with >4.0 edit-verify ratio

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_edits = 0

    edit_calls: list[int | float] = []
    verification_activities: list[int | float] = []
    edit_verify_ratios: list[float] = []
    verifications_per_edit: list[float] = []
    verify_after_edit_rates: list[float] = []
    full_read_rates: list[float] = []
    targeted_read_rates: list[float] = []

    high_discipline_sessions = 0  # <2.0 edit-verify ratio
    low_discipline_sessions = 0   # >4.0 edit-verify ratio

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_edits = _extract_int(record.get("total_edit_calls"))
        total_verifications = _extract_int(record.get("total_verification_activities"))
        verify_after = _extract_int(record.get("verify_after_edit_count"))
        full_read = _extract_int(record.get("full_read_after_edit"))
        targeted_read = _extract_int(record.get("targeted_read_after_edit"))

        # Track sessions with edits
        if total_edits is not None and total_edits > 0:
            sessions_with_edits += 1
            edit_calls.append(total_edits)

            # Calculate edit-to-verification ratio
            if total_verifications is not None:
                verification_activities.append(total_verifications)

                if total_verifications > 0:
                    ratio = total_edits / total_verifications
                    edit_verify_ratios.append(ratio)

                    # Inverse: verifications per edit
                    verifs_per_edit = total_verifications / total_edits
                    verifications_per_edit.append(verifs_per_edit)

                    # Classify discipline
                    if ratio < 2.0:
                        high_discipline_sessions += 1
                    elif ratio > 4.0:
                        low_discipline_sessions += 1

            # Calculate verify-after-edit rate
            if verify_after is not None:
                verify_after_edit_rates.append(
                    _percentage(verify_after, total_edits)
                )

            # Calculate full-read-after-edit rate
            if full_read is not None:
                full_read_rates.append(_percentage(full_read, total_edits))

            # Calculate targeted-read-after-edit rate
            if targeted_read is not None:
                targeted_read_rates.append(_percentage(targeted_read, total_edits))

    # Calculate aggregate metrics
    avg_edits = _average(edit_calls)
    avg_verifications = _average(verification_activities)
    avg_ratio = _average(edit_verify_ratios)
    avg_per_edit = _average(verifications_per_edit)
    avg_verify_after = _average(verify_after_edit_rates)
    avg_full_read = _average(full_read_rates)
    avg_targeted_read = _average(targeted_read_rates)

    return {
        "total_sessions": total_sessions,
        "sessions_with_edits": sessions_with_edits,
        "avg_edit_calls": avg_edits,
        "avg_verification_activities": avg_verifications,
        "avg_edit_to_verification_ratio": avg_ratio,
        "avg_verifications_per_edit": avg_per_edit,
        "avg_verify_after_edit_rate": avg_verify_after,
        "avg_full_read_after_edit_rate": avg_full_read,
        "avg_targeted_read_after_edit_rate": avg_targeted_read,
        "high_discipline_sessions": high_discipline_sessions,
        "low_discipline_sessions": low_discipline_sessions,
    }


def _extract_int(value: object) -> int | None:
    """Extract integer from value if available."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


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
