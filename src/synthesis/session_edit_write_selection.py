"""Session Edit vs Write tool selection discipline analyzer.

Analyzes Edit vs Write tool usage decisions in Claude Code sessions,
measuring tool selection appropriateness, Edit-Read contract adherence,
and edit precision patterns.

Tool selection dimensions:
1. Edit preference for existing files:
   - % existing files modified via Edit vs Write
   - Write-to-existing anti-pattern detection

2. Edit-Read contract:
   - % Edit calls preceded by Read (tool requirement)
   - Write calls without prior Read for existing files

3. Edit precision:
   - old_string size distribution
   - replace_all usage rate
   - Edit success rate

4. File creation justification:
   - New files: necessary (tests, features) vs unnecessary (docs without request)

Quality indicators:
- High Edit preference (>90%): Prefer Edit for existing files
- Read-before-Edit compliance (>95%): Follow tool contract
- Moderate edit size (30-200 chars): Precise targeted edits
- Low unnecessary file creation (<10%): Only when needed
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_edit_write_selection(records: object) -> dict[str, Any]:
    """Analyze Edit vs Write tool selection discipline.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_edit_calls: Edit tool invocations
            - total_write_calls: Write tool invocations
            - existing_files_edited: Files modified via Edit
            - existing_files_written: Files modified via Write (anti-pattern)
            - edit_with_prior_read: Edit calls preceded by Read
            - edit_without_prior_read: Edit calls without Read (violation)
            - write_to_existing_without_read: Write to existing without Read
            - new_files_created: New files via Write
            - necessary_new_files: Justified new files (tests, features)
            - unnecessary_new_files: Unjustified files (docs without request)
            - avg_edit_old_string_size: Average old_string character count
            - edit_replace_all_count: Edits using replace_all flag
            - edit_success_count: Successful Edit calls
            - edit_failure_count: Failed Edit calls

    Returns:
        Dict with:
            - total_sessions: Number of sessions analyzed
            - sessions_with_edits: Sessions using Edit tool
            - edit_preference_score: % existing files modified via Edit
            - write_to_existing_rate: % existing files using Write (anti-pattern)
            - read_before_edit_compliance: % Edit calls with prior Read
            - edit_without_read_rate: % Edit violations
            - write_to_existing_without_read_rate: % Write violations
            - avg_edit_old_string_size: Average edit size in characters
            - replace_all_usage_rate: % edits using replace_all
            - edit_success_rate: % successful Edit calls
            - new_file_creation_rate: New files per session
            - unnecessary_file_creation_rate: % unnecessary new files
            - tool_selection_score: Overall selection discipline score 0-1
            - high_discipline_sessions: Count with score >0.8
            - low_discipline_sessions: Count with score <0.5

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
    sessions_with_edits = 0
    total_existing_edited = 0
    total_existing_written = 0
    total_edit_with_read = 0
    total_edit_without_read = 0
    total_write_to_existing_without_read = 0
    total_new_files = 0
    total_unnecessary_new_files = 0
    total_replace_all = 0
    total_edit_success = 0
    total_edit_failure = 0

    edit_sizes: list[int | float] = []
    session_scores: list[int | float] = []

    high_discipline_sessions = 0
    low_discipline_sessions = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        edit_calls = _int(record.get("total_edit_calls", 0))
        existing_edited = _int(record.get("existing_files_edited", 0))
        existing_written = _int(record.get("existing_files_written", 0))
        edit_with_read = _int(record.get("edit_with_prior_read", 0))
        edit_without_read = _int(record.get("edit_without_prior_read", 0))
        write_without_read = _int(record.get("write_to_existing_without_read", 0))
        new_files = _int(record.get("new_files_created", 0))
        unnecessary = _int(record.get("unnecessary_new_files", 0))
        avg_size = _float(record.get("avg_edit_old_string_size", 0.0))
        replace_all = _int(record.get("edit_replace_all_count", 0))
        edit_success = _int(record.get("edit_success_count", 0))
        edit_failure = _int(record.get("edit_failure_count", 0))

        if edit_calls > 0:
            sessions_with_edits += 1

        total_existing_edited += existing_edited
        total_existing_written += existing_written
        total_edit_with_read += edit_with_read
        total_edit_without_read += edit_without_read
        total_write_to_existing_without_read += write_without_read
        total_new_files += new_files
        total_unnecessary_new_files += unnecessary
        total_replace_all += replace_all
        total_edit_success += edit_success
        total_edit_failure += edit_failure

        if avg_size > 0:
            edit_sizes.append(avg_size)

        # Calculate session score
        session_score = _calculate_session_score(
            existing_edited=existing_edited,
            existing_written=existing_written,
            edit_with_read=edit_with_read,
            edit_without_read=edit_without_read,
            unnecessary_new_files=unnecessary,
            new_files=new_files,
            avg_edit_size=avg_size,
        )
        session_scores.append(session_score)

        if session_score > 0.8:
            high_discipline_sessions += 1
        elif session_score < 0.5:
            low_discipline_sessions += 1

    # Calculate rates
    total_existing_modifications = total_existing_edited + total_existing_written
    edit_preference_score = _percentage(
        total_existing_edited, total_existing_modifications
    )
    write_to_existing_rate = _percentage(
        total_existing_written, total_existing_modifications
    )

    total_edit_calls = total_edit_with_read + total_edit_without_read
    read_before_edit_compliance = _percentage(total_edit_with_read, total_edit_calls)
    edit_without_read_rate = _percentage(total_edit_without_read, total_edit_calls)
    write_to_existing_without_read_rate = _percentage(
        total_write_to_existing_without_read, total_existing_written
    )

    replace_all_usage_rate = _percentage(total_replace_all, total_edit_calls)

    total_edit_attempts = total_edit_success + total_edit_failure
    edit_success_rate = _percentage(total_edit_success, total_edit_attempts)

    unnecessary_file_creation_rate = _percentage(
        total_unnecessary_new_files, total_new_files
    )

    # Calculate averages
    avg_edit_old_string_size = _average(edit_sizes)
    new_file_creation_rate = total_new_files / total_sessions if total_sessions > 0 else 0.0

    # Calculate overall score
    tool_selection_score = _calculate_pack_score(
        edit_preference_score=edit_preference_score,
        read_before_edit_compliance=read_before_edit_compliance,
        edit_without_read_rate=edit_without_read_rate,
        unnecessary_file_creation_rate=unnecessary_file_creation_rate,
        avg_edit_size=avg_edit_old_string_size,
    )

    return {
        "total_sessions": total_sessions,
        "sessions_with_edits": sessions_with_edits,
        "edit_preference_score": edit_preference_score,
        "write_to_existing_rate": write_to_existing_rate,
        "read_before_edit_compliance": read_before_edit_compliance,
        "edit_without_read_rate": edit_without_read_rate,
        "write_to_existing_without_read_rate": write_to_existing_without_read_rate,
        "avg_edit_old_string_size": avg_edit_old_string_size,
        "replace_all_usage_rate": replace_all_usage_rate,
        "edit_success_rate": edit_success_rate,
        "new_file_creation_rate": round(new_file_creation_rate, 2),
        "unnecessary_file_creation_rate": unnecessary_file_creation_rate,
        "tool_selection_score": tool_selection_score,
        "high_discipline_sessions": high_discipline_sessions,
        "low_discipline_sessions": low_discipline_sessions,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "sessions_with_edits": 0,
        "edit_preference_score": 0.0,
        "write_to_existing_rate": 0.0,
        "read_before_edit_compliance": 0.0,
        "edit_without_read_rate": 0.0,
        "write_to_existing_without_read_rate": 0.0,
        "avg_edit_old_string_size": 0.0,
        "replace_all_usage_rate": 0.0,
        "edit_success_rate": 0.0,
        "new_file_creation_rate": 0.0,
        "unnecessary_file_creation_rate": 0.0,
        "tool_selection_score": 0.0,
        "high_discipline_sessions": 0,
        "low_discipline_sessions": 0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_session_score(
    existing_edited: int,
    existing_written: int,
    edit_with_read: int,
    edit_without_read: int,
    unnecessary_new_files: int,
    new_files: int,
    avg_edit_size: float,
) -> float:
    """Calculate session-level tool selection score (0-1).

    Scoring components:
    - Edit preference (0-0.40): Using Edit for existing files
    - Read-before-Edit compliance (0-0.30): Following tool contract
    - Low unnecessary files (0-0.15): Not creating docs without request
    - Appropriate edit size (0-0.15): Targeted edits (30-200 chars)

    Returns:
        Session score from 0.0 to 1.0
    """
    score = 0.0

    # Edit preference component (0-0.40)
    total_modifications = existing_edited + existing_written
    if total_modifications > 0:
        edit_pref_rate = _percentage(existing_edited, total_modifications)
        if edit_pref_rate >= 90:
            score += 0.40
        elif edit_pref_rate >= 75:
            score += 0.30
        elif edit_pref_rate >= 50:
            score += 0.20

    # Read-before-Edit compliance (0-0.30)
    total_edits = edit_with_read + edit_without_read
    if total_edits > 0:
        compliance_rate = _percentage(edit_with_read, total_edits)
        if compliance_rate >= 95:
            score += 0.30
        elif compliance_rate >= 85:
            score += 0.20
        elif compliance_rate >= 70:
            score += 0.10

    # Low unnecessary file creation (0-0.15)
    if new_files > 0:
        unnecessary_rate = _percentage(unnecessary_new_files, new_files)
        if unnecessary_rate <= 10:
            score += 0.15
        elif unnecessary_rate <= 20:
            score += 0.10
        elif unnecessary_rate <= 35:
            score += 0.05

    # Appropriate edit size (0-0.15)
    if avg_edit_size > 0:
        if 30 <= avg_edit_size <= 200:
            score += 0.15
        elif 20 <= avg_edit_size < 30 or 200 < avg_edit_size <= 300:
            score += 0.10
        elif 10 <= avg_edit_size < 20 or 300 < avg_edit_size <= 500:
            score += 0.05

    return round(score, 3)


def _calculate_pack_score(
    edit_preference_score: float,
    read_before_edit_compliance: float,
    edit_without_read_rate: float,
    unnecessary_file_creation_rate: float,
    avg_edit_size: float,
) -> float:
    """Calculate overall pack tool selection score (0-1).

    Scoring components:
    - Edit preference (0-0.35): >90% existing files use Edit
    - Read-before-Edit compliance (0-0.30): >95% Edit calls have prior Read
    - Low Edit violations (0-0.15): <5% Edit without Read
    - Low unnecessary files (0-0.10): <10% unnecessary new files
    - Appropriate edit size (0-0.10): 30-200 chars average

    Returns:
        Pack score from 0.0 to 1.0
    """
    score = 0.0

    # Edit preference component (0-0.35)
    if edit_preference_score >= 90:
        score += 0.35
    elif edit_preference_score >= 75:
        score += 0.25
    elif edit_preference_score >= 50:
        score += 0.15

    # Read-before-Edit compliance (0-0.30)
    if read_before_edit_compliance >= 95:
        score += 0.30
    elif read_before_edit_compliance >= 85:
        score += 0.22
    elif read_before_edit_compliance >= 70:
        score += 0.15

    # Edit violation penalty (0-0.15)
    if edit_without_read_rate <= 5:
        score += 0.15
    elif edit_without_read_rate <= 10:
        score += 0.10
    elif edit_without_read_rate <= 20:
        score += 0.05

    # Unnecessary file creation penalty (0-0.10)
    if unnecessary_file_creation_rate <= 10:
        score += 0.10
    elif unnecessary_file_creation_rate <= 20:
        score += 0.07
    elif unnecessary_file_creation_rate <= 35:
        score += 0.04

    # Appropriate edit size (0-0.10)
    if 30 <= avg_edit_size <= 200:
        score += 0.10
    elif 20 <= avg_edit_size < 30 or 200 < avg_edit_size <= 300:
        score += 0.07
    elif 10 <= avg_edit_size < 20 or 300 < avg_edit_size <= 500:
        score += 0.04

    return round(max(0.0, min(1.0, score)), 3)
