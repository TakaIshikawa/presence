"""Session Edit precision and old_string uniqueness analyzer.

Analyzes Edit tool usage patterns in session transcripts for old_string uniqueness,
context preservation, line-number prefix exclusion, new_string validity, and
Read-before-Edit discipline.

Edit precision dimensions:
1. old_string uniqueness:
   - Single match vs ambiguous matches
   - replace_all usage when appropriate

2. Context preservation:
   - Exact indentation/whitespace matching
   - Preserves formatting from Read output

3. Line-number prefix exclusion:
   - No line numbers in old_string/new_string
   - Clean content matching

4. new_string semantic validity:
   - Non-empty replacements
   - Syntactically plausible content

5. Read-before-Edit discipline:
   - Read file before Edit operation
   - Prevents blind modifications

Quality indicators:
- High uniqueness rate (>95%)
- Perfect context preservation
- No line-number prefix errors
- All new_string values valid
- 100% Read-before-Edit adherence
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class EditCall:
    """Represents an Edit tool call."""

    turn_index: int
    file_path: str
    old_string: str
    new_string: str
    replace_all: bool
    had_prior_read: bool
    old_string_match_count: int  # 0=no match, 1=unique, 2+=ambiguous
    has_indentation_error: bool
    has_line_number_prefix: bool
    new_string_is_empty: bool


@dataclass(frozen=True)
class Finding:
    """Represents an Edit precision finding with severity."""

    severity: str  # critical, warning, info
    category: str
    message: str
    turn_index: int
    example: str


@dataclass(frozen=True)
class EditPrecisionMetrics:
    """Aggregate metrics for Edit precision."""

    total_edits: int
    unique_old_string_count: int
    ambiguous_old_string_count: int
    no_match_old_string_count: int
    uniqueness_rate: float
    context_preservation_count: int
    indentation_error_count: int
    line_number_prefix_count: int
    empty_new_string_count: int
    read_before_edit_count: int
    read_before_edit_rate: float
    findings_count: int
    critical_findings: int
    warning_findings: int


def analyze_session_edit_precision(
    edits: Sequence[EditCall],
) -> tuple[EditPrecisionMetrics, Sequence[Finding]]:
    """Analyze Edit tool precision patterns.

    Args:
        edits: Sequence of EditCall instances from session transcript

    Returns:
        Tuple of (metrics, findings)

    Raises:
        ValueError: If edits is invalid
    """
    _validate_edits(edits)

    if not edits:
        return (
            EditPrecisionMetrics(
                total_edits=0,
                unique_old_string_count=0,
                ambiguous_old_string_count=0,
                no_match_old_string_count=0,
                uniqueness_rate=0.0,
                context_preservation_count=0,
                indentation_error_count=0,
                line_number_prefix_count=0,
                empty_new_string_count=0,
                read_before_edit_count=0,
                read_before_edit_rate=0.0,
                findings_count=0,
                critical_findings=0,
                warning_findings=0,
            ),
            (),
        )

    findings: list[Finding] = []

    unique_count = sum(1 for e in edits if e.old_string_match_count == 1)
    ambiguous_count = sum(1 for e in edits if e.old_string_match_count > 1)
    no_match_count = sum(1 for e in edits if e.old_string_match_count == 0)
    indentation_error_count = sum(1 for e in edits if e.has_indentation_error)
    line_number_prefix_count = sum(1 for e in edits if e.has_line_number_prefix)
    empty_new_string_count = sum(1 for e in edits if e.new_string_is_empty)
    read_before_edit_count = sum(1 for e in edits if e.had_prior_read)

    context_preservation_count = len(edits) - indentation_error_count

    findings.extend(_detect_uniqueness_issues(edits))
    findings.extend(_detect_indentation_errors(edits))
    findings.extend(_detect_line_number_prefix_errors(edits))
    findings.extend(_detect_empty_new_string(edits))
    findings.extend(_detect_missing_read_before_edit(edits))

    critical_findings = sum(1 for f in findings if f.severity == "critical")
    warning_findings = sum(1 for f in findings if f.severity == "warning")

    total = len(edits)
    metrics = EditPrecisionMetrics(
        total_edits=total,
        unique_old_string_count=unique_count,
        ambiguous_old_string_count=ambiguous_count,
        no_match_old_string_count=no_match_count,
        uniqueness_rate=_percentage(unique_count, total),
        context_preservation_count=context_preservation_count,
        indentation_error_count=indentation_error_count,
        line_number_prefix_count=line_number_prefix_count,
        empty_new_string_count=empty_new_string_count,
        read_before_edit_count=read_before_edit_count,
        read_before_edit_rate=_percentage(read_before_edit_count, total),
        findings_count=len(findings),
        critical_findings=critical_findings,
        warning_findings=warning_findings,
    )

    return metrics, tuple(findings)


def _validate_edits(edits: Sequence[EditCall]) -> None:
    """Validate edits structure."""
    if not isinstance(edits, (list, tuple)):
        raise ValueError("edits must be a list or tuple")

    for i, edit in enumerate(edits):
        if not isinstance(edit, EditCall):
            raise ValueError(f"edits[{i}] must be an EditCall instance")


def _detect_uniqueness_issues(edits: Sequence[EditCall]) -> list[Finding]:
    """Detect old_string uniqueness issues."""
    findings: list[Finding] = []

    for edit in edits:
        if edit.old_string_match_count == 0:
            findings.append(
                Finding(
                    severity="critical",
                    category="old_string_uniqueness",
                    message="old_string not found in file. Edit will fail.",
                    turn_index=edit.turn_index,
                    example=f"{edit.file_path}: '{edit.old_string[:50]}'",
                )
            )
        elif edit.old_string_match_count > 1 and not edit.replace_all:
            findings.append(
                Finding(
                    severity="warning",
                    category="old_string_uniqueness",
                    message=(
                        f"old_string matches {edit.old_string_match_count} locations. "
                        "Use replace_all=true or add more context for uniqueness."
                    ),
                    turn_index=edit.turn_index,
                    example=f"{edit.file_path}: '{edit.old_string[:50]}'",
                )
            )

    return findings


def _detect_indentation_errors(edits: Sequence[EditCall]) -> list[Finding]:
    """Detect indentation/whitespace preservation errors."""
    findings: list[Finding] = []

    for edit in edits:
        if edit.has_indentation_error:
            findings.append(
                Finding(
                    severity="critical",
                    category="context_preservation",
                    message=(
                        "Indentation mismatch between old_string and file content. "
                        "Preserve exact whitespace from Read output."
                    ),
                    turn_index=edit.turn_index,
                    example=f"{edit.file_path}",
                )
            )

    return findings


def _detect_line_number_prefix_errors(
    edits: Sequence[EditCall],
) -> list[Finding]:
    """Detect line number prefixes in edit strings."""
    findings: list[Finding] = []

    for edit in edits:
        if edit.has_line_number_prefix:
            findings.append(
                Finding(
                    severity="critical",
                    category="line_number_prefix",
                    message=(
                        "old_string or new_string contains line number prefix. "
                        "Exclude line numbers from edit strings."
                    ),
                    turn_index=edit.turn_index,
                    example=f"{edit.file_path}",
                )
            )

    return findings


def _detect_empty_new_string(edits: Sequence[EditCall]) -> list[Finding]:
    """Detect empty new_string values."""
    findings: list[Finding] = []

    for edit in edits:
        if edit.new_string_is_empty:
            findings.append(
                Finding(
                    severity="warning",
                    category="new_string_validity",
                    message=(
                        "new_string is empty. Verify this deletion is intentional."
                    ),
                    turn_index=edit.turn_index,
                    example=f"{edit.file_path}",
                )
            )

    return findings


def _detect_missing_read_before_edit(
    edits: Sequence[EditCall],
) -> list[Finding]:
    """Detect Edit calls without prior Read."""
    findings: list[Finding] = []

    for edit in edits:
        if not edit.had_prior_read:
            findings.append(
                Finding(
                    severity="critical",
                    category="read_before_edit_discipline",
                    message=(
                        "Edit called without prior Read of file. "
                        "Always read file before editing to ensure accuracy."
                    ),
                    turn_index=edit.turn_index,
                    example=f"{edit.file_path}",
                )
            )

    return findings


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
