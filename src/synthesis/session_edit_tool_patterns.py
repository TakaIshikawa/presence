"""Session edit tool usage pattern analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


EVENT_EDIT = "edit"
EVENT_READ = "read"

ISSUE_SMALL_MATCH = "small_match"
ISSUE_NO_RECENT_READ = "no_recent_read"
ISSUE_EDIT_FAILED = "edit_failed"
ISSUE_UNNECESSARY_REPLACE_ALL = "unnecessary_replace_all"


@dataclass(frozen=True)
class EditPatternEvent:
    """Event in edit pattern analysis."""

    event_type: str
    turn_index: int
    file_path: str
    old_string: str = ""
    replace_all: bool = False
    edit_succeeded: bool = True
    read_offset: int | None = None


@dataclass(frozen=True)
class PatternIssue:
    """Details of a detected pattern issue."""

    turn_index: int
    file_path: str
    issue_type: str
    old_string_length: int
    had_recent_read: bool
    details: str


@dataclass(frozen=True)
class SessionEditToolPatternMetrics:
    """Aggregate metrics for edit tool patterns."""

    total_edits: int
    small_match_count: int
    no_recent_read_count: int
    failed_edit_count: int
    unnecessary_replace_all_count: int
    total_issue_count: int
    safe_edit_rate: float


@dataclass(frozen=True)
class SessionEditToolPatternAnalysis:
    """Complete analysis of edit tool patterns."""

    metrics: SessionEditToolPatternMetrics
    issues: tuple[PatternIssue, ...]
    insights: tuple[str, ...]


def analyze_session_edit_tool_patterns(
    events: Sequence[EditPatternEvent],
) -> SessionEditToolPatternAnalysis:
    """Identify patterns in Edit tool usage that indicate agent understanding."""

    _validate_events(events)

    if not events:
        return SessionEditToolPatternAnalysis(
            metrics=SessionEditToolPatternMetrics(0, 0, 0, 0, 0, 0, 0.0),
            issues=(),
            insights=("No events provided.",),
        )

    edit_events = [e for e in events if e.event_type == EVENT_EDIT]

    if not edit_events:
        return SessionEditToolPatternAnalysis(
            metrics=SessionEditToolPatternMetrics(0, 0, 0, 0, 0, 0, 0.0),
            issues=(),
            insights=("No edit events found.",),
        )

    # Track recent reads per file
    last_read_turn: dict[str, int] = {}
    for event in events:
        if event.event_type == EVENT_READ:
            last_read_turn[event.file_path] = event.turn_index

    issues: list[PatternIssue] = []

    for event in edit_events:
        had_recent_read = False
        if event.file_path in last_read_turn:
            turns_since_read = event.turn_index - last_read_turn[event.file_path]
            had_recent_read = turns_since_read <= 5

        old_string_length = len(event.old_string)

        # Check for small/ambiguous string matches
        if old_string_length < 10 and old_string_length > 0:
            issues.append(
                PatternIssue(
                    turn_index=event.turn_index,
                    file_path=event.file_path,
                    issue_type=ISSUE_SMALL_MATCH,
                    old_string_length=old_string_length,
                    had_recent_read=had_recent_read,
                    details=f"Edit uses small match string ({old_string_length} chars). "
                            "Risk of ambiguity or non-unique matches.",
                )
            )

        # Check for edits without recent read
        if not had_recent_read:
            issues.append(
                PatternIssue(
                    turn_index=event.turn_index,
                    file_path=event.file_path,
                    issue_type=ISSUE_NO_RECENT_READ,
                    old_string_length=old_string_length,
                    had_recent_read=False,
                    details=f"Edit to {event.file_path} without recent read. "
                            "Risk of editing stale or incorrect context.",
                )
            )

        # Check for failed edits
        if not event.edit_succeeded:
            issues.append(
                PatternIssue(
                    turn_index=event.turn_index,
                    file_path=event.file_path,
                    issue_type=ISSUE_EDIT_FAILED,
                    old_string_length=old_string_length,
                    had_recent_read=had_recent_read,
                    details="Edit failed. Likely due to non-unique match or stale context.",
                )
            )

        # Check for unnecessary replace_all
        # If old_string is long and specific, replace_all is probably unnecessary
        if event.replace_all and old_string_length > 50:
            issues.append(
                PatternIssue(
                    turn_index=event.turn_index,
                    file_path=event.file_path,
                    issue_type=ISSUE_UNNECESSARY_REPLACE_ALL,
                    old_string_length=old_string_length,
                    had_recent_read=had_recent_read,
                    details=f"Edit uses replace_all with long match string ({old_string_length} chars). "
                            "Likely unnecessary.",
                )
            )

    # Calculate metrics
    small_match_count = sum(1 for issue in issues if issue.issue_type == ISSUE_SMALL_MATCH)
    no_recent_read_count = sum(1 for issue in issues if issue.issue_type == ISSUE_NO_RECENT_READ)
    failed_edit_count = sum(1 for issue in issues if issue.issue_type == ISSUE_EDIT_FAILED)
    unnecessary_replace_all_count = sum(
        1 for issue in issues if issue.issue_type == ISSUE_UNNECESSARY_REPLACE_ALL
    )

    safe_edit_rate = (
        (len(edit_events) - len(issues)) / len(edit_events) if edit_events else 0.0
    )

    metrics = SessionEditToolPatternMetrics(
        total_edits=len(edit_events),
        small_match_count=small_match_count,
        no_recent_read_count=no_recent_read_count,
        failed_edit_count=failed_edit_count,
        unnecessary_replace_all_count=unnecessary_replace_all_count,
        total_issue_count=len(issues),
        safe_edit_rate=round(safe_edit_rate, 3),
    )

    return SessionEditToolPatternAnalysis(
        metrics=metrics,
        issues=tuple(issues[:10]),  # Limit to first 10 issues
        insights=_generate_insights(metrics),
    )


def _validate_events(events: Sequence[EditPatternEvent]) -> None:
    """Validate event sequence structure and content."""
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")

    last_turn = -1
    for index, event in enumerate(events):
        if not isinstance(event, EditPatternEvent):
            raise ValueError("events must contain EditPatternEvent instances")

        if event.event_type not in {EVENT_EDIT, EVENT_READ}:
            raise ValueError(
                f"event at index {index} has invalid event_type: {event.event_type}"
            )

        if not isinstance(event.turn_index, int) or isinstance(event.turn_index, bool):
            raise ValueError(f"turn_index at index {index} must be an integer")

        if event.turn_index < 0:
            raise ValueError(f"turn_index at index {index} must be non-negative")

        if event.turn_index < last_turn:
            raise ValueError("events must be ordered by turn_index")

        last_turn = event.turn_index

        if not isinstance(event.file_path, str) or not event.file_path.strip():
            raise ValueError(
                f"event at index {index} must have a non-empty file_path"
            )

        if event.event_type == EVENT_EDIT:
            if not isinstance(event.old_string, str):
                raise ValueError(
                    f"edit event at index {index} must have string old_string"
                )


def _generate_insights(metrics: SessionEditToolPatternMetrics) -> tuple[str, ...]:
    """Generate human-readable insights about edit patterns."""
    if metrics.total_edits == 0:
        return ("No edit events found.",)

    if metrics.total_issue_count == 0:
        return ("No edit pattern issues detected. All edits appear safe.",)

    insights = [
        f"Detected {metrics.total_issue_count} pattern issue(s) "
        f"across {metrics.total_edits} edit operation(s)."
    ]

    if metrics.safe_edit_rate < 0.5:
        insights.append(
            f"Low safe edit rate ({metrics.safe_edit_rate:.1%}). "
            "Consider reading files before editing and using longer match strings."
        )

    if metrics.small_match_count > 0:
        insights.append(
            f"{metrics.small_match_count} edit(s) use small match strings (<10 chars). "
            "Risk of ambiguous matches."
        )

    if metrics.no_recent_read_count > 0:
        insights.append(
            f"{metrics.no_recent_read_count} edit(s) without recent read of target file. "
            "Risk of stale context."
        )

    if metrics.failed_edit_count > 0:
        insights.append(
            f"{metrics.failed_edit_count} edit(s) failed. "
            "Check for non-unique matches or stale context."
        )

    if metrics.unnecessary_replace_all_count > 0:
        insights.append(
            f"{metrics.unnecessary_replace_all_count} edit(s) use replace_all "
            "with long, specific match strings."
        )

    return tuple(insights)
