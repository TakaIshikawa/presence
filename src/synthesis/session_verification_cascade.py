"""Session verification failure cascade analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


EVENT_VERIFICATION = "verification"
EVENT_FILE_MODIFICATION = "file_modification"

STATUS_FAIL = "fail"
STATUS_PASS = "pass"


@dataclass(frozen=True)
class VerificationCascadeEvent:
    """Event in a verification cascade sequence."""

    event_type: str
    turn_index: int
    status: str = ""
    command: str = ""
    error_signature: str = ""
    modified_files: tuple[str, ...] = ()


@dataclass(frozen=True)
class CascadeDetection:
    """Details of a detected verification cascade."""

    failure_signature: str
    failure_count: int
    first_turn_index: int
    last_turn_index: int
    affected_files: tuple[str, ...]
    repair_effectiveness: float
    cascade_detected: bool


@dataclass(frozen=True)
class SessionVerificationCascadeMetrics:
    """Aggregate metrics for verification cascades."""

    total_verifications: int
    cascading_failures: int
    cascade_count: int
    average_cascade_length: float
    average_repair_effectiveness: float


@dataclass(frozen=True)
class SessionVerificationCascadeAnalysis:
    """Complete analysis of verification cascades in a session."""

    metrics: SessionVerificationCascadeMetrics
    cascades: tuple[CascadeDetection, ...]
    insights: tuple[str, ...]


def analyze_session_verification_cascade(
    events: Sequence[VerificationCascadeEvent],
) -> SessionVerificationCascadeAnalysis:
    """Detect when verification failures cascade without effective repairs."""

    _validate_events(events)

    if not events:
        return SessionVerificationCascadeAnalysis(
            metrics=SessionVerificationCascadeMetrics(0, 0, 0, 0.0, 0.0),
            cascades=(),
            insights=("No events provided.",),
        )

    # Group verification failures by error signature
    signature_failures: dict[str, list[tuple[int, VerificationCascadeEvent]]] = {}
    for index, event in enumerate(events):
        if event.event_type == EVENT_VERIFICATION and event.status == STATUS_FAIL:
            sig = _normalize_signature(event.error_signature, event.command)
            if sig not in signature_failures:
                signature_failures[sig] = []
            signature_failures[sig].append((index, event))

    # Detect cascades for each signature
    cascades: list[CascadeDetection] = []
    total_verifications = sum(1 for e in events if e.event_type == EVENT_VERIFICATION)
    cascading_failures = 0

    for signature, failures in signature_failures.items():
        if len(failures) < 3:
            continue

        # Check if these failures form a cascade (consecutive failures without effective repair)
        cascade_groups = _group_into_cascades(failures, events)

        for cascade_failures in cascade_groups:
            if len(cascade_failures) < 3:
                continue

            first_idx, first_event = cascade_failures[0]
            last_idx, last_event = cascade_failures[-1]

            # Extract affected files from error signature and command
            affected_files = _extract_affected_files(cascade_failures)

            # Calculate repair effectiveness
            effectiveness = _calculate_repair_effectiveness(
                first_idx, last_idx, affected_files, events
            )

            cascades.append(
                CascadeDetection(
                    failure_signature=signature,
                    failure_count=len(cascade_failures),
                    first_turn_index=first_event.turn_index,
                    last_turn_index=last_event.turn_index,
                    affected_files=affected_files,
                    repair_effectiveness=effectiveness,
                    cascade_detected=True,
                )
            )
            cascading_failures += len(cascade_failures)

    # Calculate metrics
    avg_length = (
        sum(c.failure_count for c in cascades) / len(cascades) if cascades else 0.0
    )
    avg_effectiveness = (
        sum(c.repair_effectiveness for c in cascades) / len(cascades) if cascades else 0.0
    )

    metrics = SessionVerificationCascadeMetrics(
        total_verifications=total_verifications,
        cascading_failures=cascading_failures,
        cascade_count=len(cascades),
        average_cascade_length=round(avg_length, 2),
        average_repair_effectiveness=round(avg_effectiveness, 3),
    )

    cascades_tuple = tuple(cascades)
    return SessionVerificationCascadeAnalysis(
        metrics=metrics,
        cascades=cascades_tuple,
        insights=_generate_insights(metrics, cascades_tuple),
    )


def _validate_events(events: Sequence[VerificationCascadeEvent]) -> None:
    """Validate event sequence structure and content."""
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")

    last_turn = -1
    for index, event in enumerate(events):
        if not isinstance(event, VerificationCascadeEvent):
            raise ValueError("events must contain VerificationCascadeEvent instances")

        if event.event_type not in {EVENT_VERIFICATION, EVENT_FILE_MODIFICATION}:
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

        if event.event_type == EVENT_VERIFICATION:
            if event.status not in {STATUS_FAIL, STATUS_PASS}:
                raise ValueError(
                    f"verification event at index {index} must have fail or pass status"
                )
            if not isinstance(event.command, str) or not event.command.strip():
                raise ValueError(
                    f"verification event at index {index} must have a non-empty command"
                )

        if event.event_type == EVENT_FILE_MODIFICATION:
            if not isinstance(event.modified_files, tuple):
                raise ValueError(
                    f"file_modification event at index {index} must have tuple modified_files"
                )


def _normalize_signature(error_signature: str, command: str) -> str:
    """Normalize error signature for grouping similar failures."""
    if error_signature and error_signature.strip():
        # Use error signature if provided
        return " ".join(error_signature.strip().split())
    # Fall back to command as signature
    return " ".join(command.strip().split())


def _group_into_cascades(
    failures: list[tuple[int, VerificationCascadeEvent]],
    all_events: Sequence[VerificationCascadeEvent],
) -> list[list[tuple[int, VerificationCascadeEvent]]]:
    """Group failures into cascade sequences."""
    if len(failures) < 3:
        return []

    cascades: list[list[tuple[int, VerificationCascadeEvent]]] = []
    current_cascade: list[tuple[int, VerificationCascadeEvent]] = [failures[0]]

    for i in range(1, len(failures)):
        prev_idx, _ = failures[i - 1]
        curr_idx, _ = failures[i]

        # Check if there was a passing verification between these failures
        has_pass_between = any(
            e.event_type == EVENT_VERIFICATION and e.status == STATUS_PASS
            for e in all_events[prev_idx + 1 : curr_idx]
        )

        if has_pass_between:
            # Break cascade, start new one
            if len(current_cascade) >= 3:
                cascades.append(current_cascade)
            current_cascade = [failures[i]]
        else:
            current_cascade.append(failures[i])

    # Add final cascade if it qualifies
    if len(current_cascade) >= 3:
        cascades.append(current_cascade)

    return cascades


def _extract_affected_files(
    failures: list[tuple[int, VerificationCascadeEvent]],
) -> tuple[str, ...]:
    """Extract file paths mentioned in error signatures."""
    files: set[str] = set()

    for _, event in failures:
        # Parse file paths from error signature
        if event.error_signature:
            # Common patterns: "file.py:123", "at file.py line 123", "File 'file.py'"
            parts = event.error_signature.split()
            for part in parts:
                # Remove common delimiters and quotes
                cleaned = part.strip("'\"(),:")
                if not cleaned:
                    continue

                # Look for file-like patterns
                # Check for file extensions or path separators
                has_extension = any(
                    cleaned.endswith(ext) or f"{ext}:" in cleaned
                    for ext in (".py", ".js", ".ts", ".tsx", ".jsx", ".java", ".go", ".rs")
                )
                has_path_sep = "/" in cleaned or "\\" in cleaned

                if has_extension or has_path_sep:
                    # Extract just the file path, remove line numbers
                    file_path = cleaned.split(":")[0] if ":" in cleaned else cleaned
                    # Remove trailing commas or other punctuation
                    file_path = file_path.rstrip(",.;")
                    if file_path:
                        files.add(file_path)

    return tuple(sorted(files))


def _calculate_repair_effectiveness(
    first_failure_idx: int,
    last_failure_idx: int,
    affected_files: tuple[str, ...],
    events: Sequence[VerificationCascadeEvent],
) -> float:
    """Calculate how effective repairs were between cascade failures."""
    if not affected_files:
        # No clear affected files, can't judge effectiveness
        return 0.0

    # Count file modifications between first and last failure
    modifications_between = [
        e
        for e in events[first_failure_idx + 1 : last_failure_idx]
        if e.event_type == EVENT_FILE_MODIFICATION
    ]

    if not modifications_between:
        # No modifications attempted
        return 0.0

    # Count modification events (not individual files) that target affected files
    relevant_modification_events = 0
    total_modification_events = len(modifications_between)

    for mod_event in modifications_between:
        # Check if this modification event touched any affected file
        has_relevant_file = any(
            any(
                modified_file == af or modified_file in af or af in modified_file
                for af in affected_files
            )
            for modified_file in mod_event.modified_files
        )
        if has_relevant_file:
            relevant_modification_events += 1

    if total_modification_events == 0:
        return 0.0

    # Effectiveness is the ratio of relevant modification events to total
    return relevant_modification_events / total_modification_events


def _generate_insights(
    metrics: SessionVerificationCascadeMetrics,
    cascades: tuple[CascadeDetection, ...],
) -> tuple[str, ...]:
    """Generate human-readable insights about cascades."""
    if metrics.total_verifications == 0:
        return ("No verification events found.",)

    if metrics.cascade_count == 0:
        return ("No verification cascades detected.",)

    insights = [
        f"Detected {metrics.cascade_count} verification cascade(s) "
        f"affecting {metrics.cascading_failures} failed verification attempts."
    ]

    if metrics.average_cascade_length > 0:
        insights.append(
            f"Average cascade length: {metrics.average_cascade_length:.1f} repeated failures."
        )

    if metrics.average_repair_effectiveness < 0.3:
        insights.append(
            f"Low repair effectiveness ({metrics.average_repair_effectiveness:.1%}) "
            "suggests repairs are not targeting root causes."
        )
    elif metrics.average_repair_effectiveness < 0.7:
        insights.append(
            f"Moderate repair effectiveness ({metrics.average_repair_effectiveness:.1%}) "
            "indicates some relevant fixes but room for improvement."
        )

    # Highlight worst cascades
    worst_cascades = sorted(cascades, key=lambda c: c.failure_count, reverse=True)[:2]
    if worst_cascades and worst_cascades[0].failure_count >= 5:
        insights.append(
            f"Longest cascade had {worst_cascades[0].failure_count} failures "
            f"(turns {worst_cascades[0].first_turn_index}-{worst_cascades[0].last_turn_index})."
        )

    return tuple(insights)
