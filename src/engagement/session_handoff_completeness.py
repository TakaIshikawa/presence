"""Session handoff completeness analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class SessionHandoff:
    objective: str = ""
    changed_files: Sequence[str] = ()
    verification_status: str = ""
    blockers: Sequence[str] = ()
    next_steps: Sequence[str] = ()
    risk_notes: Sequence[str] = ()


@dataclass(frozen=True)
class HandoffCompletenessMetrics:
    completeness_score: float
    present_sections: int
    missing_sections: int


@dataclass(frozen=True)
class SessionHandoffCompleteness:
    metrics: HandoffCompletenessMetrics
    gap_labels: tuple[str, ...]
    quality: str
    insights: tuple[str, ...]


def analyze_session_handoff_completeness(
    handoff: SessionHandoff,
) -> SessionHandoffCompleteness:
    """Score whether a handoff has enough context for the next agent."""

    if handoff is None:
        handoff = SessionHandoff()
    _validate_handoff(handoff)

    checks = {
        "missing_objective": bool(handoff.objective.strip()),
        "missing_changed_files": bool(handoff.changed_files),
        "missing_verification": bool(handoff.verification_status.strip()),
        "missing_blockers": bool(handoff.blockers),
        "missing_next_steps": bool(handoff.next_steps),
        "missing_risk_notes": bool(handoff.risk_notes),
    }
    gaps = tuple(label for label, present in checks.items() if not present)
    present = len(checks) - len(gaps)
    score = round(present / len(checks), 3)
    quality = "complete" if score >= 0.85 else "partial" if score >= 0.5 else "incomplete"
    metrics = HandoffCompletenessMetrics(score, present, len(gaps))
    return SessionHandoffCompleteness(
        metrics=metrics,
        gap_labels=gaps,
        quality=quality,
        insights=_handoff_insights(gaps),
    )


def _validate_handoff(handoff: SessionHandoff) -> None:
    if not isinstance(handoff, SessionHandoff):
        raise ValueError("handoff must be a SessionHandoff instance")
    if not isinstance(handoff.objective, str):
        raise ValueError("objective must be a string")
    if not isinstance(handoff.verification_status, str):
        raise ValueError("verification_status must be a string")
    for name in ("changed_files", "blockers", "next_steps", "risk_notes"):
        value = getattr(handoff, name)
        if not isinstance(value, (list, tuple)):
            raise ValueError(f"{name} must be a list or tuple")
        if any(not isinstance(item, str) for item in value):
            raise ValueError(f"{name} must contain only strings")


def _handoff_insights(gaps: tuple[str, ...]) -> tuple[str, ...]:
    if not gaps:
        return ("Handoff contains objective, files, verification, blockers, next steps, and risks.",)
    priority = {
        "missing_objective": "Add the handoff objective so the next agent knows the target.",
        "missing_verification": "Add verification status before handoff.",
        "missing_next_steps": "Add concrete next steps for continuation.",
        "missing_changed_files": "List changed files to reduce rediscovery.",
        "missing_blockers": "State blockers explicitly, even if there are none.",
        "missing_risk_notes": "Capture risk notes for follow-up review.",
    }
    return tuple(priority[gap] for gap in gaps[:3])
