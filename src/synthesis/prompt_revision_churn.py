"""Prompt revision churn analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Sequence


SEVERITY_STABLE = "stable"
SEVERITY_MODERATE = "moderate"
SEVERITY_HIGH = "high"


@dataclass(frozen=True)
class PromptRevision:
    """One prompt revision."""

    revision_id: str
    topic: str
    prompt: str


@dataclass(frozen=True)
class PromptRevisionChurnMetrics:
    revision_count: int
    average_token_delta: float
    average_edit_distance: float
    repeated_topic_churns: int
    max_topic_revisions: int


@dataclass(frozen=True)
class PromptRevisionChurn:
    metrics: PromptRevisionChurnMetrics
    severity: str
    repeated_topics: tuple[str, ...]
    insights: tuple[str, ...]


def analyze_prompt_revision_churn(
    revisions: Sequence[PromptRevision],
) -> PromptRevisionChurn:
    """Compute churn across ordered prompt revisions."""

    _validate_revisions(revisions)
    if not revisions:
        return PromptRevisionChurn(
            metrics=PromptRevisionChurnMetrics(0, 0.0, 0.0, 0, 0),
            severity=SEVERITY_STABLE,
            repeated_topics=(),
            insights=("No prompt revisions supplied.",),
        )

    token_deltas: list[int] = []
    edit_distances: list[float] = []
    for previous, current in zip(revisions, revisions[1:]):
        token_deltas.append(abs(len(current.prompt.split()) - len(previous.prompt.split())))
        similarity = SequenceMatcher(None, previous.prompt, current.prompt).ratio()
        edit_distances.append(1.0 - similarity)

    topic_counts: dict[str, int] = {}
    for revision in revisions:
        key = revision.topic.lower().strip()
        topic_counts[key] = topic_counts.get(key, 0) + 1

    repeated_topics = tuple(sorted(topic for topic, count in topic_counts.items() if count > 1))
    repeated_topic_churns = sum(count - 1 for count in topic_counts.values() if count > 1)
    max_topic_revisions = max(topic_counts.values()) if topic_counts else 0
    avg_delta = sum(token_deltas) / len(token_deltas) if token_deltas else 0.0
    avg_edit = sum(edit_distances) / len(edit_distances) if edit_distances else 0.0

    metrics = PromptRevisionChurnMetrics(
        revision_count=len(revisions),
        average_token_delta=round(avg_delta, 2),
        average_edit_distance=round(avg_edit, 3),
        repeated_topic_churns=repeated_topic_churns,
        max_topic_revisions=max_topic_revisions,
    )
    severity = _classify_severity(metrics)
    return PromptRevisionChurn(
        metrics=metrics,
        severity=severity,
        repeated_topics=repeated_topics,
        insights=_prompt_churn_insights(metrics, severity, repeated_topics),
    )


def _validate_revisions(revisions: Sequence[PromptRevision]) -> None:
    if not isinstance(revisions, (list, tuple)):
        raise ValueError("revisions must be a list or tuple")
    seen: set[str] = set()
    for revision in revisions:
        if not isinstance(revision, PromptRevision):
            raise ValueError("revisions must contain PromptRevision instances")
        if not revision.revision_id:
            raise ValueError("revision_id must be a non-empty string")
        if revision.revision_id in seen:
            raise ValueError("revision_id values must be unique")
        if not isinstance(revision.topic, str) or not revision.topic.strip():
            raise ValueError("topic must be a non-empty string")
        if not isinstance(revision.prompt, str):
            raise ValueError("prompt must be a string")
        seen.add(revision.revision_id)


def _classify_severity(metrics: PromptRevisionChurnMetrics) -> str:
    if (
        metrics.revision_count >= 5
        or metrics.average_edit_distance >= 0.55
        or metrics.repeated_topic_churns >= 3
    ):
        return SEVERITY_HIGH
    if (
        metrics.revision_count >= 3
        or metrics.average_edit_distance >= 0.25
        or metrics.repeated_topic_churns >= 1
    ):
        return SEVERITY_MODERATE
    return SEVERITY_STABLE


def _prompt_churn_insights(
    metrics: PromptRevisionChurnMetrics,
    severity: str,
    repeated_topics: tuple[str, ...],
) -> tuple[str, ...]:
    insights = [f"Prompt churn severity is {severity} across {metrics.revision_count} revisions."]
    if repeated_topics:
        insights.append("Repeated edits clustered on: " + ", ".join(repeated_topics) + ".")
    if metrics.average_edit_distance >= 0.55:
        insights.append("Major rewrites suggest the task framing was unstable.")
    elif metrics.average_token_delta:
        insights.append("Token deltas show prompt scope changed between revisions.")
    return tuple(insights)
