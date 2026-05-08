"""Agent response latency analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence


ROLE_USER = "user"
ROLE_ASSISTANT = "assistant"
BUCKET_IMMEDIATE = "immediate"
BUCKET_NORMAL = "normal"
BUCKET_SLOW = "slow"
BUCKET_MISSING = "missing"
IMMEDIATE_SECONDS = 30.0
NORMAL_SECONDS = 300.0


@dataclass(frozen=True)
class SessionEvent:
    role: str
    timestamp: datetime
    turn_index: int
    content: str | None = None


@dataclass(frozen=True)
class ResponseLatencyExample:
    user_turn: int
    assistant_turn: int | None
    latency_seconds: float | None
    bucket: str
    content: str | None = None


@dataclass(frozen=True)
class AgentResponseLatencyMetrics:
    total_user_prompts: int
    responded_prompts: int
    missing_responses: int
    average_latency_seconds: float
    median_latency_seconds: float


@dataclass(frozen=True)
class AgentResponseLatency:
    metrics: AgentResponseLatencyMetrics
    latency_buckets: dict[str, int]
    examples: tuple[ResponseLatencyExample, ...]
    insights: tuple[str, ...]


def analyze_agent_response_latency(events: Sequence[SessionEvent]) -> AgentResponseLatency:
    """Bucket latency between user prompts and the next assistant response."""

    _validate_events(events)
    buckets = {
        BUCKET_IMMEDIATE: 0,
        BUCKET_NORMAL: 0,
        BUCKET_SLOW: 0,
        BUCKET_MISSING: 0,
    }
    latencies: list[float] = []
    examples: list[ResponseLatencyExample] = []

    for index, event in enumerate(events):
        if event.role != ROLE_USER:
            continue
        response = _next_assistant_before_next_user(events, index)
        if response is None:
            buckets[BUCKET_MISSING] += 1
            if len(examples) < 5:
                examples.append(
                    ResponseLatencyExample(
                        event.turn_index,
                        None,
                        None,
                        BUCKET_MISSING,
                        event.content,
                    )
                )
            continue
        latency = (response.timestamp - event.timestamp).total_seconds()
        bucket = _latency_bucket(latency)
        buckets[bucket] += 1
        latencies.append(latency)
        if bucket == BUCKET_SLOW and len(examples) < 5:
            examples.append(
                ResponseLatencyExample(
                    user_turn=event.turn_index,
                    assistant_turn=response.turn_index,
                    latency_seconds=round(latency, 2),
                    bucket=bucket,
                    content=event.content,
                )
            )

    total_user_prompts = sum(1 for event in events if event.role == ROLE_USER)
    metrics = AgentResponseLatencyMetrics(
        total_user_prompts=total_user_prompts,
        responded_prompts=len(latencies),
        missing_responses=buckets[BUCKET_MISSING],
        average_latency_seconds=(
            round(sum(latencies) / len(latencies), 2) if latencies else 0.0
        ),
        median_latency_seconds=_median_latency(latencies),
    )
    return AgentResponseLatency(
        metrics=metrics,
        latency_buckets=buckets,
        examples=tuple(examples),
        insights=_response_latency_insights(metrics, buckets),
    )


def _validate_events(events: Sequence[SessionEvent]) -> None:
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")
    last_turn = -1
    last_timestamp: datetime | None = None
    for event in events:
        if not isinstance(event, SessionEvent):
            raise ValueError("events must contain SessionEvent instances")
        if event.role not in {ROLE_USER, ROLE_ASSISTANT}:
            raise ValueError("role must be 'user' or 'assistant'")
        if not isinstance(event.timestamp, datetime):
            raise ValueError("timestamp must be a datetime")
        if (
            not isinstance(event.turn_index, int)
            or isinstance(event.turn_index, bool)
            or event.turn_index < 0
        ):
            raise ValueError("turn_index must be a non-negative integer")
        if event.turn_index <= last_turn:
            raise ValueError("events must have strictly increasing turn_index values")
        if last_timestamp is not None and event.timestamp < last_timestamp:
            raise ValueError("events must be ordered by timestamp")
        if event.content is not None and not isinstance(event.content, str):
            raise ValueError("content must be a string or None")
        if isinstance(event.content, str) and not event.content.strip():
            raise ValueError("content must be non-empty when provided")
        last_turn = event.turn_index
        last_timestamp = event.timestamp


def _next_assistant_before_next_user(
    events: Sequence[SessionEvent],
    user_index: int,
) -> SessionEvent | None:
    for later in events[user_index + 1 :]:
        if later.role == ROLE_USER:
            return None
        if later.role == ROLE_ASSISTANT:
            return later
    return None


def _latency_bucket(latency_seconds: float) -> str:
    if latency_seconds <= IMMEDIATE_SECONDS:
        return BUCKET_IMMEDIATE
    if latency_seconds <= NORMAL_SECONDS:
        return BUCKET_NORMAL
    return BUCKET_SLOW


def _median_latency(latencies: list[float]) -> float:
    if not latencies:
        return 0.0
    ordered = sorted(latencies)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return round(ordered[midpoint], 2)
    return round((ordered[midpoint - 1] + ordered[midpoint]) / 2, 2)


def _response_latency_insights(
    metrics: AgentResponseLatencyMetrics,
    buckets: dict[str, int],
) -> tuple[str, ...]:
    if metrics.total_user_prompts == 0:
        return ("No user prompts supplied.",)
    insights = [
        f"Responded to {metrics.responded_prompts} of {metrics.total_user_prompts} user prompts."
    ]
    if buckets[BUCKET_SLOW]:
        insights.append(f"{buckets[BUCKET_SLOW]} responses were slow.")
    if buckets[BUCKET_MISSING]:
        insights.append(f"{buckets[BUCKET_MISSING]} user prompts had no assistant response.")
    return tuple(insights)
