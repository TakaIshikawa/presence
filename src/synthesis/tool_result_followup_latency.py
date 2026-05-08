"""Tool result follow-up latency analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class ToolResultFollowupEvent:
    turn_index: int
    event_type: str
    tool_name: str
    topic: str


@dataclass(frozen=True)
class ToolResultFollowupLatencyReport:
    total_results: int
    followed_results: int
    average_followup_latency: float
    maximum_followup_latency: int
    stale_results: int
    stale_results_by_tool: tuple[tuple[str, int], ...]
    insights: tuple[str, ...]


def analyze_tool_result_followup_latency(
    events: Sequence[ToolResultFollowupEvent],
    stale_threshold: int = 2,
) -> ToolResultFollowupLatencyReport:
    if not isinstance(stale_threshold, int) or isinstance(stale_threshold, bool) or stale_threshold < 0:
        raise ValueError("stale_threshold must be a non-negative integer")
    _validate_events(events)
    result_indices = [i for i, event in enumerate(events) if event.event_type == "result"]
    latencies: list[int] = []
    stale_by_tool: dict[str, int] = {}
    consumed_action_indices: set[int] = set()

    for index in result_indices:
        result = events[index]
        followup_index = None
        for i in range(index + 1, len(events)):
            event = events[i]
            if (
                i not in consumed_action_indices
                and event.event_type == "action"
                and event.tool_name == result.tool_name
                and event.topic == result.topic
            ):
                followup_index = i
                break

        if followup_index is None:
            stale_by_tool[result.tool_name] = stale_by_tool.get(result.tool_name, 0) + 1
            continue

        consumed_action_indices.add(followup_index)
        followup = events[followup_index]
        latency = followup.turn_index - result.turn_index
        if latency > stale_threshold:
            stale_by_tool[result.tool_name] = stale_by_tool.get(result.tool_name, 0) + 1
        else:
            latencies.append(latency)

    stale_items = tuple(sorted(stale_by_tool.items()))
    average = round(sum(latencies) / len(latencies), 2) if latencies else 0.0
    return ToolResultFollowupLatencyReport(
        total_results=len(result_indices),
        followed_results=len(latencies),
        average_followup_latency=average,
        maximum_followup_latency=max(latencies) if latencies else 0,
        stale_results=sum(stale_by_tool.values()),
        stale_results_by_tool=stale_items,
        insights=_latency_insights(len(result_indices), sum(stale_by_tool.values()), average),
    )


def _validate_events(events: Sequence[ToolResultFollowupEvent]) -> None:
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")
    last_turn = -1
    for index, event in enumerate(events):
        if not isinstance(event, ToolResultFollowupEvent):
            raise ValueError(f"events[{index}] must be a ToolResultFollowupEvent")
        if event.event_type not in {"result", "action"}:
            raise ValueError("event_type must be 'result' or 'action'")
        if not isinstance(event.turn_index, int) or isinstance(event.turn_index, bool) or event.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if event.turn_index < last_turn:
            raise ValueError("events must be ordered by turn_index")
        if not isinstance(event.tool_name, str) or not event.tool_name.strip():
            raise ValueError("tool_name must be a non-empty string")
        if not isinstance(event.topic, str) or not event.topic.strip():
            raise ValueError("topic must be a non-empty string")
        last_turn = event.turn_index


def _latency_insights(total: int, stale: int, average: float) -> tuple[str, ...]:
    if total == 0:
        return ("No important tool results recorded.",)
    insights = [f"Average follow-up latency was {average:.2f} turn(s)."]
    if stale:
        insights.append(f"{stale} tool result(s) had stale or missing follow-up.")
    return tuple(insights)
