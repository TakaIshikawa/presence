"""Session tool result acknowledgement analyzer for workflow reports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence


TOOL_TYPES = {
    "read",
    "write",
    "edit",
    "grep",
    "glob",
    "bash",
    "webfetch",
    "websearch",
    "task",
}


@dataclass(frozen=True)
class ToolCall:
    turn_index: int
    tool_type: str
    tool_args: dict[str, Any]
    result_summary: str


@dataclass(frozen=True)
class AgentResponse:
    turn_index: int
    content: str


@dataclass(frozen=True)
class AcknowledgementExample:
    tool_call_turn: int
    tool_type: str
    agent_response_turn: int | None
    acknowledged: bool
    repeated_call: bool


@dataclass(frozen=True)
class ToolAcknowledgementMetrics:
    total_tool_calls: int
    acknowledged_calls: int
    silent_drops: int
    repeated_calls: int
    acknowledgement_rate: float
    repeated_call_rate: float
    by_tool_type: dict[str, dict[str, int]]


@dataclass(frozen=True)
class SessionToolResultAcknowledgement:
    metrics: ToolAcknowledgementMetrics
    examples: tuple[AcknowledgementExample, ...]
    insights: tuple[str, ...]


def analyze_session_tool_result_acknowledgement(
    tool_calls: Sequence[ToolCall],
    agent_responses: Sequence[AgentResponse],
) -> SessionToolResultAcknowledgement:
    """Detect when agents fail to acknowledge or act on tool results."""
    _validate_tool_calls(tool_calls)
    _validate_agent_responses(agent_responses)

    if not tool_calls:
        metrics = ToolAcknowledgementMetrics(
            total_tool_calls=0,
            acknowledged_calls=0,
            silent_drops=0,
            repeated_calls=0,
            acknowledgement_rate=0.0,
            repeated_call_rate=0.0,
            by_tool_type={},
        )
        return SessionToolResultAcknowledgement(
            metrics=metrics,
            examples=(),
            insights=("No tool calls detected.",),
        )

    by_tool_type: dict[str, dict[str, int]] = {}
    examples: list[AcknowledgementExample] = []
    acknowledged_count = 0
    silent_drops = 0
    repeated_count = 0

    # Track previous tool calls for repeat detection
    previous_calls: dict[str, ToolCall] = {}

    for index, tool_call in enumerate(tool_calls):
        tool_type = tool_call.tool_type
        if tool_type not in by_tool_type:
            by_tool_type[tool_type] = {
                "total": 0,
                "acknowledged": 0,
                "silent_drops": 0,
                "repeated": 0,
            }

        by_tool_type[tool_type]["total"] += 1

        # Check for repeated identical calls
        call_signature = _tool_signature(tool_call)
        is_repeated = False
        if call_signature in previous_calls:
            prev_call = previous_calls[call_signature]
            # Consider it repeated if within recent calls (last 5 turns)
            if tool_call.turn_index - prev_call.turn_index <= 5:
                is_repeated = True
                repeated_count += 1
                by_tool_type[tool_type]["repeated"] += 1

        previous_calls[call_signature] = tool_call

        # Find next agent response
        agent_response = _find_next_agent_response(tool_call, agent_responses)

        if agent_response is None:
            silent_drops += 1
            by_tool_type[tool_type]["silent_drops"] += 1
            if len(examples) < 5:
                examples.append(
                    AcknowledgementExample(
                        tool_call_turn=tool_call.turn_index,
                        tool_type=tool_type,
                        agent_response_turn=None,
                        acknowledged=False,
                        repeated_call=is_repeated,
                    )
                )
        else:
            # Check if agent acknowledges the result
            if _acknowledges_tool_result(tool_call, agent_response):
                acknowledged_count += 1
                by_tool_type[tool_type]["acknowledged"] += 1
            else:
                silent_drops += 1
                by_tool_type[tool_type]["silent_drops"] += 1
                if len(examples) < 5:
                    examples.append(
                        AcknowledgementExample(
                            tool_call_turn=tool_call.turn_index,
                            tool_type=tool_type,
                            agent_response_turn=agent_response.turn_index,
                            acknowledged=False,
                            repeated_call=is_repeated,
                        )
                    )

    metrics = ToolAcknowledgementMetrics(
        total_tool_calls=len(tool_calls),
        acknowledged_calls=acknowledged_count,
        silent_drops=silent_drops,
        repeated_calls=repeated_count,
        acknowledgement_rate=_percentage(acknowledged_count, len(tool_calls)),
        repeated_call_rate=_percentage(repeated_count, len(tool_calls)),
        by_tool_type=by_tool_type,
    )

    return SessionToolResultAcknowledgement(
        metrics=metrics,
        examples=tuple(examples),
        insights=_generate_insights(metrics),
    )


def _validate_tool_calls(calls: Sequence[ToolCall]) -> None:
    """Validate tool call structure and ordering."""
    if not isinstance(calls, (list, tuple)):
        raise ValueError("tool_calls must be a list or tuple")

    last_turn = -1
    for call in calls:
        if not isinstance(call, ToolCall):
            raise ValueError("tool_calls must contain ToolCall instances")
        if not isinstance(call.turn_index, int) or isinstance(call.turn_index, bool):
            raise ValueError("turn_index must be an integer")
        if call.turn_index < 0:
            raise ValueError("turn_index must be non-negative")
        if not isinstance(call.tool_type, str):
            raise ValueError("tool_type must be a string")
        if not call.tool_type.strip():
            raise ValueError("tool_type must not be empty")
        if not isinstance(call.tool_args, dict):
            raise ValueError("tool_args must be a dict")
        if not isinstance(call.result_summary, str):
            raise ValueError("result_summary must be a string")

        if call.turn_index <= last_turn:
            raise ValueError("tool_calls must have strictly increasing turn_index")

        last_turn = call.turn_index


def _validate_agent_responses(responses: Sequence[AgentResponse]) -> None:
    """Validate agent response structure and ordering."""
    if not isinstance(responses, (list, tuple)):
        raise ValueError("agent_responses must be a list or tuple")

    last_turn = -1
    for response in responses:
        if not isinstance(response, AgentResponse):
            raise ValueError("agent_responses must contain AgentResponse instances")
        if not isinstance(response.turn_index, int) or isinstance(response.turn_index, bool):
            raise ValueError("turn_index must be an integer")
        if response.turn_index < 0:
            raise ValueError("turn_index must be non-negative")
        if not isinstance(response.content, str):
            raise ValueError("content must be a string")
        if not response.content.strip():
            raise ValueError("content must not be empty")

        if response.turn_index <= last_turn:
            raise ValueError("agent_responses must have strictly increasing turn_index")

        last_turn = response.turn_index


def _tool_signature(tool_call: ToolCall) -> str:
    """Generate signature for tool call to detect duplicates."""
    # Create a simple signature from tool type and key args
    tool_type = tool_call.tool_type
    args_str = ""

    if tool_type == "read" and "file_path" in tool_call.tool_args:
        args_str = tool_call.tool_args["file_path"]
    elif tool_type in ("write", "edit") and "file_path" in tool_call.tool_args:
        args_str = tool_call.tool_args["file_path"]
    elif tool_type == "bash" and "command" in tool_call.tool_args:
        args_str = tool_call.tool_args["command"]
    elif tool_type == "grep" and "pattern" in tool_call.tool_args:
        args_str = tool_call.tool_args["pattern"]
    elif tool_type == "glob" and "pattern" in tool_call.tool_args:
        args_str = tool_call.tool_args["pattern"]

    return f"{tool_type}:{args_str}"


def _find_next_agent_response(
    tool_call: ToolCall,
    agent_responses: Sequence[AgentResponse],
) -> AgentResponse | None:
    """Find the first agent response after the tool call."""
    for response in agent_responses:
        if response.turn_index > tool_call.turn_index:
            return response
    return None


def _acknowledges_tool_result(tool_call: ToolCall, agent_response: AgentResponse) -> bool:
    """Check if agent response acknowledges or references the tool result."""
    content_lower = agent_response.content.lower()
    tool_type = tool_call.tool_type.lower()
    result_lower = tool_call.result_summary.lower()

    # Check if tool type is mentioned
    if tool_type in content_lower:
        return True

    # Check if result content is referenced
    if result_lower and len(result_lower) > 10:
        # Look for significant substring matches
        result_words = set(result_lower.split())
        content_words = set(content_lower.split())
        overlap = result_words.intersection(content_words)
        if len(overlap) >= 3:  # At least 3 words overlap
            return True

    # Check for tool-specific acknowledgement patterns
    acknowledgement_keywords = [
        "found",
        "shows",
        "indicates",
        "reveals",
        "contains",
        "output",
        "result",
        "returned",
        "see",
        "looks like",
        "passed",
        "failed",
        "error",
        "success",
    ]

    return any(keyword in content_lower for keyword in acknowledgement_keywords)


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage with 2 decimal precision."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _generate_insights(metrics: ToolAcknowledgementMetrics) -> tuple[str, ...]:
    """Generate human-readable insights from metrics."""
    if metrics.total_tool_calls == 0:
        return ("No tool calls detected.",)

    insights = []

    insights.append(
        f"{metrics.acknowledged_calls} of {metrics.total_tool_calls} tool calls acknowledged "
        f"({metrics.acknowledgement_rate}%)."
    )

    if metrics.silent_drops > 0:
        insights.append(
            f"{metrics.silent_drops} tool results silently dropped or ignored "
            f"({_percentage(metrics.silent_drops, metrics.total_tool_calls)}%)."
        )

    if metrics.repeated_calls > 0:
        insights.append(
            f"{metrics.repeated_calls} repeated identical tool calls detected "
            f"({metrics.repeated_call_rate}% repeat rate)."
        )

    # Find tool types with worst acknowledgement rates
    poor_ack_tools = []
    for tool_type, stats in metrics.by_tool_type.items():
        if stats["total"] >= 3:  # Only consider tools used multiple times
            ack_rate = _percentage(stats["acknowledged"], stats["total"])
            if ack_rate < 50.0:
                poor_ack_tools.append((tool_type, ack_rate))

    if poor_ack_tools:
        poor_ack_tools.sort(key=lambda x: x[1])
        worst_tool, worst_rate = poor_ack_tools[0]
        insights.append(
            f"Tool type '{worst_tool}' has lowest acknowledgement rate at {worst_rate}%."
        )

    return tuple(insights)
