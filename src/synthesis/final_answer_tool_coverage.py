"""Final answer tool coverage analyzer for workflow reports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence


@dataclass(frozen=True)
class SessionToolCall:
    tool_type: str
    result_summary: str
    is_critical: bool  # data-gathering or verification tools


@dataclass(frozen=True)
class ToolCoverageExample:
    tool_type: str
    result_summary: str
    mentioned_in_final_answer: bool


@dataclass(frozen=True)
class ToolCoverageMetrics:
    total_tools: int
    critical_tools: int
    tools_mentioned: int
    critical_tools_mentioned: int
    coverage_rate: float
    critical_coverage_rate: float
    by_tool_type: dict[str, dict[str, int]]


@dataclass(frozen=True)
class FinalAnswerToolCoverage:
    metrics: ToolCoverageMetrics
    examples: tuple[ToolCoverageExample, ...]
    insights: tuple[str, ...]


CRITICAL_TOOL_TYPES = {
    "grep",
    "read",
    "bash",
    "webfetch",
    "websearch",
    "glob",
}

VERIFICATION_TOOL_TYPES = {
    "bash",  # when running tests
}


def analyze_final_answer_tool_coverage(
    tool_calls: Sequence[SessionToolCall],
    final_answer: str,
) -> FinalAnswerToolCoverage:
    """Measure whether final answers reference key tool results from the session."""
    _validate_tool_calls(tool_calls)
    _validate_final_answer(final_answer)

    if not tool_calls:
        metrics = ToolCoverageMetrics(
            total_tools=0,
            critical_tools=0,
            tools_mentioned=0,
            critical_tools_mentioned=0,
            coverage_rate=0.0,
            critical_coverage_rate=0.0,
            by_tool_type={},
        )
        return FinalAnswerToolCoverage(
            metrics=metrics,
            examples=(),
            insights=("No tool calls detected.",),
        )

    if not final_answer.strip():
        metrics = ToolCoverageMetrics(
            total_tools=len(tool_calls),
            critical_tools=sum(1 for t in tool_calls if t.is_critical),
            tools_mentioned=0,
            critical_tools_mentioned=0,
            coverage_rate=0.0,
            critical_coverage_rate=0.0,
            by_tool_type={},
        )
        return FinalAnswerToolCoverage(
            metrics=metrics,
            examples=(),
            insights=("No final answer provided.",),
        )

    final_answer_lower = final_answer.lower()
    by_tool_type: dict[str, dict[str, int]] = {}
    examples: list[ToolCoverageExample] = []
    mentioned_count = 0
    critical_mentioned_count = 0
    critical_count = 0

    for tool_call in tool_calls:
        tool_type = tool_call.tool_type
        if tool_type not in by_tool_type:
            by_tool_type[tool_type] = {
                "total": 0,
                "mentioned": 0,
                "omitted": 0,
            }

        by_tool_type[tool_type]["total"] += 1

        if tool_call.is_critical:
            critical_count += 1

        # Check if tool result is mentioned in final answer
        is_mentioned = _is_tool_result_mentioned(tool_call, final_answer_lower)

        if is_mentioned:
            mentioned_count += 1
            by_tool_type[tool_type]["mentioned"] += 1
            if tool_call.is_critical:
                critical_mentioned_count += 1
        else:
            by_tool_type[tool_type]["omitted"] += 1
            # Add examples of omitted critical tool results
            if tool_call.is_critical and len(examples) < 5:
                examples.append(
                    ToolCoverageExample(
                        tool_type=tool_type,
                        result_summary=tool_call.result_summary[:100],  # Truncate long results
                        mentioned_in_final_answer=False,
                    )
                )

    coverage_rate = _percentage(mentioned_count, len(tool_calls))
    critical_coverage_rate = _percentage(critical_mentioned_count, critical_count)

    metrics = ToolCoverageMetrics(
        total_tools=len(tool_calls),
        critical_tools=critical_count,
        tools_mentioned=mentioned_count,
        critical_tools_mentioned=critical_mentioned_count,
        coverage_rate=coverage_rate,
        critical_coverage_rate=critical_coverage_rate,
        by_tool_type=by_tool_type,
    )

    return FinalAnswerToolCoverage(
        metrics=metrics,
        examples=tuple(examples),
        insights=_generate_insights(metrics),
    )


def _validate_tool_calls(calls: Sequence[SessionToolCall]) -> None:
    """Validate tool call structure."""
    if not isinstance(calls, (list, tuple)):
        raise ValueError("tool_calls must be a list or tuple")

    for call in calls:
        if not isinstance(call, SessionToolCall):
            raise ValueError("tool_calls must contain SessionToolCall instances")
        if not isinstance(call.tool_type, str):
            raise ValueError("tool_type must be a string")
        if not call.tool_type.strip():
            raise ValueError("tool_type must not be empty")
        if not isinstance(call.result_summary, str):
            raise ValueError("result_summary must be a string")
        if not isinstance(call.is_critical, bool):
            raise ValueError("is_critical must be a boolean")


def _validate_final_answer(answer: str) -> None:
    """Validate final answer structure."""
    if not isinstance(answer, str):
        raise ValueError("final_answer must be a string")


def _is_tool_result_mentioned(tool_call: SessionToolCall, final_answer_lower: str) -> bool:
    """Check if tool result is mentioned in final answer."""
    if not final_answer_lower or not tool_call.result_summary:
        return False

    result_lower = tool_call.result_summary.lower()

    # Extract meaningful words from result and answer
    result_words = set(result_lower.split())
    answer_words = set(final_answer_lower.split())

    # Remove common words
    common_words = {
        "the", "a", "an", "is", "are", "was", "were", "in", "on", "at",
        "to", "for", "of", "with", "by", "i", "and", "or", "it", "that",
        "this", "these", "those", "as", "be", "been", "has", "have", "had",
    }
    meaningful_result = result_words - common_words
    meaningful_answer = answer_words - common_words

    # Check for significant word overlap (at least 2 meaningful words for short summaries, 3 for long)
    overlap = meaningful_result.intersection(meaningful_answer)
    min_overlap = 3 if len(result_lower) > 50 else 2
    if len(overlap) >= min_overlap:
        return True

    # Check for tool type mention with context
    tool_type_lower = tool_call.tool_type.lower()
    if tool_type_lower in final_answer_lower and len(overlap) >= 1:
        return True

    # Check for verification-related mentions if it's a bash tool
    if tool_call.tool_type == "bash":
        verification_keywords = ["test", "passed", "failed", "build", "verify", "check"]
        result_has_verification = any(keyword in result_lower for keyword in verification_keywords)
        answer_has_verification = any(keyword in final_answer_lower for keyword in verification_keywords)
        if result_has_verification and answer_has_verification:
            return True

    # Special handling for common action verbs
    action_verbs = {
        "read": ["read", "loaded", "examined", "checked"],
        "write": ["wrote", "created", "saved", "wrote"],
        "edit": ["edited", "updated", "modified", "changed"],
        "grep": ["found", "searched", "located"],
        "bash": ["ran", "executed", "tested"],
    }

    if tool_call.tool_type in action_verbs:
        verbs = action_verbs[tool_call.tool_type]
        if any(verb in final_answer_lower for verb in verbs) and len(overlap) >= 1:
            return True

    return False


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage with 2 decimal precision."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _generate_insights(metrics: ToolCoverageMetrics) -> tuple[str, ...]:
    """Generate human-readable insights from metrics."""
    if metrics.total_tools == 0:
        return ("No tool calls detected.",)

    insights = []

    insights.append(
        f"{metrics.tools_mentioned} of {metrics.total_tools} tool results mentioned "
        f"in final answer ({metrics.coverage_rate}%)."
    )

    if metrics.critical_tools > 0:
        insights.append(
            f"{metrics.critical_tools_mentioned} of {metrics.critical_tools} critical tool results mentioned "
            f"({metrics.critical_coverage_rate}%)."
        )

    # Flag low coverage
    if metrics.coverage_rate < 30.0 and metrics.total_tools >= 3:
        insights.append(
            "Low coverage: Final answer omits most tool findings."
        )

    if metrics.critical_coverage_rate < 50.0 and metrics.critical_tools >= 2:
        insights.append(
            "Critical findings missing: Final answer lacks key verification or data-gathering results."
        )

    # Identify tool types with worst coverage
    poor_coverage_tools = []
    for tool_type, stats in metrics.by_tool_type.items():
        if stats["total"] >= 2:  # Only consider tools used multiple times
            coverage = _percentage(stats["mentioned"], stats["total"])
            if coverage < 50.0:
                poor_coverage_tools.append((tool_type, coverage))

    if poor_coverage_tools:
        poor_coverage_tools.sort(key=lambda x: x[1])
        worst_tool, worst_rate = poor_coverage_tools[0]
        insights.append(
            f"Tool type '{worst_tool}' has lowest coverage at {worst_rate}%."
        )

    return tuple(insights)
