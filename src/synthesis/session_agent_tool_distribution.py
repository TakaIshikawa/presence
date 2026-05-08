"""Session agent tool distribution analyzer for workflow pattern analysis.

Analyzes the distribution of tool calls across different tools (Read, Write, Edit,
Bash, Grep, Glob, etc.) in a session. Identifies usage patterns and anomalies like
writes before reads or excessive searches without subsequent reads.

Tool distribution metrics:
- Tool call counts and percentages for each tool type
- Total tool calls
- Tool diversity
- Anomalous patterns (e.g., writes without reads, excessive grep)
"""

from __future__ import annotations

from collections import Counter
from typing import Any


def analyze_session_agent_tool_distribution(records: object) -> dict[str, Any]:
    """Analyze distribution of tool calls across different tool types.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Read, Write, Edit, Bash, etc.)
            - turn_index: Turn number when tool was called

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls
            - tool_distribution: Dict mapping tool names to counts
            - tool_percentages: Dict mapping tool names to percentages
            - anomalies: List of detected anomalous patterns
            - tool_diversity: Number of unique tools used

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    tool_counts: Counter[str] = Counter()
    first_tool: str | None = None
    has_read = False
    has_write = False
    grep_count = 0

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        tool_counts[tool_name] += 1

        if first_tool is None:
            first_tool = tool_name

        if tool_name.lower() in ("read", "glob", "grep"):
            has_read = True
        if tool_name.lower() in ("write", "edit"):
            has_write = True
        if tool_name.lower() == "grep":
            grep_count += 1

    total_calls = sum(tool_counts.values())

    # Calculate percentages
    tool_percentages = {
        tool: round((count / total_calls) * 100.0, 2) if total_calls > 0 else 0.0
        for tool, count in tool_counts.items()
    }

    # Detect anomalies
    anomalies = []
    if has_write and not has_read:
        anomalies.append("writes_without_reads")
    if first_tool and first_tool.lower() in ("write", "edit"):
        anomalies.append("write_before_read")
    if grep_count > 5 and total_calls > 0 and grep_count / total_calls > 0.5:
        anomalies.append("excessive_grep")

    return {
        "total_tool_calls": total_calls,
        "tool_distribution": dict(tool_counts),
        "tool_percentages": tool_percentages,
        "anomalies": anomalies,
        "tool_diversity": len(tool_counts),
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""
