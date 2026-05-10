"""Pack parallel tool execution efficiency analyzer.

Analyzes parallel tool execution patterns across Claude Code execution packs
to measure parallelization efficiency, identify missed opportunities, and detect
anti-patterns where sequential tool calls could have been parallelized.

Parallel execution metrics:
- Parallel batches: Count of messages with 2+ tool calls
- Sequential batches: Count of messages with single tool calls
- Parallelization rate: Ratio of parallel to total batches
- Average tools per parallel batch: Mean tools in parallel messages
- Potential parallel opportunities: Sequential calls that could be parallel

Anti-patterns detected:
- Sequential Read calls for different files
- Sequential Grep/Glob calls that could batch
- Sequential Bash commands with no dependencies

Quality indicators:
- High parallelization rate (>40% of batches use parallel calls)
- High average tools per parallel batch (>3 tools)
- Low missed opportunities (<20% of sequential batches)
- Efficient parallel execution score (>0.7)
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_pack_parallel_tool_execution(records: object) -> dict[str, Any]:
    """Analyze parallel tool execution efficiency across execution packs.

    Evaluates how effectively agents use parallel tool calls, identifies
    missed parallelization opportunities, and detects anti-patterns.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Pack identifier
            - sessions: List of session dictionaries with:
                - session_id: Session identifier
                - messages: List of assistant message dictionaries with:
                    - message_index: Message number
                    - tool_calls: List of tool call dictionaries with:
                        - tool_name: Name of tool (Read, Write, Edit, Bash, Grep, Glob, etc.)
                        - file_path: Optional file being operated on
                        - command: Optional bash command

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - total_batches: Total tool call batches (messages with tools)
            - parallel_batches: Messages with 2+ tool calls
            - sequential_batches: Messages with single tool call
            - parallelization_rate: Percentage of batches that are parallel
            - total_tools_in_parallel: Total tools executed in parallel
            - avg_tools_per_parallel_batch: Mean tools per parallel message
            - max_tools_per_parallel_batch: Largest parallel batch
            - potential_parallel_opportunities: Sequential calls that could be parallel
            - missed_read_parallelization: Sequential Read calls for different files
            - missed_search_parallelization: Sequential Grep/Glob calls
            - missed_bash_parallelization: Sequential independent Bash calls
            - parallelization_efficiency: Score 0-1 based on actual vs potential
            - common_parallel_patterns: Most frequent tool combinations
            - example_good_parallel: Example of effective parallelization
            - example_missed_opportunity: Example of missed parallelization

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_batches = 0
    parallel_batches = 0
    sequential_batches = 0
    parallel_group_sizes: list[int] = []

    # Track parallel patterns
    parallel_patterns: Counter[tuple[str, ...]] = Counter()

    # Track missed opportunities
    potential_parallel_opportunities = 0
    missed_read_parallelization = 0
    missed_search_parallelization = 0
    missed_bash_parallelization = 0

    # Track examples
    example_good_parallel: dict[str, Any] = {}
    example_missed_opportunity: dict[str, Any] = {}

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        sessions = _get_sessions(record)
        for session in sessions:
            if not isinstance(session, Mapping):
                continue

            messages = _get_messages(session)

            # Track consecutive single-tool messages for missed opportunities
            prev_message_tools: list[dict[str, Any]] = []

            for message in messages:
                if not isinstance(message, Mapping):
                    continue

                tool_calls = _get_tool_calls(message)
                if not tool_calls:
                    prev_message_tools = []
                    continue

                # Get valid tool names (filters out malformed entries)
                valid_tools = _tool_names(tool_calls)
                if not valid_tools:
                    prev_message_tools = []
                    continue

                total_batches += 1
                num_tools = len(valid_tools)

                if num_tools == 1:
                    sequential_batches += 1

                    # Check if this could have been parallel with previous message
                    if prev_message_tools:
                        if _could_be_parallel(prev_message_tools, tool_calls):
                            potential_parallel_opportunities += 1

                            # Categorize missed opportunity
                            if _is_read_parallelization_miss(prev_message_tools, tool_calls):
                                missed_read_parallelization += 1
                                if not example_missed_opportunity:
                                    example_missed_opportunity = {
                                        "type": "sequential_reads",
                                        "prev_tools": _tool_names(prev_message_tools),
                                        "current_tools": _tool_names(tool_calls),
                                    }
                            elif _is_search_parallelization_miss(prev_message_tools, tool_calls):
                                missed_search_parallelization += 1
                                if not example_missed_opportunity:
                                    example_missed_opportunity = {
                                        "type": "sequential_searches",
                                        "prev_tools": _tool_names(prev_message_tools),
                                        "current_tools": _tool_names(tool_calls),
                                    }
                            elif _is_bash_parallelization_miss(prev_message_tools, tool_calls):
                                missed_bash_parallelization += 1
                                if not example_missed_opportunity:
                                    example_missed_opportunity = {
                                        "type": "sequential_bash",
                                        "prev_tools": _tool_names(prev_message_tools),
                                        "current_tools": _tool_names(tool_calls),
                                    }

                    prev_message_tools = tool_calls
                else:
                    # Parallel execution
                    parallel_batches += 1
                    parallel_group_sizes.append(num_tools)

                    # Track pattern
                    tool_names = tuple(sorted(_tool_names(tool_calls)))
                    parallel_patterns[tool_names] += 1

                    # Capture good example
                    if not example_good_parallel or num_tools > example_good_parallel.get("count", 0):
                        example_good_parallel = {
                            "tools": _tool_names(tool_calls),
                            "count": num_tools,
                        }

                    prev_message_tools = []

    # Calculate metrics
    parallelization_rate = _percentage(parallel_batches, total_batches)

    total_tools_in_parallel = sum(parallel_group_sizes)
    avg_tools_per_parallel_batch = _average(parallel_group_sizes)
    max_tools_per_parallel_batch = max(parallel_group_sizes) if parallel_group_sizes else 0

    # Calculate efficiency score
    parallelization_efficiency = _calculate_efficiency_score(
        parallelization_rate,
        avg_tools_per_parallel_batch,
        potential_parallel_opportunities,
        total_batches,
    )

    # Format common patterns
    common_parallel_patterns = [
        {"tools": list(pattern), "count": count}
        for pattern, count in parallel_patterns.most_common(5)
    ]

    return {
        "total_packs": total_packs,
        "total_batches": total_batches,
        "parallel_batches": parallel_batches,
        "sequential_batches": sequential_batches,
        "parallelization_rate": parallelization_rate,
        "total_tools_in_parallel": total_tools_in_parallel,
        "avg_tools_per_parallel_batch": avg_tools_per_parallel_batch,
        "max_tools_per_parallel_batch": max_tools_per_parallel_batch,
        "potential_parallel_opportunities": potential_parallel_opportunities,
        "missed_read_parallelization": missed_read_parallelization,
        "missed_search_parallelization": missed_search_parallelization,
        "missed_bash_parallelization": missed_bash_parallelization,
        "parallelization_efficiency": parallelization_efficiency,
        "common_parallel_patterns": common_parallel_patterns,
        "example_good_parallel": example_good_parallel,
        "example_missed_opportunity": example_missed_opportunity,
    }


def _get_sessions(record: Mapping[str, Any]) -> list[Any]:
    """Extract sessions list from pack record."""
    sessions = record.get("sessions")
    if isinstance(sessions, list):
        return sessions
    return []


def _get_messages(session: Mapping[str, Any]) -> list[Any]:
    """Extract messages list from session."""
    messages = session.get("messages")
    if isinstance(messages, list):
        return messages
    return []


def _get_tool_calls(message: Mapping[str, Any]) -> list[Any]:
    """Extract tool calls list from message."""
    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list):
        return tool_calls
    return []


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _tool_names(tool_calls: list[Any]) -> list[str]:
    """Extract tool names from tool call list."""
    names = []
    for call in tool_calls:
        if isinstance(call, Mapping):
            tool_name = _string(call.get("tool_name", ""))
            if tool_name:
                names.append(tool_name)
    return names


def _could_be_parallel(prev_tools: list[Any], current_tools: list[Any]) -> bool:
    """Check if consecutive single-tool messages could have been parallel.

    Returns True if the tools are independent (no data dependencies).
    """
    if not prev_tools or not current_tools:
        return False

    if len(prev_tools) != 1 or len(current_tools) != 1:
        return False

    prev_call = prev_tools[0]
    current_call = current_tools[0]

    if not isinstance(prev_call, Mapping) or not isinstance(current_call, Mapping):
        return False

    prev_tool = _string(prev_call.get("tool_name", ""))
    current_tool = _string(current_call.get("tool_name", ""))

    # Check for obvious independence
    # Read operations on different files can be parallel
    if prev_tool == "Read" and current_tool == "Read":
        prev_file = _string(prev_call.get("file_path", ""))
        current_file = _string(current_call.get("file_path", ""))
        return bool(prev_file and current_file and prev_file != current_file)

    # Grep/Glob searches can be parallel
    if prev_tool in ("Grep", "Glob") and current_tool in ("Grep", "Glob"):
        return True

    # Independent Bash commands (simple heuristic: different commands)
    if prev_tool == "Bash" and current_tool == "Bash":
        prev_cmd = _string(prev_call.get("command", ""))
        current_cmd = _string(current_call.get("command", ""))
        # Simple check: if commands don't share file paths, likely independent
        if prev_cmd and current_cmd and prev_cmd != current_cmd:
            # Avoid false positives: chained commands with && or ; are intentional
            if "&&" not in prev_cmd and ";" not in prev_cmd:
                return True

    return False


def _is_read_parallelization_miss(prev_tools: list[Any], current_tools: list[Any]) -> bool:
    """Check if this is a missed Read parallelization opportunity."""
    if len(prev_tools) != 1 or len(current_tools) != 1:
        return False

    prev_tool = _string(prev_tools[0].get("tool_name", "")) if isinstance(prev_tools[0], Mapping) else ""
    current_tool = _string(current_tools[0].get("tool_name", "")) if isinstance(current_tools[0], Mapping) else ""

    return prev_tool == "Read" and current_tool == "Read"


def _is_search_parallelization_miss(prev_tools: list[Any], current_tools: list[Any]) -> bool:
    """Check if this is a missed Grep/Glob parallelization opportunity."""
    if len(prev_tools) != 1 or len(current_tools) != 1:
        return False

    prev_tool = _string(prev_tools[0].get("tool_name", "")) if isinstance(prev_tools[0], Mapping) else ""
    current_tool = _string(current_tools[0].get("tool_name", "")) if isinstance(current_tools[0], Mapping) else ""

    return (prev_tool in ("Grep", "Glob") and current_tool in ("Grep", "Glob"))


def _is_bash_parallelization_miss(prev_tools: list[Any], current_tools: list[Any]) -> bool:
    """Check if this is a missed Bash parallelization opportunity."""
    if len(prev_tools) != 1 or len(current_tools) != 1:
        return False

    prev_tool = _string(prev_tools[0].get("tool_name", "")) if isinstance(prev_tools[0], Mapping) else ""
    current_tool = _string(current_tools[0].get("tool_name", "")) if isinstance(current_tools[0], Mapping) else ""

    return prev_tool == "Bash" and current_tool == "Bash"


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_efficiency_score(
    parallelization_rate: float,
    avg_tools_per_batch: float,
    missed_opportunities: int,
    total_batches: int,
) -> float:
    """Calculate overall parallelization efficiency score (0-1).

    Scoring components:
    - Parallelization rate (0-0.40): Percentage of batches using parallel calls
    - Average batch size (0-0.30): Quality of parallel batches
    - Missed opportunities (0-0.30): Penalty for sequential calls that could be parallel
    """
    # If no batches, score is 0
    if total_batches == 0:
        return 0.0

    # Parallelization rate component (target: >40%)
    if parallelization_rate >= 40.0:
        rate_component = 0.40
    else:
        rate_component = (parallelization_rate / 40.0) * 0.40

    # Average batch size component (target: >3 tools)
    if avg_tools_per_batch >= 3.0:
        size_component = 0.30
    else:
        size_component = (avg_tools_per_batch / 3.0) * 0.30

    # Missed opportunities penalty (target: <20% of batches)
    missed_rate = _percentage(missed_opportunities, total_batches)
    if missed_rate <= 20.0:
        opportunity_component = 0.30
    else:
        penalty = min(missed_rate - 20.0, 80.0) / 80.0
        opportunity_component = 0.30 * (1.0 - penalty)

    score = rate_component + size_component + opportunity_component
    return round(max(0.0, min(1.0, score)), 3)
