"""Session parallel tool execution pattern analyzer.

Analyzes how effectively agents execute independent tool calls in parallel within
single messages. Focuses on parameter independence detection, wasted sequencing
patterns, and correlation with session token efficiency.

Parallel execution pattern metrics:
- Multi-tool message analysis: Percentage using parallel vs sequential execution
- Average tools per parallel batch: Batch size for parallel execution
- Wasted sequencing ratio: Independent calls executed sequentially
- Parameter independence detection: Tool calls that could be parallelized
- Token efficiency correlation: Relationship between parallelization and token usage

Pattern quality indicators:
- High parallel batch percentage (>80%): Most multi-tool messages use parallelization
- Large average batch size (>3): Effective use of parallel execution
- Low wasted sequencing ratio (<10%): Few missed parallelization opportunities
- Strong token correlation: Parallel execution reduces token consumption
- High independence detection: Accurate identification of parallelizable calls

Independence detection heuristics:
- Read calls with different file paths are independent
- Grep calls with different patterns or paths are independent
- Glob calls with different patterns are independent
- Read/Grep/Glob combinations without parameter overlap are independent
- Edit/Write calls on different files are independent
- Bash commands without pipeline dependencies are independent
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping


def analyze_session_parallel_tool_execution(records: object) -> dict[str, Any]:
    """Analyze parallel tool execution patterns in agent sessions.

    Evaluates effectiveness of parallel tool execution by detecting parameter
    independence, measuring wasted sequencing, and correlating with token efficiency.

    Args:
        records: List of message dictionaries with keys:
            - message_index: Message number in session
            - tool_calls: List of tool call dicts with:
                - tool_name: Name of the tool
                - parameters: Dict of tool parameters
                - execution_mode: "parallel" or "sequential"
            - tokens_used: Optional token count for this message
            - is_parallel_block: Boolean indicating parallel tool execution

    Returns:
        Dict with:
            - total_messages: Total number of messages analyzed
            - multi_tool_messages: Messages with 2+ tool calls
            - parallel_messages: Multi-tool messages using parallel execution
            - sequential_messages: Multi-tool messages using sequential execution
            - parallel_batch_percentage: % of multi-tool messages that are parallel
            - avg_tools_per_batch: Average tools in parallel batches
            - max_tools_per_batch: Largest parallel batch observed
            - total_tool_calls: Total tool calls across all messages
            - parallel_tool_calls: Tool calls in parallel batches
            - sequential_tool_calls: Tool calls in sequential execution
            - wasted_sequencing_count: Independent calls executed sequentially
            - wasted_sequencing_ratio: % of sequential calls that could be parallel
            - independence_detection_accuracy: % of parallel calls that are truly independent
            - common_parallel_patterns: Most frequent tool combinations in parallel
            - common_wasted_patterns: Most frequent missed parallelization patterns
            - token_efficiency_correlation: Correlation coefficient (-1 to 1)
            - avg_tokens_parallel: Average tokens per parallel message
            - avg_tokens_sequential: Average tokens per sequential message

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of message dictionaries")

    if not records:
        return _empty_result()

    total_messages = 0
    multi_tool_messages = 0
    parallel_messages = 0
    sequential_messages = 0

    total_tool_calls = 0
    parallel_tool_calls = 0
    sequential_tool_calls = 0

    parallel_batch_sizes: list[int] = []
    parallel_patterns: defaultdict[tuple[str, ...], int] = defaultdict(int)
    wasted_patterns: defaultdict[tuple[str, ...], int] = defaultdict(int)

    wasted_sequencing_count = 0
    sequential_independent_count = 0

    parallel_truly_independent = 0
    parallel_potentially_dependent = 0

    # Token tracking for correlation
    parallel_message_tokens: list[int] = []
    sequential_message_tokens: list[int] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_messages += 1

        tool_calls = record.get("tool_calls")
        if not isinstance(tool_calls, list) or len(tool_calls) < 2:
            # Skip messages with 0 or 1 tool call
            if isinstance(tool_calls, list) and len(tool_calls) == 1:
                total_tool_calls += 1
                sequential_tool_calls += 1
            continue

        multi_tool_messages += 1
        call_count = len(tool_calls)
        total_tool_calls += call_count

        # Determine if this is a parallel or sequential execution
        is_parallel = _is_parallel_execution(record, tool_calls)

        tokens = _extract_number(record.get("tokens_used"))

        if is_parallel:
            parallel_messages += 1
            parallel_tool_calls += call_count
            parallel_batch_sizes.append(call_count)

            # Track tokens for correlation
            if tokens is not None and tokens > 0:
                parallel_message_tokens.append(int(tokens))

            # Extract tool names for pattern tracking
            tool_names = _extract_tool_names(tool_calls)
            if tool_names:
                pattern = tuple(sorted(tool_names))
                parallel_patterns[pattern] += 1

            # Check if parallel calls are truly independent
            independent = _check_parameter_independence(tool_calls)
            if independent:
                parallel_truly_independent += call_count
            else:
                parallel_potentially_dependent += call_count

        else:
            # Sequential execution
            sequential_messages += 1
            sequential_tool_calls += call_count

            # Track tokens for correlation
            if tokens is not None and tokens > 0:
                sequential_message_tokens.append(int(tokens))

            # Check if sequential calls could have been parallel (wasted sequencing)
            if _could_be_parallel(tool_calls):
                wasted_sequencing_count += call_count
                sequential_independent_count += call_count

                # Track wasted pattern
                tool_names = _extract_tool_names(tool_calls)
                if tool_names:
                    pattern = tuple(sorted(tool_names))
                    wasted_patterns[pattern] += 1

    # Calculate aggregate metrics
    parallel_batch_pct = _percentage(parallel_messages, multi_tool_messages)
    avg_batch_size = _average(parallel_batch_sizes)
    max_batch_size = max(parallel_batch_sizes) if parallel_batch_sizes else 0

    wasted_sequencing_ratio = _percentage(
        wasted_sequencing_count, sequential_tool_calls
    )

    total_parallel_calls = parallel_truly_independent + parallel_potentially_dependent
    independence_accuracy = _percentage(
        parallel_truly_independent, total_parallel_calls
    )

    # Format common patterns
    common_parallel = [
        {"tools": list(pattern), "count": count}
        for pattern, count in sorted(
            parallel_patterns.items(), key=lambda x: x[1], reverse=True
        )[:10]
    ]

    common_wasted = [
        {"tools": list(pattern), "count": count}
        for pattern, count in sorted(
            wasted_patterns.items(), key=lambda x: x[1], reverse=True
        )[:10]
    ]

    # Calculate token efficiency correlation
    token_correlation = _calculate_correlation(
        parallel_message_tokens, sequential_message_tokens
    )

    avg_tokens_parallel = _average(
        [float(t) for t in parallel_message_tokens]
    ) if parallel_message_tokens else 0.0

    avg_tokens_sequential = _average(
        [float(t) for t in sequential_message_tokens]
    ) if sequential_message_tokens else 0.0

    return {
        "total_messages": total_messages,
        "multi_tool_messages": multi_tool_messages,
        "parallel_messages": parallel_messages,
        "sequential_messages": sequential_messages,
        "parallel_batch_percentage": parallel_batch_pct,
        "avg_tools_per_batch": avg_batch_size,
        "max_tools_per_batch": max_batch_size,
        "total_tool_calls": total_tool_calls,
        "parallel_tool_calls": parallel_tool_calls,
        "sequential_tool_calls": sequential_tool_calls,
        "wasted_sequencing_count": wasted_sequencing_count,
        "wasted_sequencing_ratio": wasted_sequencing_ratio,
        "independence_detection_accuracy": independence_accuracy,
        "common_parallel_patterns": common_parallel,
        "common_wasted_patterns": common_wasted,
        "token_efficiency_correlation": token_correlation,
        "avg_tokens_parallel": avg_tokens_parallel,
        "avg_tokens_sequential": avg_tokens_sequential,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_messages": 0,
        "multi_tool_messages": 0,
        "parallel_messages": 0,
        "sequential_messages": 0,
        "parallel_batch_percentage": 0.0,
        "avg_tools_per_batch": 0.0,
        "max_tools_per_batch": 0,
        "total_tool_calls": 0,
        "parallel_tool_calls": 0,
        "sequential_tool_calls": 0,
        "wasted_sequencing_count": 0,
        "wasted_sequencing_ratio": 0.0,
        "independence_detection_accuracy": 0.0,
        "common_parallel_patterns": [],
        "common_wasted_patterns": [],
        "token_efficiency_correlation": 0.0,
        "avg_tokens_parallel": 0.0,
        "avg_tokens_sequential": 0.0,
    }


def _is_parallel_execution(record: Mapping[str, Any], tool_calls: list[Any]) -> bool:
    """Determine if tool calls are executed in parallel.

    Args:
        record: Message record
        tool_calls: List of tool calls

    Returns:
        True if parallel execution, False otherwise
    """
    # Check explicit is_parallel_block flag
    is_parallel_block = record.get("is_parallel_block")
    if isinstance(is_parallel_block, bool):
        return is_parallel_block

    # Check if any tool call has execution_mode set to parallel
    for call in tool_calls:
        if isinstance(call, Mapping):
            mode = call.get("execution_mode")
            if mode == "parallel":
                return True

    # Default heuristic: multiple tools in same message = parallel
    # (This matches Claude Code's actual behavior)
    return True


def _extract_tool_names(tool_calls: list[Any]) -> list[str]:
    """Extract tool names from tool calls.

    Args:
        tool_calls: List of tool call dicts

    Returns:
        List of tool names
    """
    names = []
    for call in tool_calls:
        if isinstance(call, Mapping):
            name = call.get("tool_name")
            if isinstance(name, str) and name.strip():
                names.append(name.strip())
    return names


def _check_parameter_independence(tool_calls: list[Any]) -> bool:
    """Check if tool calls have independent parameters.

    Analyzes tool parameters to determine if calls are truly independent
    and can be safely executed in parallel.

    Args:
        tool_calls: List of tool call dicts

    Returns:
        True if all calls are independent, False otherwise
    """
    if len(tool_calls) < 2:
        return True

    # Extract file paths, patterns, and other relevant parameters
    file_paths: set[str] = set()
    patterns: set[str] = set()

    for call in tool_calls:
        if not isinstance(call, Mapping):
            continue

        tool_name = call.get("tool_name", "")
        params = call.get("parameters")
        if not isinstance(params, Mapping):
            continue

        # Check for file path parameters
        for param_name in ["file_path", "path", "notebook_path"]:
            path = params.get(param_name)
            if isinstance(path, str) and path.strip():
                # If same file appears multiple times, may not be independent
                if path in file_paths:
                    # Write/Edit on same file = dependent
                    if tool_name in ["Write", "Edit", "NotebookEdit"]:
                        return False
                file_paths.add(path)

        # Check for pattern parameters
        for param_name in ["pattern", "query", "glob"]:
            pattern = params.get(param_name)
            if isinstance(pattern, str) and pattern.strip():
                patterns.add(pattern)

    # If we found overlapping file writes or edits, not independent
    # Otherwise, assume independent
    return True


def _could_be_parallel(tool_calls: list[Any]) -> bool:
    """Check if sequential tool calls could have been executed in parallel.

    Detects wasted sequencing where independent calls were executed sequentially.

    Args:
        tool_calls: List of tool call dicts

    Returns:
        True if calls could be parallel, False otherwise
    """
    if len(tool_calls) < 2:
        return False

    # Check if calls are independent
    return _check_parameter_independence(tool_calls)


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value if available.

    Args:
        value: Value to extract number from

    Returns:
        Numeric value or None
    """
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                pass
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator.

    Args:
        numerator: Numerator value
        denominator: Denominator value

    Returns:
        Percentage value (0.0-100.0)
    """
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values.

    Args:
        values: List of numeric values

    Returns:
        Average value
    """
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_correlation(
    parallel_tokens: list[int], sequential_tokens: list[int]
) -> float:
    """Calculate correlation between parallelization and token efficiency.

    A negative correlation indicates that parallel execution reduces tokens.
    A positive correlation indicates parallel execution increases tokens.

    Args:
        parallel_tokens: Token counts for parallel messages
        sequential_tokens: Token counts for sequential messages

    Returns:
        Correlation coefficient (-1.0 to 1.0), or 0.0 if cannot calculate
    """
    if not parallel_tokens or not sequential_tokens:
        return 0.0

    # Simple comparison: if parallel average < sequential average, negative correlation
    avg_parallel = sum(parallel_tokens) / len(parallel_tokens)
    avg_sequential = sum(sequential_tokens) / len(sequential_tokens)

    if avg_parallel < avg_sequential:
        # Parallel is more efficient (fewer tokens)
        # Return negative correlation proportional to difference
        diff_ratio = (avg_sequential - avg_parallel) / avg_sequential
        return round(-diff_ratio, 2)
    elif avg_parallel > avg_sequential:
        # Parallel is less efficient (more tokens)
        # Return positive correlation proportional to difference
        diff_ratio = (avg_parallel - avg_sequential) / avg_parallel
        return round(diff_ratio, 2)
    else:
        # No difference
        return 0.0
