"""Pack tool call density analyzer for execution efficiency.

Analyzes tool call density and distribution in execution pack batches to measure
tool usage efficiency and parallelization patterns. Tracks tool usage intensity,
distribution across tool types, and parallel invocation patterns.

Tool call density metrics:
- Total tool calls: Aggregate tool invocations across batch
- Tool calls per task: Average tool calls per task in pack
- Tool type distribution: Count by tool type (Read/Edit/Write/Grep/Glob/Bash/Task)
- Parallel tool call blocks: Single responses with multiple tool invocations
- Tool call clustering: Sequential calls of same tool type
- Average tool calls per task: Mean tool usage per task

Quality indicators:
- Balanced tool distribution: No single tool dominates (e.g., not 90% Read)
- High parallel block ratio (>30%): Good batching of independent operations
- Moderate clustering (30-60%): Reasonable tool reuse without repetition
- Efficient tool call density (10-50 per task): Not too sparse or excessive
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_tool_call_density(records: object) -> dict[str, Any]:
    """Analyze tool call density and distribution in execution pack batches.

    Evaluates tool usage patterns, density, parallelization, and clustering
    to measure execution efficiency within packs.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_tool_calls: Total tool invocations across batch
            - task_count: Number of tasks in pack
            - read_calls: Number of Read tool calls
            - edit_calls: Number of Edit tool calls
            - write_calls: Number of Write tool calls
            - grep_calls: Number of Grep tool calls
            - glob_calls: Number of Glob tool calls
            - bash_calls: Number of Bash tool calls
            - task_calls: Number of Task tool calls (subagents)
            - parallel_tool_blocks: Responses with multiple tool calls
            - clustered_tool_calls: Sequential same-tool invocations
            - pack_title: Optional pack title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_total_tool_calls: Average tool calls per pack
            - avg_tool_calls_per_task: Average tool calls per task
            - avg_read_call_ratio: Average % of Read calls
            - avg_edit_call_ratio: Average % of Edit calls
            - avg_write_call_ratio: Average % of Write calls
            - avg_grep_call_ratio: Average % of Grep calls
            - avg_glob_call_ratio: Average % of Glob calls
            - avg_bash_call_ratio: Average % of Bash calls
            - avg_task_call_ratio: Average % of Task calls
            - avg_parallel_block_ratio: Average % parallel tool blocks
            - avg_clustering_ratio: Average % clustered tool calls
            - high_density_packs: Count of packs with >50 calls/task
            - low_density_packs: Count of packs with <10 calls/task

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_tool_calls_list: list[int | float] = []
    tool_calls_per_task_list: list[float] = []

    read_ratios: list[float] = []
    edit_ratios: list[float] = []
    write_ratios: list[float] = []
    grep_ratios: list[float] = []
    glob_ratios: list[float] = []
    bash_ratios: list[float] = []
    task_ratios: list[float] = []

    parallel_block_ratios: list[float] = []
    clustering_ratios: list[float] = []

    high_density_packs = 0  # >50 calls/task
    low_density_packs = 0   # <10 calls/task

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        total_calls = _extract_int(record.get("total_tool_calls"))
        task_count = _extract_int(record.get("task_count"))
        read_calls = _extract_int(record.get("read_calls"))
        edit_calls = _extract_int(record.get("edit_calls"))
        write_calls = _extract_int(record.get("write_calls"))
        grep_calls = _extract_int(record.get("grep_calls"))
        glob_calls = _extract_int(record.get("glob_calls"))
        bash_calls = _extract_int(record.get("bash_calls"))
        task_calls = _extract_int(record.get("task_calls"))
        parallel_blocks = _extract_int(record.get("parallel_tool_blocks"))
        clustered = _extract_int(record.get("clustered_tool_calls"))

        if total_calls is not None:
            total_tool_calls_list.append(total_calls)

            # Calculate tool calls per task
            if task_count is not None and task_count > 0:
                calls_per_task = total_calls / task_count
                tool_calls_per_task_list.append(calls_per_task)

                # Classify density
                if calls_per_task > 50:
                    high_density_packs += 1
                elif calls_per_task < 10:
                    low_density_packs += 1

            # Calculate tool type ratios
            if total_calls > 0:
                if read_calls is not None:
                    read_ratios.append(_percentage(read_calls, total_calls))
                if edit_calls is not None:
                    edit_ratios.append(_percentage(edit_calls, total_calls))
                if write_calls is not None:
                    write_ratios.append(_percentage(write_calls, total_calls))
                if grep_calls is not None:
                    grep_ratios.append(_percentage(grep_calls, total_calls))
                if glob_calls is not None:
                    glob_ratios.append(_percentage(glob_calls, total_calls))
                if bash_calls is not None:
                    bash_ratios.append(_percentage(bash_calls, total_calls))
                if task_calls is not None:
                    task_ratios.append(_percentage(task_calls, total_calls))

                # Calculate parallel and clustering ratios
                if parallel_blocks is not None:
                    parallel_block_ratios.append(
                        _percentage(parallel_blocks, total_calls)
                    )
                if clustered is not None:
                    clustering_ratios.append(_percentage(clustered, total_calls))

    # Calculate aggregate metrics
    avg_total_calls = _average(total_tool_calls_list)
    avg_per_task = _average(tool_calls_per_task_list)
    avg_read = _average(read_ratios)
    avg_edit = _average(edit_ratios)
    avg_write = _average(write_ratios)
    avg_grep = _average(grep_ratios)
    avg_glob = _average(glob_ratios)
    avg_bash = _average(bash_ratios)
    avg_task = _average(task_ratios)
    avg_parallel = _average(parallel_block_ratios)
    avg_clustering = _average(clustering_ratios)

    return {
        "total_packs": total_packs,
        "avg_total_tool_calls": avg_total_calls,
        "avg_tool_calls_per_task": avg_per_task,
        "avg_read_call_ratio": avg_read,
        "avg_edit_call_ratio": avg_edit,
        "avg_write_call_ratio": avg_write,
        "avg_grep_call_ratio": avg_grep,
        "avg_glob_call_ratio": avg_glob,
        "avg_bash_call_ratio": avg_bash,
        "avg_task_call_ratio": avg_task,
        "avg_parallel_block_ratio": avg_parallel,
        "avg_clustering_ratio": avg_clustering,
        "high_density_packs": high_density_packs,
        "low_density_packs": low_density_packs,
    }


def _extract_int(value: object) -> int | None:
    """Extract integer from value if available."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
