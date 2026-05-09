"""Session file read redundancy analyzer.

Detects redundant file reads within a session to identify opportunities for
caching and context reuse. Distinguishes between justified re-reads (after edits)
and wasteful re-reads (exploratory redundancy).

Redundancy patterns:
- Exact duplicate reads: Same file with identical offset/limit parameters
- Post-edit verification reads: Re-reading after modifications (justified)
- Exploratory re-reads: Re-reading without intervening modifications (wasteful)
- Cache-avoidable re-reads: Reads that could be satisfied from cache

Temporal analysis:
- Average time between re-reads of same file
- Turn distance between duplicate reads
- Read clustering: Multiple reads of same file in short time window

Efficiency indicators:
- Low redundancy: Each file read once, or re-read after edits
- High redundancy: Multiple reads without intervening modifications
- Cache potential: Files read multiple times with same parameters
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_file_read_redundancy(records: object) -> dict[str, Any]:
    """Analyze file read redundancy within a session.

    Detects redundant file reads and distinguishes between justified
    (post-edit) and wasteful (exploratory) re-reads.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool
            - file_path: Path to file being read
            - offset: Optional read offset parameter
            - limit: Optional read limit parameter
            - turn_index: Turn number when tool was invoked
            - timestamp: Optional timestamp of the call

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls
            - read_call_count: Number of Read tool calls
            - unique_files_read: Number of unique files read
            - files_read_multiple_times: Number of files read more than once
            - total_rereads: Total number of re-reads
            - exact_duplicate_reads: Reads with identical offset/limit
            - post_edit_rereads: Re-reads after Edit operations (justified)
            - exploratory_rereads: Re-reads without Edit (wasteful)
            - cache_avoidable_reads: Reads that could use cache
            - redundancy_ratio: Percentage of reads that are re-reads
            - avg_turns_between_rereads: Average turn distance between re-reads
            - max_reread_count: Maximum times any file was re-read
            - most_reread_files: Top files by read count

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    read_call_count = 0

    # Track file reads with parameters
    file_reads: dict[str, list[dict[str, Any]]] = {}  # file -> [read records]
    edited_files: set[str] = set()

    # For temporal analysis
    turn_distances: list[int] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name")).lower()
        if not tool_name:
            continue

        total_tool_calls += 1
        turn_index = _int(record.get("turn_index", 0))

        if tool_name == "read":
            read_call_count += 1
            file_path = _string(record.get("file_path"))

            if not file_path:
                continue

            offset = record.get("offset")
            limit = record.get("limit")

            read_info = {
                "turn_index": turn_index,
                "offset": offset,
                "limit": limit,
                "has_params": offset is not None or limit is not None,
            }

            if file_path not in file_reads:
                file_reads[file_path] = []

            # Track turn distance from previous read
            if file_reads[file_path]:
                last_read = file_reads[file_path][-1]
                distance = turn_index - last_read["turn_index"]
                turn_distances.append(distance)

            file_reads[file_path].append(read_info)

        elif tool_name == "edit":
            file_path = _string(record.get("file_path"))
            if file_path:
                edited_files.add(file_path)

    # Calculate metrics
    unique_files_read = len(file_reads)
    files_read_multiple_times = sum(1 for reads in file_reads.values() if len(reads) > 1)
    total_rereads = sum(len(reads) - 1 for reads in file_reads.values() if len(reads) > 1)

    exact_duplicate_reads = 0
    post_edit_rereads = 0
    exploratory_rereads = 0
    cache_avoidable_reads = 0

    for file_path, reads in file_reads.items():
        if len(reads) <= 1:
            continue

        was_edited = file_path in edited_files

        for i in range(1, len(reads)):
            # Check if this is an exact duplicate
            prev_read = reads[i - 1]
            curr_read = reads[i]

            if (prev_read["offset"] == curr_read["offset"] and
                prev_read["limit"] == curr_read["limit"]):
                exact_duplicate_reads += 1

                # Could be satisfied from cache
                if curr_read["has_params"] and prev_read["has_params"]:
                    cache_avoidable_reads += 1

            # Classify re-read type
            if was_edited:
                post_edit_rereads += 1
            else:
                exploratory_rereads += 1

    # Redundancy metrics
    redundancy_ratio = _percentage(total_rereads, read_call_count)
    avg_turns_between_rereads = _average(turn_distances)

    # Find most re-read files
    max_reread_count = max((len(reads) for reads in file_reads.values()), default=0)
    most_reread_files = _get_most_reread_files(file_reads)

    return {
        "total_tool_calls": total_tool_calls,
        "read_call_count": read_call_count,
        "unique_files_read": unique_files_read,
        "files_read_multiple_times": files_read_multiple_times,
        "total_rereads": total_rereads,
        "exact_duplicate_reads": exact_duplicate_reads,
        "post_edit_rereads": post_edit_rereads,
        "exploratory_rereads": exploratory_rereads,
        "cache_avoidable_reads": cache_avoidable_reads,
        "redundancy_ratio": redundancy_ratio,
        "avg_turns_between_rereads": avg_turns_between_rereads,
        "max_reread_count": max_reread_count,
        "most_reread_files": most_reread_files,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _get_most_reread_files(file_reads: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Get top files by read count.

    Args:
        file_reads: Dict mapping file paths to read records

    Returns:
        List of dicts with file and read_count, sorted by count, limited to top 5
    """
    file_counts = [
        {"file": file, "read_count": len(reads)}
        for file, reads in file_reads.items()
        if len(reads) > 1
    ]

    # Sort by read count descending
    file_counts.sort(key=lambda x: x["read_count"], reverse=True)

    return file_counts[:5]  # Top 5
