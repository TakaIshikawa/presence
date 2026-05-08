"""Session write tool frequency analyzer for file creation patterns.

Analyzes usage patterns of the Write tool in agent sessions. Write tool
should be used for creating new files, while Edit tool is preferred for
modifying existing files. This analyzer identifies anti-patterns where
Write is overused instead of Edit.

Usage metrics:
- Write call frequency: How often Write tool is used
- Write-to-edit ratio: Balance of new file creation vs modifications
- Average file size: Mean size of files written
- Overwrites without prior read: Anti-pattern of writing without reading
- Write-then-immediate-read: Pattern of writing and immediately re-reading

Anti-pattern detection:
- Excessive Write usage for existing files (should use Edit)
- Writing files without prior Read (overwrite risk)
- Write followed by immediate Read (verification pattern or mistake)
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_write_tool_frequency(records: object) -> dict[str, Any]:
    """Analyze Write tool usage patterns in agent sessions.

    Tracks Write tool calls, compares with Edit usage, and identifies
    anti-patterns where Write is used inappropriately.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Write, Edit, Read, etc.)
            - file_path: Path to file being written/edited/read
            - file_size: Optional size of written file in bytes
            - turn_index: Turn number when tool was invoked
            - had_prior_read: Optional boolean indicating prior Read

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls analyzed
            - write_call_count: Number of Write tool calls
            - edit_call_count: Number of Edit tool calls (for comparison)
            - write_to_edit_ratio: Percentage Write/(Write+Edit)
            - avg_file_size_written: Average size of written files (bytes)
            - overwrites_without_prior_read: Write without reading first
            - write_then_immediate_read_count: Write followed by Read
            - unique_files_written: Number of unique file paths written
            - duplicate_writes_count: Files written multiple times

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    write_call_count = 0
    edit_call_count = 0

    file_sizes: list[int | float] = []
    overwrites_without_read = 0
    write_then_read_count = 0

    written_files: dict[str, int] = {}  # Track write counts per file
    read_files: set[str] = set()  # Track which files have been read

    # Track previous tool for immediate-read detection
    previous_tool: str | None = None
    previous_file: str | None = None

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        total_tool_calls += 1
        tool_lower = tool_name.lower()
        file_path = _string(record.get("file_path", ""))

        if tool_lower == "write":
            write_call_count += 1

            if file_path:
                # Track file write
                written_files[file_path] = written_files.get(file_path, 0) + 1

                # Check for overwrite without prior read
                if file_path not in read_files:
                    # Check if record indicates prior read
                    had_prior_read = record.get("had_prior_read")
                    if had_prior_read is not True:
                        overwrites_without_read += 1

            # Track file size
            file_size = _extract_file_size(record)
            if file_size is not None:
                file_sizes.append(file_size)

            # Store for immediate-read detection
            previous_tool = "write"
            previous_file = file_path

        elif tool_lower == "edit":
            edit_call_count += 1
            previous_tool = "edit"
            previous_file = file_path

        elif tool_lower == "read":
            if file_path:
                read_files.add(file_path)

                # Check for write-then-immediate-read pattern
                if previous_tool == "write" and previous_file == file_path:
                    write_then_read_count += 1

            previous_tool = "read"
            previous_file = file_path

        else:
            # Other tools break the immediate sequence
            previous_tool = None
            previous_file = None

    # Calculate metrics
    write_edit_total = write_call_count + edit_call_count
    write_to_edit_ratio = _percentage(write_call_count, write_edit_total)
    avg_file_size = _average(file_sizes)

    unique_files_written = len(written_files)
    duplicate_writes = sum(1 for count in written_files.values() if count > 1)

    return {
        "total_tool_calls": total_tool_calls,
        "write_call_count": write_call_count,
        "edit_call_count": edit_call_count,
        "write_to_edit_ratio": write_to_edit_ratio,
        "avg_file_size_written": avg_file_size,
        "overwrites_without_prior_read": overwrites_without_read,
        "write_then_immediate_read_count": write_then_read_count,
        "unique_files_written": unique_files_written,
        "duplicate_writes_count": duplicate_writes,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_file_size(record: Mapping[str, Any]) -> int | None:
    """Extract file size from record if available."""
    file_size = record.get("file_size")
    if isinstance(file_size, int) and not isinstance(file_size, bool):
        return file_size
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
