"""Session file creation ratio analyzer for file operation patterns.

Analyzes the ratio of file creation (Write tool for new files) vs file modification
(Edit tool for existing files) to identify potential over-engineering patterns where
agents create many new files instead of editing existing ones.

Key metrics:
- Creation-to-modification ratio: Ratio of new file writes to edits
- File creation count: Total new files created
- File modification count: Total edits to existing files
- High creation ratio threshold: Configurable threshold for flagging sessions
- File type distribution: Categorization of created files (test, source, config, docs)
- Average file size: Comparison of created vs modified file sizes

Indicators of over-engineering:
- High creation ratio (>0.5): Creating many new files instead of editing
- Many small new files: Potential over-abstraction
- Config/docs heavy: Creating many non-code files
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_file_creation_ratio(
    records: object,
    high_ratio_threshold: float = 0.5,
) -> dict[str, Any]:
    """Analyze file creation vs modification ratio in a session.

    Measures the ratio of Write tool calls for new files vs Edit tool calls
    for existing files to identify over-engineering patterns.

    Args:
        records: List of file operation dictionaries with keys:
            - tool_name: Name of the tool (Write, Edit)
            - file_path: Path to the file
            - is_new_file: Boolean indicating if file is being created
            - file_size: Optional size in bytes
            - turn_index: Turn number when operation occurred
        high_ratio_threshold: Threshold for flagging high creation ratio (default 0.5)

    Returns:
        Dict with:
            - total_file_operations: Total Write and Edit operations
            - write_new_file_count: Number of Write operations for new files
            - edit_count: Number of Edit operations
            - creation_to_modification_ratio: Ratio of new files to edits
            - has_high_creation_ratio: Boolean indicating if ratio exceeds threshold
            - file_type_distribution: Dict categorizing created files by type
            - avg_created_file_size: Average size of created files (bytes)
            - avg_modified_file_size: Average size of modified files (bytes)
            - write_existing_file_count: Write operations on existing files

    Raises:
        ValueError: If records is not a list or threshold is invalid
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of file operation dictionaries")
    if not isinstance(high_ratio_threshold, (int, float)) or high_ratio_threshold < 0:
        raise ValueError("high_ratio_threshold must be a non-negative number")

    write_new_file_count = 0
    write_existing_file_count = 0
    edit_count = 0

    created_file_sizes: list[int | float] = []
    modified_file_sizes: list[int | float] = []

    # File type counters
    test_files = 0
    source_files = 0
    config_files = 0
    doc_files = 0
    other_files = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        tool_lower = tool_name.lower()
        is_new_file = record.get("is_new_file") is True
        file_path = _string(record.get("file_path"))
        file_size = record.get("file_size")

        if tool_lower == "write":
            if is_new_file:
                write_new_file_count += 1

                # Categorize file type
                file_type = _categorize_file_type(file_path)
                if file_type == "test":
                    test_files += 1
                elif file_type == "source":
                    source_files += 1
                elif file_type == "config":
                    config_files += 1
                elif file_type == "docs":
                    doc_files += 1
                else:
                    other_files += 1

                # Track file size
                if isinstance(file_size, (int, float)) and not isinstance(file_size, bool):
                    created_file_sizes.append(file_size)
            else:
                write_existing_file_count += 1
                if isinstance(file_size, (int, float)) and not isinstance(file_size, bool):
                    modified_file_sizes.append(file_size)

        elif tool_lower == "edit":
            edit_count += 1
            if isinstance(file_size, (int, float)) and not isinstance(file_size, bool):
                modified_file_sizes.append(file_size)

    total_file_operations = write_new_file_count + edit_count + write_existing_file_count
    creation_to_modification_ratio = _calculate_creation_ratio(
        write_new_file_count, edit_count
    )
    has_high_creation_ratio = creation_to_modification_ratio > high_ratio_threshold

    file_type_distribution = {
        "test_files": test_files,
        "source_files": source_files,
        "config_files": config_files,
        "doc_files": doc_files,
        "other_files": other_files,
        "test_percentage": _percentage(test_files, write_new_file_count),
        "source_percentage": _percentage(source_files, write_new_file_count),
        "config_percentage": _percentage(config_files, write_new_file_count),
        "doc_percentage": _percentage(doc_files, write_new_file_count),
        "other_percentage": _percentage(other_files, write_new_file_count),
    }

    avg_created_file_size = _average(created_file_sizes)
    avg_modified_file_size = _average(modified_file_sizes)

    return {
        "total_file_operations": total_file_operations,
        "write_new_file_count": write_new_file_count,
        "edit_count": edit_count,
        "write_existing_file_count": write_existing_file_count,
        "creation_to_modification_ratio": creation_to_modification_ratio,
        "has_high_creation_ratio": has_high_creation_ratio,
        "file_type_distribution": file_type_distribution,
        "avg_created_file_size": avg_created_file_size,
        "avg_modified_file_size": avg_modified_file_size,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _categorize_file_type(file_path: str) -> str:
    """Categorize file by path into test, source, config, docs, or other.

    Args:
        file_path: Path to the file

    Returns:
        Category: "test", "source", "config", "docs", or "other"
    """
    if not file_path:
        return "other"

    path_lower = file_path.lower()

    # Test files
    if (
        "/test" in path_lower
        or path_lower.startswith("test")
        or "test_" in path_lower
        or "_test." in path_lower
        or ".test." in path_lower
        or path_lower.endswith("_test.py")
        or path_lower.endswith("_test.js")
        or path_lower.endswith("_test.ts")
        or ".spec." in path_lower
    ):
        return "test"

    # Documentation files
    if (
        path_lower.endswith(".md")
        or path_lower.endswith(".rst")
        or path_lower.endswith(".txt")
        or "/docs/" in path_lower
        or path_lower.startswith("readme")
    ):
        return "docs"

    # Config files
    if (
        path_lower.endswith(".json")
        or path_lower.endswith(".yaml")
        or path_lower.endswith(".yml")
        or path_lower.endswith(".toml")
        or path_lower.endswith(".ini")
        or path_lower.endswith(".cfg")
        or path_lower.endswith(".conf")
        or path_lower.endswith(".config")
        or path_lower.endswith("rc")
        or "config" in path_lower
        or path_lower.startswith(".")
    ):
        return "config"

    # Source files (code files)
    if (
        path_lower.endswith(".py")
        or path_lower.endswith(".js")
        or path_lower.endswith(".ts")
        or path_lower.endswith(".tsx")
        or path_lower.endswith(".jsx")
        or path_lower.endswith(".java")
        or path_lower.endswith(".go")
        or path_lower.endswith(".rs")
        or path_lower.endswith(".c")
        or path_lower.endswith(".cpp")
        or path_lower.endswith(".h")
    ):
        return "source"

    return "other"


def _calculate_creation_ratio(
    write_new_count: int, edit_count: int
) -> float:
    """Calculate creation-to-modification ratio.

    Args:
        write_new_count: Number of new file creations
        edit_count: Number of edit operations

    Returns:
        Ratio of creations to edits. Returns 0.0 if no edits (to avoid division by zero).
        Returns infinity if only creations and no edits.
    """
    if edit_count == 0:
        # If there are no edits, return a special value
        # Return 0.0 if no creations, otherwise a large number to indicate high ratio
        return 0.0 if write_new_count == 0 else float("inf")
    return round(write_new_count / edit_count, 3)


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
