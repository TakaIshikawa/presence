"""Pack file creation ratio analyzer for file operation patterns.

Analyzes execution packs to measure the ratio of file creations vs edits across
all sessions in a pack. Detects antipatterns like high creation ratios, Write
operations without prior Read, and creation of unrequested documentation files.

Pack file operation metrics:
- Total file operations: Write + Edit across all pack sessions
- New file creation count: Write tool on non-existent files
- File overwrite count: Write tool on existing files without prior Read
- Edit count: Edit tool operations
- Creation ratio: Percentage of new file creations vs total operations
- File creation clustering: Whether new files concentrated in specific sessions
- expectedFiles alignment: Created files compared against pack expectedFiles

Antipattern detection:
- High creation ratio (>40%): Over-engineering or ignoring existing files
- Write-without-Read: Overwriting files without reading them first
- Documentation antipattern: Creating README/docs files not explicitly requested
- Unexpected file creations: Files created but not in expectedFiles list

Quality indicators:
- Low creation ratio (<20%): Preference for editing existing files
- High edit-to-create ratio (>4:1): Strong preference for modifying existing code
- Low overwrite rate (<5%): Reading files before overwriting
- High expectedFiles match (>90%): Created files align with expectations
- Low documentation creation (<10%): Only creating docs when requested
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_file_creation_ratio(records: object) -> dict[str, Any]:
    """Analyze file creation vs edit ratio across execution pack sessions.

    Measures file operation patterns and detects antipatterns in file creation.

    Args:
        records: List of pack file operation dictionaries with keys:
            - pack_id: Execution pack identifier
            - session_id: Session within pack
            - tool_name: Name of tool (Write, Edit)
            - file_path: Path to the file operated on
            - is_new_file: Boolean indicating new file creation
            - had_prior_read: Boolean indicating Read before Write
            - is_documentation: Boolean indicating doc/README file
            - expected_files: List of files expected in pack
            - turn_index: Turn number when operation occurred

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - total_file_operations: Total Write + Edit operations
            - new_file_creations: Count of Write on new files
            - file_overwrites: Count of Write on existing files
            - file_edits: Count of Edit operations
            - creation_ratio: Percentage of new file creations
            - overwrite_ratio: Percentage of overwrites without prior Read
            - edit_ratio: Percentage of edit operations
            - avg_creation_ratio_per_pack: Average creation ratio per pack
            - high_creation_ratio_packs: Packs with >40% creation ratio
            - write_without_read_count: Overwrites without prior Read
            - documentation_creations: Count of README/doc file creations
            - unexpected_file_creations: Files created not in expectedFiles
            - expected_files_match_rate: % created files in expectedFiles
            - creation_clustering_detected: Packs with concentrated creations
            - avg_files_per_session: Average file ops per session

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack file operation dictionaries")

    # Pack-level tracking
    pack_data: dict[str, dict[str, Any]] = {}

    # Global counters
    total_operations = 0
    new_file_creations = 0
    file_overwrites = 0
    file_edits = 0
    write_without_read_count = 0
    documentation_creations = 0
    unexpected_file_creations = 0
    total_created_files = 0
    total_expected_matches = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or "default_pack"
        session_id = _string(record.get("session_id")) or "default_session"
        tool_name = _string(record.get("tool_name"))
        file_path = _normalize_path(_string(record.get("file_path")))
        is_new_file = record.get("is_new_file") is True
        had_prior_read = record.get("had_prior_read") is True
        is_documentation = record.get("is_documentation") is True
        expected_files = _normalize_files(record.get("expected_files"))

        # Initialize pack data
        if pack_id not in pack_data:
            pack_data[pack_id] = {
                "sessions": set(),
                "operations": 0,
                "new_files": 0,
                "overwrites": 0,
                "edits": 0,
                "expected_files": set(),
                "created_files": set(),
                "session_file_counts": {},
            }

        pack_info = pack_data[pack_id]
        pack_info["sessions"].add(session_id)

        # Update expected files set (aggregate from all records)
        if expected_files:
            pack_info["expected_files"].update(expected_files)

        # Track session-level file operations
        if session_id not in pack_info["session_file_counts"]:
            pack_info["session_file_counts"][session_id] = 0
        pack_info["session_file_counts"][session_id] += 1

        # Process tool operations
        if not tool_name:
            continue

        tool_lower = tool_name.lower()

        if tool_lower == "write":
            total_operations += 1
            pack_info["operations"] += 1

            if is_new_file:
                new_file_creations += 1
                pack_info["new_files"] += 1
                pack_info["created_files"].add(file_path)
                total_created_files += 1

                # Check if in expected files
                if expected_files and file_path in pack_info["expected_files"]:
                    total_expected_matches += 1
                elif expected_files:
                    unexpected_file_creations += 1

                # Check for documentation antipattern
                if is_documentation:
                    documentation_creations += 1
            else:
                # Overwrite existing file
                file_overwrites += 1
                pack_info["overwrites"] += 1

                # Check if had prior Read
                if not had_prior_read:
                    write_without_read_count += 1

        elif tool_lower == "edit":
            total_operations += 1
            pack_info["operations"] += 1
            file_edits += 1
            pack_info["edits"] += 1

    # Calculate pack-level metrics
    total_packs = len(pack_data)
    high_creation_ratio_packs = 0
    creation_clustering_detected = 0
    creation_ratios: list[float] = []
    files_per_session: list[float] = []

    for pack_info in pack_data.values():
        pack_ops = pack_info["operations"]
        pack_new_files = pack_info["new_files"]

        if pack_ops > 0:
            pack_creation_ratio = _percentage(pack_new_files, pack_ops)
            creation_ratios.append(pack_creation_ratio)

            # High creation ratio antipattern
            if pack_creation_ratio > 40.0:
                high_creation_ratio_packs += 1

        # Check for creation clustering
        session_counts = pack_info["session_file_counts"]
        if len(session_counts) > 1:
            max_session_files = max(session_counts.values())
            total_session_files = sum(session_counts.values())

            # If >70% of operations in a single session, it's clustered
            if max_session_files / total_session_files > 0.7:
                creation_clustering_detected += 1

        # Calculate files per session
        if len(pack_info["sessions"]) > 0:
            avg_files = pack_ops / len(pack_info["sessions"])
            files_per_session.append(avg_files)

    # Calculate aggregate metrics
    creation_ratio = _percentage(new_file_creations, total_operations)
    overwrite_ratio = _percentage(file_overwrites, total_operations)
    edit_ratio = _percentage(file_edits, total_operations)

    avg_creation_ratio = _average(creation_ratios)
    avg_files_per_session = _average(files_per_session)

    # Calculate expected files match rate
    expected_files_match_rate = _percentage(total_expected_matches, total_created_files)

    return {
        "total_packs": total_packs,
        "total_file_operations": total_operations,
        "new_file_creations": new_file_creations,
        "file_overwrites": file_overwrites,
        "file_edits": file_edits,
        "creation_ratio": creation_ratio,
        "overwrite_ratio": overwrite_ratio,
        "edit_ratio": edit_ratio,
        "avg_creation_ratio_per_pack": avg_creation_ratio,
        "high_creation_ratio_packs": high_creation_ratio_packs,
        "write_without_read_count": write_without_read_count,
        "documentation_creations": documentation_creations,
        "unexpected_file_creations": unexpected_file_creations,
        "expected_files_match_rate": expected_files_match_rate,
        "creation_clustering_detected": creation_clustering_detected,
        "avg_files_per_session": avg_files_per_session,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _normalize_path(path: str) -> str:
    """Normalize a single file path.

    Args:
        path: File path to normalize

    Returns:
        Normalized path with forward slashes and no leading ./
    """
    if not path:
        return ""

    # Convert backslashes to forward slashes
    path = path.replace("\\", "/")
    # Remove leading ./
    if path.startswith("./"):
        path = path[2:]

    return path


def _normalize_files(value: object) -> list[str]:
    """Normalize file list, handling various input types."""
    if isinstance(value, str):
        files = [value]
    elif isinstance(value, (list, tuple)):
        files = [f for f in value if isinstance(f, str)]
    else:
        return []

    # Normalize file paths
    normalized = []
    for file in files:
        file = file.strip()
        if not file:
            continue
        # Convert backslashes to forward slashes
        file = file.replace("\\", "/")
        # Remove leading ./
        if file.startswith("./"):
            file = file[2:]
        normalized.append(file)

    return normalized


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
