"""Pack targeted read coverage analyzer for per-file read optimization.

Analyzes read optimization patterns in execution packs by measuring targeted
read usage for each changed file. Calculates percentage using offset/limit,
average lines read per file, and per-file read efficiency scores.

Targeted read coverage metrics:
- Targeted read percentage: Percentage using offset/limit per file
- Average lines per file: Mean lines read across all file operations
- Full-file vs targeted count: Balance of exploration vs focused reads
- Read efficiency score: Weighted score considering file size and frequency

Optimization indicators:
- 85%+ targeted reads: High optimization adoption
- <70 lines average: Efficient focused reading
- Verification reads detected: Reads following Edit/Write operations
- Per-file efficiency scores: Granular optimization measurement
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_targeted_read_coverage(
    records: object,
    expected_files: object = None
) -> dict[str, Any]:
    """Analyze targeted read coverage for each file in pack expected files.

    Tracks Read tool calls targeting files in expected_files, measures
    offset/limit usage, and calculates per-file efficiency scores.

    Args:
        records: List of tool call dictionaries from pack transcript with keys:
            - tool_name: Name of the tool (Read, Edit, Write, etc.)
            - file_path: Path for file operations
            - offset: Optional starting line for read
            - limit: Optional line count limit for read
            - lines_read: Optional actual lines read
            - turn_index: Turn number when tool was invoked
        expected_files: List of file paths that pack expects to modify

    Returns:
        Dict with:
            - total_files_analyzed: Number of files in expected_files
            - total_reads: Total Read tool calls across all files
            - targeted_reads: Reads using offset or limit parameters
            - full_reads: Reads without offset/limit
            - targeted_read_percentage: Percentage of targeted reads
            - avg_lines_per_read: Average lines read across all reads
            - per_file_metrics: List of dicts with per-file analysis
            - verification_reads_detected: Reads following Edit/Write
            - read_efficiency_score: Weighted efficiency score (0-100)

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    if expected_files is None:
        expected_files = []
    if not isinstance(expected_files, list):
        expected_files = []

    # Normalize expected files
    expected_file_set = {_string(f) for f in expected_files if isinstance(f, str)}

    total_reads = 0
    targeted_reads = 0
    full_reads = 0
    all_lines_read: list[int | float] = []
    verification_reads_detected = 0

    # Track per-file reads
    file_reads: dict[str, list[dict[str, Any]]] = {}
    recent_edits: dict[str, int] = {}  # file_path -> turn_index

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        tool_lower = tool_name.lower()
        file_path = _string(record.get("file_path", ""))
        turn_index = record.get("turn_index", 0)

        if tool_lower == "read" and file_path:
            # Only track reads for files in expected_files
            if not expected_file_set or file_path in expected_file_set:
                total_reads += 1

                offset = record.get("offset")
                limit = record.get("limit")
                is_targeted = offset is not None or limit is not None

                if is_targeted:
                    targeted_reads += 1
                else:
                    full_reads += 1

                # Track lines read
                lines_read = _extract_lines_read(record)
                if lines_read is not None:
                    all_lines_read.append(lines_read)

                # Check if this is a verification read (after edit)
                if file_path in recent_edits:
                    edit_turn = recent_edits[file_path]
                    if turn_index - edit_turn <= 3:
                        verification_reads_detected += 1

                # Store per-file read
                if file_path not in file_reads:
                    file_reads[file_path] = []

                file_reads[file_path].append({
                    "is_targeted": is_targeted,
                    "lines_read": lines_read,
                    "turn_index": turn_index,
                })

        elif tool_lower in ("edit", "write") and file_path:
            # Track edits for verification read detection
            recent_edits[file_path] = turn_index

    # Calculate overall metrics
    targeted_read_percentage = _percentage(targeted_reads, total_reads)
    avg_lines_per_read = _average(all_lines_read)

    # Calculate per-file metrics
    per_file_metrics = []
    for file_path in sorted(file_reads.keys()):
        reads = file_reads[file_path]
        file_targeted = sum(1 for r in reads if r["is_targeted"])
        file_full = len(reads) - file_targeted
        file_lines = [r["lines_read"] for r in reads if r["lines_read"] is not None]

        file_efficiency = _calculate_file_efficiency(
            file_targeted,
            len(reads),
            file_lines
        )

        per_file_metrics.append({
            "file_path": file_path,
            "total_reads": len(reads),
            "targeted_reads": file_targeted,
            "full_reads": file_full,
            "targeted_percentage": _percentage(file_targeted, len(reads)),
            "avg_lines": _average(file_lines),
            "efficiency_score": file_efficiency,
        })

    # Calculate overall read efficiency score
    read_efficiency_score = _calculate_overall_efficiency(
        targeted_read_percentage,
        avg_lines_per_read,
        verification_reads_detected,
        total_reads
    )

    return {
        "total_files_analyzed": len(expected_file_set) if expected_file_set else len(file_reads),
        "total_reads": total_reads,
        "targeted_reads": targeted_reads,
        "full_reads": full_reads,
        "targeted_read_percentage": targeted_read_percentage,
        "avg_lines_per_read": avg_lines_per_read,
        "per_file_metrics": per_file_metrics,
        "verification_reads_detected": verification_reads_detected,
        "read_efficiency_score": read_efficiency_score,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_lines_read(record: Mapping[str, Any]) -> int | None:
    """Extract lines read count from record if available."""
    lines_read = record.get("lines_read")
    if isinstance(lines_read, int) and not isinstance(lines_read, bool):
        return lines_read

    # Infer from limit parameter
    limit = record.get("limit")
    if isinstance(limit, int) and not isinstance(limit, bool) and limit > 0:
        return limit

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


def _calculate_file_efficiency(
    targeted_count: int,
    total_count: int,
    lines_read: list[int | float]
) -> float:
    """Calculate file-level read efficiency score (0-100).

    Higher score indicates better optimization:
    - High targeted read percentage
    - Low average lines per read
    """
    if total_count == 0:
        return 0.0

    targeted_pct = (targeted_count / total_count) * 100
    avg_lines = sum(lines_read) / len(lines_read) if lines_read else 0

    # Score starts at 50
    score = 50.0

    # Bonus for high targeted percentage (up to +30)
    score += (targeted_pct - 50) * 0.6  # +30 at 100%, 0 at 50%

    # Penalty for high average lines (down to -30)
    if avg_lines > 70:
        score -= min(30, (avg_lines - 70) * 0.2)  # -30 at 220+ lines

    return round(max(0.0, min(100.0, score)), 2)


def _calculate_overall_efficiency(
    targeted_percentage: float,
    avg_lines: float,
    verification_reads: int,
    total_reads: int
) -> float:
    """Calculate overall read efficiency score (0-100).

    Weighted score considering:
    - Targeted read adoption
    - Average lines per read
    - Verification read ratio
    """
    score = 50.0  # Base score

    # Bonus for high targeted percentage (up to +25)
    score += (targeted_percentage - 50) * 0.5

    # Penalty for high average lines (up to -20)
    if avg_lines > 70:
        penalty = min(20, (avg_lines - 70) * 0.15)
        score -= penalty

    # Bonus for low verification reads (up to +5)
    # Lower verification reads = more strategic use of verify skill
    if total_reads > 0:
        verify_ratio = (verification_reads / total_reads) * 100
        if verify_ratio < 30:
            score += (30 - verify_ratio) * 0.15  # +4.5 at 0%

    return round(max(0.0, min(100.0, score)), 2)
