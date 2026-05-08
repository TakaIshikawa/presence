"""Session file read size distribution analyzer for read efficiency reports.

Analyzes the distribution of file read sizes (in lines) across a session to
identify patterns like repeated full-file reads vs targeted offset/limit reads.
This helps optimize token usage by detecting opportunities for more efficient
targeted reads instead of full file reads.

Read size buckets:
- <50 lines: Targeted reads (highly efficient)
- 50-200 lines: Small reads (moderately efficient)
- 200-500 lines: Medium reads (less efficient)
- 500+ lines: Large reads (potentially inefficient)

Read efficiency patterns:
- Highly targeted: >75% reads under 50 lines
- Moderately targeted: 50-75% reads under 200 lines
- Mixed usage: Balance between targeted and full reads
- Full-file heavy: >50% reads over 500 lines
"""

from __future__ import annotations

from typing import Any, Mapping


# Size bucket thresholds (in lines)
BUCKET_SMALL = 50
BUCKET_MEDIUM = 200
BUCKET_LARGE = 500

# Targeted read threshold (percentage of reads in small bucket)
TARGETED_THRESHOLD_HIGH = 0.75  # >75% small reads = highly targeted
TARGETED_THRESHOLD_MODERATE = 0.50  # 50-75% small reads = moderately targeted


def analyze_session_file_read_size_distribution(records: object) -> dict[str, Any]:
    """Analyze distribution of file read sizes across a session.

    Categorizes file reads into size buckets and calculates the percentage
    of targeted reads (using offset/limit parameters).

    Args:
        records: List of read operation dictionaries with keys:
            - file_path: Path to the file read
            - lines_read: Number of lines read
            - offset: Optional offset parameter
            - limit: Optional limit parameter
            - turn_index: Turn number when read occurred

    Returns:
        Dict with:
            - total_reads: Total number of read operations
            - bucket_counts: Dict mapping bucket names to counts
            - targeted_read_count: Number of reads using offset/limit
            - targeted_read_percentage: Percentage of targeted reads
            - avg_lines_per_read: Average lines read per operation
            - efficiency_pattern: Classification of read efficiency
            - examples: Sample reads from each bucket

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of read operation dictionaries")

    bucket_counts = {
        "under_50": 0,
        "50_to_200": 0,
        "200_to_500": 0,
        "over_500": 0,
    }
    targeted_read_count = 0
    total_lines = 0
    examples: dict[str, list[dict[str, Any]]] = {
        "under_50": [],
        "50_to_200": [],
        "200_to_500": [],
        "over_500": [],
    }

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        lines_read = _number(record.get("lines_read"))
        if lines_read is None or lines_read < 0:
            continue

        # Determine bucket
        bucket = _categorize_bucket(lines_read)
        bucket_counts[bucket] += 1
        total_lines += lines_read

        # Check if targeted read (uses offset or limit)
        offset = record.get("offset")
        limit = record.get("limit")
        if offset is not None or limit is not None:
            targeted_read_count += 1

        # Collect example
        _add_example(examples, bucket, record, index, lines_read)

    total_reads = sum(bucket_counts.values())
    targeted_percentage = _percentage(targeted_read_count, total_reads)
    avg_lines = _average(total_lines, total_reads)
    efficiency_pattern = _classify_efficiency_pattern(bucket_counts, total_reads)

    return {
        "total_reads": total_reads,
        "bucket_counts": bucket_counts,
        "targeted_read_count": targeted_read_count,
        "targeted_read_percentage": targeted_percentage,
        "avg_lines_per_read": avg_lines,
        "efficiency_pattern": efficiency_pattern,
        "examples": {k: v[:3] for k, v in examples.items()},  # Limit to 3 per bucket
    }


def _number(value: object) -> int | None:
    """Extract integer from value, handling various types."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _categorize_bucket(lines: int) -> str:
    """Categorize read size into a bucket."""
    if lines < BUCKET_SMALL:
        return "under_50"
    elif lines < BUCKET_MEDIUM:
        return "50_to_200"
    elif lines < BUCKET_LARGE:
        return "200_to_500"
    else:
        return "over_500"


def _add_example(
    examples: dict[str, list[dict[str, Any]]],
    bucket: str,
    record: Mapping[str, Any],
    index: int,
    lines_read: int,
) -> None:
    """Add an example to the bucket if we have fewer than 3."""
    if len(examples[bucket]) < 3:
        file_path = record.get("file_path", "")
        turn_index = record.get("turn_index", index)
        examples[bucket].append({
            "file_path": str(file_path) if file_path else "unknown",
            "lines_read": lines_read,
            "turn_index": turn_index,
        })


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(total: int, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)


def _classify_efficiency_pattern(bucket_counts: dict[str, int], total_reads: int) -> str:
    """Classify the read efficiency pattern based on bucket distribution.

    Patterns:
    - highly_targeted: >75% reads under 50 lines
    - moderately_targeted: 50-75% reads under 200 lines
    - mixed: Balanced distribution
    - full_file_heavy: >50% reads over 500 lines
    - empty: No reads
    """
    if total_reads == 0:
        return "empty"

    small_ratio = bucket_counts["under_50"] / total_reads
    small_medium_ratio = (bucket_counts["under_50"] + bucket_counts["50_to_200"]) / total_reads
    large_ratio = bucket_counts["over_500"] / total_reads

    if small_ratio > TARGETED_THRESHOLD_HIGH:
        return "highly_targeted"
    elif small_medium_ratio > TARGETED_THRESHOLD_HIGH:
        return "moderately_targeted"
    elif large_ratio > 0.5:
        return "full_file_heavy"
    else:
        return "mixed"
