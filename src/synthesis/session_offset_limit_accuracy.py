"""Session offset/limit read accuracy analyzer.

Analyzes how accurately agents use offset/limit parameters to read exactly the
needed context. Measures whether targeted reads include the actual error/edit
location, precision of window sizing, and over/under-reading patterns.

Read accuracy metrics:
- Coverage accuracy: % of targeted reads that include target location
- Read precision: Lines read vs lines actually needed ratio
- Over-reading waste: Lines read beyond necessary context
- Under-reading rate: Reads requiring follow-up due to insufficient context
- Window sizing accuracy: Match between requested and optimal window size

Accuracy patterns:
- High coverage (>90%): Reads consistently capture target context
- High precision (<1.2 ratio): Minimal waste in reading
- Low over-reading (<20%): Efficient window sizing
- Low under-reading (<5%): Rare need for re-reads
- Optimal window sizing: Aligns with <70 lines average target
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_offset_limit_accuracy(records: object) -> dict[str, Any]:
    """Analyze offset/limit read accuracy in agent sessions.

    Evaluates how accurately agents size their Read windows to capture exactly
    the needed context without excessive over-reading or under-reading.

    Args:
        records: List of read event dictionaries with keys:
            - read_index: Sequential read number
            - file_path: File being read
            - offset: Offset parameter used (negative for tail reads)
            - limit: Limit parameter used
            - lines_read: Actual number of lines read
            - target_location: Optional line number being targeted
            - includes_target: Boolean indicating if read captured target
            - follow_up_read: Boolean indicating if another read was needed
            - purpose: "error_context", "edit_verification", "exploration"
            - lines_needed: Estimated lines actually needed for context

    Returns:
        Dict with:
            - total_reads: Total Read calls analyzed
            - targeted_reads: Reads with offset/limit parameters
            - coverage_accuracy: % of reads that include target location
            - avg_read_precision: Ratio of lines_read to lines_needed
            - over_reading_ratio: % of lines that were unnecessary
            - under_reading_rate: % of reads requiring follow-up
            - avg_window_size: Average limit value used
            - optimal_window_reads: Reads with <70 lines (optimized mode target)
            - excessive_window_reads: Reads with >200 lines
            - precision_by_purpose: Accuracy breakdown by read purpose
            - high_accuracy_sessions: Count with >90% coverage accuracy

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of read event dictionaries")

    if not records:
        return _empty_result()

    total_reads = 0
    targeted_reads = 0

    coverage_hits = 0  # Reads that included target
    coverage_attempts = 0  # Reads with known target

    precision_ratios: list[float] = []
    over_read_lines = 0
    total_lines_read = 0
    total_lines_needed = 0

    under_reading_count = 0

    window_sizes: list[int] = []
    optimal_window_count = 0  # <70 lines
    excessive_window_count = 0  # >200 lines

    purpose_stats: dict[str, dict[str, Any]] = {
        "error_context": {"total": 0, "accurate": 0, "precision": []},
        "edit_verification": {"total": 0, "accurate": 0, "precision": []},
        "exploration": {"total": 0, "accurate": 0, "precision": []},
    }

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_reads += 1

        offset = record.get("offset")
        limit = record.get("limit")

        # Count as targeted if offset or limit is specified
        if offset is not None or limit is not None:
            targeted_reads += 1

        # Track window size
        limit_val = _int(limit) if limit is not None else None
        if limit_val is not None and limit_val > 0:
            window_sizes.append(limit_val)
            if limit_val < 70:
                optimal_window_count += 1
            elif limit_val > 200:
                excessive_window_count += 1

        # Check coverage accuracy
        includes_target = record.get("includes_target")
        if includes_target is not None:
            coverage_attempts += 1
            if includes_target is True:
                coverage_hits += 1

        # Calculate precision
        lines_read = _int(record.get("lines_read"))
        lines_needed = _int(record.get("lines_needed"))

        if lines_read is not None and lines_read > 0:
            total_lines_read += lines_read

        if lines_needed is not None and lines_needed > 0:
            total_lines_needed += lines_needed

            if lines_read is not None and lines_read > 0:
                precision = lines_read / lines_needed
                precision_ratios.append(precision)

                # Track over-reading
                if lines_read > lines_needed:
                    over_read_lines += (lines_read - lines_needed)

        # Check under-reading
        follow_up = record.get("follow_up_read")
        if follow_up is True:
            under_reading_count += 1

        # Track by purpose
        purpose = _string(record.get("purpose"))
        if purpose in purpose_stats:
            purpose_stats[purpose]["total"] += 1

            if includes_target is True:
                purpose_stats[purpose]["accurate"] += 1

            if lines_read and lines_needed and lines_needed > 0:
                precision = lines_read / lines_needed
                purpose_stats[purpose]["precision"].append(precision)

    # Calculate aggregate metrics
    coverage_accuracy = _percentage(coverage_hits, coverage_attempts)
    avg_precision = _average(precision_ratios)
    over_reading_ratio = _percentage(over_read_lines, total_lines_read)
    under_reading_rate = _percentage(under_reading_count, total_reads)
    avg_window_size = _average([float(w) for w in window_sizes])

    # Format purpose breakdown
    precision_by_purpose = []
    for purpose, stats in sorted(purpose_stats.items()):
        if stats["total"] > 0:
            accuracy = _percentage(stats["accurate"], stats["total"])
            avg_prec = _average(stats["precision"]) if stats["precision"] else 0.0

            precision_by_purpose.append({
                "purpose": purpose,
                "total_reads": stats["total"],
                "accuracy": accuracy,
                "avg_precision": avg_prec,
            })

    # High accuracy threshold: >90% coverage
    high_accuracy = 1 if coverage_accuracy > 90 else 0

    return {
        "total_reads": total_reads,
        "targeted_reads": targeted_reads,
        "coverage_accuracy": coverage_accuracy,
        "avg_read_precision": avg_precision,
        "over_reading_ratio": over_reading_ratio,
        "under_reading_rate": under_reading_rate,
        "avg_window_size": avg_window_size,
        "optimal_window_reads": optimal_window_count,
        "excessive_window_reads": excessive_window_count,
        "precision_by_purpose": precision_by_purpose,
        "high_accuracy_sessions": high_accuracy,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_reads": 0,
        "targeted_reads": 0,
        "coverage_accuracy": 0.0,
        "avg_read_precision": 0.0,
        "over_reading_ratio": 0.0,
        "under_reading_rate": 0.0,
        "avg_window_size": 0.0,
        "optimal_window_reads": 0,
        "excessive_window_reads": 0,
        "precision_by_purpose": [],
        "high_accuracy_sessions": 0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace.

    Args:
        value: Value to convert

    Returns:
        String value
    """
    return value.strip() if isinstance(value, str) else ""


def _int(value: object) -> int | None:
    """Convert value to int.

    Args:
        value: Value to convert

    Returns:
        Int value, or None if invalid
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
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


def _average(values: list[float]) -> float:
    """Calculate average of numeric values.

    Args:
        values: List of numeric values

    Returns:
        Average value
    """
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
