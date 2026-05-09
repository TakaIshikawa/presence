"""Session Read tool strategy and offset/limit usage analyzer.

Analyzes Read tool usage patterns in Claude Code sessions to measure reading
efficiency, re-read behavior, and verification patterns. Tracks targeted vs
full-file reads, re-read frequency, and Read-after-Edit verification patterns.

Read strategy metrics:
- Total Read calls: Number of Read tool invocations
- Full file reads: Reads without offset/limit parameters
- Targeted reads: Reads using offset/limit for precision
- Average lines per read: Mean lines read per invocation
- Re-read frequency: Number of times same file read multiple times
- Read-after-Edit pattern: Reads following Edit/Write calls on same file
- Offset/limit precision: Whether ranges capture intended context
- Read efficiency score: 0-100 score (targeted reads and low re-reads = higher)

Quality indicators:
- High targeted read ratio (>85%): Good offset/limit usage discipline
- Low average lines per read (<70): Precise, focused reads
- Low re-read frequency (<30%): Minimal redundant reading
- High Read-after-Edit ratio (>60%): Good verification discipline
- High efficiency score (>80): Optimal reading strategy
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_read_strategy(records: object) -> dict[str, Any]:
    """Analyze Read tool usage patterns and efficiency in Claude Code sessions.

    Evaluates reading efficiency through targeted reads, re-read frequency,
    and Read-after-Edit verification patterns.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_read_calls: Number of Read tool invocations
            - full_file_reads: Reads without offset/limit
            - targeted_reads: Reads with offset/limit parameters
            - total_lines_read: Total lines read across all calls
            - reread_calls: Number of times same file read multiple times
            - unique_files_read: Number of unique files read
            - read_after_edit_calls: Reads following Edit/Write on same file
            - total_edit_calls: Number of Edit/Write tool invocations
            - avg_lines_per_read: Average lines per Read call
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_reads: Count using Read tool
            - avg_read_calls: Average Read invocations per session
            - avg_targeted_read_ratio: Average % of reads with offset/limit
            - avg_lines_per_read: Average lines read per call
            - avg_reread_frequency: Average % of re-read calls
            - avg_read_after_edit_ratio: Average % of post-Edit reads
            - read_efficiency_score: Score 0-100 (higher = better efficiency)
            - high_efficiency_sessions: Count with score >80
            - low_efficiency_sessions: Count with score <50
            - baseline_mode_sessions: Count with low targeted ratio (baseline behavior)
            - optimized_mode_sessions: Count with high targeted ratio (optimized behavior)

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_reads = 0

    read_calls: list[int | float] = []
    targeted_ratios: list[float] = []
    lines_per_read: list[float] = []
    reread_frequencies: list[float] = []
    read_after_edit_ratios: list[float] = []
    efficiency_scores: list[float] = []

    high_efficiency_sessions = 0  # >80 score
    low_efficiency_sessions = 0   # <50 score
    baseline_mode_sessions = 0    # <30% targeted reads
    optimized_mode_sessions = 0   # >85% targeted reads

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_reads = _extract_number(record.get("total_read_calls"))
        full_file = _extract_number(record.get("full_file_reads"))
        targeted = _extract_number(record.get("targeted_reads"))
        total_lines = _extract_number(record.get("total_lines_read"))
        rereads = _extract_number(record.get("reread_calls"))
        unique_files = _extract_number(record.get("unique_files_read"))
        read_after_edit = _extract_number(record.get("read_after_edit_calls"))
        total_edits = _extract_number(record.get("total_edit_calls"))
        avg_lines = _extract_number(record.get("avg_lines_per_read"))

        # Track sessions using Read
        if total_reads is not None and total_reads > 0:
            sessions_with_reads += 1
            read_calls.append(total_reads)

            # Calculate targeted read ratio
            targeted_ratio = 0.0
            if targeted is not None:
                targeted_ratio = _percentage(targeted, total_reads)
                targeted_ratios.append(targeted_ratio)
            elif full_file is not None:
                # Infer targeted from full_file
                targeted_count = total_reads - full_file
                targeted_ratio = _percentage(targeted_count, total_reads)
                targeted_ratios.append(targeted_ratio)

            # Classify baseline vs optimized mode
            if targeted_ratio < 30.0:
                baseline_mode_sessions += 1
            elif targeted_ratio > 85.0:
                optimized_mode_sessions += 1

            # Calculate average lines per read
            if avg_lines is not None:
                lines_per_read.append(avg_lines)
            elif total_lines is not None:
                avg_lines_calc = total_lines / total_reads
                lines_per_read.append(avg_lines_calc)

            # Calculate re-read frequency
            if rereads is not None:
                reread_freq = _percentage(rereads, total_reads)
                reread_frequencies.append(reread_freq)

            # Calculate Read-after-Edit ratio
            if read_after_edit is not None and total_edits is not None and total_edits > 0:
                rae_ratio = _percentage(read_after_edit, total_edits)
                read_after_edit_ratios.append(rae_ratio)

            # Calculate efficiency score
            efficiency_score = _calculate_efficiency_score(
                targeted_ratio=targeted_ratio if targeted_ratios and len(targeted_ratios) > len(efficiency_scores) else None,
                avg_lines=lines_per_read[-1] if lines_per_read and len(lines_per_read) > len(efficiency_scores) else None,
                reread_freq=reread_frequencies[-1] if reread_frequencies and len(reread_frequencies) > len(efficiency_scores) else None,
                read_after_edit_ratio=read_after_edit_ratios[-1] if read_after_edit_ratios and len(read_after_edit_ratios) > len(efficiency_scores) else None,
            )
            efficiency_scores.append(efficiency_score)

            # Classify efficiency quality
            if efficiency_score > 80.0:
                high_efficiency_sessions += 1
            elif efficiency_score < 50.0:
                low_efficiency_sessions += 1

    # Calculate aggregate metrics
    avg_reads = _average(read_calls)
    avg_targeted = _average(targeted_ratios)
    avg_lines = _average(lines_per_read)
    avg_reread = _average(reread_frequencies)
    avg_rae = _average(read_after_edit_ratios)
    avg_efficiency = _average(efficiency_scores)

    return {
        "total_sessions": total_sessions,
        "sessions_with_reads": sessions_with_reads,
        "avg_read_calls": avg_reads,
        "avg_targeted_read_ratio": avg_targeted,
        "avg_lines_per_read": avg_lines,
        "avg_reread_frequency": avg_reread,
        "avg_read_after_edit_ratio": avg_rae,
        "read_efficiency_score": avg_efficiency,
        "high_efficiency_sessions": high_efficiency_sessions,
        "low_efficiency_sessions": low_efficiency_sessions,
        "baseline_mode_sessions": baseline_mode_sessions,
        "optimized_mode_sessions": optimized_mode_sessions,
    }


def _calculate_efficiency_score(
    targeted_ratio: float | None,
    avg_lines: float | None,
    reread_freq: float | None,
    read_after_edit_ratio: float | None,
) -> float:
    """Calculate read efficiency score (0-100).

    Higher scores indicate better efficiency:
    - High targeted read ratio (>85%)
    - Low average lines per read (<70)
    - Low re-read frequency (<30%)
    - High Read-after-Edit ratio (>60%)

    Scoring breakdown:
    - Targeted reads: 40 points (85% threshold)
    - Lines per read: 25 points (70 lines threshold)
    - Re-read discipline: 20 points (30% threshold)
    - Verification pattern: 15 points (60% threshold)
    """
    score = 0.0

    # Targeted reads component (40 points)
    if targeted_ratio is not None:
        if targeted_ratio > 85:  # >85% = excellent
            score += 40.0
        elif targeted_ratio > 70:  # >70% = good
            score += 30.0
        elif targeted_ratio > 50:  # >50% = acceptable
            score += 20.0
        elif targeted_ratio > 30:  # >30% = poor
            score += 10.0
        # <30% = 0 points (baseline mode)

    # Lines per read component (25 points)
    if avg_lines is not None:
        if avg_lines < 70:  # <70 lines = excellent
            score += 25.0
        elif avg_lines < 100:  # <100 lines = good
            score += 20.0
        elif avg_lines < 150:  # <150 lines = acceptable
            score += 15.0
        elif avg_lines < 200:  # <200 lines = poor
            score += 10.0
        # >200 lines = 0 points

    # Re-read discipline component (20 points)
    if reread_freq is not None:
        if reread_freq < 30:  # <30% = excellent
            score += 20.0
        elif reread_freq < 50:  # <50% = good
            score += 15.0
        elif reread_freq < 70:  # <70% = acceptable
            score += 10.0
        # >70% = 0 points

    # Verification pattern component (15 points)
    if read_after_edit_ratio is not None:
        if read_after_edit_ratio > 60:  # >60% = excellent
            score += 15.0
        elif read_after_edit_ratio > 40:  # >40% = good
            score += 10.0
        elif read_after_edit_ratio > 20:  # >20% = acceptable
            score += 5.0
        # <20% = 0 points

    return round(score, 2)


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value (int or float) if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
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
