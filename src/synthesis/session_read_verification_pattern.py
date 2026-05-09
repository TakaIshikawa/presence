"""Session Read tool verification pattern analyzer for optimization compliance.

Analyzes the relationship between Read tool calls and subsequent verification commands.
Identifies patterns: reads followed by targeted verification (good), reads without
verification (risky), redundant re-reads after verification (wasteful), and reads
with offset/limit that skip verification (optimization gap).

Verification patterns:
- Read-to-verify ratio: Proportion of reads followed by verification
- Verification coverage: Percentage of edited files that are verified
- Redundant re-reads: Re-reading files after verification passes
- Optimization mode compliance: Baseline vs. optimized behavior per CLAUDE.md

Efficiency indicators:
- Good pattern: Edit → Targeted read → Verify (if needed)
- Risky pattern: Edit → No verification
- Wasteful pattern: Verify → Full re-read of same file
- Optimization gap: Targeted read → No verify for complex changes
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_read_verification_pattern(records: object) -> dict[str, Any]:
    """Analyze Read tool usage and verification command patterns.

    Tracks Read tool calls, Edit operations, and verification commands to measure
    verification discipline and optimization mode compliance.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Read, Edit, Bash, etc.)
            - file_path: Path to file being read/edited
            - offset: Optional read offset parameter
            - limit: Optional read limit parameter
            - command: Optional command string (for Bash verification commands)
            - is_verification: Optional boolean indicating verification command
            - turn_index: Turn number when tool was invoked
            - optimization_mode: Optional baseline|optimized mode indicator

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls analyzed
            - read_call_count: Number of Read tool calls
            - edit_call_count: Number of Edit tool calls
            - verification_command_count: Number of verification commands
            - targeted_read_count: Reads using offset/limit parameters
            - full_read_count: Reads without offset/limit
            - targeted_read_ratio: Percentage of reads using offset/limit
            - reads_followed_by_verify: Reads followed by verification
            - read_to_verify_ratio: Percentage of reads verified
            - edited_files_count: Number of unique files edited
            - verified_files_count: Number of edited files verified
            - verification_coverage: Percentage of edited files verified
            - redundant_rereads_after_verify: Re-reads after passing verification
            - optimization_mode: detected baseline|optimized|unknown
            - optimization_mode_compliant: Boolean compliance with mode strategy
            - avg_lines_per_read: Average lines read per Read call

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    read_call_count = 0
    edit_call_count = 0
    verification_command_count = 0

    targeted_read_count = 0  # Reads with offset/limit
    full_read_count = 0  # Reads without offset/limit

    # Track files and verification state
    edited_files: set[str] = set()
    verified_files: set[str] = set()
    read_files: dict[str, int] = {}  # file -> read count

    # Track verification patterns
    reads_followed_by_verify = 0
    redundant_rereads = 0

    # Track previous operations for pattern detection
    previous_reads: list[str] = []  # Recent read files
    last_verification_turn: int = -1

    # Track lines read for optimization metrics
    lines_per_read: list[int] = []

    # Detect optimization mode
    detected_mode: str | None = None

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        total_tool_calls += 1
        tool_lower = tool_name.lower()
        file_path = _string(record.get("file_path", ""))
        turn_index = _int(record.get("turn_index", 0))

        # Track optimization mode from records
        mode_indicator = _string(record.get("optimization_mode", "")).lower()
        if mode_indicator in ("baseline", "optimized") and detected_mode is None:
            detected_mode = mode_indicator

        if tool_lower == "read":
            read_call_count += 1

            # Check if targeted read (using offset/limit)
            has_offset = record.get("offset") is not None
            has_limit = record.get("limit") is not None

            if has_offset or has_limit:
                targeted_read_count += 1
                # Estimate lines read from limit parameter
                limit = _int(record.get("limit", 0))
                if limit > 0:
                    lines_per_read.append(limit)
                else:
                    lines_per_read.append(30)  # Default estimate for targeted reads
            else:
                full_read_count += 1
                # Estimate full file read
                lines_per_read.append(250)  # Estimate for full reads

            # Track file reads
            if file_path:
                read_files[file_path] = read_files.get(file_path, 0) + 1
                previous_reads.append(file_path)

                # Keep only recent reads (last 5)
                if len(previous_reads) > 5:
                    previous_reads.pop(0)

                # Check for redundant re-read after verification
                if turn_index > last_verification_turn > 0:
                    turns_since_verify = turn_index - last_verification_turn
                    if turns_since_verify <= 3 and read_files.get(file_path, 0) > 1:
                        redundant_rereads += 1

        elif tool_lower == "edit":
            edit_call_count += 1
            if file_path:
                edited_files.add(file_path)

        elif _is_verification_command(tool_lower, record):
            verification_command_count += 1
            last_verification_turn = turn_index

            # Check if recent reads were followed by this verification
            if previous_reads:
                reads_followed_by_verify += len(previous_reads)
                previous_reads.clear()

            # Try to determine which files were verified
            # This is heuristic - we assume verification covers edited files
            for edited_file in edited_files:
                verified_files.add(edited_file)

    # Calculate metrics
    targeted_read_ratio = _percentage(targeted_read_count, read_call_count)
    read_to_verify_ratio = _percentage(verification_command_count, read_call_count)

    edited_count = len(edited_files)
    verified_count = len(verified_files)
    verification_coverage = _percentage(verified_count, edited_count)

    avg_lines_per_read = _average(lines_per_read)

    # Determine optimization mode if not detected from records
    if detected_mode is None:
        detected_mode = _infer_optimization_mode(targeted_read_ratio, avg_lines_per_read)

    # Check compliance with optimization mode strategy
    is_compliant = _check_optimization_compliance(
        detected_mode,
        targeted_read_ratio,
        avg_lines_per_read,
        read_to_verify_ratio,
    )

    return {
        "total_tool_calls": total_tool_calls,
        "read_call_count": read_call_count,
        "edit_call_count": edit_call_count,
        "verification_command_count": verification_command_count,
        "targeted_read_count": targeted_read_count,
        "full_read_count": full_read_count,
        "targeted_read_ratio": targeted_read_ratio,
        "reads_followed_by_verify": reads_followed_by_verify,
        "read_to_verify_ratio": read_to_verify_ratio,
        "edited_files_count": edited_count,
        "verified_files_count": verified_count,
        "verification_coverage": verification_coverage,
        "redundant_rereads_after_verify": redundant_rereads,
        "optimization_mode": detected_mode,
        "optimization_mode_compliant": is_compliant,
        "avg_lines_per_read": avg_lines_per_read,
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
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _is_verification_command(tool_name: str, record: Mapping[str, Any]) -> bool:
    """Detect if a tool call is a verification command.

    Verification indicators:
    - tool_name contains "verify" (Skill tool with verify command)
    - is_verification flag is True
    - Bash command contains verification patterns (pytest, npm test, etc.)

    Args:
        tool_name: Name of the tool (lowercased)
        record: Tool call record

    Returns:
        True if this is a verification command
    """
    # Check explicit verification flag
    if record.get("is_verification") is True:
        return True

    # Check tool name
    if "verify" in tool_name:
        return True

    # Check command content for verification patterns
    command = _string(record.get("command", ""))
    if command:
        verification_patterns = [
            "pytest",
            "npm test",
            "npm run test",
            "yarn test",
            "cargo test",
            "go test",
            "python -m pytest",
            "python -m unittest",
            "uv run --with pytest",
            "/verify",
        ]
        command_lower = command.lower()
        for pattern in verification_patterns:
            if pattern in command_lower:
                return True

    return False


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


def _infer_optimization_mode(
    targeted_read_ratio: float,
    avg_lines_per_read: float,
) -> str:
    """Infer optimization mode from behavioral metrics.

    Per CLAUDE.md Run #1 targets:
    - Optimized: 85-90% targeted reads, <70 lines per read
    - Baseline: Natural behavior, likely <30% targeted, >150 lines per read

    Args:
        targeted_read_ratio: Percentage of reads using offset/limit
        avg_lines_per_read: Average lines per read

    Returns:
        "optimized", "baseline", or "unknown"
    """
    # Strong optimized indicators
    if targeted_read_ratio >= 70 and avg_lines_per_read <= 80:
        return "optimized"

    # Strong baseline indicators
    if targeted_read_ratio < 30 and avg_lines_per_read > 150:
        return "baseline"

    return "unknown"


def _check_optimization_compliance(
    mode: str,
    targeted_read_ratio: float,
    avg_lines_per_read: float,
    verify_ratio: float,
) -> bool:
    """Check if session complies with optimization mode strategy.

    Per CLAUDE.md:
    - Optimized mode: Should have high targeted read ratio (>85%), low lines per read (<70)
    - Baseline mode: Natural behavior, no specific requirements
    - Strategic verification: Present but not excessive (<50% is acceptable)

    Args:
        mode: Detected optimization mode
        targeted_read_ratio: Percentage of reads using offset/limit
        avg_lines_per_read: Average lines per read
        verify_ratio: Percentage of verify commands vs reads

    Returns:
        True if compliant with mode strategy
    """
    if mode == "optimized":
        # Check Run #1 targets: 85-90% targeted, <70 lines
        meets_targeted_target = targeted_read_ratio >= 85
        meets_lines_target = avg_lines_per_read <= 70
        # Strategic verify: present but not excessive (<50% is acceptable)
        strategic_verify = verify_ratio <= 50

        return meets_targeted_target and meets_lines_target and strategic_verify

    elif mode == "baseline":
        # Baseline mode: should NOT use optimization strategies
        not_optimized = targeted_read_ratio < 50 and avg_lines_per_read > 100
        return not_optimized

    else:
        # Unknown mode: no compliance check
        return True
