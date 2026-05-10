"""Session CLAUDE_OPTIMIZATION_MODE compliance and token efficiency analyzer.

Analyzes session-level compliance with CLAUDE_OPTIMIZATION_MODE directives
from CLAUDE.md global instructions, evaluating adherence to optimization
strategies and measuring token efficiency.

Optimization mode metrics:
- Mode detection: Identifies baseline vs optimized mode from context
- Offset/limit usage: % Read calls using targeted parameters (optimized target: 85-90%)
- Average lines read: Mean lines per Read (optimized target: <70)
- Cache command usage: Rate of /cache commands in optimized mode
- Verify command usage: Rate of /verify commands in optimized mode
- Optimization compliance score: Overall adherence (0-1)

Mode-specific validation:
- Optimized mode: Expects high offset/limit usage, cache/verify commands
- Baseline mode: Expects natural behavior, no optimization commands

Quality indicators:
- High offset usage (optimized): >85% reads use offset/limit
- Low average lines (optimized): <70 lines per read
- Cache usage (optimized): >10% repeated reads use /cache
- Verify usage (optimized): >5% complex edits use /verify
- Baseline purity: No optimization commands in baseline mode
- Token savings estimate: >50% reduction vs baseline
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_optimization_mode_compliance(records: object) -> dict[str, Any]:
    """Analyze CLAUDE_OPTIMIZATION_MODE compliance and token efficiency.

    Validates adherence to optimization directives and measures efficiency.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number
            - tool_name: Tool used (Read, Bash, etc.)
            - command: Bash command (for detecting /cache, /verify)
            - file_path: File being read
            - offset: Optional read offset
            - limit: Optional read limit
            - lines_read: Number of lines read
            - optimization_mode: Optional mode indicator (baseline/optimized)
            - environment_vars: Optional dict with CLAUDE_OPTIMIZATION_MODE

    Returns:
        Dict with:
            - total_turns: Total turns analyzed
            - optimization_mode_detected: Detected mode (baseline/optimized/unknown)
            - total_read_calls: Total Read tool invocations
            - reads_with_offset_limit: Read calls using offset/limit
            - offset_limit_usage_rate: % reads using targeted parameters
            - total_lines_read: Total lines read across all reads
            - avg_lines_per_read: Mean lines per read
            - cache_command_count: Count of /cache commands
            - verify_command_count: Count of /verify commands
            - cache_command_rate: % turns using /cache
            - verify_command_rate: % turns using /verify
            - optimization_compliance_score: Overall compliance (0-1)
            - estimated_token_savings: Estimated % token reduction vs baseline
            - mode_violations: Count of mode-specific violations
            - example_good_targeted_read: Example of good offset/limit usage
            - example_missed_targeting: Example of full read that could be targeted
            - example_mode_violation: Example of violation (e.g., /cache in baseline)

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    total_turns = 0
    optimization_mode_detected = "unknown"

    # Read metrics
    total_read_calls = 0
    reads_with_offset_limit = 0
    total_lines_read = 0

    # Command metrics
    cache_command_count = 0
    verify_command_count = 0

    # Mode violations
    mode_violations = 0

    # File read tracking for re-read detection
    file_read_tracker: dict[str, int] = {}
    full_file_rereads = 0

    # Examples
    example_good_targeted_read: dict[str, Any] = {}
    example_missed_targeting: dict[str, Any] = {}
    example_mode_violation: dict[str, Any] = {}

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1

        # Detect optimization mode
        if optimization_mode_detected == "unknown":
            mode = _detect_optimization_mode(record)
            if mode != "unknown":
                optimization_mode_detected = mode

        tool_name = _string(record.get("tool_name"))

        # Track Read calls
        if tool_name.lower() == "read":
            total_read_calls += 1

            offset = record.get("offset")
            limit = record.get("limit")
            has_offset_or_limit = offset is not None or limit is not None

            if has_offset_or_limit:
                reads_with_offset_limit += 1

                # Capture good example
                if not example_good_targeted_read:
                    example_good_targeted_read = {
                        "file_path": _string(record.get("file_path", "")),
                        "offset": offset,
                        "limit": limit,
                    }

            lines_read = _int(record.get("lines_read", 0))
            if lines_read > 0:
                total_lines_read += lines_read

            # Track file re-reads
            file_path = _string(record.get("file_path", ""))
            if file_path:
                file_read_tracker[file_path] = file_read_tracker.get(file_path, 0) + 1

                # Detect missed targeting: full-file re-read
                if file_read_tracker[file_path] > 1 and not has_offset_or_limit:
                    full_file_rereads += 1
                    if not example_missed_targeting:
                        example_missed_targeting = {
                            "file_path": file_path,
                            "read_count": file_read_tracker[file_path],
                        }

        # Track cache/verify commands
        if tool_name == "Bash":
            command = _string(record.get("command", ""))

            if "/cache" in command or "cache query" in command.lower():
                cache_command_count += 1

                # Check for mode violation (cache in baseline)
                if optimization_mode_detected == "baseline":
                    mode_violations += 1
                    if not example_mode_violation:
                        example_mode_violation = {
                            "type": "cache_in_baseline",
                            "command": command,
                        }

            if "/verify" in command or "verify check" in command.lower() or "verify build" in command.lower():
                verify_command_count += 1

                # Check for mode violation (verify in baseline)
                if optimization_mode_detected == "baseline":
                    mode_violations += 1
                    if not example_mode_violation:
                        example_mode_violation = {
                            "type": "verify_in_baseline",
                            "command": command,
                        }

    # Calculate metrics
    offset_limit_usage_rate = _percentage(reads_with_offset_limit, total_read_calls)
    avg_lines_per_read = _average_int(total_lines_read, total_read_calls)

    cache_command_rate = _percentage(cache_command_count, total_turns)
    verify_command_rate = _percentage(verify_command_count, total_turns)

    # Calculate compliance score
    optimization_compliance_score = _calculate_compliance_score(
        optimization_mode_detected,
        offset_limit_usage_rate,
        avg_lines_per_read,
        cache_command_rate,
        verify_command_rate,
        mode_violations,
    )

    # Estimate token savings
    estimated_token_savings = _estimate_token_savings(
        optimization_mode_detected,
        avg_lines_per_read,
        offset_limit_usage_rate,
    )

    return {
        "total_turns": total_turns,
        "optimization_mode_detected": optimization_mode_detected,
        "total_read_calls": total_read_calls,
        "reads_with_offset_limit": reads_with_offset_limit,
        "offset_limit_usage_rate": offset_limit_usage_rate,
        "total_lines_read": total_lines_read,
        "avg_lines_per_read": avg_lines_per_read,
        "cache_command_count": cache_command_count,
        "verify_command_count": verify_command_count,
        "cache_command_rate": cache_command_rate,
        "verify_command_rate": verify_command_rate,
        "optimization_compliance_score": optimization_compliance_score,
        "estimated_token_savings": estimated_token_savings,
        "mode_violations": mode_violations,
        "example_good_targeted_read": example_good_targeted_read,
        "example_missed_targeting": example_missed_targeting,
        "example_mode_violation": example_mode_violation,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "optimization_mode_detected": "unknown",
        "total_read_calls": 0,
        "reads_with_offset_limit": 0,
        "offset_limit_usage_rate": 0.0,
        "total_lines_read": 0,
        "avg_lines_per_read": 0.0,
        "cache_command_count": 0,
        "verify_command_count": 0,
        "cache_command_rate": 0.0,
        "verify_command_rate": 0.0,
        "optimization_compliance_score": 0.0,
        "estimated_token_savings": 0.0,
        "mode_violations": 0,
        "example_good_targeted_read": {},
        "example_missed_targeting": {},
        "example_mode_violation": {},
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _int(value: object) -> int:
    """Convert value to int, returning 0 if not numeric."""
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _detect_optimization_mode(record: Mapping[str, Any]) -> str:
    """Detect optimization mode from record.

    Checks for mode indicator in record fields.
    """
    # Check explicit mode field
    mode = _string(record.get("optimization_mode"))
    if mode in ("baseline", "optimized"):
        return mode

    # Check environment vars
    env_vars = record.get("environment_vars")
    if isinstance(env_vars, Mapping):
        claude_mode = _string(env_vars.get("CLAUDE_OPTIMIZATION_MODE"))
        if claude_mode in ("baseline", "optimized"):
            return claude_mode

    return "unknown"


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average_int(total: int, count: int) -> float:
    """Calculate average from total and count."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)


def _calculate_compliance_score(
    mode: str,
    offset_usage_rate: float,
    avg_lines: float,
    cache_rate: float,
    verify_rate: float,
    violations: int,
) -> float:
    """Calculate overall optimization compliance score (0-1).

    Scoring depends on detected mode:
    - Optimized: High score for high offset usage, low avg lines, cache/verify usage
    - Baseline: High score for natural behavior (no optimization commands)
    - Unknown: Neutral score
    """
    if mode == "unknown":
        return 0.5  # Neutral when mode can't be determined

    if mode == "baseline":
        # Baseline expects no optimization commands
        if violations > 0:
            return 0.0  # Failed baseline purity
        return 1.0  # Pure baseline

    # Optimized mode scoring
    if mode == "optimized":
        score = 0.0

        # Offset/limit usage component (0-0.40): target 85-90%
        if offset_usage_rate >= 85.0:
            score += 0.40
        else:
            score += (offset_usage_rate / 85.0) * 0.40

        # Average lines component (0-0.30): target <70 lines
        if avg_lines <= 70.0:
            score += 0.30
        else:
            # Penalty for exceeding target
            penalty = min((avg_lines - 70.0) / 100.0, 1.0)
            score += 0.30 * (1.0 - penalty)

        # Cache usage component (0-0.15): target >10%
        if cache_rate >= 10.0:
            score += 0.15
        else:
            score += (cache_rate / 10.0) * 0.15

        # Verify usage component (0-0.15): target >5%
        if verify_rate >= 5.0:
            score += 0.15
        else:
            score += (verify_rate / 5.0) * 0.15

        return round(max(0.0, min(1.0, score)), 3)

    return 0.0


def _estimate_token_savings(
    mode: str,
    avg_lines: float,
    offset_usage_rate: float,
) -> float:
    """Estimate token savings percentage vs baseline.

    Calculation based on:
    - Baseline average: ~237 lines per read
    - Optimized target: <70 lines per read
    - Expected savings: ~58% with 87% offset usage
    """
    if mode != "optimized":
        return 0.0

    # Baseline reference: 237 lines per read
    baseline_avg_lines = 237.0

    if avg_lines <= 0 or avg_lines >= baseline_avg_lines:
        return 0.0

    # Calculate reduction based on average lines
    line_reduction = (baseline_avg_lines - avg_lines) / baseline_avg_lines

    # Weight by offset usage rate (only targeted reads contribute to savings)
    weighted_savings = line_reduction * (offset_usage_rate / 100.0)

    return round(weighted_savings * 100.0, 2)
