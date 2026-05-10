"""Pack verification command diversity and coverage analyzer.

Analyzes verification command diversity and test coverage discipline across
Claude Code execution packs. Measures breadth of verification tools used,
density of verification commands, timing distribution, and test scope patterns.

Verification diversity metrics:
- Unique verification tools: Count of distinct verification types
- Verification density: Ratio of verification to total tool calls
- Verification timing: Early/mid/late session distribution
- Test scope breadth: Ratio of targeted vs broad test suites

Verification patterns detected:
- Over-reliance on single verification method
- Missing type checking when typed code edited
- Missing build verification for core infrastructure changes
- Test-before-implementation discipline (TDD signals)

Quality indicators:
- High tool diversity (>3 verification types)
- Good verification density (>15% of tool calls)
- Balanced timing distribution (verification throughout session)
- Appropriate test scope (mix of targeted and broad verification)
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


# Common verification tools by category
VERIFICATION_TOOLS = {
    "test": ["pytest", "npm test", "cargo test", "go test", "jest", "mocha", "vitest"],
    "type": ["mypy", "pyright", "tsc", "typescript", "flow"],
    "lint": ["eslint", "pylint", "ruff", "flake8", "clippy", "golint"],
    "build": ["npm run build", "cargo build", "make", "go build", "tsc"],
    "format": ["prettier", "black", "rustfmt", "gofmt"],
}


def analyze_pack_verification_command_diversity(records: object) -> dict[str, Any]:
    """Analyze verification command diversity and patterns across packs.

    Evaluates breadth of verification tools, verification density, timing,
    and test scope to identify gaps in verification discipline.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Pack identifier
            - sessions: List of session dictionaries with:
                - session_id: Session identifier
                - tool_calls: List of tool call dictionaries with:
                    - tool_name: Name of tool
                    - command: Bash command (for verification commands)
                    - turn_index: Turn number for timing
                    - file_path: File being edited (to detect verification gaps)
                - total_turns: Total turns in session
                - edited_files: List of files edited in session

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - total_verification_commands: Total verification commands
            - unique_verification_tools: Count of distinct verification types
            - verification_tools_used: List of verification tools identified
            - verification_density: Percentage of tool calls that are verification
            - verification_by_category: Breakdown by verification category
            - early_verification_rate: Percentage of verifications in first third
            - mid_verification_rate: Percentage in middle third
            - late_verification_rate: Percentage in last third
            - targeted_test_count: Count of targeted test runs (single file)
            - broad_test_count: Count of broad test runs (full suite)
            - test_scope_breadth: Ratio of targeted to broad tests
            - missing_type_check_count: Typed code edited without type check
            - missing_build_check_count: Core files edited without build
            - tdd_signals_count: Tests run before implementation
            - verification_coverage_score: Overall score 0-1
            - over_reliance_on_single_tool: Boolean if >75% one tool
            - dominant_verification_tool: Most used verification tool

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_tool_calls = 0
    total_verification_commands = 0

    # Track verification tools
    verification_tools_counter: Counter[str] = Counter()
    verification_categories: Counter[str] = Counter()

    # Track timing
    early_verifications = 0
    mid_verifications = 0
    late_verifications = 0

    # Track test scope
    targeted_test_count = 0
    broad_test_count = 0

    # Track gaps
    missing_type_check_count = 0
    missing_build_check_count = 0
    tdd_signals_count = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        sessions = _get_sessions(record)
        for session in sessions:
            if not isinstance(session, Mapping):
                continue

            tool_calls = _get_tool_calls(session)
            total_turns = _int(session.get("total_turns", len(tool_calls)))
            edited_files = _get_edited_files(session)

            session_tool_count = 0
            session_verification_count = 0
            has_type_check = False
            has_build_check = False
            edits_typed_code = False
            edits_core_code = False

            for tool_call in tool_calls:
                if not isinstance(tool_call, Mapping):
                    continue

                session_tool_count += 1
                tool_name = _string(tool_call.get("tool_name", ""))

                if tool_name == "Bash":
                    command = _string(tool_call.get("command", ""))
                    verification_info = _identify_verification(command)

                    if verification_info:
                        session_verification_count += 1
                        verification_tools_counter[verification_info["tool"]] += 1
                        verification_categories[verification_info["category"]] += 1

                        # Track timing
                        turn_index = _int(tool_call.get("turn_index", 0))
                        timing = _calculate_timing_bucket(turn_index, total_turns)
                        if timing == "early":
                            early_verifications += 1
                        elif timing == "mid":
                            mid_verifications += 1
                        elif timing == "late":
                            late_verifications += 1

                        # Track test scope
                        if verification_info["category"] == "test":
                            if _is_targeted_test(command):
                                targeted_test_count += 1
                            else:
                                broad_test_count += 1

                        # Track type and build checks
                        if verification_info["category"] == "type":
                            has_type_check = True
                        if verification_info["category"] == "build":
                            has_build_check = True

                        # TDD signal: verification before Edit
                        if turn_index > 0 and _has_prior_edit(tool_calls, turn_index):
                            pass  # Normal: verification after edit
                        elif turn_index == 0 or not _has_prior_edit(tool_calls, turn_index):
                            tdd_signals_count += 1

                elif tool_name == "Edit":
                    file_path = _string(tool_call.get("file_path", ""))
                    if _is_typed_file(file_path):
                        edits_typed_code = True
                    if _is_core_file(file_path):
                        edits_core_code = True

            # Check for gaps
            if edits_typed_code and not has_type_check:
                missing_type_check_count += 1
            if edits_core_code and not has_build_check:
                missing_build_check_count += 1

            total_tool_calls += session_tool_count
            total_verification_commands += session_verification_count

    # Calculate metrics
    unique_verification_tools = len(verification_tools_counter)
    verification_tools_used = list(verification_tools_counter.keys())
    verification_density = _percentage(total_verification_commands, total_tool_calls)

    # Timing distribution
    total_verifications = early_verifications + mid_verifications + late_verifications
    early_verification_rate = _percentage(early_verifications, total_verifications)
    mid_verification_rate = _percentage(mid_verifications, total_verifications)
    late_verification_rate = _percentage(late_verifications, total_verifications)

    # Test scope breadth
    total_tests = targeted_test_count + broad_test_count
    test_scope_breadth = _ratio(targeted_test_count, total_tests)

    # Over-reliance detection
    dominant_tool = ""
    over_reliance = False
    if verification_tools_counter:
        dominant_tool, dominant_count = verification_tools_counter.most_common(1)[0]
        if total_verification_commands > 0:
            over_reliance = (dominant_count / total_verification_commands) > 0.75

    # Calculate coverage score
    verification_coverage_score = _calculate_coverage_score(
        unique_verification_tools,
        verification_density,
        missing_type_check_count,
        missing_build_check_count,
        total_packs,
    )

    return {
        "total_packs": total_packs,
        "total_verification_commands": total_verification_commands,
        "unique_verification_tools": unique_verification_tools,
        "verification_tools_used": verification_tools_used,
        "verification_density": verification_density,
        "verification_by_category": dict(verification_categories),
        "early_verification_rate": early_verification_rate,
        "mid_verification_rate": mid_verification_rate,
        "late_verification_rate": late_verification_rate,
        "targeted_test_count": targeted_test_count,
        "broad_test_count": broad_test_count,
        "test_scope_breadth": test_scope_breadth,
        "missing_type_check_count": missing_type_check_count,
        "missing_build_check_count": missing_build_check_count,
        "tdd_signals_count": tdd_signals_count,
        "verification_coverage_score": verification_coverage_score,
        "over_reliance_on_single_tool": over_reliance,
        "dominant_verification_tool": dominant_tool,
    }


def _get_sessions(record: Mapping[str, Any]) -> list[Any]:
    """Extract sessions list from pack record."""
    sessions = record.get("sessions")
    if isinstance(sessions, list):
        return sessions
    return []


def _get_tool_calls(session: Mapping[str, Any]) -> list[Any]:
    """Extract tool calls list from session."""
    tool_calls = session.get("tool_calls")
    if isinstance(tool_calls, list):
        return tool_calls
    return []


def _get_edited_files(session: Mapping[str, Any]) -> list[str]:
    """Extract edited files list from session."""
    edited_files = session.get("edited_files")
    if isinstance(edited_files, list):
        return [_string(f) for f in edited_files]
    return []


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


def _identify_verification(command: str) -> dict[str, str] | None:
    """Identify if command is a verification command and its type.

    Returns:
        Dict with 'tool' and 'category' or None if not verification
    """
    command_lower = command.lower()

    for category, tools in VERIFICATION_TOOLS.items():
        for tool in tools:
            if tool in command_lower:
                return {"tool": tool, "category": category}

    return None


def _is_targeted_test(command: str) -> bool:
    """Check if test command targets a specific file/test."""
    # Heuristic: command contains "test_" or "tests/" followed by specific path
    return "test_" in command or ("tests/" in command and "::" in command)


def _is_typed_file(file_path: str) -> bool:
    """Check if file is likely typed code (TypeScript, Python with types)."""
    return file_path.endswith((".ts", ".tsx", ".py"))


def _is_core_file(file_path: str) -> bool:
    """Check if file is core infrastructure (src, lib, build config)."""
    return any(
        part in file_path.lower()
        for part in ["src/", "lib/", "package.json", "cargo.toml", "setup.py", "pyproject.toml"]
    )


def _has_prior_edit(tool_calls: list[Any], current_turn: int) -> bool:
    """Check if there was an Edit before the current turn."""
    for tool_call in tool_calls:
        if not isinstance(tool_call, Mapping):
            continue
        turn = _int(tool_call.get("turn_index", 0))
        if turn >= current_turn:
            break
        if _string(tool_call.get("tool_name", "")) == "Edit":
            return True
    return False


def _calculate_timing_bucket(turn_index: int, total_turns: int) -> str:
    """Calculate timing bucket (early/mid/late) for a turn."""
    if total_turns <= 0:
        return "early"
    ratio = turn_index / total_turns
    if ratio < 0.33:
        return "early"
    elif ratio < 0.67:
        return "mid"
    else:
        return "late"


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _ratio(numerator: int | float, denominator: int | float) -> float:
    """Calculate ratio, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 2)


def _calculate_coverage_score(
    unique_tools: int,
    verification_density: float,
    missing_type_checks: int,
    missing_build_checks: int,
    total_packs: int,
) -> float:
    """Calculate overall verification coverage score (0-1).

    Scoring components:
    - Tool diversity (0-0.30): More tools = better coverage
    - Verification density (0-0.30): >15% is good
    - Type check discipline (0-0.20): Penalty for missing checks
    - Build check discipline (0-0.20): Penalty for missing checks
    """
    # Tool diversity component (target: >3 tools)
    if unique_tools >= 3:
        diversity_component = 0.30
    else:
        diversity_component = (unique_tools / 3.0) * 0.30

    # Verification density component (target: >15%)
    if verification_density >= 15.0:
        density_component = 0.30
    else:
        density_component = (verification_density / 15.0) * 0.30

    # Type check discipline (penalty for missing checks)
    if total_packs == 0:
        type_component = 0.20
    else:
        missing_rate = missing_type_checks / total_packs
        penalty = min(missing_rate, 1.0)
        type_component = 0.20 * (1.0 - penalty)

    # Build check discipline (penalty for missing checks)
    if total_packs == 0:
        build_component = 0.20
    else:
        missing_rate = missing_build_checks / total_packs
        penalty = min(missing_rate, 1.0)
        build_component = 0.20 * (1.0 - penalty)

    score = diversity_component + density_component + type_component + build_component
    return round(max(0.0, min(1.0, score)), 3)
