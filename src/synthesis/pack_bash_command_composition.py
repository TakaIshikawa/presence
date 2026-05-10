"""Pack Bash command composition and shell best practices analyzer.

Analyzes Bash tool usage patterns across Claude Code execution packs
to evaluate command composition safety, efficiency, and adherence to
shell best practices and tool guidance.

Bash composition metrics:
- Sequential chaining: % commands using && vs ;
- Parallel execution: % parallel bash opportunities taken
- Path quoting: % paths properly quoted (detecting unquoted paths with spaces)
- Heredoc usage: % heredocs used for multi-line input (git commits, etc.)
- Description quality: Average length, active voice compliance
- Tool preference: % commands violating tool-preference rules

Anti-patterns detected:
- Sequential commands that could be parallelized
- Unquoted paths containing spaces
- Missing command descriptions
- Overly verbose descriptions (>100 chars for simple commands)
- Using bash for file operations (grep/cat/find instead of Grep/Read/Glob)

Quality indicators:
- High && usage: >80% sequential chains use && (not ;)
- High parallel rate: >60% parallel bash opportunities taken
- High quoting compliance: >95% paths properly quoted
- High heredoc usage: >90% multi-line inputs use heredoc
- Concise descriptions: Average 5-10 words for simple commands
- Low tool violations: <10% commands use bash grep/cat/find
"""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_pack_bash_command_composition(records: object) -> dict[str, Any]:
    """Analyze Bash command composition and shell best practices.

    Evaluates command safety, efficiency, and adherence to guidelines.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Pack identifier
            - sessions: List of session dictionaries with:
                - session_id: Session identifier
                - messages: List of assistant message dictionaries with:
                    - message_index: Message number
                    - tool_calls: List of tool call dictionaries with:
                        - tool_name: Name of tool (Bash, etc.)
                        - command: Bash command string
                        - description: Optional command description

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - total_bash_calls: Total Bash tool invocations
            - sequential_chaining_and_usage: % using && for chaining
            - sequential_chaining_semicolon_usage: % using ; for chaining
            - parallel_bash_opportunities: Count of potential parallel calls
            - parallel_bash_opportunities_taken: Count where parallel was used
            - parallel_execution_rate: % parallel opportunities taken
            - total_paths_in_commands: Total file paths detected
            - properly_quoted_paths: Paths with proper quoting
            - path_quoting_compliance: % paths properly quoted
            - unquoted_space_paths: Count of unquoted paths with spaces
            - heredoc_usage_count: Count of heredocs used
            - multi_line_opportunities: Count of multi-line scenarios
            - heredoc_usage_rate: % multi-line using heredoc
            - total_descriptions: Total commands with descriptions
            - avg_description_length: Mean description length in words
            - overly_verbose_descriptions: Count of descriptions >100 chars
            - missing_descriptions: Count of commands without descriptions
            - tool_preference_violations: Count of bash grep/cat/find usage
            - tool_violation_rate: % commands violating tool preference
            - example_good_composition: Example of best practices
            - example_missed_parallel: Example of missed parallelization
            - example_quoting_violation: Example of unquoted path
            - example_tool_violation: Example of tool preference violation

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_bash_calls = 0

    # Chaining analysis
    commands_using_and = 0
    commands_using_semicolon = 0

    # Parallel execution tracking
    parallel_bash_opportunities = 0
    parallel_bash_opportunities_taken = 0

    # Path quoting analysis
    total_paths_in_commands = 0
    properly_quoted_paths = 0
    unquoted_space_paths = 0

    # Heredoc analysis
    heredoc_usage_count = 0
    multi_line_opportunities = 0

    # Description quality
    description_lengths: list[int] = []
    missing_descriptions = 0
    overly_verbose_descriptions = 0

    # Tool preference violations
    tool_preference_violations = 0

    # Examples
    example_good_composition: dict[str, Any] = {}
    example_missed_parallel: dict[str, Any] = {}
    example_quoting_violation: dict[str, Any] = {}
    example_tool_violation: dict[str, Any] = {}

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        sessions = _get_sessions(record)
        for session in sessions:
            if not isinstance(session, Mapping):
                continue

            messages = _get_messages(session)

            # Track consecutive bash calls for parallel detection
            prev_bash_commands: list[str] = []

            for message in messages:
                if not isinstance(message, Mapping):
                    continue

                tool_calls = _get_tool_calls(message)
                if not tool_calls:
                    prev_bash_commands = []
                    continue

                # Count Bash calls in this message
                bash_calls_in_message = [
                    tc for tc in tool_calls
                    if isinstance(tc, Mapping) and _string(tc.get("tool_name")) == "Bash"
                ]

                if not bash_calls_in_message:
                    prev_bash_commands = []
                    continue

                # Check for parallel Bash execution (multiple bash in one message)
                if len(bash_calls_in_message) > 1:
                    parallel_bash_opportunities_taken += len(bash_calls_in_message) - 1

                # Process each Bash call
                for bash_call in bash_calls_in_message:
                    total_bash_calls += 1

                    command = _string(bash_call.get("command", ""))
                    description = _string(bash_call.get("description", ""))

                    if not command:
                        continue

                    # Analyze chaining
                    if "&&" in command:
                        commands_using_and += 1
                    if ";" in command and ";" not in command.split("&&")[0] if "&&" in command else True:
                        # Only count ; if it's not part of a heredoc or inside &&
                        if not _is_heredoc_semicolon(command):
                            commands_using_semicolon += 1

                    # Analyze heredoc usage
                    if "<<" in command or "EOF" in command or "cat <<" in command:
                        heredoc_usage_count += 1

                    # Detect multi-line opportunities (git commit, multi-line strings)
                    if "git commit" in command or "-m \"" in command:
                        multi_line_opportunities += 1

                    # Path quoting analysis
                    paths = _extract_paths(command)
                    for path in paths:
                        total_paths_in_commands += 1

                        if _is_path_properly_quoted(path, command):
                            properly_quoted_paths += 1
                        elif " " in path:
                            unquoted_space_paths += 1
                            if not example_quoting_violation:
                                example_quoting_violation = {
                                    "command": command,
                                    "path": path,
                                }

                    # Description quality analysis
                    if description:
                        word_count = len(description.split())
                        description_lengths.append(word_count)

                        if len(description) > 100:
                            overly_verbose_descriptions += 1
                    else:
                        missing_descriptions += 1

                    # Tool preference violations
                    if _has_tool_preference_violation(command):
                        tool_preference_violations += 1
                        if not example_tool_violation:
                            example_tool_violation = {
                                "command": command,
                                "violation_type": _detect_violation_type(command),
                            }

                    # Capture good example
                    if (
                        not example_good_composition
                        and description
                        and len(description.split()) <= 10
                        and "&&" in command
                        and not _has_tool_preference_violation(command)
                    ):
                        example_good_composition = {
                            "command": command,
                            "description": description,
                        }

                # Check for missed parallel opportunities
                current_bash_commands = [
                    _string(tc.get("command", ""))
                    for tc in bash_calls_in_message
                ]

                if prev_bash_commands and len(bash_calls_in_message) == 1:
                    # Single bash call following previous single bash call
                    if len(prev_bash_commands) == 1:
                        if _are_bash_commands_independent(prev_bash_commands[0], current_bash_commands[0]):
                            parallel_bash_opportunities += 1
                            if not example_missed_parallel:
                                example_missed_parallel = {
                                    "prev_command": prev_bash_commands[0],
                                    "current_command": current_bash_commands[0],
                                }

                # Update prev for next iteration
                if len(bash_calls_in_message) == 1:
                    prev_bash_commands = current_bash_commands
                else:
                    prev_bash_commands = []

    # Calculate metrics
    sequential_chaining_and_usage = _percentage(commands_using_and, total_bash_calls)
    sequential_chaining_semicolon_usage = _percentage(commands_using_semicolon, total_bash_calls)

    parallel_execution_rate = _percentage(
        parallel_bash_opportunities_taken,
        parallel_bash_opportunities + parallel_bash_opportunities_taken
    )

    path_quoting_compliance = _percentage(properly_quoted_paths, total_paths_in_commands)
    heredoc_usage_rate = _percentage(heredoc_usage_count, multi_line_opportunities)

    avg_description_length = _average([float(x) for x in description_lengths])

    tool_violation_rate = _percentage(tool_preference_violations, total_bash_calls)

    return {
        "total_packs": total_packs,
        "total_bash_calls": total_bash_calls,
        "sequential_chaining_and_usage": sequential_chaining_and_usage,
        "sequential_chaining_semicolon_usage": sequential_chaining_semicolon_usage,
        "parallel_bash_opportunities": parallel_bash_opportunities,
        "parallel_bash_opportunities_taken": parallel_bash_opportunities_taken,
        "parallel_execution_rate": parallel_execution_rate,
        "total_paths_in_commands": total_paths_in_commands,
        "properly_quoted_paths": properly_quoted_paths,
        "path_quoting_compliance": path_quoting_compliance,
        "unquoted_space_paths": unquoted_space_paths,
        "heredoc_usage_count": heredoc_usage_count,
        "multi_line_opportunities": multi_line_opportunities,
        "heredoc_usage_rate": heredoc_usage_rate,
        "total_descriptions": len(description_lengths),
        "avg_description_length": avg_description_length,
        "overly_verbose_descriptions": overly_verbose_descriptions,
        "missing_descriptions": missing_descriptions,
        "tool_preference_violations": tool_preference_violations,
        "tool_violation_rate": tool_violation_rate,
        "example_good_composition": example_good_composition,
        "example_missed_parallel": example_missed_parallel,
        "example_quoting_violation": example_quoting_violation,
        "example_tool_violation": example_tool_violation,
    }


def _get_sessions(record: Mapping[str, Any]) -> list[Any]:
    """Extract sessions list from pack record."""
    sessions = record.get("sessions")
    if isinstance(sessions, list):
        return sessions
    return []


def _get_messages(session: Mapping[str, Any]) -> list[Any]:
    """Extract messages list from session."""
    messages = session.get("messages")
    if isinstance(messages, list):
        return messages
    return []


def _get_tool_calls(message: Mapping[str, Any]) -> list[Any]:
    """Extract tool calls list from message."""
    tool_calls = message.get("tool_calls")
    if isinstance(tool_calls, list):
        return tool_calls
    return []


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_paths(command: str) -> list[str]:
    """Extract file paths from command string.

    Simple heuristic: looks for common path patterns.
    """
    paths = []

    # Pattern 1: quoted paths "path/to/file" or 'path/to/file'
    quoted_pattern = r'["\']([^"\']+/[^"\']+)["\']'
    paths.extend(re.findall(quoted_pattern, command))

    # Pattern 2: unquoted paths (word containing /)
    # Skip common flags like -A/-B/-C
    unquoted_pattern = r'(?:^|\s)([A-Za-z0-9_\-./]+/[A-Za-z0-9_\-./\s]+)(?:\s|$)'
    unquoted_matches = re.findall(unquoted_pattern, command)
    for match in unquoted_matches:
        # Filter out flags and obvious non-paths
        if not match.startswith("-") and match not in paths:
            paths.append(match)

    return paths


def _is_path_properly_quoted(path: str, command: str) -> bool:
    """Check if path is properly quoted in command.

    A path with spaces MUST be quoted. Paths without spaces are OK unquoted.
    """
    if " " not in path:
        return True  # No spaces, quoting optional

    # Check if path appears within quotes in command
    quoted_path_patterns = [
        f'"{path}"',
        f"'{path}'",
    ]

    return any(pattern in command for pattern in quoted_path_patterns)


def _is_heredoc_semicolon(command: str) -> bool:
    """Check if semicolon is part of heredoc, not command separator."""
    # Simple heuristic: if command has heredoc markers and ;, assume it's in heredoc
    return "<<" in command and "EOF" in command


def _has_tool_preference_violation(command: str) -> bool:
    """Check if command violates tool preference rules.

    Should use specialized tools instead:
    - Grep instead of grep/rg
    - Read instead of cat/head/tail
    - Glob instead of find
    - Edit instead of sed/awk
    - Write instead of echo >/cat <<EOF
    """
    violations = [
        r'\bgrep\b',
        r'\brg\b',
        r'\bcat\b(?!\s+<<)',  # cat not for heredoc
        r'\bhead\b',
        r'\btail\b',
        r'\bfind\b',
        r'\bsed\b',
        r'\bawk\b',
        r'\becho\s+[^|]+>\s*\w+',  # echo > file (writing)
    ]

    for pattern in violations:
        if re.search(pattern, command):
            return True

    return False


def _detect_violation_type(command: str) -> str:
    """Detect type of tool preference violation."""
    if re.search(r'\bgrep\b|\brg\b', command):
        return "should_use_grep_tool"
    if re.search(r'\bcat\b(?!\s+<<)', command):
        return "should_use_read_tool"
    if re.search(r'\bfind\b', command):
        return "should_use_glob_tool"
    if re.search(r'\bsed\b|\bawk\b', command):
        return "should_use_edit_tool"
    if re.search(r'\becho\s+[^|]+>\s*\w+', command):
        return "should_use_write_tool"
    return "unknown_violation"


def _are_bash_commands_independent(cmd1: str, cmd2: str) -> bool:
    """Check if two bash commands are independent and could run in parallel.

    Returns False if commands likely have dependencies.
    """
    if not cmd1 or not cmd2:
        return False

    # If either command uses chaining (&&, ;), they're complex - assume dependent
    if "&&" in cmd1 or ";" in cmd1 or "&&" in cmd2 or ";" in cmd2:
        return False

    # If commands are identical, not independent
    if cmd1 == cmd2:
        return False

    # Extract file paths from both commands
    files1 = set(_extract_paths(cmd1))
    files2 = set(_extract_paths(cmd2))

    # If they share files, might be dependent
    if files1 & files2:
        return False

    # Simple heuristic: if one command writes and another reads, dependent
    writes_or_modifies = ["git add", "git commit", "echo >", "sed -i", "rm ", "mv ", "cp "]

    cmd1_modifies = any(w in cmd1 for w in writes_or_modifies)
    cmd2_reads = any(r in cmd2 for r in ["cat ", "grep ", "git diff", "git status"])

    if cmd1_modifies and cmd2_reads:
        return False

    # Otherwise, assume independent (e.g., two ls commands on different dirs)
    return True


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
