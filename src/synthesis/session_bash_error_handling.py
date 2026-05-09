"""Session Bash error handling and command chaining analyzer.

Analyzes Bash tool usage in session transcripts for error handling discipline,
command chaining correctness, retry patterns, destructive command safety, and
quoting discipline.

Bash safety dimensions:
1. Command chaining discipline:
   - && usage for error propagation
   - ; usage awareness (commands run regardless of failure)
   - Appropriate sequential vs parallel execution

2. Error output inspection:
   - Checking stderr after failed commands
   - Reading error messages before retry
   - Acknowledging failure context

3. Retry patterns after failures:
   - Attempting fixes after errors
   - Modified retry with corrections
   - Giving up vs infinite retry loops

4. Destructive command safety:
   - git push --force checks
   - rm -rf usage
   - git reset --hard warnings
   - Branch protection (main/master)

5. Quoting discipline for paths:
   - Proper quoting of paths with spaces
   - Escaping special characters
   - Preventing word splitting

Quality indicators:
- && used for dependent command chains
- Error output inspected after failures
- Intelligent retry with fixes
- No destructive commands on protected branches
- All paths with spaces properly quoted
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class BashCommand:
    """Represents a Bash command execution."""

    turn_index: int
    command: str
    exit_code: int
    stderr: str
    stdout: str
    uses_and_operator: bool  # Uses &&
    uses_semicolon: bool  # Uses ;
    is_destructive: bool
    destructive_type: str  # "force_push", "rm_rf", "reset_hard", etc.
    has_unquoted_spaces: bool
    following_response: str  # Agent's next response
    was_retried: bool
    retry_had_fix: bool


@dataclass(frozen=True)
class Finding:
    """Represents an error handling finding with severity."""

    severity: str  # critical, warning, info
    category: str
    message: str
    turn_index: int
    example: str  # Concrete command excerpt


@dataclass(frozen=True)
class BashErrorHandlingMetrics:
    """Aggregate metrics for Bash error handling."""

    total_commands: int
    failed_commands: int
    and_operator_usage_count: int
    and_operator_usage_rate: float
    semicolon_usage_count: int
    error_inspection_count: int
    error_inspection_rate: float
    retry_after_failure_count: int
    retry_with_fix_count: int
    retry_success_rate: float
    destructive_commands: int
    destructive_on_protected_branch: int
    unquoted_space_paths: int
    findings_count: int
    critical_findings: int
    warning_findings: int
    info_findings: int


def analyze_session_bash_error_handling(
    commands: Sequence[BashCommand],
) -> tuple[BashErrorHandlingMetrics, Sequence[Finding]]:
    """Analyze Bash error handling and safety patterns.

    Args:
        commands: Sequence of BashCommand instances from session transcript

    Returns:
        Tuple of (metrics, findings) where:
            - metrics: Aggregate statistics about Bash safety
            - findings: Sequence of safety findings with severity

    Raises:
        ValueError: If commands is invalid or contains invalid data
    """
    _validate_commands(commands)

    if not commands:
        return (
            BashErrorHandlingMetrics(
                total_commands=0,
                failed_commands=0,
                and_operator_usage_count=0,
                and_operator_usage_rate=0.0,
                semicolon_usage_count=0,
                error_inspection_count=0,
                error_inspection_rate=0.0,
                retry_after_failure_count=0,
                retry_with_fix_count=0,
                retry_success_rate=0.0,
                destructive_commands=0,
                destructive_on_protected_branch=0,
                unquoted_space_paths=0,
                findings_count=0,
                critical_findings=0,
                warning_findings=0,
                info_findings=0,
            ),
            (),
        )

    findings: list[Finding] = []

    # Count metrics
    failed_commands = sum(1 for cmd in commands if cmd.exit_code != 0)
    and_operator_usage_count = sum(1 for cmd in commands if cmd.uses_and_operator)
    semicolon_usage_count = sum(1 for cmd in commands if cmd.uses_semicolon)
    destructive_commands = sum(1 for cmd in commands if cmd.is_destructive)
    unquoted_space_paths = sum(1 for cmd in commands if cmd.has_unquoted_spaces)

    # Error inspection
    error_inspection_count = 0
    for cmd in commands:
        if cmd.exit_code != 0 and cmd.stderr:
            # Check if following response mentions error
            if _mentions_error(cmd.following_response):
                error_inspection_count += 1

    # Retry patterns
    retry_after_failure_count = sum(1 for cmd in commands if cmd.was_retried)
    retry_with_fix_count = sum(
        1 for cmd in commands if cmd.was_retried and cmd.retry_had_fix
    )

    # Destructive commands on protected branches
    destructive_on_protected_branch = sum(
        1
        for cmd in commands
        if cmd.is_destructive
        and ("main" in cmd.command.lower() or "master" in cmd.command.lower())
        and "force" in cmd.command.lower()
    )

    # Detect findings
    findings.extend(_detect_command_chaining_issues(commands))
    findings.extend(_detect_missing_error_inspection(commands))
    findings.extend(_detect_retry_pattern_issues(commands))
    findings.extend(_detect_destructive_command_issues(commands))
    findings.extend(_detect_quoting_issues(commands))

    # Count findings by severity
    critical_findings = sum(1 for f in findings if f.severity == "critical")
    warning_findings = sum(1 for f in findings if f.severity == "warning")
    info_findings = sum(1 for f in findings if f.severity == "info")

    total = len(commands)
    retry_success_rate = (
        _percentage(retry_with_fix_count, retry_after_failure_count)
        if retry_after_failure_count > 0
        else 0.0
    )

    metrics = BashErrorHandlingMetrics(
        total_commands=total,
        failed_commands=failed_commands,
        and_operator_usage_count=and_operator_usage_count,
        and_operator_usage_rate=_percentage(and_operator_usage_count, total),
        semicolon_usage_count=semicolon_usage_count,
        error_inspection_count=error_inspection_count,
        error_inspection_rate=_percentage(error_inspection_count, failed_commands),
        retry_after_failure_count=retry_after_failure_count,
        retry_with_fix_count=retry_with_fix_count,
        retry_success_rate=retry_success_rate,
        destructive_commands=destructive_commands,
        destructive_on_protected_branch=destructive_on_protected_branch,
        unquoted_space_paths=unquoted_space_paths,
        findings_count=len(findings),
        critical_findings=critical_findings,
        warning_findings=warning_findings,
        info_findings=info_findings,
    )

    return metrics, tuple(findings)


def _validate_commands(commands: Sequence[BashCommand]) -> None:
    """Validate commands structure and content."""
    if not isinstance(commands, (list, tuple)):
        raise ValueError("commands must be a list or tuple")

    for i, cmd in enumerate(commands):
        if not isinstance(cmd, BashCommand):
            raise ValueError(f"commands[{i}] must be a BashCommand instance")

        if not isinstance(cmd.turn_index, int) or isinstance(cmd.turn_index, bool):
            raise ValueError(f"commands[{i}].turn_index must be an integer")

        if cmd.turn_index < 0:
            raise ValueError(f"commands[{i}].turn_index must be non-negative")

        if not isinstance(cmd.command, str):
            raise ValueError(f"commands[{i}].command must be a string")


def _mentions_error(response: str) -> bool:
    """Check if response mentions error or failure."""
    if not response:
        return False

    error_keywords = [
        "error",
        "fail",
        "stderr",
        "exit code",
        "exception",
        "problem",
        "issue",
    ]

    response_lower = response.lower()
    return any(keyword in response_lower for keyword in error_keywords)


def _detect_command_chaining_issues(
    commands: Sequence[BashCommand],
) -> list[Finding]:
    """Detect improper command chaining patterns."""
    findings: list[Finding] = []

    for cmd in commands:
        # Using ; when && would be safer
        if cmd.uses_semicolon and not cmd.uses_and_operator:
            # Check if this is a multi-step operation
            if ";" in cmd.command and ("git" in cmd.command or "npm" in cmd.command):
                findings.append(
                    Finding(
                        severity="warning",
                        category="command_chaining",
                        message=(
                            "Command uses ';' for chaining. Consider '&&' to stop on failure. "
                            "Semicolon runs all commands regardless of errors."
                        ),
                        turn_index=cmd.turn_index,
                        example=cmd.command[:100],
                    )
                )

    return findings


def _detect_missing_error_inspection(
    commands: Sequence[BashCommand],
) -> list[Finding]:
    """Detect failures without error inspection."""
    findings: list[Finding] = []

    for cmd in commands:
        # Command failed with stderr but no acknowledgement
        if cmd.exit_code != 0 and cmd.stderr and not _mentions_error(
            cmd.following_response
        ):
            findings.append(
                Finding(
                    severity="critical",
                    category="error_inspection",
                    message=(
                        f"Command failed (exit {cmd.exit_code}) with stderr output "
                        "but error was not acknowledged or inspected in following response."
                    ),
                    turn_index=cmd.turn_index,
                    example=f"{cmd.command[:80]} -> {cmd.stderr[:100]}",
                )
            )

    return findings


def _detect_retry_pattern_issues(
    commands: Sequence[BashCommand],
) -> list[Finding]:
    """Detect retry patterns without fixes."""
    findings: list[Finding] = []

    for cmd in commands:
        # Retry without modification
        if cmd.was_retried and not cmd.retry_had_fix:
            findings.append(
                Finding(
                    severity="warning",
                    category="retry_discipline",
                    message=(
                        "Command was retried after failure without modification. "
                        "Identical retries are unlikely to succeed."
                    ),
                    turn_index=cmd.turn_index,
                    example=cmd.command[:100],
                )
            )

    return findings


def _detect_destructive_command_issues(
    commands: Sequence[BashCommand],
) -> list[Finding]:
    """Detect dangerous destructive commands."""
    findings: list[Finding] = []

    for cmd in commands:
        if not cmd.is_destructive:
            continue

        # Force push to main/master
        if cmd.destructive_type == "force_push":
            if "main" in cmd.command.lower() or "master" in cmd.command.lower():
                findings.append(
                    Finding(
                        severity="critical",
                        category="destructive_command",
                        message=(
                            "Force push to main/master branch detected. "
                            "This is extremely dangerous and can destroy work."
                        ),
                        turn_index=cmd.turn_index,
                        example=cmd.command,
                    )
                )
            else:
                findings.append(
                    Finding(
                        severity="warning",
                        category="destructive_command",
                        message="Force push detected. Ensure this is intentional.",
                        turn_index=cmd.turn_index,
                        example=cmd.command,
                    )
                )

        # rm -rf
        elif cmd.destructive_type == "rm_rf":
            findings.append(
                Finding(
                    severity="critical",
                    category="destructive_command",
                    message=(
                        "rm -rf detected. Verify path is correct to avoid data loss."
                    ),
                    turn_index=cmd.turn_index,
                    example=cmd.command,
                )
            )

        # git reset --hard
        elif cmd.destructive_type == "reset_hard":
            findings.append(
                Finding(
                    severity="warning",
                    category="destructive_command",
                    message=(
                        "git reset --hard detected. Uncommitted changes will be lost."
                    ),
                    turn_index=cmd.turn_index,
                    example=cmd.command,
                )
            )

    return findings


def _detect_quoting_issues(commands: Sequence[BashCommand]) -> list[Finding]:
    """Detect paths with spaces that aren't quoted."""
    findings: list[Finding] = []

    for cmd in commands:
        if cmd.has_unquoted_spaces:
            findings.append(
                Finding(
                    severity="warning",
                    category="quoting_discipline",
                    message=(
                        "Path with spaces not properly quoted. "
                        'Use double quotes around paths: cd "path with spaces"'
                    ),
                    turn_index=cmd.turn_index,
                    example=cmd.command[:100],
                )
            )

    return findings


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
