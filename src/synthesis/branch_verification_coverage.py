"""Branch verification coverage analyzer for workflow reports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence


@dataclass(frozen=True)
class SessionSummary:
    session_id: str
    verification_commands: list[str]
    files_changed: list[str]


@dataclass(frozen=True)
class VerificationCoverageMetrics:
    total_sessions: int
    sessions_with_verification: int
    sessions_without_verification: int
    total_files_changed: int
    files_with_verification: int
    files_without_verification: int
    verification_to_change_ratio: float
    session_verification_rate: float
    file_verification_rate: float
    command_diversity_score: float


@dataclass(frozen=True)
class VerificationCoverageExample:
    session_id: str
    files_changed: list[str]
    verification_commands: list[str]
    missing_verification: bool


@dataclass(frozen=True)
class BranchVerificationCoverage:
    metrics: VerificationCoverageMetrics
    verification_commands_by_type: dict[str, int]
    examples: tuple[VerificationCoverageExample, ...]
    insights: tuple[str, ...]


VERIFICATION_COMMAND_PATTERNS = {
    "test": ["pytest", "python -m pytest", "npm test", "yarn test", "pnpm test", "go test", "cargo test"],
    "typecheck": ["mypy", "pyright", "tsc", "flow"],
    "lint": ["eslint", "ruff", "pylint", "flake8", "clippy"],
    "build": ["npm run build", "yarn build", "cargo build", "go build", "make"],
}


def analyze_branch_verification_coverage(
    sessions: Sequence[SessionSummary],
) -> BranchVerificationCoverage:
    """Measure verification command coverage across all sessions in a branch."""
    _validate_sessions(sessions)

    if not sessions:
        metrics = VerificationCoverageMetrics(
            total_sessions=0,
            sessions_with_verification=0,
            sessions_without_verification=0,
            total_files_changed=0,
            files_with_verification=0,
            files_without_verification=0,
            verification_to_change_ratio=0.0,
            session_verification_rate=0.0,
            file_verification_rate=0.0,
            command_diversity_score=0.0,
        )
        return BranchVerificationCoverage(
            metrics=metrics,
            verification_commands_by_type={},
            examples=(),
            insights=("No sessions provided.",),
        )

    sessions_with_verification = 0
    all_files_changed: set[str] = set()
    files_with_verification: set[str] = set()
    examples: list[VerificationCoverageExample] = []
    verification_commands_by_type: dict[str, int] = {}

    for session in sessions:
        has_verification = len(session.verification_commands) > 0

        if has_verification:
            sessions_with_verification += 1
            # Files in sessions with verification are considered "verified"
            for file_path in session.files_changed:
                files_with_verification.add(file_path)

            # Categorize verification commands
            for command in session.verification_commands:
                command_type = _categorize_verification_command(command)
                verification_commands_by_type[command_type] = (
                    verification_commands_by_type.get(command_type, 0) + 1
                )
        else:
            # Add example of session without verification
            if len(examples) < 5 and session.files_changed:
                examples.append(
                    VerificationCoverageExample(
                        session_id=session.session_id,
                        files_changed=session.files_changed[:5],  # Truncate long lists
                        verification_commands=[],
                        missing_verification=True,
                    )
                )

        # Track all changed files
        for file_path in session.files_changed:
            all_files_changed.add(file_path)

    sessions_without_verification = len(sessions) - sessions_with_verification
    total_files_changed = len(all_files_changed)
    files_without_verification_count = total_files_changed - len(files_with_verification)

    # Calculate diversity score (0-1 based on how many command types are used)
    diversity_score = len(verification_commands_by_type) / 4.0  # 4 types: test, typecheck, lint, build
    diversity_score = min(diversity_score, 1.0)

    verification_to_change_ratio = _ratio(sessions_with_verification, total_files_changed)
    session_verification_rate = _percentage(sessions_with_verification, len(sessions))
    file_verification_rate = _percentage(len(files_with_verification), total_files_changed)

    metrics = VerificationCoverageMetrics(
        total_sessions=len(sessions),
        sessions_with_verification=sessions_with_verification,
        sessions_without_verification=sessions_without_verification,
        total_files_changed=total_files_changed,
        files_with_verification=len(files_with_verification),
        files_without_verification=files_without_verification_count,
        verification_to_change_ratio=verification_to_change_ratio,
        session_verification_rate=session_verification_rate,
        file_verification_rate=file_verification_rate,
        command_diversity_score=round(diversity_score, 2),
    )

    return BranchVerificationCoverage(
        metrics=metrics,
        verification_commands_by_type=verification_commands_by_type,
        examples=tuple(examples),
        insights=_generate_insights(metrics, verification_commands_by_type),
    )


def _validate_sessions(sessions: Sequence[SessionSummary]) -> None:
    """Validate session structure."""
    if not isinstance(sessions, (list, tuple)):
        raise ValueError("sessions must be a list or tuple")

    for session in sessions:
        if not isinstance(session, SessionSummary):
            raise ValueError("sessions must contain SessionSummary instances")
        if not isinstance(session.session_id, str):
            raise ValueError("session_id must be a string")
        if not isinstance(session.verification_commands, list):
            raise ValueError("verification_commands must be a list")
        if not isinstance(session.files_changed, list):
            raise ValueError("files_changed must be a list")

        for command in session.verification_commands:
            if not isinstance(command, str):
                raise ValueError("verification_commands must contain strings")

        for file_path in session.files_changed:
            if not isinstance(file_path, str):
                raise ValueError("files_changed must contain strings")


def _categorize_verification_command(command: str) -> str:
    """Categorize verification command by type."""
    command_lower = command.lower()

    for command_type, patterns in VERIFICATION_COMMAND_PATTERNS.items():
        for pattern in patterns:
            if pattern in command_lower:
                return command_type

    return "other"


def _ratio(numerator: int, denominator: int) -> float:
    """Calculate ratio with 2 decimal precision."""
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 2)


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage with 2 decimal precision."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _generate_insights(
    metrics: VerificationCoverageMetrics,
    commands_by_type: dict[str, int],
) -> tuple[str, ...]:
    """Generate human-readable insights from metrics."""
    if metrics.total_sessions == 0:
        return ("No sessions provided.",)

    insights = []

    insights.append(
        f"{metrics.sessions_with_verification} of {metrics.total_sessions} sessions "
        f"included verification ({metrics.session_verification_rate}%)."
    )

    if metrics.sessions_without_verification > 0:
        insights.append(
            f"{metrics.sessions_without_verification} sessions had no verification commands."
        )

    if metrics.total_files_changed > 0:
        insights.append(
            f"{metrics.files_with_verification} of {metrics.total_files_changed} changed files "
            f"had verification coverage ({metrics.file_verification_rate}%)."
        )

    # Command diversity insight
    if metrics.command_diversity_score < 0.5 and metrics.sessions_with_verification > 0:
        insights.append(
            f"Low command diversity score ({metrics.command_diversity_score}): "
            f"only {len(commands_by_type)} verification types used."
        )

    # Missing command types
    missing_types = []
    for command_type in ["test", "typecheck", "lint", "build"]:
        if command_type not in commands_by_type:
            missing_types.append(command_type)

    if missing_types and metrics.sessions_with_verification > 0:
        insights.append(
            f"Missing verification types: {', '.join(missing_types)}."
        )

    # Coverage warnings
    if metrics.session_verification_rate < 50.0 and metrics.total_sessions >= 3:
        insights.append(
            "Low session coverage: fewer than half of sessions include verification."
        )

    if metrics.file_verification_rate < 50.0 and metrics.total_files_changed >= 5:
        insights.append(
            "Low file coverage: fewer than half of changed files have verification."
        )

    return tuple(insights)
