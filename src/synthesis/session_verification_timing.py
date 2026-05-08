"""Session verification timing analyzer for workflow reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence


VERIFICATION_COMMANDS = {
    "pytest",
    "python -m pytest",
    "npm test",
    "npm run test",
    "yarn test",
    "mypy",
    "pyright",
    "tsc",
    "eslint",
    "ruff",
    "cargo test",
    "go test",
    "npm run build",
    "yarn build",
    "cargo build",
}

EXCESSIVE_DELAY_THRESHOLD_SECONDS = 180.0  # 3 minutes


@dataclass(frozen=True)
class VerificationTurn:
    turn_index: int
    timestamp: datetime
    command: str
    exit_code: int


@dataclass(frozen=True)
class AgentTurn:
    turn_index: int
    timestamp: datetime
    content: str


@dataclass(frozen=True)
class VerificationTimingMetrics:
    total_verifications: int
    acknowledged_verifications: int
    abandoned_verifications: int
    median_latency_seconds: float
    excessive_delay_count: int
    abandonment_rate: float
    acknowledgement_rate: float


@dataclass(frozen=True)
class VerificationTimingExample:
    verification_turn: int
    verification_command: str
    verification_status: str
    agent_turn: int | None
    latency_seconds: float | None
    abandoned: bool


@dataclass(frozen=True)
class SessionVerificationTiming:
    metrics: VerificationTimingMetrics
    examples: tuple[VerificationTimingExample, ...]
    insights: tuple[str, ...]


def analyze_session_verification_timing(
    verification_turns: Sequence[VerificationTurn],
    agent_turns: Sequence[AgentTurn],
) -> SessionVerificationTiming:
    """Analyze latency between verification execution and agent acknowledgment."""
    _validate_verification_turns(verification_turns)
    _validate_agent_turns(agent_turns)

    if not verification_turns:
        metrics = VerificationTimingMetrics(
            total_verifications=0,
            acknowledged_verifications=0,
            abandoned_verifications=0,
            median_latency_seconds=0.0,
            excessive_delay_count=0,
            abandonment_rate=0.0,
            acknowledgement_rate=0.0,
        )
        return SessionVerificationTiming(
            metrics=metrics,
            examples=(),
            insights=("No verification commands executed.",),
        )

    latencies: list[float] = []
    examples: list[VerificationTimingExample] = []
    abandoned_count = 0
    excessive_delay_count = 0

    for verification in verification_turns:
        agent_response = _find_next_agent_response(verification, agent_turns)
        if agent_response is None:
            abandoned_count += 1
            if len(examples) < 5:
                examples.append(
                    VerificationTimingExample(
                        verification_turn=verification.turn_index,
                        verification_command=verification.command,
                        verification_status=_format_exit_code(verification.exit_code),
                        agent_turn=None,
                        latency_seconds=None,
                        abandoned=True,
                    )
                )
        else:
            latency = (agent_response.timestamp - verification.timestamp).total_seconds()
            latencies.append(latency)
            if latency > EXCESSIVE_DELAY_THRESHOLD_SECONDS:
                excessive_delay_count += 1
            if (
                len(examples) < 5
                and (latency > EXCESSIVE_DELAY_THRESHOLD_SECONDS or agent_response is None)
            ):
                examples.append(
                    VerificationTimingExample(
                        verification_turn=verification.turn_index,
                        verification_command=verification.command,
                        verification_status=_format_exit_code(verification.exit_code),
                        agent_turn=agent_response.turn_index,
                        latency_seconds=round(latency, 2),
                        abandoned=False,
                    )
                )

    acknowledged = len(verification_turns) - abandoned_count
    abandonment_rate = _percentage(abandoned_count, len(verification_turns))
    acknowledgement_rate = _percentage(acknowledged, len(verification_turns))

    metrics = VerificationTimingMetrics(
        total_verifications=len(verification_turns),
        acknowledged_verifications=acknowledged,
        abandoned_verifications=abandoned_count,
        median_latency_seconds=_median_latency(latencies),
        excessive_delay_count=excessive_delay_count,
        abandonment_rate=abandonment_rate,
        acknowledgement_rate=acknowledgement_rate,
    )

    return SessionVerificationTiming(
        metrics=metrics,
        examples=tuple(examples),
        insights=_generate_insights(metrics),
    )


def _validate_verification_turns(turns: Sequence[VerificationTurn]) -> None:
    """Validate verification turn structure and ordering."""
    if not isinstance(turns, (list, tuple)):
        raise ValueError("verification_turns must be a list or tuple")

    last_turn = -1
    last_timestamp: datetime | None = None

    for turn in turns:
        if not isinstance(turn, VerificationTurn):
            raise ValueError("verification_turns must contain VerificationTurn instances")
        if not isinstance(turn.turn_index, int) or isinstance(turn.turn_index, bool):
            raise ValueError("turn_index must be an integer")
        if turn.turn_index < 0:
            raise ValueError("turn_index must be non-negative")
        if not isinstance(turn.timestamp, datetime):
            raise ValueError("timestamp must be a datetime")
        if not isinstance(turn.command, str):
            raise ValueError("command must be a string")
        if not turn.command.strip():
            raise ValueError("command must not be empty")
        if not isinstance(turn.exit_code, int):
            raise ValueError("exit_code must be an integer")

        if turn.turn_index <= last_turn:
            raise ValueError("verification_turns must have strictly increasing turn_index")
        if last_timestamp is not None and turn.timestamp < last_timestamp:
            raise ValueError("verification_turns must be ordered by timestamp")

        last_turn = turn.turn_index
        last_timestamp = turn.timestamp


def _validate_agent_turns(turns: Sequence[AgentTurn]) -> None:
    """Validate agent turn structure and ordering."""
    if not isinstance(turns, (list, tuple)):
        raise ValueError("agent_turns must be a list or tuple")

    last_turn = -1
    last_timestamp: datetime | None = None

    for turn in turns:
        if not isinstance(turn, AgentTurn):
            raise ValueError("agent_turns must contain AgentTurn instances")
        if not isinstance(turn.turn_index, int) or isinstance(turn.turn_index, bool):
            raise ValueError("turn_index must be an integer")
        if turn.turn_index < 0:
            raise ValueError("turn_index must be non-negative")
        if not isinstance(turn.timestamp, datetime):
            raise ValueError("timestamp must be a datetime")
        if not isinstance(turn.content, str):
            raise ValueError("content must be a string")
        if not turn.content.strip():
            raise ValueError("content must not be empty")

        if turn.turn_index <= last_turn:
            raise ValueError("agent_turns must have strictly increasing turn_index")
        if last_timestamp is not None and turn.timestamp < last_timestamp:
            raise ValueError("agent_turns must be ordered by timestamp")

        last_turn = turn.turn_index
        last_timestamp = turn.timestamp


def _find_next_agent_response(
    verification: VerificationTurn,
    agent_turns: Sequence[AgentTurn],
) -> AgentTurn | None:
    """Find the first agent turn after verification that acknowledges the result."""
    for agent_turn in agent_turns:
        if agent_turn.turn_index > verification.turn_index:
            # Check if agent acknowledges verification in content
            if _acknowledges_verification(verification, agent_turn):
                return agent_turn
    return None


def _acknowledges_verification(
    verification: VerificationTurn,
    agent_turn: AgentTurn,
) -> bool:
    """Check if agent turn acknowledges verification result."""
    content_lower = agent_turn.content.lower()

    # Look for verification-related keywords
    verification_keywords = [
        "test",
        "pass",
        "fail",
        "error",
        "build",
        "verify",
        "check",
        "pytest",
        "mypy",
        "lint",
        "type",
    ]

    return any(keyword in content_lower for keyword in verification_keywords)


def _format_exit_code(exit_code: int) -> str:
    """Format exit code as pass/fail status."""
    return "pass" if exit_code == 0 else "fail"


def _median_latency(latencies: list[float]) -> float:
    """Calculate median latency from list of latency values."""
    if not latencies:
        return 0.0
    ordered = sorted(latencies)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return round(ordered[midpoint], 2)
    return round((ordered[midpoint - 1] + ordered[midpoint]) / 2, 2)


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage with 2 decimal precision."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _generate_insights(metrics: VerificationTimingMetrics) -> tuple[str, ...]:
    """Generate human-readable insights from metrics."""
    if metrics.total_verifications == 0:
        return ("No verification commands executed.",)

    insights = []

    insights.append(
        f"{metrics.acknowledged_verifications} of {metrics.total_verifications} "
        f"verifications acknowledged by agent ({metrics.acknowledgement_rate}%)."
    )

    if metrics.abandoned_verifications > 0:
        insights.append(
            f"{metrics.abandoned_verifications} verification results ignored "
            f"({metrics.abandonment_rate}% abandonment rate)."
        )

    if metrics.excessive_delay_count > 0:
        insights.append(
            f"{metrics.excessive_delay_count} verifications had excessive delay "
            f"(>{EXCESSIVE_DELAY_THRESHOLD_SECONDS}s) before acknowledgment."
        )

    if metrics.median_latency_seconds > 0:
        insights.append(
            f"Median verification-to-acknowledgment latency: "
            f"{metrics.median_latency_seconds}s."
        )

    return tuple(insights)
