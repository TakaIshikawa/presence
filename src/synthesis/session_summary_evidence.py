"""Session summary evidence analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class SessionEvidence:
    edited_files: tuple[str, ...] = ()
    commands: tuple[str, ...] = ()
    test_outcomes: tuple[str, ...] = ()


@dataclass(frozen=True)
class SessionSummaryEvidenceReport:
    mentioned_files: tuple[str, ...]
    untouched_file_mentions: tuple[str, ...]
    mentioned_commands: tuple[str, ...]
    mentioned_passing_tests: int
    mentioned_failing_tests: int
    missing_evidence_categories: tuple[str, ...]
    evidence_quality: str
    insights: tuple[str, ...]


def analyze_session_summary_evidence(summary: str, evidence: SessionEvidence) -> SessionSummaryEvidenceReport:
    if not isinstance(summary, str):
        raise ValueError("summary must be a string")
    _validate_evidence(evidence)
    normalized = _normalize_text(summary)
    mentioned_files = tuple(path for path in evidence.edited_files if path.lower() in normalized)
    all_file_mentions = tuple(sorted({token.strip(".,:;()[]") for token in summary.split() if "/" in token and "." in token}))
    untouched = tuple(path for path in all_file_mentions if path not in evidence.edited_files)
    mentioned_commands = tuple(command for command in evidence.commands if _command_mentioned(command, normalized))
    passing = sum(1 for outcome in evidence.test_outcomes if "pass" in outcome.lower() and outcome.lower() in normalized)
    failing = sum(1 for outcome in evidence.test_outcomes if "fail" in outcome.lower() and outcome.lower() in normalized)

    missing: list[str] = []
    if evidence.edited_files and not mentioned_files:
        missing.append("files")
    if (evidence.commands or evidence.test_outcomes) and not (mentioned_commands or passing or failing):
        missing.append("verification")
    quality = _evidence_quality(summary, mentioned_files, mentioned_commands, passing, failing, missing)
    return SessionSummaryEvidenceReport(
        mentioned_files=mentioned_files,
        untouched_file_mentions=untouched,
        mentioned_commands=mentioned_commands,
        mentioned_passing_tests=passing,
        mentioned_failing_tests=failing,
        missing_evidence_categories=tuple(missing),
        evidence_quality=quality,
        insights=_evidence_insights(missing, untouched, quality),
    )


def _validate_evidence(evidence: SessionEvidence) -> None:
    if not isinstance(evidence, SessionEvidence):
        raise ValueError("evidence must be a SessionEvidence instance")
    for attr in ("edited_files", "commands", "test_outcomes"):
        value = getattr(evidence, attr)
        if not isinstance(value, tuple) or any(not isinstance(item, str) for item in value):
            raise ValueError(f"{attr} must be a tuple of strings")


def _command_mentioned(command: str, normalized_summary: str) -> bool:
    command_norm = _normalize_text(command)
    command_canonical = _canonical_test_command(command_norm)
    summary_canonical = _canonical_test_command(normalized_summary)
    if command_norm in normalized_summary or command_canonical in summary_canonical:
        return True
    command_target = _test_target(command_canonical)
    summary_targets = _summary_test_targets(summary_canonical)
    if command_target and command_target in summary_canonical:
        return True
    if command_target and summary_targets and command_target not in summary_targets:
        return False
    head = command_canonical.split()[0] if command_canonical else ""
    return bool(
        head
        and head in summary_canonical
        and any(token in command_canonical for token in ("pytest", "test", "build", "mypy"))
    )


def _normalize_text(value: str) -> str:
    return " ".join(value.lower().split())


def _canonical_test_command(value: str) -> str:
    tokens = value.split()
    if tokens[:2] == ["uv", "run"]:
        tokens = tokens[2:]
    if tokens[:3] == ["python", "-m", "pytest"]:
        tokens = ["pytest", *tokens[3:]]
    return " ".join(tokens)


def _test_target(command: str) -> str:
    for token in command.split():
        stripped = token.strip(".,:;()[]")
        if stripped.startswith("tests/") or stripped.startswith("test/"):
            return stripped
    return ""


def _summary_test_targets(summary: str) -> set[str]:
    return {
        token.strip(".,:;()[]")
        for token in summary.split()
        if token.strip(".,:;()[]").startswith(("tests/", "test/"))
    }


def _evidence_quality(
    summary: str,
    files: tuple[str, ...],
    commands: tuple[str, ...],
    passing: int,
    failing: int,
    missing: list[str],
) -> str:
    if not summary.strip():
        return "none"
    if not missing and files and (commands or passing or failing):
        return "strong"
    if files or commands or passing or failing:
        return "partial"
    return "weak"


def _evidence_insights(missing: list[str], untouched: tuple[str, ...], quality: str) -> tuple[str, ...]:
    insights: list[str] = [f"Summary evidence quality is {quality}."]
    if missing:
        insights.append("Missing evidence categories: " + ", ".join(missing) + ".")
    if untouched:
        insights.append("Summary mentioned files that were not touched: " + ", ".join(untouched) + ".")
    return tuple(insights)
