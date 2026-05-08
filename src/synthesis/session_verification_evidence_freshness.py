"""Session verification evidence freshness analyzer for workflow reports."""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_verification_evidence_freshness(records: object) -> dict[str, Any]:
    """Measure whether verification commands were run after the last edit they claim to validate."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of verification record dictionaries")

    total_artifacts = 0
    fresh_verifications = 0
    stale_verifications = 0
    missing_verifications = 0
    examples: list[dict[str, Any]] = []
    seen_artifacts: set[str] = set()

    for record in records:
        if not isinstance(record, Mapping):
            raise ValueError("records must be a list of verification record dictionaries")

        artifact_id = _string(record.get("artifact_id"))
        if not artifact_id:
            raise ValueError("artifact_id must be a non-empty string")

        if artifact_id in seen_artifacts:
            raise ValueError("duplicate artifact_id found in records")
        seen_artifacts.add(artifact_id)

        last_edit_turn = record.get("last_edit_turn")
        verification_turn = record.get("verification_turn")
        command = _string(record.get("command"))
        status = _string(record.get("status"))

        if not isinstance(last_edit_turn, int) or isinstance(last_edit_turn, bool):
            raise ValueError("last_edit_turn must be an integer")
        if last_edit_turn < 0:
            raise ValueError("last_edit_turn must be non-negative")

        if status and status not in ("pass", "fail", "skip"):
            raise ValueError("status must be one of: pass, fail, skip, or empty")

        total_artifacts += 1

        if not command or verification_turn is None:
            missing_verifications += 1
            continue

        if not isinstance(verification_turn, int) or isinstance(verification_turn, bool):
            raise ValueError("verification_turn must be an integer")
        if verification_turn < 0:
            raise ValueError("verification_turn must be non-negative")

        if verification_turn >= last_edit_turn:
            fresh_verifications += 1
        else:
            stale_verifications += 1
            _example(
                examples,
                artifact_id,
                last_edit_turn,
                verification_turn,
                command,
                status,
            )

    freshness_rate = _percentage(fresh_verifications, total_artifacts, decimals=3)

    return {
        "total_artifacts": total_artifacts,
        "fresh_verifications": fresh_verifications,
        "stale_verifications": stale_verifications,
        "missing_verifications": missing_verifications,
        "freshness_rate": freshness_rate,
        "examples": examples,
    }


def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _example(
    examples: list[dict[str, Any]],
    artifact_id: str,
    last_edit_turn: int,
    verification_turn: int,
    command: str,
    status: str,
) -> None:
    if len(examples) < 5:
        examples.append(
            {
                "artifact_id": artifact_id,
                "last_edit_turn": last_edit_turn,
                "verification_turn": verification_turn,
                "command": command,
                "status": status,
            }
        )


def _percentage(numerator: int, denominator: int, decimals: int = 2) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, decimals)
