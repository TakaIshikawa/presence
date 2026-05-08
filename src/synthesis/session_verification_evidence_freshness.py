"""Session verification evidence freshness analyzer for workflow reports."""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_verification_evidence_freshness(records: object) -> dict[str, Any]:
    """Measure whether verification commands were run after the last edit."""
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

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ValueError(f"record at index {index} is not a dictionary")

        artifact_id = _artifact_id(record, index)
        if artifact_id in seen_artifacts:
            raise ValueError(f"duplicate artifact_id '{artifact_id}' at index {index}")
        seen_artifacts.add(artifact_id)

        last_edit_turn = _turn(record.get("last_edit_turn"), index, "last_edit_turn")
        verification_turn = record.get("verification_turn")
        command = _string(record.get("command"))
        status = _status(record.get("status"), index)

        total_artifacts += 1

        if not command or verification_turn is None:
            missing_verifications += 1
            continue

        verification_turn_value = _turn(verification_turn, index, "verification_turn")

        if verification_turn_value < last_edit_turn:
            stale_verifications += 1
            _example(examples, artifact_id, last_edit_turn, verification_turn_value, command, status)
        else:
            fresh_verifications += 1

    freshness_rate = _freshness_rate(fresh_verifications, total_artifacts)

    return {
        "total_artifacts": total_artifacts,
        "fresh_verifications": fresh_verifications,
        "stale_verifications": stale_verifications,
        "missing_verifications": missing_verifications,
        "freshness_rate": freshness_rate,
        "examples": examples,
    }


def _artifact_id(record: Mapping[str, Any], index: int) -> str:
    value = record.get("artifact_id")
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"record at index {index} has invalid artifact_id")
    return value.strip()


def _turn(value: object, index: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"record at index {index} has invalid {field_name}")
    if value < 0:
        raise ValueError(f"record at index {index} has negative {field_name}")
    return value


def _status(value: object, index: int) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError(f"record at index {index} has invalid status")
    normalized = value.strip().lower()
    valid_statuses = {"passed", "failed", "skipped", ""}
    if normalized and normalized not in valid_statuses:
        raise ValueError(f"record at index {index} has invalid status '{value}'")
    return normalized


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
        examples.append({
            "artifact_id": artifact_id,
            "last_edit_turn": last_edit_turn,
            "verification_turn": verification_turn,
            "command": command,
            "status": status,
        })


def _freshness_rate(fresh: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((fresh / total) * 100.0, 3)
