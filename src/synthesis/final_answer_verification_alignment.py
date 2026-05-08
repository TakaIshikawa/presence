"""Final-answer verification alignment analyzer."""

from __future__ import annotations

import re
from typing import Any, Mapping


COMMAND_RE = re.compile(r"(?:uv run pytest|pytest|npm test|pnpm test|yarn test)[^\n`]*")


def analyze_final_answer_verification_alignment(records: object) -> dict[str, Any]:
    """Compare final-answer verification claims with recorded outcomes."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session summary dictionaries")

    aligned_count = 0
    contradiction_count = 0
    unknown_count = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            unknown_count += 1
            continue
        text = _string(record.get("final_answer")) or _string(record.get("final_message"))
        claim = _claim(text)
        status = _status(record)
        if claim == "unknown":
            unknown_count += 1
            continue
        if _contradicts(claim, status):
            contradiction_count += 1
            _example(examples, record, index, claim, status, _mentioned_command(text))
        else:
            aligned_count += 1

    return {
        "total_records": len(records),
        "aligned_count": aligned_count,
        "contradiction_count": contradiction_count,
        "unknown_count": unknown_count,
        "examples": examples,
    }


def _claim(text: str) -> str:
    lowered = text.lower()
    if any(term in lowered for term in ("not run", "did not run", "wasn't run", "could not run", "unable to run")):
        return "not_run"
    if any(term in lowered for term in ("passed", "passing", "success", "succeeded")):
        return "passed"
    if any(term in lowered for term in ("failed", "failing", "failure")):
        return "failed"
    return "unknown"


def _status(record: Mapping[str, Any]) -> str:
    value = _string(record.get("verification_status")) or _string(record.get("status"))
    lowered = value.lower()
    if any(term in lowered for term in ("pass", "success")):
        return "passed"
    if any(term in lowered for term in ("fail", "error")):
        return "failed"
    if any(term in lowered for term in ("missing", "not_run", "not run", "skipped")):
        return "missing"
    return "missing"


def _contradicts(claim: str, status: str) -> bool:
    if claim == "passed":
        return status != "passed"
    if claim == "failed":
        return status != "failed"
    if claim == "not_run":
        return status == "passed"
    return False


def _mentioned_command(text: str) -> str:
    match = COMMAND_RE.search(text)
    return " ".join(match.group(0).split()) if match else ""


def _example(
    examples: list[dict[str, Any]],
    record: Mapping[str, Any],
    index: int,
    claim: str,
    status: str,
    mentioned_command: str,
) -> None:
    if len(examples) < 5:
        examples.append(
            {
                "session_id": _session_id(record, index),
                "claim": claim,
                "verification_status": status,
                "mentioned_command": mentioned_command,
            }
        )


def _session_id(record: Mapping[str, Any], fallback: int) -> str:
    value = record.get("session_id")
    return value.strip() if isinstance(value, str) and value.strip() else str(fallback)


def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""
