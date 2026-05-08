"""Session blocker resolution trace analyzer."""

from __future__ import annotations

from typing import Any, Iterable, Mapping


RESOLUTION_TERMS = ("resolved", "fixed", "unblocked", "reran", "verified", "workaround", "closed")


def analyze_session_blocker_resolution_trace(records: object) -> dict[str, Any]:
    """Check whether session blockers have visible resolution evidence."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_blockers = 0
    resolved_blockers = 0
    unresolved_blockers = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            unresolved_blockers += 1
            _example(examples, str(index), "", "malformed_record")
            continue
        session_id = _record_id(record, index)
        for blocker in _blockers(record):
            total_blockers += 1
            if _is_resolved(blocker, record):
                resolved_blockers += 1
            else:
                unresolved_blockers += 1
                _example(examples, session_id, _blocker_text(blocker), "missing_resolution_trace")

    return {
        "total_records": len(records),
        "total_blockers": total_blockers,
        "resolved_blockers": resolved_blockers,
        "unresolved_blockers": unresolved_blockers,
        "resolution_percentage": _percentage(resolved_blockers, total_blockers),
        "examples": examples[:5],
    }


def _blockers(record: Mapping[str, Any]) -> list[object]:
    value = record.get("blockers", record.get("blocker_trace", record.get("issues")))
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        return list(value)
    return []


def _is_resolved(blocker: object, record: Mapping[str, Any]) -> bool:
    if isinstance(blocker, Mapping):
        status = str(blocker.get("status", "")).lower()
        if status in {"resolved", "closed", "done"}:
            return True
        text = " ".join(str(blocker.get(field, "")) for field in ("resolution", "evidence", "summary")).lower()
    else:
        text = str(blocker).lower()
    session_resolution = str(record.get("resolution", record.get("final_message", ""))).lower()
    return any(term in text or term in session_resolution for term in RESOLUTION_TERMS)


def _blocker_text(blocker: object) -> str:
    if isinstance(blocker, Mapping):
        for key in ("description", "blocker", "title"):
            value = blocker.get(key)
            if isinstance(value, str):
                return value
    return str(blocker)


def _example(examples: list[dict[str, Any]], session_id: str, blocker: str, reason: str) -> None:
    if len(examples) < 5:
        examples.append({"session_id": session_id, "blocker": blocker, "reason": reason})


def _record_id(record: Mapping[str, Any], index: int) -> str:
    return str(record.get("session_id") or record.get("id") or record.get("task_id") or index)


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
