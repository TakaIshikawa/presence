"""Edit preflight hygiene analyzer for workflow reports."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable, Mapping


READ_TERMS = ("sed", "rg", "cat", "open", "read_file", "read")
EDIT_TERMS = ("apply_patch", "write", "edit")


def analyze_edit_preflight_hygiene(records: object) -> dict[str, Any]:
    """Flag edits that occur before a read/search of the same file in a session."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of edit event dictionaries")

    read_paths: dict[str, set[str]] = defaultdict(set)
    session_counts: dict[str, dict[str, int]] = {}
    total_edits = 0
    preflighted_edits = 0
    edit_before_read_violations = 0
    missing_path_count = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue
        session_id = _session_id(record)
        paths = _paths(record)
        kind = _event_kind(record)
        counts = session_counts.setdefault(
            session_id,
            {"total_edits": 0, "preflighted_edits": 0, "edit_before_read_violations": 0},
        )

        if kind == "read":
            for path in paths:
                read_paths[session_id].add(path)
            continue
        if kind != "edit":
            continue

        if not paths:
            missing_path_count += 1
            _example(examples, session_id, "", _event_index(record, index), "missing_path")
            continue
        for path in paths:
            total_edits += 1
            counts["total_edits"] += 1
            if path in read_paths[session_id]:
                preflighted_edits += 1
                counts["preflighted_edits"] += 1
            else:
                edit_before_read_violations += 1
                counts["edit_before_read_violations"] += 1
                _example(examples, session_id, path, _event_index(record, index), "edit_before_read")

    return {
        "total_edits": total_edits,
        "preflighted_edits": preflighted_edits,
        "edit_before_read_violations": edit_before_read_violations,
        "missing_path_count": missing_path_count,
        "preflight_percentage": _percentage(preflighted_edits, total_edits),
        "session_counts": session_counts,
        "examples": examples,
    }


def _event_kind(record: Mapping[str, Any]) -> str:
    text = " ".join(
        value.lower()
        for value in (_string(record.get("event_type")), _string(record.get("tool")), _string(record.get("action")))
        if value
    )
    if any(term in text for term in EDIT_TERMS):
        return "edit"
    if any(term in text for term in READ_TERMS):
        return "read"
    return ""


def _paths(record: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("path", "file", "filepath"):
        value = _string(record.get(key))
        if value:
            values.append(value)
    for key in ("paths", "files"):
        values.extend(_string_items(record.get(key)))
    return _dedupe(values)


def _string_items(value: object) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        return [item.strip() for item in value if isinstance(item, str) and item.strip()]
    return []


def _session_id(record: Mapping[str, Any]) -> str:
    value = record.get("session_id")
    return value.strip() if isinstance(value, str) and value.strip() else "unknown"


def _event_index(record: Mapping[str, Any], fallback: int) -> int:
    value = record.get("index", record.get("turn_index", record.get("timestamp")))
    return value if isinstance(value, int) else fallback


def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _dedupe(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


def _example(examples: list[dict[str, Any]], session_id: str, path: str, event_index: int, reason: str) -> None:
    if len(examples) < 5:
        examples.append({"session_id": session_id, "path": path, "event_index": event_index, "reason": reason})


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
