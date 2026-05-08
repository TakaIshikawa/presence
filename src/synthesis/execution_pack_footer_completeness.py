"""Execution pack footer completeness analyzer."""

from __future__ import annotations

from typing import Any, Mapping


REQUIRED_SECTIONS = ("outcome", "changed_files", "verification", "residual_risk")
_TEXT_FIELDS = ("final_message", "summary", "batch_footer", "footer")


def analyze_execution_pack_footer_completeness(records: object) -> dict[str, Any]:
    """Check whether task/session summaries include expected completion footer fields."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task/session dictionaries")

    complete = 0
    total_score = 0.0
    section_counts = {name: 0 for name in REQUIRED_SECTIONS}
    weak_examples: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        text = _footer_text(record)
        present = _present_sections(text)
        missing = [name for name in REQUIRED_SECTIONS if name not in present]
        score = _percentage(len(present), len(REQUIRED_SECTIONS))
        total_score += score
        if not missing:
            complete += 1
        for section in present:
            section_counts[section] += 1
        record_id = _record_id(record, index)
        summaries.append({"index": index, "record_id": record_id, "completeness_percentage": score, "missing_fields": missing})
        if missing and len(weak_examples) < 5:
            weak_examples.append({"record_id": record_id, "missing_fields": missing})

    return {
        "total_records": len(records),
        "complete_records": complete,
        "incomplete_records": len(records) - complete,
        "average_completeness_percentage": _percentage(total_score, len(records) * 100),
        "section_counts": section_counts,
        "weak_examples": weak_examples,
        "record_summaries": summaries,
    }


def _footer_text(record: object) -> str:
    if not isinstance(record, Mapping):
        return ""
    parts = [record.get(field) for field in _TEXT_FIELDS]
    return "\n".join(part for part in parts if isinstance(part, str)).lower()


def _present_sections(text: str) -> set[str]:
    present: set[str] = set()
    if any(token in text for token in ("outcome", "status", "completed", "failed")):
        present.add("outcome")
    if any(token in text for token in ("changed files", "files changed", "modified files", "files:")):
        present.add("changed_files")
    if any(token in text for token in ("verification", "verified", "pytest", "test command", "uv run")):
        present.add("verification")
    if any(token in text for token in ("residual risk", "risks", "risk:", "none remaining")):
        present.add("residual_risk")
    return present


def _record_id(record: object, index: int) -> str:
    if isinstance(record, Mapping):
        for key in ("id", "task_id", "session_id", "title"):
            value = record.get(key)
            if value:
                return str(value)
    return str(index)


def _percentage(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
