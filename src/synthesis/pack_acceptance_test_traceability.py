"""Pack acceptance test traceability analyzer."""

from __future__ import annotations

import re
from typing import Any, Mapping


SIGNAL_TYPES = ("test_name", "test_file", "command_output", "summary")


def analyze_pack_acceptance_test_traceability(records: object) -> dict[str, Any]:
    """Link acceptance criteria to observable verification evidence."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task traceability dictionaries")

    total_criteria = 0
    traced_criteria = 0
    untraced_criteria = 0
    signal_type_counts = {signal_type: 0 for signal_type in SIGNAL_TYPES}
    examples: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        evidence = _extract_evidence(record)
        for criterion in _extract_criteria(record):
            total_criteria += 1
            matched_signals = _matched_signals(criterion, evidence)

            if matched_signals:
                traced_criteria += 1
                for signal_type in matched_signals:
                    signal_type_counts[signal_type] += 1
            else:
                untraced_criteria += 1
                if len(examples) < 5:
                    examples.append(
                        {
                            "task_id": _string_or_none(record.get("task_id")),
                            "criterion": criterion,
                            "missing_signal": True,
                        }
                    )

    return {
        "total_criteria": total_criteria,
        "traced_criteria": traced_criteria,
        "untraced_criteria": untraced_criteria,
        "traceability_rate_percent": _percent(traced_criteria, total_criteria),
        "signal_type_counts": signal_type_counts,
        "examples": examples,
    }


def _extract_criteria(record: Mapping[str, Any]) -> list[str]:
    value = record.get("acceptanceCriteria", record.get("acceptance_criteria"))
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        criteria: list[str] = []
        for item in value:
            if isinstance(item, str) and item.strip():
                criteria.append(item.strip())
            elif isinstance(item, Mapping):
                text = item.get("text", item.get("criterion"))
                if isinstance(text, str) and text.strip():
                    criteria.append(text.strip())
        return criteria
    if isinstance(value, Mapping):
        nested = value.get("criteria")
        if isinstance(nested, list):
            return [item.strip() for item in nested if isinstance(item, str) and item.strip()]
    return []


def _extract_evidence(record: Mapping[str, Any]) -> dict[str, str]:
    return {
        "test_name": _join_values(record, ("test_names", "tests", "testName")),
        "test_file": _join_values(record, ("test_files", "test_file_paths", "testFile")),
        "command_output": _join_values(record, ("command_output", "verification_output", "stdout", "stderr")),
        "summary": _join_values(record, ("summary", "final_summary", "final_answer")),
    }


def _join_values(record: Mapping[str, Any], keys: tuple[str, ...]) -> str:
    parts: list[str] = []
    for key in keys:
        value = record.get(key)
        if isinstance(value, str):
            parts.append(value)
        elif isinstance(value, list):
            parts.extend(str(item) for item in value if isinstance(item, (str, int, float)))
        elif isinstance(value, Mapping):
            parts.extend(str(item) for item in value.values() if isinstance(item, (str, int, float)))
    return " ".join(parts)


def _matched_signals(criterion: str, evidence: dict[str, str]) -> list[str]:
    keywords = _keywords(criterion)
    if not keywords:
        return []

    matched: list[str] = []
    for signal_type, text in evidence.items():
        signal_keywords = set(_keywords(text))
        if _has_keyword_overlap(keywords, signal_keywords):
            matched.append(signal_type)
    return matched


def _has_keyword_overlap(criterion_keywords: list[str], signal_keywords: set[str]) -> bool:
    if not signal_keywords:
        return False
    matches = sum(1 for keyword in criterion_keywords if keyword in signal_keywords)
    required = 1 if len(criterion_keywords) <= 2 else 2
    return matches >= required


def _keywords(text: str) -> list[str]:
    stopwords = {
        "accepts",
        "criteria",
        "handles",
        "reports",
        "returns",
        "should",
        "with",
        "from",
        "that",
        "this",
        "when",
        "then",
        "must",
        "list",
        "input",
    }
    words = re.findall(r"[a-zA-Z][a-zA-Z0-9]{2,}", text.lower().replace("_", " "))
    normalized = [word.rstrip("s") for word in words if word not in stopwords]
    return list(dict.fromkeys(normalized))


def _string_or_none(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _percent(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator * 100, 2)
