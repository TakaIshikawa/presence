"""Agent follow-up quality analyzer for workflow hygiene reports."""

from __future__ import annotations

from typing import Any, Iterable, Mapping


ACTION_TERMS = ("will", "next", "follow up", "todo", "verify", "fix", "investigate", "rerun")
VAGUE_TERMS = ("later", "soon", "eventually", "maybe", "should")


def analyze_agent_followup_quality(records: object) -> dict[str, Any]:
    """Score whether agent follow-ups are concrete and traceable."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of follow-up dictionaries")

    total_followups = 0
    actionable_followups = 0
    weak_followups = 0
    missing_owner_count = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            weak_followups += 1
            _example(examples, str(index), "", "malformed_record")
            continue
        followups = _followups(record)
        if not followups:
            weak_followups += 1
            _example(examples, _record_id(record, index), "", "missing_followup")
            continue
        for followup in followups:
            total_followups += 1
            reasons = _weak_reasons(followup, record)
            if reasons:
                weak_followups += 1
                if "missing_owner" in reasons:
                    missing_owner_count += 1
                _example(examples, _record_id(record, index), followup, reasons[0])
            else:
                actionable_followups += 1

    return {
        "total_records": len(records),
        "total_followups": total_followups,
        "actionable_followups": actionable_followups,
        "weak_followups": weak_followups,
        "missing_owner_count": missing_owner_count,
        "quality_percentage": _percentage(actionable_followups, total_followups),
        "examples": examples[:5],
    }


def _followups(record: Mapping[str, Any]) -> list[str]:
    value = record.get("followups", record.get("follow_ups", record.get("next_steps")))
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        return [item.strip() for item in value if isinstance(item, str) and item.strip()]
    return []


def _weak_reasons(followup: str, record: Mapping[str, Any]) -> list[str]:
    lowered = followup.lower()
    reasons: list[str] = []
    if not any(term in lowered for term in ACTION_TERMS):
        reasons.append("not_actionable")
    if any(term in lowered for term in VAGUE_TERMS) and not any(char.isdigit() for char in lowered):
        reasons.append("vague_timing")
    owner = record.get("owner", record.get("assignee"))
    if not (isinstance(owner, str) and owner.strip()) and "owner:" not in lowered:
        reasons.append("missing_owner")
    return reasons


def _example(examples: list[dict[str, Any]], record_id: str, followup: str, reason: str) -> None:
    if len(examples) < 5:
        examples.append({"record_id": record_id, "followup": followup, "reason": reason})


def _record_id(record: Mapping[str, Any], index: int) -> str:
    return str(record.get("id") or record.get("task_id") or record.get("session_id") or index)


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
