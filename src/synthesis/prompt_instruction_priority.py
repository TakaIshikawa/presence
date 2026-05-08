"""Prompt instruction priority analyzer."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Sequence


HARD_TERMS = ("must", "never", "avoid")
SOFT_TERMS = ("should", "optional", "prefer")
ORDERING_TERMS = ("first", "second", "third", "priority", "rank", "order", "before", "after")


@dataclass(frozen=True)
class PromptInstructionRecord:
    prompt_id: str
    text: str


@dataclass(frozen=True)
class PromptInstructionPriorityReport:
    total_prompts: int
    hard_constraints: int
    soft_preferences: int
    overloaded_prompts: tuple[str, ...]
    priority_quality: str
    insights: tuple[str, ...]


def analyze_prompt_instruction_priority(records: Sequence[PromptInstructionRecord]) -> PromptInstructionPriorityReport:
    _validate_records(records)
    hard = 0
    soft = 0
    overloaded: list[str] = []
    for record in records:
        text = record.text.lower()
        hard_count = sum(_term_count(text, term) for term in HARD_TERMS)
        soft_count = sum(_term_count(text, term) for term in SOFT_TERMS)
        hard += hard_count
        soft += soft_count
        has_ordering = any(re.search(rf"\b{re.escape(term)}\b", text) for term in ORDERING_TERMS)
        if hard_count >= 3 and not has_ordering:
            overloaded.append(record.prompt_id)
    quality = _priority_quality(len(records), hard, soft, overloaded)
    return PromptInstructionPriorityReport(
        total_prompts=len(records),
        hard_constraints=hard,
        soft_preferences=soft,
        overloaded_prompts=tuple(overloaded),
        priority_quality=quality,
        insights=_priority_insights(overloaded, hard, soft),
    )


def _validate_records(records: Sequence[PromptInstructionRecord]) -> None:
    if not isinstance(records, (list, tuple)):
        raise ValueError("records must be a list or tuple")
    seen_prompt_ids: set[str] = set()
    for index, record in enumerate(records):
        if not isinstance(record, PromptInstructionRecord):
            raise ValueError(f"records[{index}] must be a PromptInstructionRecord")
        if not isinstance(record.prompt_id, str) or not record.prompt_id.strip():
            raise ValueError("prompt_id must be a non-empty string")
        if record.prompt_id in seen_prompt_ids:
            raise ValueError(f"duplicate prompt ids are not supported: {record.prompt_id}")
        seen_prompt_ids.add(record.prompt_id)
        if not isinstance(record.text, str):
            raise ValueError("text must be a string")


def _term_count(text: str, term: str) -> int:
    return len(re.findall(rf"\b{re.escape(term)}\b", text))


def _priority_quality(total: int, hard: int, soft: int, overloaded: list[str]) -> str:
    if not total or (hard == 0 and soft == 0):
        return "clear"
    if overloaded:
        return "overloaded"
    if hard and soft:
        return "mixed"
    return "clear"


def _priority_insights(overloaded: list[str], hard: int, soft: int) -> tuple[str, ...]:
    if overloaded:
        return ("Prompts with multiple hard constraints and no ordering language: " + ", ".join(overloaded) + ".",)
    if hard or soft:
        return (f"Found {hard} hard constraint(s) and {soft} soft preference(s).",)
    return ("No explicit priority markers found.",)
