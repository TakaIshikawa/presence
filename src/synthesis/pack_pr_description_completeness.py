"""Pack PR description completeness analyzer."""

from __future__ import annotations

import re
from typing import Any, Mapping


COMPONENTS = (
    "summary",
    "test_evidence",
    "changed_files",
    "user_visible_behavior",
    "risk_rollback",
    "follow_up_items",
)
DESCRIPTION_KEYS = ("pr_body", "pull_request_body", "merge_request_body", "final_answer", "summary")


def analyze_pack_pr_description_completeness(records: object) -> dict[str, Any]:
    """Analyze whether PR/final branch descriptions cover expected components."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of PR description dictionaries")

    total_descriptions = 0
    complete_descriptions = 0
    component_hits = {component: 0 for component in COMPONENTS}
    missing_component_counts = {component: 0 for component in COMPONENTS}
    examples: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue
        description = _extract_description(record)
        if not description:
            continue

        total_descriptions += 1
        covered = _covered_components(description, record)
        missing = [component for component in COMPONENTS if component not in covered]

        for component in covered:
            component_hits[component] += 1
        for component in missing:
            missing_component_counts[component] += 1

        if not missing:
            complete_descriptions += 1
        elif len(examples) < 5:
            examples.append(
                {
                    "pack_id": _string_or_none(record.get("pack_id")),
                    "missing_components": missing,
                    "description_excerpt": description[:180],
                }
            )

    return {
        "total_descriptions": total_descriptions,
        "complete_descriptions": complete_descriptions,
        "completeness_rate_percent": _percent(complete_descriptions, total_descriptions),
        "component_coverage": {
            component: _percent(count, total_descriptions)
            for component, count in component_hits.items()
        },
        "missing_component_counts": missing_component_counts,
        "examples": examples,
    }


def _extract_description(record: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in DESCRIPTION_KEYS:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    return "\n".join(parts)


def _covered_components(description: str, record: Mapping[str, Any]) -> set[str]:
    text = description.lower()
    covered: set[str] = set()

    if re.search(r"\b(summary|overview|changed|implemented|adds?|updates?|fixes?)\b", text):
        covered.add("summary")
    if re.search(r"\b(pytest|test(?:s|ed|ing)?|verification|verified|passes?|npm test|cargo test)\b", text):
        covered.add("test_evidence")
    if re.search(r"\b[\w./-]+\.(?:py|js|ts|tsx|jsx|go|rs|rb|java|md|yml|yaml|json|toml|css|html)\b", description):
        covered.add("changed_files")
    if re.search(r"\b(user-visible|user visible|behavior|cli|ui|output|api|workflow|users?)\b", text):
        covered.add("user_visible_behavior")
    if re.search(r"\b(risk|rollback|revert|migration|compatib|fallback|safe to rollback)\b", text):
        covered.add("risk_rollback")
    if re.search(r"\b(follow[- ]?up|todo|next step|remaining|future work|none)\b", text):
        covered.add("follow_up_items")

    changed_files = record.get("changed_files")
    if isinstance(changed_files, list) and changed_files:
        covered.add("changed_files")

    return covered


def _string_or_none(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _percent(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator * 100, 2)
