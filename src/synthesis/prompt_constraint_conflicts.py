"""Prompt constraint conflict analyzer."""

from __future__ import annotations

from typing import Any


_CONFLICT_PAIRS = (
    ("no tests", "add tests", "testing_conflict"),
    ("do not add tests", "add tests", "testing_conflict"),
    ("do not browse", "search the web", "browsing_conflict"),
    ("do not edit", "implement", "editing_conflict"),
    ("concise", "comprehensive", "verbosity_conflict"),
    ("highest priority", "low priority", "priority_conflict"),
    ("must", "optional", "priority_conflict"),
    ("required", "nice to have", "priority_conflict"),
)


def analyze_prompt_constraint_conflicts(prompts: object) -> dict[str, Any]:
    """Detect simple contradictory prompt constraints."""
    if prompts is None:
        prompts = []
    if isinstance(prompts, str):
        prompts = [{"text": prompts}]
    if not isinstance(prompts, list):
        raise ValueError("prompts must be a list or string")

    conflicts: list[dict[str, Any]] = []
    for index, prompt in enumerate(prompts):
        text = prompt.get("text", prompt.get("prompt", "")) if isinstance(prompt, dict) else str(prompt)
        normalized = text.lower()
        for left, right, conflict_type in _CONFLICT_PAIRS:
            if left in normalized and right in normalized:
                conflicts.append(
                    {
                        "index": index,
                        "conflict_type": conflict_type,
                        "left_constraint": left,
                        "right_constraint": right,
                    }
                )

    return {
        "prompt_count": len(prompts),
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
        "has_conflicts": bool(conflicts),
    }
