"""Final answer changed file summary analyzer for workflow reports."""

from __future__ import annotations

import os
from typing import Any


def analyze_final_answer_changed_file_summary(
    changed_files: object,
    final_answer: object,
) -> dict[str, Any]:
    """Check whether changed files are mentioned in a final answer summary."""
    if not isinstance(final_answer, str):
        raise ValueError("final_answer must be a string")

    files = _normalize_changed_files(changed_files)
    normalized_answer = _normalize_text(final_answer)

    mentioned_files: set[str] = set()
    omitted_files: list[str] = []

    for file_path in files:
        if _is_mentioned(file_path, normalized_answer):
            mentioned_files.add(file_path)
        else:
            omitted_files.append(file_path)

    return {
        "changed_file_count": len(files),
        "mentioned_file_count": len(mentioned_files),
        "omitted_file_count": len(omitted_files),
        "mention_rate": _percentage(len(mentioned_files), len(files)),
        "omitted_files": sorted(omitted_files),
    }


def _normalize_changed_files(value: object) -> list[str]:
    """Convert changed_files input to a normalized list of paths."""
    if value is None:
        return []
    if isinstance(value, str):
        if not value.strip():
            return []
        raise ValueError("changed_files must be a list, not a string")
    if not isinstance(value, list):
        raise ValueError("changed_files must be a list")

    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("changed_files must contain only strings")
        if item.strip():
            normalized.append(item.strip())

    return normalized


def _normalize_text(text: str) -> str:
    """Normalize whitespace in text for matching."""
    import re

    return re.sub(r"\s+", " ", text.strip())


def _is_mentioned(file_path: str, normalized_answer: str) -> bool:
    """Check if file_path is mentioned either as full path or basename."""
    if not normalized_answer:
        return False

    # Check full path
    if file_path in normalized_answer:
        return True

    # Check basename
    basename = os.path.basename(file_path)
    if basename and basename in normalized_answer:
        return True

    return False


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
