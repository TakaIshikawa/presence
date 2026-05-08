"""Final answer changed file summary analyzer for workflow reports."""

from __future__ import annotations

import re
from typing import Any


def analyze_final_answer_changed_file_summary(
    changed_files: object,
    final_answer: object,
) -> dict[str, Any]:
    """Check whether changed files are mentioned in a final answer summary."""
    if changed_files is None:
        changed_files = []

    if not isinstance(changed_files, list):
        raise ValueError("changed_files must be a list of file paths")

    if not isinstance(final_answer, str):
        raise ValueError("final_answer must be a string")

    normalized_files = _normalize_file_list(changed_files)
    normalized_answer = _normalize_whitespace(final_answer)

    mentioned_files: list[str] = []
    omitted_files: list[str] = []

    for file_path in normalized_files:
        if _is_mentioned(file_path, normalized_answer):
            mentioned_files.append(file_path)
        else:
            omitted_files.append(file_path)

    changed_file_count = len(normalized_files)
    mentioned_file_count = len(mentioned_files)
    omitted_file_count = len(omitted_files)
    mention_rate = _percentage(mentioned_file_count, changed_file_count)

    return {
        "changed_file_count": changed_file_count,
        "mentioned_file_count": mentioned_file_count,
        "omitted_file_count": omitted_file_count,
        "mention_rate": mention_rate,
        "omitted_files": sorted(omitted_files),
    }


def _normalize_file_list(files: list[object]) -> list[str]:
    """Normalize and deduplicate file paths."""
    normalized: list[str] = []
    seen: set[str] = set()

    for item in files:
        if not isinstance(item, str):
            raise ValueError("changed_files must be a list of file paths")
        path = item.strip()
        if path and path not in seen:
            normalized.append(path)
            seen.add(path)

    return normalized


def _normalize_whitespace(text: str) -> str:
    """Normalize repeated whitespace to single spaces."""
    return re.sub(r"\s+", " ", text)


def _is_mentioned(file_path: str, text: str) -> bool:
    """Check if file path or basename is mentioned in text."""
    # Check full path mention
    if file_path in text:
        return True

    # Check basename mention
    basename = file_path.split("/")[-1]
    if basename and basename in text:
        return True

    return False


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
