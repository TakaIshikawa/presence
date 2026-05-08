"""Final answer changed file summary analyzer for workflow reports."""

from __future__ import annotations

import os
import re
from typing import Any, Iterable


def analyze_final_answer_changed_file_summary(
    changed_files: object,
    final_answer: object,
) -> dict[str, Any]:
    """Check whether changed files are mentioned in final answer summary."""
    file_list = _validate_changed_files(changed_files)
    answer_text = _validate_final_answer(final_answer)

    normalized_answer = _normalize_whitespace(answer_text)

    mentioned_files: list[str] = []
    omitted_files: list[str] = []

    for file_path in file_list:
        if _is_mentioned(file_path, normalized_answer):
            mentioned_files.append(file_path)
        else:
            omitted_files.append(file_path)

    changed_file_count = len(file_list)
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


def _validate_changed_files(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, dict)):
        result: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str):
                raise ValueError("changed_files must contain only strings")
            normalized = item.strip()
            if normalized and normalized not in seen:
                result.append(normalized)
                seen.add(normalized)
        return result
    raise ValueError("changed_files must be a list of file paths or None")


def _validate_final_answer(value: object) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("final_answer must be a string or None")
    return value


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _is_mentioned(file_path: str, normalized_answer: str) -> bool:
    # Check full path mention
    if file_path in normalized_answer:
        return True
    # Check basename mention
    basename = os.path.basename(file_path)
    if basename and basename in normalized_answer:
        return True
    return False


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
