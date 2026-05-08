"""Final answer changed file summary analyzer for workflow reports."""

from __future__ import annotations

<<<<<<< HEAD
import re
from typing import Any
=======
import os
import re
from typing import Any, Iterable
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD


def analyze_final_answer_changed_file_summary(
    changed_files: object,
    final_answer: object,
) -> dict[str, Any]:
<<<<<<< HEAD
    """Check whether changed files are mentioned in a final answer summary."""
    if changed_files is None:
        changed_files = []

    if not isinstance(changed_files, list):
        raise ValueError("changed_files must be a list of file paths")

    if not isinstance(final_answer, str):
        raise ValueError("final_answer must be a string")

    normalized_files = _normalize_file_list(changed_files)
    normalized_answer = _normalize_whitespace(final_answer)
=======
    """Check whether changed files are mentioned in final answer summary."""
    file_list = _validate_changed_files(changed_files)
    answer_text = _validate_final_answer(final_answer)

    normalized_answer = _normalize_whitespace(answer_text)
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD

    mentioned_files: list[str] = []
    omitted_files: list[str] = []

<<<<<<< HEAD
    for file_path in normalized_files:
=======
    for file_path in file_list:
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
        if _is_mentioned(file_path, normalized_answer):
            mentioned_files.append(file_path)
        else:
            omitted_files.append(file_path)

<<<<<<< HEAD
    changed_file_count = len(normalized_files)
=======
    changed_file_count = len(file_list)
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
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


<<<<<<< HEAD
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

=======
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
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
    return False


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
