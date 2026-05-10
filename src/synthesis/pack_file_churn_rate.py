"""Pack file churn rate analyzer for file modification stability.

Analyzes file modification patterns across tasks in execution packs to measure
file stability and identify hotspot files that are modified repeatedly. High
churn indicates poor task scoping, incomplete initial changes, or unstable
requirements.

Churn metrics:
- Churn rate: Percentage of unique files modified more than once
- Avg modifications per file: Total modifications divided by unique files
- Hotspot files: Files modified more than 3 times with their counts
- Single-touch rate: Percentage of files modified exactly once

Stability indicators:
- Low churn rate (<25%): Good task scoping, stable requirements
- High churn rate (>25%): Poor scoping, iterative fixes, requirement drift
- Hotspot files: Files requiring repeated attention across tasks
- High single-touch rate: Files modified once and done
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_pack_file_churn_rate(records: object) -> dict[str, Any]:
    """Analyze file modification churn across tasks in execution packs.

    Tracks file modification frequency to identify churn rate, hotspot files,
    and stability patterns.

    Args:
        records: List of file modification event dictionaries with keys:
            - file_path: Path to the modified file
            - task_id: Optional task identifier for grouping
            - pack_id: Optional pack identifier

    Returns:
        Dict with:
            - total_files: Total number of unique files modified
            - total_modifications: Total number of file modification events
            - churn_rate: Percentage of files modified more than once
            - avg_modifications_per_file: Average modifications per unique file
            - single_touch_rate: Percentage of files modified exactly once
            - hotspot_files: List of files modified >3 times with counts
            - max_modifications: Highest modification count for any file
            - warning: Optional warning if churn_rate exceeds threshold

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of file modification events")

    # Track file modification counts
    file_counter: Counter[str] = Counter()

    for record in records:
        if not isinstance(record, Mapping):
            continue

        file_path = _string(record.get("file_path"))
        if not file_path:
            continue

        # Normalize file path
        file_path = _normalize_path(file_path)
        file_counter[file_path] += 1

    total_modifications = sum(file_counter.values())
    total_files = len(file_counter)

    if total_files == 0:
        return {
            "total_files": 0,
            "total_modifications": 0,
            "churn_rate": 0.0,
            "avg_modifications_per_file": 0.0,
            "single_touch_rate": 0.0,
            "hotspot_files": [],
            "max_modifications": 0,
        }

    # Calculate churn metrics
    files_modified_once = sum(1 for count in file_counter.values() if count == 1)
    files_modified_multiple = sum(1 for count in file_counter.values() if count > 1)
    hotspot_files = [
        {"file_path": file_path, "modification_count": count}
        for file_path, count in file_counter.most_common()
        if count > 3
    ]
    max_modifications = max(file_counter.values()) if file_counter else 0

    churn_rate = _percentage(files_modified_multiple, total_files)
    avg_modifications = round(total_modifications / total_files, 2)
    single_touch_rate = _percentage(files_modified_once, total_files)

    result: dict[str, Any] = {
        "total_files": total_files,
        "total_modifications": total_modifications,
        "churn_rate": churn_rate,
        "avg_modifications_per_file": avg_modifications,
        "single_touch_rate": single_touch_rate,
        "hotspot_files": hotspot_files,
        "max_modifications": max_modifications,
    }

    # Add warning if churn rate exceeds threshold
    if churn_rate > 25.0:
        result["warning"] = (
            f"High churn rate ({churn_rate}%) indicates poor task scoping "
            "or unstable requirements"
        )

    return result


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _normalize_path(file_path: str) -> str:
    """Normalize file path for consistent tracking."""
    # Convert backslashes to forward slashes
    file_path = file_path.replace("\\", "/")
    # Remove leading ./
    if file_path.startswith("./"):
        file_path = file_path[2:]
    return file_path


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
