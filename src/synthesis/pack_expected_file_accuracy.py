"""Pack expected file accuracy analyzer for prediction quality assessment.

Measures how accurately execution packs predict the files they will modify.
Compares expectedFiles arrays against actual changed files from pack outcomes,
using precision, recall, and F1 metrics to evaluate prediction quality.

Accuracy metrics:
- Precision: What percentage of expected files were actually changed
- Recall: What percentage of changed files were in expected list
- F1 score: Harmonic mean of precision and recall
- False positives: Expected files that weren't changed
- False negatives: Changed files that weren't expected

Common prediction patterns:
- Over-prediction: Many expected files not changed (low precision)
- Under-prediction: Many changed files not expected (low recall)
- Balanced: Good precision and recall (high F1)
- Category misses: Commonly missed file types (tests, configs, types)
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Mapping


def analyze_pack_expected_file_accuracy(records: object) -> dict[str, Any]:
    """Analyze accuracy of expected file predictions in execution packs.

    Compares expectedFiles to actual changed files, calculating precision,
    recall, and F1 scores to measure prediction accuracy.

    Args:
        records: List of pack execution dictionaries with keys:
            - pack_id: Execution pack identifier
            - expected_files: List of files expected to be modified
            - changed_files: List of files actually changed during execution
            - task_title: Optional task title for context

    Returns:
        Dict with:
            - total_packs: Total number of pack executions analyzed
            - perfect_predictions: Number of packs with perfect prediction
            - average_precision: Mean precision across all packs
            - average_recall: Mean recall across all packs
            - average_f1: Mean F1 score across all packs
            - total_false_positives: Count of expected but unchanged files
            - total_false_negatives: Count of changed but unexpected files
            - commonly_missed_categories: File categories often missed
            - prediction_examples: Examples of prediction accuracy patterns

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack execution dictionaries")

    total_packs = 0
    perfect_predictions = 0
    precision_sum = 0.0
    recall_sum = 0.0
    f1_sum = 0.0
    total_false_positives = 0
    total_false_negatives = 0
    missed_categories: Counter[str] = Counter()
    prediction_examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        expected_files = _normalize_files(record.get("expected_files"))
        changed_files = _normalize_files(record.get("changed_files"))
        task_title = _string(record.get("task_title"))

        if not expected_files and not changed_files:
            # Skip packs with no files
            continue

        total_packs += 1

        # Calculate sets
        expected_set = set(expected_files)
        changed_set = set(changed_files)

        true_positives = expected_set & changed_set
        false_positives_set = expected_set - changed_set
        false_negatives_set = changed_set - expected_set

        # Calculate metrics
        precision = _calculate_precision(len(true_positives), len(expected_set))
        recall = _calculate_recall(len(true_positives), len(changed_set))
        f1 = _calculate_f1(precision, recall)

        precision_sum += precision
        recall_sum += recall
        f1_sum += f1

        total_false_positives += len(false_positives_set)
        total_false_negatives += len(false_negatives_set)

        # Track missed categories
        for file in false_negatives_set:
            category = _categorize_file(file)
            missed_categories[category] += 1

        # Check for perfect prediction
        if precision == 1.0 and recall == 1.0:
            perfect_predictions += 1

        # Collect examples of interesting patterns
        if len(prediction_examples) < 5 and (precision < 0.75 or recall < 0.75):
            prediction_examples.append({
                "pack_id": pack_id,
                "task_title": task_title or "unknown",
                "precision": precision,
                "recall": recall,
                "f1": f1,
                "false_positives": sorted(false_positives_set)[:3],
                "false_negatives": sorted(false_negatives_set)[:3],
            })

    # Calculate averages
    average_precision = _average(precision_sum, total_packs)
    average_recall = _average(recall_sum, total_packs)
    average_f1 = _average(f1_sum, total_packs)

    # Format commonly missed categories
    commonly_missed = [
        {"category": category, "count": count}
        for category, count in missed_categories.most_common(5)
    ]

    return {
        "total_packs": total_packs,
        "perfect_predictions": perfect_predictions,
        "average_precision": average_precision,
        "average_recall": average_recall,
        "average_f1": average_f1,
        "total_false_positives": total_false_positives,
        "total_false_negatives": total_false_negatives,
        "commonly_missed_categories": commonly_missed,
        "prediction_examples": prediction_examples,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _normalize_files(value: object) -> list[str]:
    """Normalize file list, handling various input types."""
    if isinstance(value, str):
        files = [value]
    elif isinstance(value, (list, tuple)):
        files = [f for f in value if isinstance(f, str)]
    else:
        return []

    # Normalize file paths
    normalized = []
    for file in files:
        file = file.strip()
        if not file:
            continue
        # Convert backslashes to forward slashes
        file = file.replace("\\", "/")
        # Remove leading ./
        if file.startswith("./"):
            file = file[2:]
        normalized.append(file)

    return normalized


def _calculate_precision(true_positives: int, total_expected: int) -> float:
    """Calculate precision: TP / (TP + FP) = TP / total_expected.

    Precision measures: Of all expected files, what percentage were actually changed?
    Returns 1.0 if no files expected (no false predictions possible).
    """
    if total_expected <= 0:
        return 1.0
    return round(true_positives / total_expected, 3)


def _calculate_recall(true_positives: int, total_changed: int) -> float:
    """Calculate recall: TP / (TP + FN) = TP / total_changed.

    Recall measures: Of all changed files, what percentage were in expected list?
    Returns 1.0 if no files changed (no files missed).
    """
    if total_changed <= 0:
        return 1.0
    return round(true_positives / total_changed, 3)


def _calculate_f1(precision: float, recall: float) -> float:
    """Calculate F1 score: 2 * (precision * recall) / (precision + recall).

    F1 is the harmonic mean of precision and recall.
    Returns 0.0 if both precision and recall are 0.
    """
    if precision + recall <= 0:
        return 0.0
    return round(2 * (precision * recall) / (precision + recall), 3)


def _categorize_file(file_path: str) -> str:
    """Categorize a file based on path patterns.

    Categories:
    - test: Test files
    - config: Configuration files
    - types: Type definition files
    - docs: Documentation files
    - source: Regular source code files
    """
    path = Path(file_path)
    name = path.name.lower()
    parts = [p.lower() for p in path.parts]

    # Test files
    if "test" in parts or name.startswith("test_") or name.endswith("_test.py"):
        return "test"

    # Config files
    config_patterns = [
        "config", ".json", ".yaml", ".yml", ".toml", ".ini",
        "package.json", "tsconfig", "pyproject", "setup.py"
    ]
    if any(pattern in name for pattern in config_patterns):
        return "config"

    # Type definition files
    if name.endswith(".d.ts") or "types" in parts or name.endswith("_types.py"):
        return "types"

    # Documentation
    if name.endswith(".md") or "docs" in parts or name == "readme":
        return "docs"

    # Default to source
    return "source"


def _average(total: float, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 3)
