"""Pack expected file accuracy analyzer for execution pack predictions.

Analyzes how accurately execution packs predict the files they will modify by
comparing expectedFiles arrays against actual changed files from pack outcomes.
Calculates precision, recall, and F1 score metrics to measure prediction quality.

Prediction accuracy metrics:
- Precision: Percentage of expected files that were actually modified
- Recall: Percentage of actually modified files that were expected
- F1 Score: Harmonic mean of precision and recall
- False Positives: Expected files that were not modified
- False Negatives: Modified files that were not expected

File type patterns:
- Commonly missed: Tests, configs, types, documentation
- False positives: Over-estimated scope
- Directory drift: Modified files in unexpected directories
- Related module drift: Changes to related but unplanned modules
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_expected_file_accuracy(records: object) -> dict[str, Any]:
    """Analyze accuracy of file predictions in execution packs.

    Compares expectedFiles arrays to actual changed files from pack outcomes,
    calculating precision, recall, and F1 score metrics.

    Args:
        records: List of pack execution dictionaries with keys:
            - pack_id: Execution pack identifier
            - expected_files: List of files expected to be modified
            - actual_files: List of files actually modified during execution
            - task_title: Optional task title for context

    Returns:
        Dict with:
            - total_packs: Total number of pack executions analyzed
            - avg_precision: Average precision across all packs
            - avg_recall: Average recall across all packs
            - avg_f1_score: Average F1 score across all packs
            - perfect_predictions_count: Packs with perfect predictions (F1 = 1.0)
            - total_false_positives: Total count of false positive predictions
            - total_false_negatives: Total count of false negative predictions
            - commonly_missed_categories: File categories often not predicted
            - prediction_examples: Examples of prediction accuracy/inaccuracy

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack execution dictionaries")

    total_packs = 0
    perfect_predictions_count = 0
    precision_sum = 0.0
    recall_sum = 0.0
    f1_sum = 0.0
    total_false_positives = 0
    total_false_negatives = 0

    # Track missed file categories
    missed_categories: dict[str, int] = {}
    prediction_examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        expected_files = _normalize_files(record.get("expected_files"))
        actual_files = _normalize_files(record.get("actual_files"))
        task_title = _string(record.get("task_title"))

        total_packs += 1

        # Calculate sets
        expected_set = set(expected_files)
        actual_set = set(actual_files)

        true_positives = expected_set & actual_set
        false_positives_set = expected_set - actual_set
        false_negatives_set = actual_set - expected_set

        # Calculate metrics
        precision = _calculate_precision(len(true_positives), len(expected_set))
        recall = _calculate_recall(len(true_positives), len(actual_set))
        f1_score = _calculate_f1_score(precision, recall)

        precision_sum += precision
        recall_sum += recall
        f1_sum += f1_score

        if f1_score == 1.0 and len(expected_set) > 0:
            perfect_predictions_count += 1

        # Track false positives and negatives
        total_false_positives += len(false_positives_set)
        total_false_negatives += len(false_negatives_set)

        # Categorize missed files
        for missed_file in false_negatives_set:
            category = _categorize_file(missed_file)
            missed_categories[category] = missed_categories.get(category, 0) + 1

        # Collect examples
        if f1_score < 1.0 or len(prediction_examples) < 3:
            _add_prediction_example(
                prediction_examples,
                pack_id,
                task_title,
                precision,
                recall,
                f1_score,
                sorted(false_positives_set),
                sorted(false_negatives_set),
                sorted(true_positives),
            )

    avg_precision = _average(precision_sum, total_packs)
    avg_recall = _average(recall_sum, total_packs)
    avg_f1_score = _average(f1_sum, total_packs)

    # Sort missed categories by frequency
    commonly_missed_categories = sorted(
        [{"category": cat, "count": count} for cat, count in missed_categories.items()],
        key=lambda x: x["count"],
        reverse=True,
    )[:5]

    return {
        "total_packs": total_packs,
        "avg_precision": avg_precision,
        "avg_recall": avg_recall,
        "avg_f1_score": avg_f1_score,
        "perfect_predictions_count": perfect_predictions_count,
        "total_false_positives": total_false_positives,
        "total_false_negatives": total_false_negatives,
        "commonly_missed_categories": commonly_missed_categories,
        "prediction_examples": prediction_examples[:5],
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _normalize_files(value: object) -> list[str]:
    """Normalize file list, handling various input types."""
    if isinstance(value, str):
        # Single file as string
        files = [value]
    elif isinstance(value, (list, tuple)):
        # List or tuple of files
        files = [f for f in value if isinstance(f, str)]
    else:
        return []

    # Normalize file paths
    normalized = []
    for file in files:
        file = file.strip()
        if not file:
            continue
        # Convert backslashes to forward slashes for Windows paths
        file = file.replace("\\", "/")
        # Remove leading ./ if present
        if file.startswith("./"):
            file = file[2:]
        normalized.append(file)

    return normalized


def _calculate_precision(true_positives: int, total_expected: int) -> float:
    """Calculate precision: TP / (TP + FP) = TP / total_expected.

    Precision measures what percentage of expected files were actually modified.
    Returns 1.0 if no files expected (edge case).
    """
    if total_expected == 0:
        return 1.0
    return round(true_positives / total_expected, 3)


def _calculate_recall(true_positives: int, total_actual: int) -> float:
    """Calculate recall: TP / (TP + FN) = TP / total_actual.

    Recall measures what percentage of actually modified files were expected.
    Returns 1.0 if no files modified (edge case).
    """
    if total_actual == 0:
        return 1.0
    return round(true_positives / total_actual, 3)


def _calculate_f1_score(precision: float, recall: float) -> float:
    """Calculate F1 score: harmonic mean of precision and recall.

    F1 = 2 * (precision * recall) / (precision + recall)
    Returns 0.0 if both precision and recall are 0.0.
    """
    if precision + recall == 0.0:
        return 0.0
    return round(2 * (precision * recall) / (precision + recall), 3)


def _categorize_file(file_path: str) -> str:
    """Categorize file by type for missed file analysis.

    Categories:
    - test: Test files
    - config: Configuration files
    - types: Type definition files
    - docs: Documentation files
    - other: Other files
    """
    file_lower = file_path.lower()

    if "test" in file_lower or file_lower.endswith("_test.py"):
        return "test"
    elif any(
        file_lower.endswith(ext)
        for ext in [".json", ".yaml", ".yml", ".toml", ".ini", ".conf", ".config"]
    ):
        return "config"
    elif file_lower.endswith((".d.ts", ".pyi")) or "/types/" in file_lower:
        return "types"
    elif any(
        file_lower.endswith(ext)
        for ext in [".md", ".rst", ".txt", ".adoc"]
    ):
        return "docs"
    else:
        return "other"


def _add_prediction_example(
    examples: list[dict[str, Any]],
    pack_id: str,
    task_title: str,
    precision: float,
    recall: float,
    f1_score: float,
    false_positives: list[str],
    false_negatives: list[str],
    true_positives: list[str],
) -> None:
    """Add a prediction example if we have fewer than 5."""
    if len(examples) < 5:
        examples.append({
            "pack_id": pack_id,
            "task_title": task_title or "unknown",
            "precision": precision,
            "recall": recall,
            "f1_score": f1_score,
            "false_positives": false_positives[:5],  # Limit to 5 files
            "false_negatives": false_negatives[:5],  # Limit to 5 files
            "true_positives_count": len(true_positives),
        })


def _average(total: float, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 3)
