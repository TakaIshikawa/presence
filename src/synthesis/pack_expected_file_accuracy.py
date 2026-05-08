"""Pack expected file accuracy analyzer for prediction quality.

Analyzes how accurately execution packs predict the files they will modify
by comparing expectedFiles arrays against actual changed files from pack
outcomes. Calculates precision and recall metrics to identify commonly missed
file types and patterns of file drift.

Accuracy metrics:
- Precision: Percentage of expected files that were actually changed
- Recall: Percentage of changed files that were expected
- F1 score: Harmonic mean of precision and recall
- False positives: Expected files not changed
- False negatives: Changed files not expected

Prediction patterns:
- Perfect prediction: All expected files changed, no unexpected changes
- Over-prediction: Many expected files not changed (low precision)
- Under-prediction: Many changed files not expected (low recall)
- Commonly missed: Tests, configs, type files often not predicted
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_pack_expected_file_accuracy(records: object) -> dict[str, Any]:
    """Analyze accuracy of expected file predictions in execution packs.

    Compares expectedFiles to actual changed files to measure prediction
    quality and identify patterns in prediction errors.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - expected_files: List of files expected to be modified
            - changed_files: List of files actually changed
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - perfect_predictions: Count of packs with perfect predictions
            - avg_precision: Average precision across packs
            - avg_recall: Average recall across packs
            - avg_f1_score: Average F1 score across packs
            - false_positives_count: Total false positive files
            - false_negatives_count: Total false negative files
            - commonly_missed_types: File types often not predicted
            - examples: Example packs with prediction errors

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    perfect_predictions = 0
    precision_sum = 0.0
    recall_sum = 0.0
    f1_sum = 0.0
    total_false_positives = 0
    total_false_negatives = 0
    false_negative_files: list[str] = []
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        expected_files = _normalize_files(record.get("expected_files"))
        changed_files = _normalize_files(record.get("changed_files"))
        task_title = _string(record.get("task_title"))

        total_packs += 1

        # Calculate sets
        expected_set = set(expected_files)
        changed_set = set(changed_files)

        true_positives = expected_set & changed_set
        false_positives = expected_set - changed_set
        false_negatives = changed_set - expected_set

        # Calculate metrics
        precision = _precision(len(true_positives), len(expected_set))
        recall = _recall(len(true_positives), len(changed_set))
        f1 = _f1_score(precision, recall)

        precision_sum += precision
        recall_sum += recall
        f1_sum += f1

        total_false_positives += len(false_positives)
        total_false_negatives += len(false_negatives)

        # Collect false negatives for pattern analysis
        false_negative_files.extend(false_negatives)

        # Check for perfect prediction
        if precision == 100.0 and recall == 100.0:
            perfect_predictions += 1

        # Collect examples of imperfect predictions
        if (precision < 100.0 or recall < 100.0) and len(examples) < 5:
            examples.append({
                "pack_id": pack_id,
                "task_title": task_title or "unknown",
                "precision": precision,
                "recall": recall,
                "f1_score": f1,
                "false_positives": sorted(false_positives)[:5],
                "false_negatives": sorted(false_negatives)[:5],
            })

    # Calculate averages
    avg_precision = _average(precision_sum, total_packs)
    avg_recall = _average(recall_sum, total_packs)
    avg_f1 = _average(f1_sum, total_packs)

    # Analyze commonly missed file types
    commonly_missed = _analyze_missed_types(false_negative_files)

    return {
        "total_packs": total_packs,
        "perfect_predictions": perfect_predictions,
        "avg_precision": avg_precision,
        "avg_recall": avg_recall,
        "avg_f1_score": avg_f1,
        "false_positives_count": total_false_positives,
        "false_negatives_count": total_false_negatives,
        "commonly_missed_types": commonly_missed,
        "examples": examples,
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


def _precision(true_positives: int, predicted: int) -> float:
    """Calculate precision: TP / (TP + FP) = TP / predicted."""
    if predicted == 0:
        return 100.0 if true_positives == 0 else 0.0
    return round((true_positives / predicted) * 100.0, 2)


def _recall(true_positives: int, actual: int) -> float:
    """Calculate recall: TP / (TP + FN) = TP / actual."""
    if actual == 0:
        return 100.0 if true_positives == 0 else 0.0
    return round((true_positives / actual) * 100.0, 2)


def _f1_score(precision: float, recall: float) -> float:
    """Calculate F1 score: 2 * (precision * recall) / (precision + recall)."""
    if precision + recall == 0:
        return 0.0
    # Convert from percentage back to ratio for calculation
    p = precision / 100.0
    r = recall / 100.0
    f1 = 2 * (p * r) / (p + r)
    return round(f1 * 100.0, 2)


def _average(total: float, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)


def _analyze_missed_types(false_negative_files: list[str]) -> list[dict[str, Any]]:
    """Analyze commonly missed file types from false negatives.

    Returns list of file type patterns with counts.
    """
    if not false_negative_files:
        return []

    # Extract file categories
    type_counter: Counter[str] = Counter()
    for file_path in false_negative_files:
        file_type = _categorize_file(file_path)
        type_counter[file_type] += 1

    # Return top 5 most common
    return [
        {"file_type": file_type, "count": count}
        for file_type, count in type_counter.most_common(5)
    ]


def _categorize_file(file_path: str) -> str:
    """Categorize file by type based on path and extension."""
    path_lower = file_path.lower()

    # Check for test files
    if "test" in path_lower or path_lower.startswith("tests/"):
        return "test"

    # Check for config files
    config_patterns = ["config", ".json", ".yaml", ".yml", ".toml", ".ini"]
    if any(pattern in path_lower for pattern in config_patterns):
        return "config"

    # Check for type definition files
    if path_lower.endswith((".d.ts", ".types.ts", ".interface.ts")):
        return "types"

    # Check for documentation
    if path_lower.endswith((".md", ".rst", ".txt")) or "doc" in path_lower:
        return "docs"

    # Get extension as fallback
    if "." in file_path:
        ext = file_path.rsplit(".", 1)[-1]
        return ext

    return "other"
