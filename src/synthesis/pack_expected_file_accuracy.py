<<<<<<< HEAD
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
=======
"""Pack expected file accuracy analyzer for pack planning quality.

Analyzes how accurately execution packs predict the files they will modify.
Compares expectedFiles arrays against actual changed files from pack outcomes
to measure planning precision and identify systematic blind spots.

Accuracy metrics:
- Precision: Percentage of expected files actually changed
- Recall: Percentage of changed files that were expected
- F1 score: Harmonic mean of precision and recall
- False positives: Expected files not changed
- False negatives: Changed files not expected

Drift patterns:
- Test files: Commonly missed test file updates
- Config files: Forgotten configs, types, or build files
- Related modules: Changes in unexpected but related files
- Directory drift: Files in same directory as expected changes
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_expected_file_accuracy(records: object) -> dict[str, Any]:
<<<<<<< HEAD
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
=======
    """Analyze expected file prediction accuracy for execution packs.

    Compares expectedFiles to actual changed files to measure planning quality.

    Args:
        records: Dict with keys:
            - expected_files: List of files expected to change
            - changed_files: List of files actually changed
            - file_categories: Optional dict mapping files to categories

    Returns:
        Dict with:
            - expected_files: List of expected files
            - changed_files: List of actually changed files
            - correctly_expected: Files both expected and changed
            - false_positives: Expected but not changed
            - false_negatives: Changed but not expected
            - precision: Percentage of expected files that changed
            - recall: Percentage of changed files that were expected
            - f1_score: Harmonic mean of precision and recall
            - missed_categories: Categories of commonly missed files
            - accuracy_pattern: Classification of prediction quality

    Raises:
        ValueError: If records is not a dict
    """
    if records is None:
        records = {}
    if not isinstance(records, Mapping):
        raise ValueError("records must be a dictionary")

    expected_files = _normalize_files(records.get("expected_files"))
    changed_files = _normalize_files(records.get("changed_files"))
    file_categories = records.get("file_categories", {})

    expected_set = set(expected_files)
    changed_set = set(changed_files)

    correctly_expected = sorted(expected_set & changed_set)
    false_positives = sorted(expected_set - changed_set)
    false_negatives = sorted(changed_set - expected_set)

    precision = _calculate_precision(len(correctly_expected), len(expected_files))
    recall = _calculate_recall(len(correctly_expected), len(changed_files))
    f1_score = _calculate_f1_score(precision, recall)

    missed_categories = _identify_missed_categories(false_negatives, file_categories)
    accuracy_pattern = _classify_accuracy_pattern(
        precision,
        recall,
        len(false_positives),
        len(false_negatives),
    )

    return {
        "expected_files": expected_files,
        "changed_files": changed_files,
        "correctly_expected": correctly_expected,
        "false_positives": false_positives,
        "false_negatives": false_negatives,
        "precision": precision,
        "recall": recall,
        "f1_score": f1_score,
        "missed_categories": missed_categories,
        "accuracy_pattern": accuracy_pattern,
    }


def _normalize_files(value: object) -> list[str]:
    """Normalize file list, handling various input types."""
    if isinstance(value, str):
        files = [value]
    elif isinstance(value, (list, tuple)):
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
        files = [f for f in value if isinstance(f, str)]
    else:
        return []

    # Normalize file paths
    normalized = []
    for file in files:
        file = file.strip()
        if not file:
            continue
<<<<<<< HEAD
        # Convert backslashes to forward slashes for Windows paths
        file = file.replace("\\", "/")
        # Remove leading ./ if present
=======
        # Convert backslashes to forward slashes
        file = file.replace("\\", "/")
        # Remove leading ./
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
        if file.startswith("./"):
            file = file[2:]
        normalized.append(file)

    return normalized


<<<<<<< HEAD
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
=======
def _calculate_precision(correct: int, expected: int) -> float:
    """Calculate precision: correct / expected.

    Precision measures what percentage of expected files actually changed.
    """
    if expected <= 0:
        return 0.0
    return round((correct / expected) * 100.0, 2)


def _calculate_recall(correct: int, changed: int) -> float:
    """Calculate recall: correct / changed.

    Recall measures what percentage of changed files were expected.
    """
    if changed <= 0:
        return 0.0
    return round((correct / changed) * 100.0, 2)


def _calculate_f1_score(precision: float, recall: float) -> float:
    """Calculate F1 score: harmonic mean of precision and recall."""
    if precision + recall == 0:
        return 0.0
    return round(2 * (precision * recall) / (precision + recall), 2)


def _identify_missed_categories(
    missed_files: list[str],
    file_categories: Mapping[str, str],
) -> list[dict[str, Any]]:
    """Identify categories of commonly missed files.

    Returns list of categories with counts, sorted by frequency.
    """
    if not missed_files:
        return []

    # Auto-detect categories if not provided
    category_counts: dict[str, int] = {}

    for file in missed_files:
        # Use provided category or auto-detect
        if isinstance(file_categories, Mapping) and file in file_categories:
            category = file_categories[file]
        else:
            category = _auto_categorize_file(file)

        category_counts[category] = category_counts.get(category, 0) + 1

    # Sort by count descending
    sorted_categories = sorted(
        category_counts.items(),
        key=lambda x: x[1],
        reverse=True,
    )

    return [
        {"category": category, "count": count}
        for category, count in sorted_categories
    ]


def _auto_categorize_file(file_path: str) -> str:
    """Auto-categorize file based on path and extension.

    Categories:
    - test: Test files
    - config: Config and build files
    - types: Type definition files
    - docs: Documentation
    - source: Regular source files
    """
    file_lower = file_path.lower()

    # Test files
    if "/test" in file_lower or "test_" in file_lower or "_test." in file_lower:
        return "test"

    # Config files
    config_indicators = (
        "config",
        "package.json",
        "tsconfig",
        "pyproject.toml",
        "setup.py",
        "cargo.toml",
        ".env",
        "dockerfile",
        "makefile",
    )
    if any(indicator in file_lower for indicator in config_indicators):
        return "config"

    # Type definitions
    if file_lower.endswith((".d.ts", ".types.ts", ".pyi")):
        return "types"

    # Documentation
    if file_lower.endswith((".md", ".rst", ".txt")) or "/docs/" in file_lower:
        return "docs"

    # Default: source
    return "source"


def _classify_accuracy_pattern(
    precision: float,
    recall: float,
    false_positive_count: int,
    false_negative_count: int,
) -> str:
    """Classify accuracy pattern based on metrics.

    Patterns:
    - perfect: 100% precision and recall
    - accurate: High precision and recall (>=80%)
    - over_predicted: High precision, low recall (many false positives)
    - under_predicted: Low precision, high recall (many false negatives)
    - poor: Low precision and recall (<50%)
    - empty: No predictions or changes
    """
    # Check for edge cases - only "empty" if no predictions AND no changes
    if precision == 0.0 and recall == 0.0:
        # If there were predictions or changes, it's poor, not empty
        if false_positive_count > 0 or false_negative_count > 0:
            return "poor"
        return "empty"

    # Perfect match
    if precision == 100.0 and recall == 100.0:
        return "perfect"

    # High accuracy
    if precision >= 80.0 and recall >= 80.0:
        return "accurate"

    # Over-predicted (expected more than changed)
    if false_positive_count > false_negative_count and recall >= 60.0:
        return "over_predicted"

    # Under-predicted (missed many changes)
    if false_negative_count > false_positive_count and precision >= 60.0:
        return "under_predicted"

    # Poor predictions
    if precision < 50.0 or recall < 50.0:
        return "poor"

    # Default: moderate accuracy
    return "moderate"
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
