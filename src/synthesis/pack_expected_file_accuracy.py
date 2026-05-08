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
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_expected_file_accuracy(records: object) -> dict[str, Any]:
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
