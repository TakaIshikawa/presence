"""Pack expectedFiles prediction accuracy analyzer.

Analyzes expectedFiles declarations vs actual files changed to measure prediction
accuracy, identify unexpected modifications, and track test companion coverage.

ExpectedFiles accuracy metrics:
- Precision: Declared files actually changed
- Recall: Changed files that were declared
- Unexpected modifications: Files changed but not declared
- Missing test companions: Tests missing for source changes
- Impact-hint correlation: Declared impact matches actual risk

Quality indicators:
- High precision: >90% declared files changed
- High recall: >85% changed files declared
- Few unexpected mods: <15% undeclared changes
- High test coverage: >80% source files have test companions
- Strong impact correlation: Impact hints match actual complexity
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_expectedfiles_accuracy(records: object) -> dict[str, Any]:
    """Analyze expectedFiles prediction accuracy across pack sessions."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    declared_files_count = 0
    actually_changed_count = 0
    correctly_predicted = 0
    unexpected_modifications = 0
    missing_test_companions = 0
    total_source_files = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        declared = _int(record.get("declared_files_count", 0))
        changed = _int(record.get("actually_changed_count", 0))
        correct = _int(record.get("correctly_predicted", 0))
        unexpected = _int(record.get("unexpected_modifications", 0))
        missing_tests = _int(record.get("missing_test_companions", 0))
        source_files = _int(record.get("total_source_files", 0))

        declared_files_count += declared
        actually_changed_count += changed
        correctly_predicted += correct
        unexpected_modifications += unexpected
        missing_test_companions += missing_tests
        total_source_files += source_files

    precision = _percentage(correctly_predicted, declared_files_count)
    recall = _percentage(correctly_predicted, actually_changed_count)
    unexpected_ratio = _percentage(unexpected_modifications, actually_changed_count)
    test_coverage_ratio = _percentage(
        total_source_files - missing_test_companions,
        total_source_files
    )

    f1_score = (
        2 * (precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    accuracy_score = _calculate_accuracy_score(
        precision,
        recall,
        unexpected_ratio,
        test_coverage_ratio,
    )

    return {
        "total_sessions": total_sessions,
        "declared_files_count": declared_files_count,
        "actually_changed_count": actually_changed_count,
        "correctly_predicted": correctly_predicted,
        "precision": precision,
        "recall": recall,
        "f1_score": round(f1_score, 2),
        "unexpected_modifications": unexpected_modifications,
        "unexpected_ratio": unexpected_ratio,
        "missing_test_companions": missing_test_companions,
        "total_source_files": total_source_files,
        "test_coverage_ratio": test_coverage_ratio,
        "accuracy_score": accuracy_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "declared_files_count": 0,
        "actually_changed_count": 0,
        "correctly_predicted": 0,
        "precision": 0.0,
        "recall": 0.0,
        "f1_score": 0.0,
        "unexpected_modifications": 0,
        "unexpected_ratio": 0.0,
        "missing_test_companions": 0,
        "total_source_files": 0,
        "test_coverage_ratio": 0.0,
        "accuracy_score": 0.0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _calculate_accuracy_score(
    precision: float,
    recall: float,
    unexpected_ratio: float,
    test_coverage_ratio: float,
) -> float:
    """Calculate overall accuracy score (0-1)."""
    # Precision component (0-0.30)
    precision_component = (precision / 100.0) * 0.30

    # Recall component (0-0.30)
    recall_component = (recall / 100.0) * 0.30

    # Unexpected penalty (0-0.20)
    if unexpected_ratio <= 15.0:
        unexpected_component = 0.20
    else:
        penalty = min(unexpected_ratio - 15.0, 85.0) / 85.0
        unexpected_component = 0.20 * (1.0 - penalty)

    # Test coverage component (0-0.20)
    coverage_component = (test_coverage_ratio / 100.0) * 0.20

    score = (
        precision_component +
        recall_component +
        unexpected_component +
        coverage_component
    )
    return round(max(0.0, min(1.0, score)), 3)
