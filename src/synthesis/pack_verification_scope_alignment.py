"""Pack verification scope alignment analyzer for execution pack hygiene.

Analyzes the alignment between expected files in an execution pack and the
actual files modified during execution. Detects scope drift where agents
edit files outside the expected set, which can indicate requirements creep,
poor task scoping, or incomplete planning.

Alignment metrics:
- Alignment score: 0.0 to 1.0, measures overlap between expected and actual
- Unexpected files: Files modified but not in expected set
- Missing expected files: Files in expected set but not modified
- Perfect alignment: All expected files modified, no unexpected files

Scope drift patterns:
- Perfect alignment: Score = 1.0, all expected files modified, no drift
- Minor drift: Score >= 0.75, some unexpected files or missing expected files
- Moderate drift: Score 0.5-0.75, significant deviation from expected scope
- Major drift: Score < 0.5, most files differ from expected set
"""

from __future__ import annotations

from typing import Any, Mapping


# Alignment score thresholds
ALIGNMENT_PERFECT = 1.0
ALIGNMENT_HIGH = 0.75
ALIGNMENT_MODERATE = 0.5


def analyze_pack_verification_scope_alignment(records: object) -> dict[str, Any]:
    """Analyze alignment between expected and actual modified files in pack execution.

    Compares the expectedFiles in an execution pack against the actual files
    modified during execution to detect scope drift.

    Args:
        records: List of pack execution dictionaries with keys:
            - pack_id: Execution pack identifier
            - expected_files: List of files expected to be modified
            - modified_files: List of files actually modified during execution
            - task_title: Optional task title for context

    Returns:
        Dict with:
            - total_packs: Total number of pack executions analyzed
            - perfect_alignment_count: Packs with perfect alignment
            - alignment_scores: Dict mapping pack_id to alignment score
            - avg_alignment_score: Average alignment score across all packs
            - unexpected_files_count: Total count of unexpected file modifications
            - missing_expected_count: Total count of expected files not modified
            - drift_examples: Examples of packs with scope drift

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack execution dictionaries")

    total_packs = 0
    perfect_alignment_count = 0
    alignment_scores: dict[str, float] = {}
    total_unexpected = 0
    total_missing = 0
    drift_examples: list[dict[str, Any]] = []
    score_sum = 0.0

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        expected_files = _normalize_files(record.get("expected_files"))
        modified_files = _normalize_files(record.get("modified_files"))
        task_title = _string(record.get("task_title"))

        total_packs += 1

        # Calculate alignment
        expected_set = set(expected_files)
        modified_set = set(modified_files)

        unexpected_files = sorted(modified_set - expected_set)
        missing_expected = sorted(expected_set - modified_set)
        matched_files = sorted(expected_set & modified_set)

        # Calculate alignment score using Jaccard similarity
        alignment_score = _calculate_alignment_score(expected_set, modified_set)
        alignment_scores[pack_id] = alignment_score
        score_sum += alignment_score

        if alignment_score == ALIGNMENT_PERFECT and len(expected_files) > 0:
            perfect_alignment_count += 1

        total_unexpected += len(unexpected_files)
        total_missing += len(missing_expected)

        # Collect drift examples (low alignment or has drift)
        if alignment_score < ALIGNMENT_HIGH or unexpected_files or missing_expected:
            _add_drift_example(
                drift_examples,
                pack_id,
                task_title,
                alignment_score,
                unexpected_files,
                missing_expected,
                matched_files,
            )

    avg_alignment_score = _average(score_sum, total_packs)

    return {
        "total_packs": total_packs,
        "perfect_alignment_count": perfect_alignment_count,
        "alignment_scores": alignment_scores,
        "avg_alignment_score": avg_alignment_score,
        "unexpected_files_count": total_unexpected,
        "missing_expected_count": total_missing,
        "drift_examples": drift_examples[:5],  # Limit to 5 examples
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


def _calculate_alignment_score(expected: set[str], modified: set[str]) -> float:
    """Calculate alignment score using Jaccard similarity.

    Jaccard similarity = |intersection| / |union|
    Returns 1.0 for perfect match, 0.0 for no overlap.
    """
    if not expected and not modified:
        # Both empty - perfect alignment
        return 1.0

    if not expected or not modified:
        # One is empty, other is not - no alignment
        return 0.0

    intersection = expected & modified
    union = expected | modified

    if not union:
        return 0.0

    return round(len(intersection) / len(union), 3)


def _add_drift_example(
    examples: list[dict[str, Any]],
    pack_id: str,
    task_title: str,
    alignment_score: float,
    unexpected_files: list[str],
    missing_expected: list[str],
    matched_files: list[str],
) -> None:
    """Add a drift example if we have fewer than 5."""
    if len(examples) < 5:
        examples.append({
            "pack_id": pack_id,
            "task_title": task_title or "unknown",
            "alignment_score": alignment_score,
            "unexpected_files": unexpected_files[:5],  # Limit to 5 files
            "missing_expected": missing_expected[:5],  # Limit to 5 files
            "matched_files_count": len(matched_files),
        })


def _average(total: float, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 3)
