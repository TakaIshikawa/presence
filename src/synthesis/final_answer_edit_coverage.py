"""Final answer edit coverage analyzer for session summary accuracy.

Analyzes whether all files mentioned in the final answer summary were actually
edited during the session, and whether all edited files are mentioned. Detects
cases where the agent claims to have modified files that show no edit operations,
or where edits occurred but weren't mentioned in the summary.

Coverage metrics:
- Claimed files: Files mentioned in final answer
- Actually edited files: Files with edit/write operations
- Phantom edits: Files claimed but not edited
- Unreported edits: Files edited but not claimed

Summary accuracy patterns:
- Accurate: Perfect match between claimed and edited
- Over-claimed: More files claimed than edited
- Under-reported: More files edited than claimed
- Mixed: Both phantom and unreported edits
"""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_final_answer_edit_coverage(records: object) -> dict[str, Any]:
    """Analyze coverage between final answer claims and actual edit operations.

    Verifies whether files mentioned in the final answer summary match the
    files actually edited during the session.

    Args:
        records: Dict with keys:
            - final_answer: The final answer text/summary
            - edited_files: List of files with edit/write operations

    Returns:
        Dict with:
            - has_final_answer: Whether a final answer was provided
            - claimed_files: List of files mentioned in final answer
            - edited_files: List of files actually edited
            - claimed_but_unedited: Files mentioned but not edited (phantom)
            - edited_but_unclaimed: Files edited but not mentioned (unreported)
            - coverage_score: Percentage of claimed files that were edited
            - accuracy_pattern: Classification of summary accuracy

    Raises:
        ValueError: If records is not a dict
    """
    if records is None:
        records = {}
    if not isinstance(records, Mapping):
        raise ValueError("records must be a dictionary")

    final_answer = _string(records.get("final_answer"))
    edited_files = _normalize_files(records.get("edited_files"))

    has_final_answer = bool(final_answer)

    if not has_final_answer:
        return {
            "has_final_answer": False,
            "claimed_files": [],
            "edited_files": edited_files,
            "claimed_but_unedited": [],
            "edited_but_unclaimed": edited_files,
            "coverage_score": 0.0,
            "accuracy_pattern": "no_final_answer",
        }

    # Extract file paths from final answer
    claimed_files = _extract_file_paths(final_answer)

    # Compare claimed vs edited
    claimed_set = set(claimed_files)
    edited_set = set(edited_files)

    claimed_but_unedited = sorted(claimed_set - edited_set)
    edited_but_unclaimed = sorted(edited_set - claimed_set)

    # Calculate coverage score (what percentage of claimed files were actually edited)
    coverage_score = _coverage_score(claimed_set, edited_set)

    # Classify accuracy pattern
    accuracy_pattern = _classify_accuracy_pattern(
        claimed_but_unedited,
        edited_but_unclaimed,
        claimed_set,
        edited_set,
    )

    return {
        "has_final_answer": True,
        "claimed_files": sorted(claimed_set),
        "edited_files": sorted(edited_set),
        "claimed_but_unedited": claimed_but_unedited,
        "edited_but_unclaimed": edited_but_unclaimed,
        "coverage_score": coverage_score,
        "accuracy_pattern": accuracy_pattern,
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


def _extract_file_paths(text: str) -> list[str]:
    """Extract file paths from final answer text.

    Looks for common file path patterns:
    - src/foo/bar.py
    - tests/test_foo.py
    - path/to/file.ext
    - Quoted paths: "src/foo.py" or 'src/foo.py'
    - Backtick paths: `src/foo.py`
    """
    if not text:
        return []

    # Pattern to match file paths with common extensions
    # Matches: path/to/file.ext or ./path/to/file.ext
    pattern = r'(?:^|[\s`"\'(])((?:\.?/)?(?:[a-zA-Z0-9_-]+/)*[a-zA-Z0-9_-]+\.[a-zA-Z0-9]+)(?:[\s`"\'):,.]|$)'

    matches = re.findall(pattern, text, re.MULTILINE)

    # Normalize and deduplicate
    files = []
    seen = set()
    for match in matches:
        normalized = match.strip()
        # Remove leading ./
        if normalized.startswith("./"):
            normalized = normalized[2:]
        # Filter out common false positives
        if normalized and not _is_false_positive(normalized):
            if normalized not in seen:
                files.append(normalized)
                seen.add(normalized)

    return files


def _is_false_positive(path: str) -> bool:
    """Check if path is likely a false positive.

    Common false positives:
    - URLs (example.com)
    - Email-like patterns
    - Version numbers (v1.0.0)
    - Very short paths without directory (file.py)
    """
    # Must have at least one directory separator
    if "/" not in path and "\\" not in path:
        return True

    # Skip URLs
    if "http://" in path or "https://" in path or "www." in path:
        return True

    # Skip email-like patterns
    if "@" in path:
        return True

    return False


def _coverage_score(claimed: set[str], edited: set[str]) -> float:
    """Calculate coverage score.

    Coverage = number of claimed files that were actually edited / total claimed
    Returns 0.0 if no files claimed.
    """
    if not claimed:
        return 0.0

    actually_edited = claimed & edited
    return round((len(actually_edited) / len(claimed)) * 100.0, 2)


def _classify_accuracy_pattern(
    phantom: list[str],
    unreported: list[str],
    claimed: set[str],
    edited: set[str],
) -> str:
    """Classify the accuracy pattern.

    Patterns:
    - accurate: Perfect match
    - over_claimed: More files claimed than actually edited
    - under_reported: Files edited but not mentioned
    - mixed: Both phantom and unreported edits
    """
    has_phantom = len(phantom) > 0
    has_unreported = len(unreported) > 0

    if not has_phantom and not has_unreported and claimed and edited:
        return "accurate"
    elif has_phantom and has_unreported:
        return "mixed"
    elif has_phantom:
        return "over_claimed"
    elif has_unreported:
        return "under_reported"
    else:
        return "empty"
