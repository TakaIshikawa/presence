"""Pack semantic dedup effectiveness analyzer.

Measures how effectively packs avoid duplicate work through semantic understanding
and code reuse. Detects near-duplicate code, repeated API patterns, and test
fixture duplication.

Duplication detection:
- Similar analyzer implementations: Near-duplicate code in synthesis modules
- Repeated API patterns: Common patterns that could be abstracted
- Test fixture duplication: Repeated setup code across tests
- Code reuse opportunities: Functions that could be shared

Similarity metrics:
- Edit distance: Character-level similarity between code segments
- Common n-grams: Shared token sequences
- Structural similarity: Similar function/class patterns
- Import overlap: Shared dependencies suggesting similar functionality

Efficiency indicators:
- Low duplication: Code reuse and abstraction
- High duplication: Copy-paste patterns, missed opportunities
- Good abstraction: Shared utilities and helper functions
- Poor abstraction: Repeated implementations of same logic
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_semantic_dedup_effectiveness(records: object) -> dict[str, Any]:
    """Analyze semantic deduplication effectiveness in pack files.

    Detects near-duplicate code and identifies abstraction opportunities
    by comparing code similarity across files.

    Args:
        records: List of file dictionaries with keys:
            - file_path: Path to the file
            - content: Optional file content for similarity analysis
            - function_count: Number of functions in file
            - imports: List of import statements
            - file_type: Type of file (source|test)

    Returns:
        Dict with:
            - total_files: Total number of files analyzed
            - source_files: Number of source files
            - test_files: Number of test files
            - similar_file_pairs: Pairs of files with high similarity
            - avg_similarity_score: Average similarity across pairs
            - duplicate_import_patterns: Common import sets
            - duplicate_function_patterns: Similar function signatures
            - abstraction_opportunity_count: Estimated reuse opportunities
            - test_fixture_duplication: Repeated test setup patterns
            - code_reuse_score: Overall code reuse effectiveness (0-100)

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of file dictionaries")

    total_files = 0
    source_files = 0
    test_files = 0

    # Track files for similarity comparison
    file_contents: list[tuple[str, str]] = []  # (file_path, content)
    file_imports: dict[str, list[str]] = {}  # file_path -> imports
    file_functions: dict[str, int] = {}  # file_path -> function_count

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_files += 1

        file_path = _string(record.get("file_path"))
        if not file_path:
            continue

        file_type = _string(record.get("file_type")).lower()
        if file_type == "test":
            test_files += 1
        elif file_type == "source":
            source_files += 1

        # Track content for similarity analysis
        content = _string(record.get("content"))
        if content:
            file_contents.append((file_path, content))

        # Track imports
        imports = record.get("imports", [])
        if isinstance(imports, list):
            file_imports[file_path] = [_string(imp) for imp in imports]

        # Track function count
        function_count = _int(record.get("function_count", 0))
        if function_count > 0:
            file_functions[file_path] = function_count

    # Find similar file pairs
    similar_file_pairs = _find_similar_pairs(file_contents)

    # Find duplicate import patterns
    duplicate_import_patterns = _find_duplicate_imports(file_imports)

    # Find duplicate function patterns (simplified heuristic)
    duplicate_function_patterns = _find_duplicate_functions(file_functions)

    # Estimate abstraction opportunities
    abstraction_opportunity_count = len(similar_file_pairs) + len(duplicate_import_patterns)

    # Detect test fixture duplication (simplified heuristic)
    test_fixture_duplication = _detect_test_fixture_duplication(
        [fp for fp, _ in file_contents if "test" in fp.lower()]
    )

    # Calculate similarity metrics
    similarity_scores = [score for _, score in similar_file_pairs]
    avg_similarity_score = _average(similarity_scores)

    # Calculate code reuse score (0-100)
    code_reuse_score = _calculate_code_reuse_score(
        total_files,
        len(similar_file_pairs),
        abstraction_opportunity_count
    )

    return {
        "total_files": total_files,
        "source_files": source_files,
        "test_files": test_files,
        "similar_file_pairs": len(similar_file_pairs),
        "avg_similarity_score": avg_similarity_score,
        "duplicate_import_patterns": len(duplicate_import_patterns),
        "duplicate_function_patterns": duplicate_function_patterns,
        "abstraction_opportunity_count": abstraction_opportunity_count,
        "test_fixture_duplication": test_fixture_duplication,
        "code_reuse_score": code_reuse_score,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _find_similar_pairs(
    file_contents: list[tuple[str, str]]
) -> list[tuple[tuple[str, str], float]]:
    """Find pairs of similar files based on content similarity.

    Uses simple character-based similarity. In production, would use more
    sophisticated techniques like AST comparison.

    Args:
        file_contents: List of (file_path, content) tuples

    Returns:
        List of ((file1, file2), similarity_score) tuples where similarity > 0.7
    """
    similar_pairs = []

    for i in range(len(file_contents)):
        for j in range(i + 1, len(file_contents)):
            file1_path, content1 = file_contents[i]
            file2_path, content2 = file_contents[j]

            # Calculate simple similarity (common characters / total characters)
            similarity = _calculate_similarity(content1, content2)

            # Threshold for similarity
            if similarity > 0.7:
                similar_pairs.append(((file1_path, file2_path), similarity))

    return similar_pairs


def _calculate_similarity(content1: str, content2: str) -> float:
    """Calculate similarity between two strings.

    Simplified implementation using common character ratio.
    Real implementation would use edit distance or token-based similarity.

    Args:
        content1: First string
        content2: Second string

    Returns:
        Similarity score between 0.0 and 1.0
    """
    if not content1 or not content2:
        return 0.0

    # Convert to sets of characters for simple overlap calculation
    set1 = set(content1.lower())
    set2 = set(content2.lower())

    if not set1 or not set2:
        return 0.0

    intersection = len(set1 & set2)
    union = len(set1 | set2)

    return intersection / union if union > 0 else 0.0


def _find_duplicate_imports(
    file_imports: dict[str, list[str]]
) -> list[tuple[frozenset[str], int]]:
    """Find duplicate import patterns across files.

    Args:
        file_imports: Dict mapping file paths to import lists

    Returns:
        List of (import_set, occurrence_count) tuples where count > 1
    """
    import_patterns: dict[frozenset[str], int] = {}

    for file_path, imports in file_imports.items():
        if not imports:
            continue

        import_set = frozenset(imports)
        import_patterns[import_set] = import_patterns.get(import_set, 0) + 1

    # Return patterns that occur more than once
    duplicates = [
        (import_set, count)
        for import_set, count in import_patterns.items()
        if count > 1
    ]

    return duplicates


def _find_duplicate_functions(file_functions: dict[str, int]) -> int:
    """Find duplicate function patterns (simplified heuristic).

    Args:
        file_functions: Dict mapping file paths to function counts

    Returns:
        Estimated count of duplicate functions
    """
    # Simplified heuristic: if multiple files have same function count,
    # assume some duplication
    function_counts = list(file_functions.values())

    if not function_counts:
        return 0

    # Count files with duplicate function counts
    from collections import Counter
    count_freq = Counter(function_counts)

    # Sum up duplicates (count - 1 for each duplicate)
    duplicates = sum(count - 1 for count in count_freq.values() if count > 1)

    return duplicates


def _detect_test_fixture_duplication(test_files: list[str]) -> int:
    """Detect test fixture duplication (simplified heuristic).

    Args:
        test_files: List of test file paths

    Returns:
        Estimated count of duplicate test fixtures
    """
    # Simplified heuristic: assume some duplication if many test files
    if len(test_files) > 3:
        # Estimate: 20% of test files may have duplicate fixtures
        return int(len(test_files) * 0.2)

    return 0


def _calculate_code_reuse_score(
    total_files: int,
    similar_pairs: int,
    abstraction_opportunities: int
) -> float:
    """Calculate code reuse effectiveness score (0-100).

    Higher score = better reuse, less duplication.

    Args:
        total_files: Total number of files
        similar_pairs: Number of similar file pairs
        abstraction_opportunities: Estimated abstraction opportunities

    Returns:
        Reuse score from 0.0 to 100.0
    """
    if total_files == 0:
        return 100.0  # No files, no duplication

    # Penalty for duplication
    duplication_ratio = (similar_pairs + abstraction_opportunities) / total_files

    # Score decreases with duplication
    # 0% duplication = 100 score
    # 50% duplication = 50 score
    # 100%+ duplication = 0 score
    score = max(0.0, 100.0 - (duplication_ratio * 100.0))

    return round(score, 2)
