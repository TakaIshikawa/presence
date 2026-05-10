"""Pack test coverage and assertion quality analyzer.

Analyzes test quality and coverage across session packs by parsing test file
content to evaluate test density, assertion quality, edge case coverage,
fixture usage, and test isolation.

Metrics tracked:
- tests_per_source_file: Ratio of test functions to source files
- assertions_per_test: Mean assertion count per test function
- edge_case_tests_count: Tests targeting error/boundary/empty conditions
- fixture_usage_count: Number of @pytest.fixture definitions
- test_isolation_violations: Global state or order dependency patterns

Scores returned (0-1):
- test_density: Test function count relative to source files
- assertion_quality: Mean assertions per test, scaled
- edge_case_coverage: Proportion of tests covering edge cases
- test_isolation: Penalized by isolation violations
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any


# --- Patterns ---

_TEST_FUNC_RE = re.compile(r"^\s*def\s+(test_\w+)\s*\(", re.MULTILINE)
_ASSERT_RE = re.compile(r"^\s*assert\s+", re.MULTILINE)
_PYTEST_RAISES_RE = re.compile(r"pytest\.raises\s*\(", re.MULTILINE)
_FIXTURE_RE = re.compile(r"@pytest\.fixture", re.MULTILINE)

# Edge case indicators in test names
_EDGE_CASE_KEYWORDS = (
    "empty", "none", "null", "zero", "negative", "boundary",
    "error", "invalid", "missing", "raises", "fail", "edge",
    "overflow", "underflow", "limit", "max", "min", "default",
    "no_", "without", "malformed", "corrupt", "unexpected",
)

# Test isolation violation patterns
_GLOBAL_STATE_RE = re.compile(r"^\s*(?:global|nonlocal)\s+\w+", re.MULTILINE)
_MODULE_LEVEL_MUTATE_RE = re.compile(
    r"^[A-Z_][A-Z_0-9]*\s*(?:\[.*\])?\s*=\s*", re.MULTILINE
)
_ORDER_DEPENDENCY_RE = re.compile(
    r"(?:pytest\.mark\.order|@pytest\.mark\.run\s*\(\s*order)",
    re.MULTILINE,
)


def _int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _safe_str(value: object) -> str:
    if isinstance(value, str):
        return value
    return ""


def analyze_pack_test_coverage(records: object) -> dict[str, Any]:
    """Analyze test coverage and assertion quality across pack sessions.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - test_files: List of dicts with 'path' and 'content' keys
            - source_file_count: Number of source files in the session

    Returns:
        Dict with metrics and scores for test coverage quality.

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    total_test_functions = 0
    total_assertions = 0
    total_source_files = 0
    total_edge_case_tests = 0
    total_fixture_count = 0
    total_isolation_violations = 0
    total_pytest_raises = 0

    per_test_assertion_counts: list[int] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        source_file_count = _int(record.get("source_file_count"))
        total_source_files += source_file_count

        test_files = record.get("test_files")
        if not isinstance(test_files, list):
            continue

        for test_file in test_files:
            if not isinstance(test_file, Mapping):
                continue

            content = _safe_str(test_file.get("content"))
            if not content:
                continue

            # Extract test functions
            test_funcs = _TEST_FUNC_RE.findall(content)
            total_test_functions += len(test_funcs)

            # Count assertions
            assert_count = len(_ASSERT_RE.findall(content))
            total_assertions += assert_count

            # Count pytest.raises
            raises_count = len(_PYTEST_RAISES_RE.findall(content))
            total_pytest_raises += raises_count

            # Per-test assertion counts (split by test function boundaries)
            func_assertion_counts = _count_assertions_per_test(content)
            per_test_assertion_counts.extend(func_assertion_counts)

            # Edge case tests
            for func_name in test_funcs:
                if _is_edge_case_test(func_name):
                    total_edge_case_tests += 1

            # Fixtures
            fixture_count = len(_FIXTURE_RE.findall(content))
            total_fixture_count += fixture_count

            # Isolation violations
            violations = _count_isolation_violations(content)
            total_isolation_violations += violations

    if total_sessions == 0:
        return _empty_result()

    # Metrics
    tests_per_source_file = (
        round(total_test_functions / total_source_files, 2)
        if total_source_files > 0
        else 0.0
    )

    assertions_per_test = (
        round(sum(per_test_assertion_counts) / len(per_test_assertion_counts), 2)
        if per_test_assertion_counts
        else 0.0
    )

    # Score: test_density (0-1)
    # 5+ tests per source file = 1.0, scale linearly below
    if total_source_files == 0:
        test_density = 0.0 if total_test_functions == 0 else 1.0
    else:
        ratio = total_test_functions / total_source_files
        test_density = round(min(ratio / 5.0, 1.0), 3)

    # Score: assertion_quality (0-1)
    # 3+ assertions per test = 1.0, scale linearly below
    if not per_test_assertion_counts:
        assertion_quality = 0.0
    else:
        avg = sum(per_test_assertion_counts) / len(per_test_assertion_counts)
        assertion_quality = round(min(avg / 3.0, 1.0), 3)

    # Score: edge_case_coverage (0-1)
    # 30%+ of tests cover edge cases = 1.0
    if total_test_functions == 0:
        edge_case_coverage = 0.0
    else:
        edge_ratio = total_edge_case_tests / total_test_functions
        edge_case_coverage = round(min(edge_ratio / 0.3, 1.0), 3)

    # Score: test_isolation (0-1)
    # Penalize by violations relative to test count
    if total_test_functions == 0:
        test_isolation = 1.0
    else:
        violation_ratio = total_isolation_violations / total_test_functions
        test_isolation = round(max(1.0 - violation_ratio, 0.0), 3)

    return {
        "total_sessions": total_sessions,
        "total_test_functions": total_test_functions,
        "total_source_files": total_source_files,
        "tests_per_source_file": tests_per_source_file,
        "assertions_per_test": assertions_per_test,
        "total_assertions": total_assertions,
        "total_pytest_raises": total_pytest_raises,
        "edge_case_tests_count": total_edge_case_tests,
        "fixture_usage_count": total_fixture_count,
        "test_isolation_violations": total_isolation_violations,
        "test_density": test_density,
        "assertion_quality": assertion_quality,
        "edge_case_coverage": edge_case_coverage,
        "test_isolation": test_isolation,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure with default scores."""
    return {
        "total_sessions": 0,
        "total_test_functions": 0,
        "total_source_files": 0,
        "tests_per_source_file": 0.0,
        "assertions_per_test": 0.0,
        "total_assertions": 0,
        "total_pytest_raises": 0,
        "edge_case_tests_count": 0,
        "fixture_usage_count": 0,
        "test_isolation_violations": 0,
        "test_density": 0.0,
        "assertion_quality": 0.0,
        "edge_case_coverage": 0.0,
        "test_isolation": 1.0,
    }


def _is_edge_case_test(func_name: str) -> bool:
    """Check if a test function name indicates edge case testing."""
    name_lower = func_name.lower()
    return any(kw in name_lower for kw in _EDGE_CASE_KEYWORDS)


def _count_assertions_per_test(content: str) -> list[int]:
    """Count assertions within each test function body.

    Splits content by test function definitions and counts assert statements
    in each function body.
    """
    # Split by test function boundaries
    parts = re.split(r"(?=^\s*def\s+test_\w+\s*\()", content, flags=re.MULTILINE)

    counts: list[int] = []
    for part in parts:
        # Only process parts that start with a test function def
        if not re.match(r"\s*def\s+test_\w+\s*\(", part):
            continue

        assert_count = len(_ASSERT_RE.findall(part))
        raises_count = len(_PYTEST_RAISES_RE.findall(part))
        counts.append(assert_count + raises_count)

    return counts


def _count_isolation_violations(content: str) -> int:
    """Count test isolation violations in file content.

    Detects:
    - global/nonlocal statements
    - Module-level mutable state (ALL_CAPS assignments with list/dict)
    - Explicit order dependencies (@pytest.mark.order)
    """
    violations = 0
    violations += len(_GLOBAL_STATE_RE.findall(content))
    violations += len(_ORDER_DEPENDENCY_RE.findall(content))

    # Module-level mutable assignments (list/dict literals)
    for match in _MODULE_LEVEL_MUTATE_RE.finditer(content):
        line = content[match.start():content.find("\n", match.start())]
        # Only count if assigning mutable types (list/dict)
        if re.search(r"=\s*[\[\{]", line):
            violations += 1

    return violations
