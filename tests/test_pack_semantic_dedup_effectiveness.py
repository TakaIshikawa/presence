"""Tests for pack semantic dedup effectiveness analyzer."""

import pytest

from synthesis.pack_semantic_dedup_effectiveness import (
    analyze_pack_semantic_dedup_effectiveness,
    _calculate_similarity,
    _find_similar_pairs,
    _find_duplicate_imports,
    _find_duplicate_functions,
    _calculate_code_reuse_score,
)


class TestAnalyzePackSemanticDedupEffectiveness:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_semantic_dedup_effectiveness([])

        assert result["total_files"] == 0
        assert result["source_files"] == 0
        assert result["test_files"] == 0
        assert result["similar_file_pairs"] == 0
        assert result["avg_similarity_score"] == 0.0
        assert result["duplicate_import_patterns"] == 0
        assert result["duplicate_function_patterns"] == 0
        assert result["abstraction_opportunity_count"] == 0
        assert result["test_fixture_duplication"] == 0
        assert result["code_reuse_score"] == 100.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_semantic_dedup_effectiveness(None)
        assert result["total_files"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_semantic_dedup_effectiveness("not a list")

    def test_single_file_no_duplication(self):
        """Verify single file shows no duplication."""
        result = analyze_pack_semantic_dedup_effectiveness([
            {
                "file_path": "src/main.py",
                "file_type": "source",
                "content": "def main():\n    pass",
            }
        ])

        assert result["total_files"] == 1
        assert result["similar_file_pairs"] == 0
        assert result["code_reuse_score"] == 100.0

    def test_file_type_classification(self):
        """Verify file type classification."""
        result = analyze_pack_semantic_dedup_effectiveness([
            {"file_path": "src/main.py", "file_type": "source"},
            {"file_path": "tests/test_main.py", "file_type": "test"},
        ])

        assert result["source_files"] == 1
        assert result["test_files"] == 1

    def test_similar_files_detected(self):
        """Verify similar files are detected."""
        similar_content = "import pytest\n\ndef test_func():\n    assert True"

        result = analyze_pack_semantic_dedup_effectiveness([
            {
                "file_path": "tests/test_a.py",
                "content": similar_content,
                "file_type": "test",
            },
            {
                "file_path": "tests/test_b.py",
                "content": similar_content,
                "file_type": "test",
            },
        ])

        # Similar content should be detected
        assert result["similar_file_pairs"] > 0
        assert result["avg_similarity_score"] > 0.7

    def test_dissimilar_files_not_matched(self):
        """Verify dissimilar files are not matched."""
        result = analyze_pack_semantic_dedup_effectiveness([
            {
                "file_path": "tests/test_a.py",
                "content": "import pytest\ndef test_a(): pass",
                "file_type": "test",
            },
            {
                "file_path": "src/utils.py",
                "content": "from typing import Any\nclass Utils: pass",
                "file_type": "source",
            },
        ])

        # Different content should not match
        assert result["similar_file_pairs"] == 0

    def test_duplicate_import_patterns_detected(self):
        """Verify duplicate import patterns are detected."""
        result = analyze_pack_semantic_dedup_effectiveness([
            {
                "file_path": "src/a.py",
                "imports": ["pytest", "typing"],
            },
            {
                "file_path": "src/b.py",
                "imports": ["pytest", "typing"],
            },
        ])

        assert result["duplicate_import_patterns"] == 1

    def test_unique_imports_no_duplicates(self):
        """Verify unique imports show no duplicates."""
        result = analyze_pack_semantic_dedup_effectiveness([
            {
                "file_path": "src/a.py",
                "imports": ["pytest"],
            },
            {
                "file_path": "src/b.py",
                "imports": ["typing"],
            },
        ])

        assert result["duplicate_import_patterns"] == 0

    def test_duplicate_function_patterns_detected(self):
        """Verify duplicate function patterns are detected."""
        result = analyze_pack_semantic_dedup_effectiveness([
            {
                "file_path": "src/a.py",
                "function_count": 3,
            },
            {
                "file_path": "src/b.py",
                "function_count": 3,
            },
        ])

        # Same function count suggests potential duplication
        assert result["duplicate_function_patterns"] > 0

    def test_abstraction_opportunity_count(self):
        """Verify abstraction opportunity calculation."""
        result = analyze_pack_semantic_dedup_effectiveness([
            {
                "file_path": "src/a.py",
                "imports": ["pytest", "typing"],
            },
            {
                "file_path": "src/b.py",
                "imports": ["pytest", "typing"],
            },
        ])

        # Should have opportunities from duplicate imports
        assert result["abstraction_opportunity_count"] > 0

    def test_test_fixture_duplication_detected(self):
        """Verify test fixture duplication detection."""
        result = analyze_pack_semantic_dedup_effectiveness([
            {
                "file_path": "tests/test_a.py",
                "content": "def test_a(): pass",
                "file_type": "test",
            },
            {
                "file_path": "tests/test_b.py",
                "content": "def test_b(): pass",
                "file_type": "test",
            },
            {
                "file_path": "tests/test_c.py",
                "content": "def test_c(): pass",
                "file_type": "test",
            },
            {
                "file_path": "tests/test_d.py",
                "content": "def test_d(): pass",
                "file_type": "test",
            },
            {
                "file_path": "tests/test_e.py",
                "content": "def test_e(): pass",
                "file_type": "test",
            },
        ])

        # Should detect some fixture duplication in 5+ test files
        assert result["test_fixture_duplication"] > 0

    def test_code_reuse_score_decreases_with_duplication(self):
        """Verify code reuse score with varying duplication levels."""
        # High duplication case
        similar_content = "import pytest\n\ndef test_function_one():\n    assert True\n\ndef test_function_two():\n    assert True"
        result_high_dup = analyze_pack_semantic_dedup_effectiveness([
            {"file_path": "src/a.py", "content": similar_content, "imports": ["pytest", "typing"]},
            {"file_path": "src/b.py", "content": similar_content, "imports": ["pytest", "typing"]},
        ])

        # No duplication case - more files to get meaningful score
        result_no_dup = analyze_pack_semantic_dedup_effectiveness([
            {"file_path": "src/a.py", "content": "code a unique", "imports": ["os"]},
            {"file_path": "src/b.py", "content": "code b unique", "imports": ["sys"]},
            {"file_path": "src/c.py", "content": "code c unique", "imports": ["re"]},
            {"file_path": "src/d.py", "content": "code d unique", "imports": ["json"]},
        ])

        # Score should be lower with high duplication
        assert result_high_dup["code_reuse_score"] <= result_no_dup["code_reuse_score"]

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_semantic_dedup_effectiveness([
            "not a dict",
            {"file_path": "src/main.py", "file_type": "source"},
        ])

        assert result["total_files"] == 1

    def test_empty_file_path_handled(self):
        """Verify empty file path is handled."""
        result = analyze_pack_semantic_dedup_effectiveness([
            {"file_path": "", "content": "code"},
        ])

        assert result["total_files"] == 1


class TestHelperFunctions:
    """Test helper functions."""

    def test_calculate_similarity_identical(self):
        """Verify similarity of identical strings."""
        similarity = _calculate_similarity("hello world", "hello world")
        assert similarity == 1.0

    def test_calculate_similarity_different(self):
        """Verify similarity of different strings."""
        similarity = _calculate_similarity("abc", "xyz")
        assert similarity < 1.0

    def test_calculate_similarity_empty(self):
        """Verify similarity with empty strings."""
        assert _calculate_similarity("", "hello") == 0.0
        assert _calculate_similarity("hello", "") == 0.0

    def test_find_similar_pairs_high_similarity(self):
        """Verify finding similar pairs."""
        files = [
            ("a.py", "import pytest\ndef test(): pass"),
            ("b.py", "import pytest\ndef test(): pass"),
        ]
        pairs = _find_similar_pairs(files)

        assert len(pairs) > 0
        assert pairs[0][1] > 0.7  # Similarity score

    def test_find_similar_pairs_low_similarity(self):
        """Verify dissimilar pairs not matched."""
        files = [
            ("a.py", "abc"),
            ("b.py", "xyz"),
        ]
        pairs = _find_similar_pairs(files)

        assert len(pairs) == 0

    def test_find_duplicate_imports(self):
        """Verify finding duplicate imports."""
        file_imports = {
            "a.py": ["pytest", "typing"],
            "b.py": ["pytest", "typing"],
        }
        duplicates = _find_duplicate_imports(file_imports)

        assert len(duplicates) == 1
        assert duplicates[0][1] == 2  # Occurrence count

    def test_find_duplicate_imports_unique(self):
        """Verify unique imports have no duplicates."""
        file_imports = {
            "a.py": ["pytest"],
            "b.py": ["typing"],
        }
        duplicates = _find_duplicate_imports(file_imports)

        assert len(duplicates) == 0

    def test_find_duplicate_functions(self):
        """Verify finding duplicate functions."""
        file_functions = {
            "a.py": 3,
            "b.py": 3,
            "c.py": 5,
        }
        duplicates = _find_duplicate_functions(file_functions)

        assert duplicates > 0

    def test_calculate_code_reuse_score_no_duplication(self):
        """Verify reuse score with no duplication."""
        score = _calculate_code_reuse_score(10, 0, 0)
        assert score == 100.0

    def test_calculate_code_reuse_score_high_duplication(self):
        """Verify reuse score with high duplication."""
        score = _calculate_code_reuse_score(10, 5, 5)
        assert score == 0.0

    def test_calculate_code_reuse_score_zero_files(self):
        """Verify reuse score with zero files."""
        score = _calculate_code_reuse_score(0, 0, 0)
        assert score == 100.0
