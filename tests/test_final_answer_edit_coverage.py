"""Tests for final answer edit coverage analyzer."""

import pytest

from synthesis.final_answer_edit_coverage import (
    analyze_final_answer_edit_coverage,
    _extract_file_paths,
    _is_false_positive,
    _coverage_score,
    _classify_accuracy_pattern,
)


class TestAnalyzeFinalAnswerEditCoverage:
    """Test main analyzer function."""

    def test_empty_input_returns_default_metrics(self):
        """Verify empty input returns default metrics."""
        result = analyze_final_answer_edit_coverage({})

        assert result["has_final_answer"] is False
        assert result["claimed_files"] == []
        assert result["edited_files"] == []
        assert result["claimed_but_unedited"] == []
        assert result["edited_but_unclaimed"] == []
        assert result["coverage_score"] == 0.0
        assert result["accuracy_pattern"] == "no_final_answer"

    def test_none_input_treated_as_empty_dict(self):
        """Verify None input is treated as empty dict."""
        result = analyze_final_answer_edit_coverage(None)
        assert result["has_final_answer"] is False

    def test_invalid_input_type_raises_error(self):
        """Verify non-dict input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a dictionary"):
            analyze_final_answer_edit_coverage("not a dict")

    def test_missing_final_answer_marks_all_as_unreported(self):
        """Verify missing final answer marks all edits as unreported."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "",
            "edited_files": ["src/foo.py", "src/bar.py"],
        })

        assert result["has_final_answer"] is False
        assert result["edited_but_unclaimed"] == ["src/foo.py", "src/bar.py"]

    def test_perfect_accuracy_single_file(self):
        """Verify perfect accuracy with single file."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "I modified src/foo.py to fix the bug.",
            "edited_files": ["src/foo.py"],
        })

        assert result["has_final_answer"] is True
        assert result["claimed_files"] == ["src/foo.py"]
        assert result["edited_files"] == ["src/foo.py"]
        assert result["claimed_but_unedited"] == []
        assert result["edited_but_unclaimed"] == []
        assert result["coverage_score"] == 100.0
        assert result["accuracy_pattern"] == "accurate"

    def test_perfect_accuracy_multiple_files(self):
        """Verify perfect accuracy with multiple files."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "I modified src/foo.py and tests/test_foo.py.",
            "edited_files": ["src/foo.py", "tests/test_foo.py"],
        })

        assert result["accuracy_pattern"] == "accurate"
        assert result["coverage_score"] == 100.0

    def test_phantom_edit_detected(self):
        """Verify phantom edit (claimed but not edited) is detected."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "I modified src/foo.py and src/bar.py.",
            "edited_files": ["src/foo.py"],
        })

        assert result["claimed_but_unedited"] == ["src/bar.py"]
        assert result["coverage_score"] == 50.0
        assert result["accuracy_pattern"] == "over_claimed"

    def test_unreported_edit_detected(self):
        """Verify unreported edit (edited but not claimed) is detected."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "I modified src/foo.py.",
            "edited_files": ["src/foo.py", "src/bar.py"],
        })

        assert result["edited_but_unclaimed"] == ["src/bar.py"]
        assert result["accuracy_pattern"] == "under_reported"

    def test_mixed_phantom_and_unreported(self):
        """Verify mixed pattern with both phantom and unreported edits."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "I modified src/foo.py and src/bar.py.",
            "edited_files": ["src/foo.py", "src/baz.py"],
        })

        assert result["claimed_but_unedited"] == ["src/bar.py"]
        assert result["edited_but_unclaimed"] == ["src/baz.py"]
        assert result["accuracy_pattern"] == "mixed"

    def test_file_path_extraction_from_various_formats(self):
        """Verify file paths extracted from various text formats."""
        final_answer = """
        I made the following changes:
        - Modified src/foo.py
        - Updated tests/test_foo.py
        - Created `config/settings.json`
        - Fixed "utils/helper.js"
        """
        result = analyze_final_answer_edit_coverage({
            "final_answer": final_answer,
            "edited_files": ["src/foo.py", "tests/test_foo.py", "config/settings.json", "utils/helper.js"],
        })

        assert result["accuracy_pattern"] == "accurate"
        assert len(result["claimed_files"]) == 4

    def test_file_path_normalization(self):
        """Verify file paths with leading ./ are normalized."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "I modified ./src/foo.py.",
            "edited_files": ["src/foo.py"],
        })

        assert result["accuracy_pattern"] == "accurate"

    def test_duplicate_files_handled(self):
        """Verify duplicate file mentions are deduplicated."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "I modified src/foo.py and src/foo.py again.",
            "edited_files": ["src/foo.py"],
        })

        assert len(result["claimed_files"]) == 1
        assert result["accuracy_pattern"] == "accurate"

    def test_no_files_claimed_or_edited(self):
        """Verify handling when no files claimed or edited."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "I analyzed the codebase.",
            "edited_files": [],
        })

        assert result["claimed_files"] == []
        assert result["edited_files"] == []
        assert result["accuracy_pattern"] == "empty"

    def test_edited_files_as_string(self):
        """Verify single edited file as string is handled."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "I modified src/foo.py.",
            "edited_files": "src/foo.py",
        })

        assert result["edited_files"] == ["src/foo.py"]
        assert result["accuracy_pattern"] == "accurate"


class TestExtractFilePaths:
    """Test file path extraction helper."""

    def test_empty_string_returns_empty_list(self):
        """Verify empty string returns empty list."""
        assert _extract_file_paths("") == []

    def test_simple_file_path_extracted(self):
        """Verify simple file path is extracted."""
        paths = _extract_file_paths("I modified src/foo.py.")
        assert "src/foo.py" in paths

    def test_multiple_file_paths_extracted(self):
        """Verify multiple file paths are extracted."""
        text = "I modified src/foo.py and tests/test_foo.py."
        paths = _extract_file_paths(text)
        assert "src/foo.py" in paths
        assert "tests/test_foo.py" in paths

    def test_quoted_file_paths_extracted(self):
        """Verify quoted file paths are extracted."""
        assert "src/foo.py" in _extract_file_paths('I modified "src/foo.py".')
        assert "src/foo.py" in _extract_file_paths("I modified 'src/foo.py'.")
        assert "src/foo.py" in _extract_file_paths("I modified `src/foo.py`.")

    def test_leading_dot_slash_normalized(self):
        """Verify leading ./ is normalized."""
        paths = _extract_file_paths("I modified ./src/foo.py.")
        assert "src/foo.py" in paths

    def test_various_file_extensions(self):
        """Verify various file extensions are recognized."""
        text = "Modified src/foo.py, lib/bar.js, app/baz.ts, cfg/config.json"
        paths = _extract_file_paths(text)
        assert any("foo.py" in p for p in paths)
        assert any("bar.js" in p for p in paths)
        assert any("baz.ts" in p for p in paths)
        assert any("config.json" in p for p in paths)

    def test_file_without_directory_filtered(self):
        """Verify file without directory is filtered as false positive."""
        paths = _extract_file_paths("I modified file.py without a path.")
        # Should be empty because single files without directory are filtered
        assert not any(p == "file.py" for p in paths)

    def test_url_not_extracted_as_file(self):
        """Verify URLs are not extracted as file paths."""
        text = "See http://example.com/docs.html"
        paths = _extract_file_paths(text)
        assert not any("example.com" in p for p in paths)

    def test_duplicates_removed(self):
        """Verify duplicate paths are removed."""
        text = "I modified src/foo.py and src/foo.py again."
        paths = _extract_file_paths(text)
        assert paths.count("src/foo.py") == 1


class TestIsFalsePositive:
    """Test false positive detection helper."""

    def test_file_without_directory_is_false_positive(self):
        """Verify file without directory is false positive."""
        assert _is_false_positive("file.py") is True

    def test_file_with_directory_is_not_false_positive(self):
        """Verify file with directory is not false positive."""
        assert _is_false_positive("src/file.py") is False

    def test_url_is_false_positive(self):
        """Verify URLs are false positives."""
        assert _is_false_positive("http://example.com") is True
        assert _is_false_positive("https://example.com") is True
        assert _is_false_positive("www.example.com") is True

    def test_email_is_false_positive(self):
        """Verify email-like patterns are false positives."""
        assert _is_false_positive("user@example.com") is True


class TestCoverageScore:
    """Test coverage score calculation helper."""

    def test_zero_claimed_returns_zero(self):
        """Verify zero claimed files returns 0.0."""
        assert _coverage_score(set(), {"a", "b"}) == 0.0

    def test_perfect_coverage_returns_100(self):
        """Verify perfect coverage returns 100.0."""
        assert _coverage_score({"a", "b"}, {"a", "b", "c"}) == 100.0

    def test_partial_coverage_calculated_correctly(self):
        """Verify partial coverage is calculated correctly."""
        # 1 out of 2 claimed files were edited = 50%
        score = _coverage_score({"a", "b"}, {"a", "c"})
        assert score == 50.0

    def test_no_overlap_returns_zero(self):
        """Verify no overlap returns 0.0."""
        assert _coverage_score({"a", "b"}, {"c", "d"}) == 0.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        # 1 out of 3 = 33.333...
        score = _coverage_score({"a", "b", "c"}, {"a"})
        assert score == 33.33


class TestClassifyAccuracyPattern:
    """Test accuracy pattern classification helper."""

    def test_accurate_pattern(self):
        """Verify accurate pattern classification."""
        pattern = _classify_accuracy_pattern([], [], {"a"}, {"a"})
        assert pattern == "accurate"

    def test_over_claimed_pattern(self):
        """Verify over_claimed pattern classification."""
        pattern = _classify_accuracy_pattern(["b"], [], {"a", "b"}, {"a"})
        assert pattern == "over_claimed"

    def test_under_reported_pattern(self):
        """Verify under_reported pattern classification."""
        pattern = _classify_accuracy_pattern([], ["c"], {"a"}, {"a", "c"})
        assert pattern == "under_reported"

    def test_mixed_pattern(self):
        """Verify mixed pattern classification."""
        pattern = _classify_accuracy_pattern(["b"], ["c"], {"a", "b"}, {"a", "c"})
        assert pattern == "mixed"

    def test_empty_pattern(self):
        """Verify empty pattern classification."""
        pattern = _classify_accuracy_pattern([], [], set(), set())
        assert pattern == "empty"


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_accurate_final_answer(self):
        """Simulate accurate final answer."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": """I successfully completed the task by modifying:
            - src/synthesis/new_analyzer.py (created new analyzer)
            - tests/test_new_analyzer.py (added comprehensive tests)
            """,
            "edited_files": [
                "src/synthesis/new_analyzer.py",
                "tests/test_new_analyzer.py",
            ],
        })

        assert result["accuracy_pattern"] == "accurate"
        assert result["coverage_score"] == 100.0

    def test_over_claimed_work(self):
        """Simulate over-claiming (phantom edits)."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": """I completed the following:
            - Modified src/foo.py
            - Updated src/bar.py
            - Fixed src/baz.py
            - Added tests/test_all.py
            """,
            "edited_files": ["src/foo.py", "src/bar.py"],
        })

        assert result["accuracy_pattern"] == "over_claimed"
        assert len(result["claimed_but_unedited"]) == 2

    def test_under_reported_work(self):
        """Simulate under-reporting (unreported edits)."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "I modified src/main.py.",
            "edited_files": [
                "src/main.py",
                "src/utils.py",
                "src/config.py",
                "tests/test_main.py",
            ],
        })

        assert result["accuracy_pattern"] == "under_reported"
        assert len(result["edited_but_unclaimed"]) == 3

    def test_no_final_answer_provided(self):
        """Simulate session without final answer."""
        result = analyze_final_answer_edit_coverage({
            "final_answer": "",
            "edited_files": ["src/foo.py", "src/bar.py"],
        })

        assert result["has_final_answer"] is False
        assert result["accuracy_pattern"] == "no_final_answer"
        assert result["edited_but_unclaimed"] == ["src/foo.py", "src/bar.py"]

    def test_complex_final_answer_with_markdown(self):
        """Simulate complex markdown final answer."""
        final_answer = """
        ## Summary

        I implemented the new feature with the following changes:

        1. Created `src/feature/new_feature.py` with the core logic
        2. Added tests in `tests/feature/test_new_feature.py`
        3. Updated configuration in `config/settings.json`
        4. Modified `src/main.py` to integrate the feature

        All tests pass.
        """
        result = analyze_final_answer_edit_coverage({
            "final_answer": final_answer,
            "edited_files": [
                "src/feature/new_feature.py",
                "tests/feature/test_new_feature.py",
                "config/settings.json",
                "src/main.py",
            ],
        })

        assert result["accuracy_pattern"] == "accurate"
        assert len(result["claimed_files"]) == 4
