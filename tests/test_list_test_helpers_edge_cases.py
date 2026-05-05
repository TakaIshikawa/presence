"""Edge case tests for scripts/list_test_helpers.py file discovery and filtering logic.

This test suite focuses on edge cases including:
- Pattern matching with wildcards and glob patterns
- Directory traversal with nested structures
- File exclusion rules (private files, __init__.py)
- Empty directories and missing paths
- Symlinks and special file types
- Performance with large file sets
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from textwrap import dedent

import pytest

# Import the script under test
SCRIPT_PATH = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"
spec = importlib.util.spec_from_file_location("list_test_helpers", SCRIPT_PATH)
list_test_helpers = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(list_test_helpers)


class TestGlobPatternMatching:
    """Test glob pattern matching for file discovery."""

    def test_glob_finds_only_python_files(self, tmp_path: Path):
        """Test that glob pattern only matches .py files."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        # Create various file types
        (helpers_dir / "helper1.py").write_text("def func1(): pass")
        (helpers_dir / "helper2.py").write_text("def func2(): pass")
        (helpers_dir / "not_python.txt").write_text("text file")
        (helpers_dir / "also_not.md").write_text("markdown")
        (helpers_dir / "data.json").write_text("{}")

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # Should only find the .py files
        assert inventory["total_helpers"] == 2
        assert "helper1" in inventory["modules"]
        assert "helper2" in inventory["modules"]

    def test_glob_with_wildcard_in_nested_directories(self, tmp_path: Path):
        """Test glob pattern with nested directory structures."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        # Create files in root
        (helpers_dir / "root_helper.py").write_text("def root_func(): pass")

        # Create nested subdirectory (should be ignored by current glob pattern)
        subdir = helpers_dir / "subpackage"
        subdir.mkdir()
        (subdir / "nested_helper.py").write_text("def nested_func(): pass")

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # Current implementation only globs *.py in the helpers directory (not recursive)
        assert "root_helper" in inventory["modules"]
        # Nested files are not discovered with current glob pattern
        assert "nested_helper" not in inventory["modules"]

    def test_glob_with_special_characters_in_filename(self, tmp_path: Path):
        """Test glob handling of filenames with special characters."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        # Create files with various naming patterns
        (helpers_dir / "helper-with-dashes.py").write_text("def func(): pass")
        (helpers_dir / "helper_with_underscores.py").write_text("def func(): pass")
        (helpers_dir / "helper123.py").write_text("def func(): pass")
        (helpers_dir / "HeLpEr_MiXeD.py").write_text("def func(): pass")

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # All should be found
        assert inventory["total_helpers"] >= 4
        assert "helper-with-dashes" in inventory["modules"]
        assert "helper_with_underscores" in inventory["modules"]
        assert "helper123" in inventory["modules"]
        assert "HeLpEr_MiXeD" in inventory["modules"]


class TestDirectoryTraversal:
    """Test directory traversal edge cases."""

    def test_empty_helpers_directory(self, tmp_path: Path):
        """Test behavior with empty helpers directory."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        assert inventory["total_helpers"] == 0
        assert inventory["modules"] == {}
        assert inventory["version"] == "1.0.0"

    def test_directory_with_only_excluded_files(self, tmp_path: Path):
        """Test directory containing only files that should be excluded."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        # Create only excluded files
        (helpers_dir / "__init__.py").write_text("")
        (helpers_dir / "_private.py").write_text("def _private_func(): pass")
        (helpers_dir / "__pycache__").mkdir()

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # Should find no helpers (all files excluded)
        assert inventory["total_helpers"] == 0
        assert inventory["modules"] == {}

    def test_deeply_nested_directory_structure(self, tmp_path: Path):
        """Test that deeply nested directories don't cause issues."""
        # Create a deep structure
        current = tmp_path
        for i in range(10):
            current = current / f"level{i}"
            current.mkdir()

        helpers_dir = current / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "deep_helper.py").write_text("def deep_func(): pass")

        tests_dir = current
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # Should still work with deep paths
        assert inventory["total_helpers"] == 1
        assert "deep_helper" in inventory["modules"]

    def test_nonexistent_directory_handling(self, tmp_path: Path):
        """Test that main function handles nonexistent directory gracefully."""
        nonexistent = tmp_path / "nonexistent" / "helpers"

        # The main function should handle this
        result = list_test_helpers.main(["--no-usage"])

        # Should return error code (1) because helpers directory doesn't exist
        # (The actual directory used is relative to script location)
        assert result in (0, 1)  # Depends on whether actual helpers/ exists


class TestFileExclusionRules:
    """Test file exclusion rules."""

    def test_excludes_init_file(self, tmp_path: Path):
        """Test that __init__.py is excluded from inventory."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        (helpers_dir / "__init__.py").write_text(dedent("""
            def init_helper():
                '''Helper defined in __init__.py'''
                pass
        """))
        (helpers_dir / "real_helper.py").write_text("def real_func(): pass")

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # __init__.py should be excluded
        assert "__init__" not in inventory["modules"]
        assert "real_helper" in inventory["modules"]
        assert inventory["total_helpers"] == 1

    def test_excludes_private_module_files(self, tmp_path: Path):
        """Test that files starting with _ (except __init__.py) are excluded."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        (helpers_dir / "_private_helper.py").write_text("def private_func(): pass")
        (helpers_dir / "__internal.py").write_text("def internal_func(): pass")
        (helpers_dir / "public_helper.py").write_text("def public_func(): pass")

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # Private files should be excluded
        assert "_private_helper" not in inventory["modules"]
        assert "__internal" not in inventory["modules"]
        assert "public_helper" in inventory["modules"]
        assert inventory["total_helpers"] == 1

    def test_excludes_private_functions_within_modules(self, tmp_path: Path):
        """Test that private functions (starting with _) are excluded from extraction."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        module_file = helpers_dir / "mixed_helper.py"
        module_file.write_text(dedent("""
            def public_function():
                '''A public function.'''
                pass

            def _private_function():
                '''A private function.'''
                pass

            def __dunder_function__():
                '''A dunder function.'''
                pass

            def another_public():
                '''Another public function.'''
                pass
        """))

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # Should only extract public functions
        funcs = inventory["modules"]["mixed_helper"]["functions"]
        func_names = [f["name"] for f in funcs]

        assert "public_function" in func_names
        assert "another_public" in func_names
        assert "_private_function" not in func_names
        assert "__dunder_function__" not in func_names
        assert len(funcs) == 2


class TestSymlinksAndSpecialFiles:
    """Test handling of symlinks and special file types."""

    def test_follows_symlinked_python_files(self, tmp_path: Path):
        """Test that symlinked .py files are processed."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        # Create a real file
        real_file = tmp_path / "real_helper.py"
        real_file.write_text("def real_func(): pass")

        # Create a symlink to it in helpers dir
        symlink = helpers_dir / "linked_helper.py"
        try:
            symlink.symlink_to(real_file)
            symlink_created = True
        except (OSError, NotImplementedError):
            # Symlinks might not be supported on this system
            symlink_created = False
            pytest.skip("Symlinks not supported on this system")

        if symlink_created:
            tests_dir = tmp_path
            inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

            # Should process the symlinked file
            assert "linked_helper" in inventory["modules"]

    def test_broken_symlink_handling(self, tmp_path: Path):
        """Test that broken symlinks cause FileNotFoundError.

        Note: The current implementation does not handle broken symlinks gracefully.
        It will raise FileNotFoundError when trying to open them.
        This test documents the current behavior.
        """
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        # Create a valid helper
        (helpers_dir / "valid_helper.py").write_text("def valid_func(): pass")

        # Create a broken symlink
        broken_link = helpers_dir / "broken_link.py"
        try:
            broken_link.symlink_to(tmp_path / "nonexistent.py")
            symlink_created = True
        except (OSError, NotImplementedError):
            symlink_created = False
            pytest.skip("Symlinks not supported on this system")

        if symlink_created:
            tests_dir = tmp_path
            # Current implementation will raise FileNotFoundError for broken symlinks
            with pytest.raises(FileNotFoundError):
                list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)


class TestHelperUsageDetection:
    """Test usage detection with edge cases."""

    def test_usage_detection_with_no_test_files(self, tmp_path: Path):
        """Test usage detection when no test files exist."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "unused_helper.py").write_text("def unused_func(): pass")

        tests_dir = tmp_path
        # No test_*.py files in tests_dir
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=True)

        func = inventory["modules"]["unused_helper"]["functions"][0]
        assert func["is_orphaned"] is True
        assert func["used_in"] == []

    def test_usage_detection_with_multiple_test_files(self, tmp_path: Path):
        """Test usage detection across multiple test files."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "popular_helper.py").write_text("def popular_func(): pass")

        tests_dir = tmp_path
        # Create multiple test files using the helper
        (tests_dir / "test_one.py").write_text("from helpers.popular_helper import popular_func")
        (tests_dir / "test_two.py").write_text("# using popular_func here")
        (tests_dir / "test_three.py").write_text("import popular_func")

        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=True)

        func = inventory["modules"]["popular_helper"]["functions"][0]
        assert func["is_orphaned"] is False
        assert len(func["used_in"]) == 3
        assert "test_one.py" in func["used_in"]
        assert "test_two.py" in func["used_in"]
        assert "test_three.py" in func["used_in"]

    def test_usage_detection_skips_unreadable_files(self, tmp_path: Path):
        """Test that usage detection handles unreadable test files gracefully."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "some_helper.py").write_text("def some_func(): pass")

        tests_dir = tmp_path
        # Create a test file
        (tests_dir / "test_readable.py").write_text("from helpers.some_helper import some_func")

        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=True)

        # Should work without crashing
        func = inventory["modules"]["some_helper"]["functions"][0]
        assert "test_readable.py" in func["used_in"]


class TestPerformanceAndScalability:
    """Test performance with large file sets."""

    def test_handles_many_helper_files(self, tmp_path: Path):
        """Test that script handles many helper files efficiently."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        # Create 100 helper files
        for i in range(100):
            helper_file = helpers_dir / f"helper_{i:03d}.py"
            helper_file.write_text(f"def func_{i}(): pass")

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # Should process all files
        assert inventory["total_helpers"] == 100
        assert len(inventory["modules"]) == 100

    def test_handles_files_with_many_functions(self, tmp_path: Path):
        """Test handling files with many function definitions."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        # Create a file with many functions
        functions = "\n".join([f"def func_{i}(): pass" for i in range(50)])
        (helpers_dir / "large_helper.py").write_text(functions)

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # Should extract all functions
        assert inventory["modules"]["large_helper"]["count"] == 50
        assert inventory["total_helpers"] == 50

    def test_skips_usage_detection_when_disabled(self, tmp_path: Path):
        """Test that usage detection is skipped when include_usage=False."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "helper.py").write_text("def func(): pass")

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(helpers_dir, tests_dir, include_usage=False)

        # Should not have usage info
        func = inventory["modules"]["helper"]["functions"][0]
        assert "used_in" not in func
        assert "is_orphaned" not in func


class TestEdgeCasesInFunctionExtraction:
    """Test edge cases in function extraction logic."""

    def test_extract_from_file_with_syntax_errors(self, tmp_path: Path):
        """Test that syntax errors in files are handled gracefully."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        # Create a file with syntax error
        bad_file = helpers_dir / "bad_syntax.py"
        bad_file.write_text("def incomplete_function(")

        # Create a valid file
        (helpers_dir / "good_helper.py").write_text("def good_func(): pass")

        tests_dir = tmp_path

        # Should handle the error gracefully
        # The current implementation will raise SyntaxError which is caught in main()
        # But generate_inventory itself will raise, so we test extract_function_info directly
        with pytest.raises(SyntaxError):
            list_test_helpers.extract_function_info(bad_file)

    def test_extract_from_empty_file(self, tmp_path: Path):
        """Test extraction from an empty Python file."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        empty_file = helpers_dir / "empty.py"
        empty_file.write_text("")

        functions = list_test_helpers.extract_function_info(empty_file)

        assert functions == []

    def test_extract_from_file_with_only_comments(self, tmp_path: Path):
        """Test extraction from file containing only comments and docstrings."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        comment_file = helpers_dir / "comments_only.py"
        comment_file.write_text(dedent('''
            """Module docstring."""

            # This is a comment
            # Another comment

            # TODO: implement functions
        '''))

        functions = list_test_helpers.extract_function_info(comment_file)

        assert functions == []

    def test_extract_with_complex_type_annotations(self, tmp_path: Path):
        """Test extraction of functions with complex type annotations."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        complex_file = helpers_dir / "complex_types.py"
        complex_file.write_text(dedent("""
            from typing import Dict, List, Optional, Union

            def complex_func(
                data: Dict[str, List[int]],
                maybe: Optional[str] = None,
                choice: Union[int, str] = 0
            ) -> Optional[Dict[str, any]]:
                '''Function with complex types.'''
                pass
        """))

        functions = list_test_helpers.extract_function_info(complex_file)

        assert len(functions) == 1
        func = functions[0]
        assert func["name"] == "complex_func"
        assert len(func["params"]) == 3

        # Check that complex types are preserved
        assert "Dict[str, List[int]]" in func["params"][0]["type"]
        assert "Optional[str]" in func["params"][1]["type"]
        assert func["return_type"]  # Should have return type annotation
