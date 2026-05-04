"""Comprehensive tests for scripts/list_test_helpers.py script.

This module tests helper discovery, output formatting, filtering by category,
and error handling for missing directories.
"""

import json
import subprocess
import sys
from pathlib import Path
from textwrap import dedent
from unittest.mock import Mock, patch

import pytest

# Import the script under test
sys.path.insert(0, str(Path(__file__).parent.parent))
from scripts import list_test_helpers


class TestExtractFunctionInfo:
    """Test the extract_function_info function."""

    def test_extract_simple_function(self, tmp_path: Path):
        """Test extracting info from a simple function."""
        module_file = tmp_path / "test_module.py"
        module_file.write_text(dedent("""
            def simple_function():
                \"\"\"A simple function.\"\"\"
                pass
        """))

        functions = list_test_helpers.extract_function_info(module_file)

        assert len(functions) == 1
        assert functions[0]["name"] == "simple_function"
        assert functions[0]["docstring"] == "A simple function."
        assert functions[0]["params"] == []
        assert functions[0]["return_type"] == ""
        assert functions[0]["lineno"] == 2

    def test_extract_function_with_params(self, tmp_path: Path):
        """Test extracting function with parameters."""
        module_file = tmp_path / "test_module.py"
        module_file.write_text(dedent("""
            def func_with_params(name: str, count: int, flag: bool = False):
                \"\"\"Function with parameters.\"\"\"
                pass
        """))

        functions = list_test_helpers.extract_function_info(module_file)

        assert len(functions) == 1
        func = functions[0]
        assert func["name"] == "func_with_params"
        assert len(func["params"]) == 3

        # Check parameter details
        assert func["params"][0] == {"name": "name", "type": "str"}
        assert func["params"][1] == {"name": "count", "type": "int"}
        assert func["params"][2] == {"name": "flag", "type": "bool"}

    def test_extract_function_with_return_type(self, tmp_path: Path):
        """Test extracting function with return type annotation."""
        module_file = tmp_path / "test_module.py"
        module_file.write_text(dedent("""
            def func_returns_str() -> str:
                \"\"\"Returns a string.\"\"\"
                return "hello"
        """))

        functions = list_test_helpers.extract_function_info(module_file)

        assert len(functions) == 1
        assert functions[0]["return_type"] == "str"

    def test_skip_private_functions(self, tmp_path: Path):
        """Test that private functions are skipped."""
        module_file = tmp_path / "test_module.py"
        module_file.write_text(dedent("""
            def public_function():
                \"\"\"Public function.\"\"\"
                pass

            def _private_function():
                \"\"\"Private function.\"\"\"
                pass

            def __dunder_function__():
                \"\"\"Dunder function.\"\"\"
                pass
        """))

        functions = list_test_helpers.extract_function_info(module_file)

        assert len(functions) == 1
        assert functions[0]["name"] == "public_function"

    def test_extract_function_without_docstring(self, tmp_path: Path):
        """Test extracting function without docstring."""
        module_file = tmp_path / "test_module.py"
        module_file.write_text(dedent("""
            def no_docstring():
                pass
        """))

        functions = list_test_helpers.extract_function_info(module_file)

        assert len(functions) == 1
        assert functions[0]["docstring"] == ""

    def test_extract_multiple_functions(self, tmp_path: Path):
        """Test extracting multiple functions from a module."""
        module_file = tmp_path / "test_module.py"
        module_file.write_text(dedent("""
            def first_function():
                \"\"\"First.\"\"\"
                pass

            def second_function():
                \"\"\"Second.\"\"\"
                pass

            def third_function():
                \"\"\"Third.\"\"\"
                pass
        """))

        functions = list_test_helpers.extract_function_info(module_file)

        assert len(functions) == 3
        assert [f["name"] for f in functions] == [
            "first_function",
            "second_function",
            "third_function",
        ]

    def test_extract_complex_type_annotations(self, tmp_path: Path):
        """Test extracting functions with complex type annotations."""
        module_file = tmp_path / "test_module.py"
        module_file.write_text(dedent("""
            from typing import Optional, List, Dict, Any

            def complex_types(
                items: List[str],
                mapping: Dict[str, Any],
                optional: Optional[int] = None
            ) -> Optional[Dict[str, List[str]]]:
                \"\"\"Complex types.\"\"\"
                return None
        """))

        functions = list_test_helpers.extract_function_info(module_file)

        assert len(functions) == 1
        func = functions[0]
        assert func["params"][0]["type"] == "List[str]"
        assert func["params"][1]["type"] == "Dict[str, Any]"
        assert func["params"][2]["type"] == "Optional[int]"
        assert func["return_type"] == "Optional[Dict[str, List[str]]]"

    def test_extract_from_empty_file(self, tmp_path: Path):
        """Test extracting from empty file."""
        module_file = tmp_path / "empty.py"
        module_file.write_text("")

        functions = list_test_helpers.extract_function_info(module_file)

        assert functions == []

    def test_extract_with_syntax_error(self, tmp_path: Path):
        """Test that syntax errors are raised."""
        module_file = tmp_path / "bad_syntax.py"
        module_file.write_text("def broken(:\n")

        with pytest.raises(SyntaxError):
            list_test_helpers.extract_function_info(module_file)


class TestFindHelperUsages:
    """Test the find_helper_usages function."""

    def test_find_usage_in_single_file(self, tmp_path: Path):
        """Test finding usage in a single test file."""
        test_file = tmp_path / "test_something.py"
        test_file.write_text(dedent("""
            from tests.helpers import assert_valid_post

            def test_example():
                assert_valid_post("content")
        """))

        usages = list_test_helpers.find_helper_usages(tmp_path, "assert_valid_post")

        assert "test_something.py" in usages
        assert len(usages) == 1

    def test_find_usage_in_multiple_files(self, tmp_path: Path):
        """Test finding usage in multiple test files."""
        (tmp_path / "test_one.py").write_text("from tests.helpers import my_helper")
        (tmp_path / "test_two.py").write_text("my_helper()")
        (tmp_path / "test_three.py").write_text("# my_helper in comment")

        usages = list_test_helpers.find_helper_usages(tmp_path, "my_helper")

        assert len(usages) == 3
        assert "test_one.py" in usages
        assert "test_two.py" in usages
        assert "test_three.py" in usages

    def test_no_usages_found(self, tmp_path: Path):
        """Test when helper is not used anywhere."""
        (tmp_path / "test_file.py").write_text("def test(): pass")

        usages = list_test_helpers.find_helper_usages(tmp_path, "nonexistent_helper")

        assert usages == []

    def test_ignore_non_test_files(self, tmp_path: Path):
        """Test that non-test files are ignored."""
        (tmp_path / "test_real.py").write_text("my_helper()")
        (tmp_path / "not_test.py").write_text("my_helper()")
        (tmp_path / "helper.py").write_text("def my_helper(): pass")

        usages = list_test_helpers.find_helper_usages(tmp_path, "my_helper")

        assert len(usages) == 1
        assert "test_real.py" in usages

    def test_handle_read_error_gracefully(self, tmp_path: Path):
        """Test graceful handling of files that can't be read."""
        test_file = tmp_path / "test_file.py"
        test_file.write_text("content")
        test_file.chmod(0o000)  # Remove read permissions

        # Should not raise, just skip the file
        try:
            usages = list_test_helpers.find_helper_usages(tmp_path, "helper")
            assert isinstance(usages, list)
        finally:
            test_file.chmod(0o644)  # Restore permissions


class TestGenerateInventory:
    """Test the generate_inventory function."""

    def test_generate_basic_inventory(self, tmp_path: Path):
        """Test generating inventory from helpers directory."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        # Create a helper module
        (helpers_dir / "assertions.py").write_text(dedent("""
            def assert_valid_post(content: str) -> None:
                \"\"\"Validate post content.\"\"\"
                pass

            def assert_valid_thread(tweets: list) -> None:
                \"\"\"Validate thread.\"\"\"
                pass
        """))

        tests_dir = tmp_path
        inventory = list_test_helpers.generate_inventory(
            helpers_dir, tests_dir, include_usage=False
        )

        assert inventory["version"] == "1.0.0"
        assert "assertions" in inventory["modules"]
        assert inventory["total_helpers"] == 2
        assert len(inventory["modules"]["assertions"]["functions"]) == 2

    def test_inventory_includes_usage_info(self, tmp_path: Path):
        """Test inventory includes usage information when requested."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        (helpers_dir / "utils.py").write_text(dedent("""
            def my_helper():
                \"\"\"A helper.\"\"\"
                pass
        """))

        (tmp_path / "test_usage.py").write_text("my_helper()")

        inventory = list_test_helpers.generate_inventory(
            helpers_dir, tmp_path, include_usage=True
        )

        func = inventory["modules"]["utils"]["functions"][0]
        assert "used_in" in func
        assert "is_orphaned" in func
        assert "test_usage.py" in func["used_in"]
        assert func["is_orphaned"] is False

    def test_inventory_detects_orphaned_helpers(self, tmp_path: Path):
        """Test inventory detects helpers that are never used."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        (helpers_dir / "unused.py").write_text(dedent("""
            def orphaned_helper():
                \"\"\"Never used.\"\"\"
                pass
        """))

        inventory = list_test_helpers.generate_inventory(
            helpers_dir, tmp_path, include_usage=True
        )

        func = inventory["modules"]["unused"]["functions"][0]
        assert func["is_orphaned"] is True
        assert func["used_in"] == []

    def test_skip_private_modules(self, tmp_path: Path):
        """Test that private modules are skipped."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        (helpers_dir / "public.py").write_text("def helper(): pass")
        (helpers_dir / "_private.py").write_text("def helper(): pass")

        inventory = list_test_helpers.generate_inventory(
            helpers_dir, tmp_path, include_usage=False
        )

        assert "public" in inventory["modules"]
        assert "_private" not in inventory["modules"]

    def test_skip_init_module(self, tmp_path: Path):
        """Test that __init__.py is skipped."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        (helpers_dir / "__init__.py").write_text("# Init file")
        (helpers_dir / "real_helper.py").write_text("def helper(): pass")

        inventory = list_test_helpers.generate_inventory(
            helpers_dir, tmp_path, include_usage=False
        )

        assert "__init__" not in inventory["modules"]
        assert "real_helper" in inventory["modules"]

    def test_inventory_with_multiple_modules(self, tmp_path: Path):
        """Test inventory with multiple helper modules."""
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()

        for i in range(3):
            (helpers_dir / f"module{i}.py").write_text(f"def helper{i}(): pass")

        inventory = list_test_helpers.generate_inventory(
            helpers_dir, tmp_path, include_usage=False
        )

        assert len(inventory["modules"]) == 3
        assert inventory["total_helpers"] == 3

    def test_inventory_path_relative_to_project(self, tmp_path: Path):
        """Test that paths in inventory are relative to project root."""
        project_root = tmp_path
        tests_dir = project_root / "tests"
        helpers_dir = tests_dir / "helpers"
        helpers_dir.mkdir(parents=True)

        (helpers_dir / "helper.py").write_text("def func(): pass")

        inventory = list_test_helpers.generate_inventory(
            helpers_dir, tests_dir, include_usage=False
        )

        module_path = inventory["modules"]["helper"]["path"]
        assert module_path.startswith("tests/helpers/")


class TestFormatInventoryText:
    """Test the format_inventory_text function."""

    def test_format_basic_text_output(self):
        """Test formatting basic inventory as text."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 2,
            "modules": {
                "assertions": {
                    "path": "tests/helpers/assertions.py",
                    "count": 2,
                    "functions": [
                        {
                            "name": "assert_valid_post",
                            "docstring": "Validate post content.",
                            "params": [{"name": "content", "type": "str"}],
                            "return_type": "None",
                            "lineno": 5,
                        },
                        {
                            "name": "assert_valid_thread",
                            "docstring": "Validate thread.",
                            "params": [],
                            "return_type": "",
                            "lineno": 10,
                        },
                    ],
                }
            },
        }

        output = list_test_helpers.format_inventory_text(inventory)

        assert "Test Helpers Inventory" in output
        assert "Total Helpers: 2" in output
        assert "Version: 1.0.0" in output
        assert "Module: assertions" in output
        assert "assert_valid_post()" in output
        assert "assert_valid_thread()" in output
        assert "Validate post content." in output

    def test_format_includes_parameters(self):
        """Test that parameter information is included."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 1,
            "modules": {
                "test": {
                    "path": "tests/helpers/test.py",
                    "count": 1,
                    "functions": [
                        {
                            "name": "func",
                            "docstring": "Test function.",
                            "params": [
                                {"name": "x", "type": "int"},
                                {"name": "y", "type": "str"},
                            ],
                            "return_type": "",
                            "lineno": 1,
                        }
                    ],
                }
            },
        }

        output = list_test_helpers.format_inventory_text(inventory)

        assert "Parameters: x: int, y: str" in output

    def test_format_shows_usage_info(self):
        """Test that usage information is shown when available."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 1,
            "modules": {
                "test": {
                    "path": "tests/helpers/test.py",
                    "count": 1,
                    "functions": [
                        {
                            "name": "func",
                            "docstring": "",
                            "params": [],
                            "return_type": "",
                            "lineno": 1,
                            "used_in": ["test_a.py", "test_b.py"],
                            "is_orphaned": False,
                        }
                    ],
                }
            },
        }

        output = list_test_helpers.format_inventory_text(inventory)

        assert "Used in: test_a.py, test_b.py" in output

    def test_format_shows_orphaned_warning(self):
        """Test that orphaned helpers are marked with warning."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 1,
            "modules": {
                "test": {
                    "path": "tests/helpers/test.py",
                    "count": 1,
                    "functions": [
                        {
                            "name": "orphaned",
                            "docstring": "",
                            "params": [],
                            "return_type": "",
                            "lineno": 1,
                            "used_in": [],
                            "is_orphaned": True,
                        }
                    ],
                }
            },
        }

        output = list_test_helpers.format_inventory_text(inventory)

        assert "ORPHANED" in output

    def test_format_truncates_long_usage_lists(self):
        """Test that long usage lists are truncated."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 1,
            "modules": {
                "test": {
                    "path": "tests/helpers/test.py",
                    "count": 1,
                    "functions": [
                        {
                            "name": "popular",
                            "docstring": "",
                            "params": [],
                            "return_type": "",
                            "lineno": 1,
                            "used_in": [f"test_{i}.py" for i in range(10)],
                            "is_orphaned": False,
                        }
                    ],
                }
            },
        }

        output = list_test_helpers.format_inventory_text(inventory)

        # Should show first 3 and indicate more
        assert "and 7 more" in output


class TestFormatInventoryJson:
    """Test the format_inventory_json function."""

    def test_format_json_output(self):
        """Test formatting inventory as valid JSON."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 1,
            "modules": {
                "test": {
                    "path": "tests/helpers/test.py",
                    "count": 1,
                    "functions": [
                        {
                            "name": "func",
                            "docstring": "Test.",
                            "params": [],
                            "return_type": "",
                            "lineno": 1,
                        }
                    ],
                }
            },
        }

        output = list_test_helpers.format_inventory_json(inventory)

        # Should be valid JSON
        parsed = json.loads(output)
        assert parsed["version"] == "1.0.0"
        assert parsed["total_helpers"] == 1
        assert "test" in parsed["modules"]

    def test_json_is_indented(self):
        """Test that JSON output is indented for readability."""
        inventory = {"version": "1.0.0", "total_helpers": 0, "modules": {}}

        output = list_test_helpers.format_inventory_json(inventory)

        # Indented JSON should have newlines and spaces
        assert "\n" in output
        assert "  " in output


class TestGenerateMarkdownDocs:
    """Test the generate_markdown_docs function."""

    def test_generate_markdown_structure(self):
        """Test generating markdown with proper structure."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 1,
            "modules": {
                "assertions": {
                    "path": "tests/helpers/assertions.py",
                    "count": 1,
                    "functions": [
                        {
                            "name": "assert_valid_post",
                            "docstring": "Validate post content.",
                            "params": [{"name": "content", "type": "str"}],
                            "return_type": "None",
                            "lineno": 5,
                        }
                    ],
                }
            },
        }

        output = list_test_helpers.generate_markdown_docs(inventory)

        assert "# Test Helpers Reference" in output
        assert "**Version:** 1.0.0" in output
        assert "**Total Helpers:** 1" in output
        assert "### Module: `assertions`" in output
        assert "#### `assert_valid_post(content: str) -> None`" in output
        assert "Validate post content." in output

    def test_markdown_includes_usage_stats(self):
        """Test that usage statistics are included."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 1,
            "modules": {
                "test": {
                    "path": "tests/helpers/test.py",
                    "count": 1,
                    "functions": [
                        {
                            "name": "func",
                            "docstring": "Test.",
                            "params": [],
                            "return_type": "",
                            "lineno": 1,
                            "used_in": ["test_a.py", "test_b.py", "test_c.py"],
                            "is_orphaned": False,
                        }
                    ],
                }
            },
        }

        output = list_test_helpers.generate_markdown_docs(inventory)

        assert "**Used in:** 3 test file(s)" in output

    def test_markdown_omits_orphaned_helpers(self):
        """Test that orphaned helpers don't show usage stats."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 1,
            "modules": {
                "test": {
                    "path": "tests/helpers/test.py",
                    "count": 1,
                    "functions": [
                        {
                            "name": "orphaned",
                            "docstring": "Unused.",
                            "params": [],
                            "return_type": "",
                            "lineno": 1,
                            "used_in": [],
                            "is_orphaned": True,
                        }
                    ],
                }
            },
        }

        output = list_test_helpers.generate_markdown_docs(inventory)

        assert "**Used in:**" not in output


class TestDetectOrphanedHelpers:
    """Test the detect_orphaned_helpers function."""

    def test_detect_single_orphaned_helper(self):
        """Test detecting a single orphaned helper."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 1,
            "modules": {
                "test": {
                    "path": "tests/helpers/test.py",
                    "count": 1,
                    "functions": [
                        {
                            "name": "orphaned_func",
                            "docstring": "",
                            "params": [],
                            "return_type": "",
                            "lineno": 42,
                            "is_orphaned": True,
                        }
                    ],
                }
            },
        }

        orphaned = list_test_helpers.detect_orphaned_helpers(inventory)

        assert len(orphaned) == 1
        assert orphaned[0]["module"] == "test"
        assert orphaned[0]["function"] == "orphaned_func"
        assert orphaned[0]["path"] == "tests/helpers/test.py"
        assert orphaned[0]["lineno"] == 42

    def test_detect_no_orphaned_helpers(self):
        """Test when no orphaned helpers exist."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 1,
            "modules": {
                "test": {
                    "path": "tests/helpers/test.py",
                    "count": 1,
                    "functions": [
                        {
                            "name": "used_func",
                            "is_orphaned": False,
                        }
                    ],
                }
            },
        }

        orphaned = list_test_helpers.detect_orphaned_helpers(inventory)

        assert orphaned == []

    def test_detect_multiple_orphaned_helpers(self):
        """Test detecting multiple orphaned helpers across modules."""
        inventory = {
            "version": "1.0.0",
            "total_helpers": 3,
            "modules": {
                "module_a": {
                    "path": "tests/helpers/module_a.py",
                    "functions": [
                        {"name": "orphan1", "lineno": 10, "is_orphaned": True},
                    ],
                },
                "module_b": {
                    "path": "tests/helpers/module_b.py",
                    "functions": [
                        {"name": "used", "lineno": 20, "is_orphaned": False},
                        {"name": "orphan2", "lineno": 30, "is_orphaned": True},
                    ],
                },
            },
        }

        orphaned = list_test_helpers.detect_orphaned_helpers(inventory)

        assert len(orphaned) == 2
        orphan_names = {o["function"] for o in orphaned}
        assert orphan_names == {"orphan1", "orphan2"}


class TestParseArgs:
    """Test the parse_args function."""

    def test_parse_default_args(self):
        """Test parsing with no arguments."""
        args = list_test_helpers.parse_args([])

        assert args.format == "text"
        assert args.detect_orphans is False
        assert args.generate_docs is False
        assert args.no_usage is False

    def test_parse_json_format(self):
        """Test parsing --format json."""
        args = list_test_helpers.parse_args(["--format", "json"])

        assert args.format == "json"

    def test_parse_markdown_format(self):
        """Test parsing --format markdown."""
        args = list_test_helpers.parse_args(["--format", "markdown"])

        assert args.format == "markdown"

    def test_parse_detect_orphans(self):
        """Test parsing --detect-orphans flag."""
        args = list_test_helpers.parse_args(["--detect-orphans"])

        assert args.detect_orphans is True

    def test_parse_generate_docs(self):
        """Test parsing --generate-docs flag."""
        args = list_test_helpers.parse_args(["--generate-docs"])

        assert args.generate_docs is True

    def test_parse_no_usage(self):
        """Test parsing --no-usage flag."""
        args = list_test_helpers.parse_args(["--no-usage"])

        assert args.no_usage is True

    def test_parse_combined_flags(self):
        """Test parsing multiple flags together."""
        args = list_test_helpers.parse_args([
            "--format", "json",
            "--no-usage",
        ])

        assert args.format == "json"
        assert args.no_usage is True

    def test_parse_invalid_format(self):
        """Test that invalid format raises error."""
        with pytest.raises(SystemExit):
            list_test_helpers.parse_args(["--format", "invalid"])


class TestMainFunction:
    """Test the main function and script execution."""

    def test_main_with_missing_helpers_dir(self, tmp_path: Path, capsys):
        """Test main function when helpers directory doesn't exist."""
        # Create a temporary project without helpers dir
        project_root = tmp_path / "project"
        project_root.mkdir()
        (project_root / "tests").mkdir()

        # Mock Path(__file__).parent to return our temp directory
        with patch("scripts.list_test_helpers.Path") as mock_path:
            mock_file = Mock()
            mock_file.parent = project_root / "scripts"
            mock_path.return_value = mock_file

            result = list_test_helpers.main([])

            assert result == 1

        # Check that error message was printed
        captured = capsys.readouterr()
        assert "error: helpers directory not found" in captured.err

    def test_main_text_output(self, tmp_path: Path, capsys):
        """Test main function with text output."""
        # Create minimal project structure
        project_root = tmp_path
        helpers_dir = project_root / "tests" / "helpers"
        helpers_dir.mkdir(parents=True)

        (helpers_dir / "test_helper.py").write_text(dedent("""
            def my_helper():
                \"\"\"A helper function.\"\"\"
                pass
        """))

        with patch("scripts.list_test_helpers.Path") as mock_path:
            mock_file = Mock()
            mock_file.parent = project_root / "scripts"
            mock_path.return_value = mock_file

            result = list_test_helpers.main(["--no-usage"])

            assert result == 0

        captured = capsys.readouterr()
        assert "Test Helpers Inventory" in captured.out
        assert "my_helper" in captured.out

    def test_main_json_output(self, tmp_path: Path, capsys):
        """Test main function with JSON output."""
        project_root = tmp_path
        helpers_dir = project_root / "tests" / "helpers"
        helpers_dir.mkdir(parents=True)

        (helpers_dir / "helper.py").write_text("def func(): pass")

        with patch("scripts.list_test_helpers.Path") as mock_path:
            mock_file = Mock()
            mock_file.parent = project_root / "scripts"
            mock_path.return_value = mock_file

            result = list_test_helpers.main(["--format", "json", "--no-usage"])

            assert result == 0

        captured = capsys.readouterr()
        # Should be valid JSON
        data = json.loads(captured.out)
        assert "modules" in data
        assert "helper" in data["modules"]

    def test_main_detect_orphans_with_none(self, tmp_path: Path, capsys):
        """Test --detect-orphans when no orphans exist."""
        project_root = tmp_path
        helpers_dir = project_root / "tests" / "helpers"
        helpers_dir.mkdir(parents=True)

        (helpers_dir / "helper.py").write_text("def used_func(): pass")
        (project_root / "tests" / "test_usage.py").write_text("used_func()")

        with patch("scripts.list_test_helpers.Path") as mock_path:
            mock_file = Mock()
            mock_file.parent = project_root / "scripts"
            mock_path.return_value = mock_file

            result = list_test_helpers.main(["--detect-orphans"])

            assert result == 0

        captured = capsys.readouterr()
        assert "No orphaned helpers found" in captured.out

    def test_main_detect_orphans_with_some(self, tmp_path: Path, capsys):
        """Test --detect-orphans when orphans exist."""
        project_root = tmp_path
        helpers_dir = project_root / "tests" / "helpers"
        helpers_dir.mkdir(parents=True)

        (helpers_dir / "helper.py").write_text("def orphaned(): pass")

        with patch("scripts.list_test_helpers.Path") as mock_path:
            mock_file = Mock()
            mock_file.parent = project_root / "scripts"
            mock_path.return_value = mock_file

            result = list_test_helpers.main(["--detect-orphans"])

            assert result == 0

        captured = capsys.readouterr()
        assert "Found 1 orphaned helper" in captured.out
        assert "orphaned" in captured.out

    def test_main_generate_docs(self, tmp_path: Path, capsys):
        """Test --generate-docs flag."""
        project_root = tmp_path
        helpers_dir = project_root / "tests" / "helpers"
        helpers_dir.mkdir(parents=True)

        (helpers_dir / "helper.py").write_text("def func(): pass")

        with patch("scripts.list_test_helpers.Path") as mock_path:
            mock_file = Mock()
            mock_file.parent = project_root / "scripts"
            mock_path.return_value = mock_file

            result = list_test_helpers.main(["--generate-docs", "--no-usage"])

            assert result == 0

        captured = capsys.readouterr()
        assert "# Test Helpers Reference" in captured.out

    def test_main_handles_syntax_error(self, tmp_path: Path, capsys):
        """Test main function handles syntax errors gracefully."""
        project_root = tmp_path
        helpers_dir = project_root / "tests" / "helpers"
        helpers_dir.mkdir(parents=True)

        (helpers_dir / "broken.py").write_text("def broken(:\n")

        with patch("scripts.list_test_helpers.Path") as mock_path:
            mock_file = Mock()
            mock_file.parent = project_root / "scripts"
            mock_path.return_value = mock_file

            result = list_test_helpers.main(["--no-usage"])

            assert result == 1

        captured = capsys.readouterr()
        assert "error: failed to generate inventory" in captured.err


class TestScriptIntegration:
    """Integration tests for the script as a whole."""

    def test_script_runs_on_actual_helpers(self):
        """Test that script runs successfully on actual test helpers."""
        script_path = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"

        result = subprocess.run(
            [sys.executable, str(script_path), "--no-usage"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0
        assert "Test Helpers Inventory" in result.stdout
        assert "assertions" in result.stdout

    def test_script_json_output_is_valid(self):
        """Test that JSON output is valid and complete."""
        script_path = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"

        result = subprocess.run(
            [sys.executable, str(script_path), "--format", "json", "--no-usage"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0

        # Parse JSON and validate structure
        data = json.loads(result.stdout)
        assert "version" in data
        assert "modules" in data
        assert "total_helpers" in data
        assert data["total_helpers"] > 0

    def test_script_finds_real_helpers(self):
        """Test that script finds actual helper functions."""
        script_path = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"

        result = subprocess.run(
            [sys.executable, str(script_path), "--format", "json", "--no-usage"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        data = json.loads(result.stdout)

        # Should find assertions module
        assert "assertions" in data["modules"]

        # Should find known helpers
        assertions_funcs = [f["name"] for f in data["modules"]["assertions"]["functions"]]
        assert "assert_valid_post" in assertions_funcs
        assert "assert_valid_thread" in assertions_funcs
        assert "assert_valid_candidate" in assertions_funcs

    def test_script_markdown_output_has_correct_format(self):
        """Test that markdown output is well-formed."""
        script_path = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"

        result = subprocess.run(
            [sys.executable, str(script_path), "--generate-docs", "--no-usage"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0

        output = result.stdout
        # Check markdown structure
        assert output.startswith("# Test Helpers Reference")
        assert "### Module:" in output
        assert "####" in output  # Function headers
        assert "**" in output  # Bold text


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
