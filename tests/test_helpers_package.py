"""Tests for test helpers package initialization and discoverability.

This module tests that:
- All helpers are importable from tests.helpers package
- Package structure is correct
- No circular import issues exist
- Helper inventory script works correctly
- Documentation is accessible
"""

import importlib
import subprocess
import sys
from pathlib import Path

import pytest


class TestHelpersPackageStructure:
    """Test the test helpers package structure and imports."""

    def test_helpers_package_exists(self):
        """Test that tests.helpers package can be imported."""
        import tests.helpers

        assert tests.helpers is not None

    def test_helpers_package_has_docstring(self):
        """Test that helpers package has comprehensive docstring."""
        import tests.helpers

        assert tests.helpers.__doc__ is not None
        assert len(tests.helpers.__doc__) > 100
        assert "Test helpers package" in tests.helpers.__doc__
        assert "Assertions" in tests.helpers.__doc__

    def test_helpers_package_has_version(self):
        """Test that helpers package has version info."""
        from tests.helpers import __version__

        assert __version__ is not None
        assert isinstance(__version__, str)
        assert len(__version__) > 0

    def test_helpers_package_has_all(self):
        """Test that helpers package defines __all__."""
        import tests.helpers

        assert hasattr(tests.helpers, "__all__")
        assert isinstance(tests.helpers.__all__, list)
        assert len(tests.helpers.__all__) > 0

    def test_tests_package_exists(self):
        """Test that tests package can be imported."""
        import tests

        assert tests is not None

    def test_tests_package_has_docstring(self):
        """Test that tests package has docstring."""
        import tests

        assert tests.__doc__ is not None
        assert "Presence test suite" in tests.__doc__


class TestHelpersImports:
    """Test that all helpers can be imported correctly."""

    def test_import_assert_valid_post(self):
        """Test importing assert_valid_post from package."""
        from tests.helpers import assert_valid_post

        assert callable(assert_valid_post)

    def test_import_assert_valid_thread(self):
        """Test importing assert_valid_thread from package."""
        from tests.helpers import assert_valid_thread

        assert callable(assert_valid_thread)

    def test_import_assert_valid_candidate(self):
        """Test importing assert_valid_candidate from package."""
        from tests.helpers import assert_valid_candidate

        assert callable(assert_valid_candidate)

    def test_import_assert_engagement_above_threshold(self):
        """Test importing assert_engagement_above_threshold from package."""
        from tests.helpers import assert_engagement_above_threshold

        assert callable(assert_engagement_above_threshold)

    def test_import_assert_dedup_detected(self):
        """Test importing assert_dedup_detected from package."""
        from tests.helpers import assert_dedup_detected

        assert callable(assert_dedup_detected)

    def test_import_assert_evaluation_scores_valid(self):
        """Test importing assert_evaluation_scores_valid from package."""
        from tests.helpers import assert_evaluation_scores_valid

        assert callable(assert_evaluation_scores_valid)

    def test_import_assert_database_state(self):
        """Test importing assert_database_state from package."""
        from tests.helpers import assert_database_state

        assert callable(assert_database_state)

    def test_import_assert_no_data_leakage(self):
        """Test importing assert_no_data_leakage from package."""
        from tests.helpers import assert_no_data_leakage

        assert callable(assert_no_data_leakage)

    def test_import_compose_assertions(self):
        """Test importing compose_assertions from package."""
        from tests.helpers import compose_assertions

        assert callable(compose_assertions)

    def test_import_all_from_assertions_module(self):
        """Test importing from assertions submodule directly."""
        from tests.helpers.assertions import (
            assert_valid_post,
            assert_valid_thread,
            assert_valid_candidate,
            assert_engagement_above_threshold,
            assert_dedup_detected,
            assert_evaluation_scores_valid,
            assert_database_state,
            assert_no_data_leakage,
            compose_assertions,
        )

        # Verify all are callable
        helpers = [
            assert_valid_post,
            assert_valid_thread,
            assert_valid_candidate,
            assert_engagement_above_threshold,
            assert_dedup_detected,
            assert_evaluation_scores_valid,
            assert_database_state,
            assert_no_data_leakage,
            compose_assertions,
        ]

        for helper in helpers:
            assert callable(helper)

    def test_all_exports_are_importable(self):
        """Test that all items in __all__ can be imported."""
        import tests.helpers

        for name in tests.helpers.__all__:
            # Skip special attributes
            if name.startswith("__"):
                continue

            assert hasattr(tests.helpers, name), f"{name} in __all__ but not importable"
            attr = getattr(tests.helpers, name)
            assert attr is not None

    def test_no_circular_imports(self):
        """Test that importing helpers doesn't cause circular imports."""
        # This test passes if the import completes without error
        import tests.helpers

        # Force reload to ensure no caching issues
        importlib.reload(tests.helpers)

        # Should not raise ImportError or RecursionError
        assert tests.helpers is not None


class TestHelpersUsability:
    """Test that helpers are usable and work as expected."""

    def test_assert_valid_post_basic_usage(self):
        """Test basic usage of assert_valid_post."""
        from tests.helpers import assert_valid_post

        # Should pass for valid content
        assert_valid_post("This is a valid post", char_limit=280)

        # Should fail for empty content
        with pytest.raises(AssertionError, match="Post content is empty"):
            assert_valid_post("")

    def test_assert_valid_thread_basic_usage(self):
        """Test basic usage of assert_valid_thread."""
        from tests.helpers import assert_valid_thread

        tweets = ["First tweet", "Second tweet", "Third tweet"]

        # Should pass for valid thread
        assert_valid_thread(tweets, min_tweets=2, check_continuity=False)

        # Should fail for empty thread
        with pytest.raises(AssertionError, match="Thread is empty"):
            assert_valid_thread([])

    def test_assert_valid_candidate_basic_usage(self):
        """Test basic usage of assert_valid_candidate."""
        from tests.helpers import assert_valid_candidate

        candidate = {"content": "test content", "score": 8.5}

        # Should pass for valid candidate
        assert_valid_candidate(candidate)

        # Should fail for missing fields
        with pytest.raises(AssertionError, match="missing required fields"):
            assert_valid_candidate({})

    def test_compose_assertions_basic_usage(self):
        """Test basic usage of compose_assertions."""
        from tests.helpers import compose_assertions

        # All pass
        def assertion1():
            assert True

        def assertion2():
            assert 1 == 1

        def assertion3():
            assert "test" == "test"

        compose_assertions(assertion1, assertion2, assertion3)

        # Some fail - should collect all failures
        def failing1():
            assert True

        def failing2():
            assert False, "Second failed"

        def failing3():
            assert False, "Third failed"

        with pytest.raises(AssertionError, match="Composed assertion failures"):
            compose_assertions(failing1, failing2, failing3)


class TestHelperInventoryScript:
    """Test the list_test_helpers.py inventory script."""

    def test_script_exists(self):
        """Test that list_test_helpers.py script exists."""
        script_path = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"
        assert script_path.exists()
        assert script_path.is_file()

    def test_script_is_executable(self):
        """Test that script has executable permissions."""
        script_path = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"
        # On Unix systems, check execute bit
        if sys.platform != "win32":
            import os

            assert os.access(script_path, os.X_OK)

    def test_script_runs_without_errors(self):
        """Test that script runs successfully."""
        script_path = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"

        result = subprocess.run(
            [sys.executable, str(script_path), "--no-usage"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0, f"Script failed: {result.stderr}"
        assert len(result.stdout) > 0

    def test_script_generates_json_output(self):
        """Test that script can generate JSON output."""
        script_path = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"

        result = subprocess.run(
            [sys.executable, str(script_path), "--format", "json", "--no-usage"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0
        assert len(result.stdout) > 0

        # Should be valid JSON
        import json

        data = json.loads(result.stdout)
        assert "modules" in data
        assert "total_helpers" in data
        assert data["total_helpers"] > 0

    def test_script_generates_markdown_output(self):
        """Test that script can generate markdown output."""
        script_path = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"

        result = subprocess.run(
            [sys.executable, str(script_path), "--format", "markdown", "--no-usage"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0
        assert len(result.stdout) > 0
        assert "# Test Helpers Reference" in result.stdout

    def test_script_detects_orphans(self):
        """Test that script can detect orphaned helpers."""
        script_path = Path(__file__).parent.parent / "scripts" / "list_test_helpers.py"

        result = subprocess.run(
            [sys.executable, str(script_path), "--detect-orphans"],
            capture_output=True,
            text=True,
            timeout=10,
        )

        assert result.returncode == 0
        # Output should mention orphans or confirm none exist
        assert "orphaned" in result.stdout.lower() or "No orphaned" in result.stdout


class TestHelpersDocumentation:
    """Test that helpers have proper documentation."""

    def test_all_helpers_have_docstrings(self):
        """Test that all exported helpers have docstrings."""
        import tests.helpers

        for name in tests.helpers.__all__:
            if name.startswith("__"):
                continue

            helper = getattr(tests.helpers, name)
            if callable(helper):
                assert (
                    helper.__doc__ is not None
                ), f"{name} is missing a docstring"
                assert len(helper.__doc__) > 10, f"{name} has insufficient docstring"

    def test_helpers_have_usage_examples_in_package_docstring(self):
        """Test that package docstring includes usage examples."""
        import tests.helpers

        docstring = tests.helpers.__doc__
        assert docstring is not None, "Package has no docstring"
        assert "```python" in docstring, "Package docstring missing code examples"
        assert "assert_valid_post" in docstring
        assert "from tests.helpers import" in docstring


class TestNoImportSideEffects:
    """Test that importing helpers has no side effects."""

    def test_importing_helpers_creates_no_files(self):
        """Test that importing helpers doesn't create files."""
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            # Change to temp directory
            original_cwd = os.getcwd()
            try:
                os.chdir(tmpdir)

                # Count files before import
                files_before = set(Path(tmpdir).glob("**/*"))

                # Import helpers
                import tests.helpers

                importlib.reload(tests.helpers)

                # Count files after import
                files_after = set(Path(tmpdir).glob("**/*"))

                # No new files should be created
                assert files_before == files_after

            finally:
                os.chdir(original_cwd)

    def test_importing_helpers_writes_no_stdout(self, capsys):
        """Test that importing helpers produces no output."""
        import tests.helpers

        importlib.reload(tests.helpers)

        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""


class TestHelpersIntegration:
    """Test integration between different helpers."""

    def test_helpers_work_together(self):
        """Test that multiple helpers can be used together."""
        from tests.helpers import (
            assert_valid_post,
            assert_valid_candidate,
            compose_assertions,
        )

        candidate = {"content": "Valid test post", "score": 8.5}

        # Use multiple helpers together
        compose_assertions(
            lambda: assert_valid_post(candidate["content"]),
            lambda: assert_valid_candidate(candidate),
        )

    def test_helpers_can_be_imported_in_any_order(self):
        """Test that import order doesn't matter."""
        # Import in reverse alphabetical order
        from tests.helpers import (
            compose_assertions,
            assert_valid_thread,
            assert_valid_post,
            assert_no_data_leakage,
            assert_evaluation_scores_valid,
            assert_engagement_above_threshold,
            assert_dedup_detected,
            assert_database_state,
            assert_valid_candidate,
        )

        # All should be callable
        assert all(
            callable(h)
            for h in [
                compose_assertions,
                assert_valid_thread,
                assert_valid_post,
                assert_no_data_leakage,
                assert_evaluation_scores_valid,
                assert_engagement_above_threshold,
                assert_dedup_detected,
                assert_database_state,
                assert_valid_candidate,
            ]
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
