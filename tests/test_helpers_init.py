"""Tests for tests/helpers/__init__.py module initialization.

This module tests that the test helpers package initialization works correctly,
including module registration, import path resolution, and package structure.
"""

import importlib
import sys
from pathlib import Path

import pytest


class TestHelpersModuleInitialization:
    """Test tests/helpers/__init__.py initialization logic."""

    def test_module_can_be_imported(self):
        """Test that tests.helpers module can be imported successfully."""
        import tests.helpers

        assert tests.helpers is not None
        assert hasattr(tests.helpers, "__name__")
        assert tests.helpers.__name__ == "tests.helpers"

    def test_module_has_required_metadata(self):
        """Test that module has required metadata attributes."""
        import tests.helpers

        # Check for required attributes
        assert hasattr(tests.helpers, "__doc__")
        assert hasattr(tests.helpers, "__version__")
        assert hasattr(tests.helpers, "__all__")
        assert hasattr(tests.helpers, "__file__")

    def test_module_version_is_valid(self):
        """Test that __version__ is a valid version string."""
        from tests.helpers import __version__

        assert isinstance(__version__, str)
        assert len(__version__) > 0
        # Should be in format like "1.0.0"
        parts = __version__.split(".")
        assert len(parts) >= 2  # At least major.minor

    def test_module_docstring_is_comprehensive(self):
        """Test that module docstring provides comprehensive documentation."""
        import tests.helpers

        docstring = tests.helpers.__doc__
        assert docstring is not None
        assert len(docstring) > 200  # Comprehensive documentation

        # Should mention key concepts
        assert "Test helpers package" in docstring
        assert "Assertions" in docstring
        assert "Usage" in docstring or "Examples" in docstring

    def test_module_all_exports_list(self):
        """Test that __all__ exports all assertion helpers."""
        import tests.helpers

        expected_exports = [
            "assert_valid_post",
            "assert_valid_thread",
            "assert_valid_candidate",
            "assert_engagement_above_threshold",
            "assert_dedup_detected",
            "assert_evaluation_scores_valid",
            "assert_database_state",
            "assert_no_data_leakage",
            "compose_assertions",
            "__version__",
        ]

        for export in expected_exports:
            assert export in tests.helpers.__all__, f"{export} not in __all__"

    def test_module_location_is_correct(self):
        """Test that module is located in the correct directory."""
        import tests.helpers

        module_file = Path(tests.helpers.__file__)
        assert module_file.exists()
        assert module_file.name == "__init__.py"
        assert module_file.parent.name == "helpers"
        assert module_file.parent.parent.name == "tests"


class TestHelperRegistration:
    """Test that helpers are properly registered in the module."""

    def test_all_assertion_helpers_registered(self):
        """Test that all assertion helpers are registered as module attributes."""
        import tests.helpers

        assertion_helpers = [
            "assert_valid_post",
            "assert_valid_thread",
            "assert_valid_candidate",
            "assert_engagement_above_threshold",
            "assert_dedup_detected",
            "assert_evaluation_scores_valid",
            "assert_database_state",
            "assert_no_data_leakage",
            "compose_assertions",
        ]

        for helper_name in assertion_helpers:
            assert hasattr(tests.helpers, helper_name), f"{helper_name} not registered"
            helper = getattr(tests.helpers, helper_name)
            assert callable(helper), f"{helper_name} is not callable"

    def test_helpers_are_functions_not_strings(self):
        """Test that registered helpers are actual functions, not strings."""
        import tests.helpers

        for name in tests.helpers.__all__:
            if name.startswith("__"):
                continue

            attr = getattr(tests.helpers, name)
            # Should be callable or string for version
            if name == "__version__":
                assert isinstance(attr, str)
            else:
                assert callable(attr), f"{name} should be callable"

    def test_helper_names_match_exports(self):
        """Test that all helpers in __all__ exist as attributes."""
        import tests.helpers

        for name in tests.helpers.__all__:
            assert hasattr(tests.helpers, name), f"{name} in __all__ but not as attribute"

    def test_no_unexpected_public_exports(self):
        """Test that there are no unexpected public exports."""
        import tests.helpers

        # Get all public attributes (not starting with _)
        public_attrs = [
            name
            for name in dir(tests.helpers)
            if not name.startswith("_") and name not in ["annotations"]
        ]

        # All public attributes should be in __all__ or be submodules
        expected_submodules = ["assertions"]  # Submodules are OK
        for attr in public_attrs:
            assert (
                attr in tests.helpers.__all__ or attr in expected_submodules
            ), f"Unexpected public attribute: {attr}"

    def test_helpers_source_module_is_correct(self):
        """Test that imported helpers come from the correct source module."""
        from tests.helpers import assert_valid_post

        # Should be from tests.helpers.assertions
        assert assert_valid_post.__module__ == "tests.helpers.assertions"


class TestImportPathResolution:
    """Test import path resolution for helpers."""

    def test_direct_import_from_package(self):
        """Test importing helpers directly from package."""
        from tests.helpers import (
            assert_valid_post,
            assert_valid_thread,
            compose_assertions,
        )

        assert callable(assert_valid_post)
        assert callable(assert_valid_thread)
        assert callable(compose_assertions)

    def test_import_from_submodule(self):
        """Test importing helpers from submodule path."""
        from tests.helpers.assertions import assert_valid_post

        assert callable(assert_valid_post)

    def test_both_import_paths_same_object(self):
        """Test that both import paths reference the same object."""
        from tests.helpers import assert_valid_post as package_import
        from tests.helpers.assertions import assert_valid_post as module_import

        # Should be the exact same function object
        assert package_import is module_import

    def test_star_import_works(self):
        """Test that star import includes all exported helpers."""
        # Create a new namespace for star import
        namespace = {}
        exec("from tests.helpers import *", namespace)

        # Should have all __all__ exports
        import tests.helpers

        for name in tests.helpers.__all__:
            if not name.startswith("__"):
                assert name in namespace, f"{name} not imported with star import"

    def test_import_with_alias(self):
        """Test importing helpers with aliases."""
        from tests.helpers import (
            assert_valid_post as validate_post,
            compose_assertions as compose,
        )

        assert callable(validate_post)
        assert callable(compose)

    def test_nested_import_resolution(self):
        """Test that nested imports resolve correctly."""
        # Import the package first
        import tests.helpers

        # Then access attributes
        assert callable(tests.helpers.assert_valid_post)
        assert callable(tests.helpers.compose_assertions)


class TestModuleReloading:
    """Test module reloading behavior."""

    def test_module_can_be_reloaded(self):
        """Test that module can be safely reloaded."""
        import tests.helpers

        original_version = tests.helpers.__version__

        # Reload the module
        importlib.reload(tests.helpers)

        # Version should be the same
        assert tests.helpers.__version__ == original_version

    def test_helpers_remain_after_reload(self):
        """Test that helpers remain accessible after reload."""
        import tests.helpers

        # Get a reference before reload
        before_reload = tests.helpers.assert_valid_post

        # Reload
        importlib.reload(tests.helpers)

        # Helper should still be accessible
        after_reload = tests.helpers.assert_valid_post
        assert callable(after_reload)

        # Should be from same module
        assert before_reload.__module__ == after_reload.__module__

    def test_all_exports_preserved_after_reload(self):
        """Test that __all__ is preserved after reload."""
        import tests.helpers

        exports_before = set(tests.helpers.__all__)

        # Reload
        importlib.reload(tests.helpers)

        exports_after = set(tests.helpers.__all__)

        assert exports_before == exports_after


class TestNoCircularImports:
    """Test that there are no circular import issues."""

    def test_import_doesnt_cause_recursion(self):
        """Test that importing doesn't cause infinite recursion."""
        # This would fail with RecursionError if there were circular imports
        import tests.helpers

        assert tests.helpers is not None

    def test_submodule_import_no_recursion(self):
        """Test that importing submodules doesn't cause recursion."""
        import tests.helpers.assertions

        assert tests.helpers.assertions is not None

    def test_import_order_independence(self):
        """Test that import order doesn't matter."""
        # Import in different order
        from tests.helpers.assertions import compose_assertions
        from tests.helpers import assert_valid_post

        assert callable(compose_assertions)
        assert callable(assert_valid_post)


class TestModuleNamespace:
    """Test module namespace isolation."""

    def test_no_pollution_from_assertions_module(self):
        """Test that implementation details don't leak into package namespace."""
        import tests.helpers

        # Should not have these implementation details
        assert not hasattr(tests.helpers, "re")
        assert not hasattr(tests.helpers, "SequenceMatcher")
        assert not hasattr(tests.helpers, "Optional")
        assert not hasattr(tests.helpers, "Callable")

    def test_only_exports_in_all_are_public(self):
        """Test that only items in __all__ are considered public API."""
        import tests.helpers

        # Get attributes not in __all__
        non_exported = [
            name
            for name in dir(tests.helpers)
            if not name.startswith("_")
            and name not in tests.helpers.__all__
            and name not in ["annotations", "assertions"]  # submodules are OK
        ]

        # Should be minimal or empty
        assert len(non_exported) == 0, f"Unexpected exports: {non_exported}"

    def test_version_in_all_is_accessible(self):
        """Test that __version__ is in __all__ and accessible."""
        import tests.helpers

        assert "__version__" in tests.helpers.__all__
        assert hasattr(tests.helpers, "__version__")
        assert isinstance(tests.helpers.__version__, str)


class TestImportPerformance:
    """Test import performance and efficiency."""

    def test_import_is_fast(self):
        """Test that importing the module is reasonably fast."""
        import time

        # Remove from cache if present
        if "tests.helpers" in sys.modules:
            del sys.modules["tests.helpers"]
        if "tests.helpers.assertions" in sys.modules:
            del sys.modules["tests.helpers.assertions"]

        start = time.time()
        import tests.helpers

        elapsed = time.time() - start

        # Should import in less than 1 second (very generous)
        assert elapsed < 1.0, f"Import took {elapsed:.3f}s"

    def test_repeated_imports_use_cache(self):
        """Test that repeated imports use module cache."""
        import tests.helpers as first_import

        import tests.helpers as second_import

        # Should be the exact same object from cache
        assert first_import is second_import


class TestPackageStructure:
    """Test overall package structure."""

    def test_helpers_is_subpackage_of_tests(self):
        """Test that helpers is properly nested under tests package."""
        import tests
        import tests.helpers

        assert hasattr(tests, "helpers")

    def test_package_has_assertions_submodule(self):
        """Test that assertions submodule exists."""
        import tests.helpers.assertions

        assert tests.helpers.assertions is not None

    def test_package_directory_structure(self):
        """Test that package has correct directory structure."""
        import tests.helpers

        helpers_dir = Path(tests.helpers.__file__).parent

        # Should have assertions.py
        assertions_file = helpers_dir / "assertions.py"
        assert assertions_file.exists()

        # Should have __init__.py
        init_file = helpers_dir / "__init__.py"
        assert init_file.exists()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
