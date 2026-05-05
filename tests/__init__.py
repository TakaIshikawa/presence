"""Presence test suite package.

This package marks the tests directory as a Python package, enabling proper
import of test helpers, fixtures, and utilities across test modules.

## Test Organization

The test suite is organized into:
- `tests/test_*.py` - Individual test modules
- `tests/conftest.py` - Shared pytest fixtures
- `tests/helpers/` - Custom assertion helpers and utilities
- `tests/fixtures/` - Test data files

## Importing Test Helpers

Import helpers directly from the helpers package:

```python
from tests.helpers import assert_valid_post, assert_valid_thread
```

## Importing Fixtures

Fixtures defined in conftest.py are automatically available to all tests:

```python
def test_database_operations(db, sample_message):
    # db and sample_message fixtures are auto-injected
    pass
```

## Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_module.py

# Run with coverage
pytest --cov=src --cov-report=term-missing
```

See tests/README.md for comprehensive testing documentation.
"""

import sys as _sys
import warnings as _warnings
from pathlib import Path as _Path


# Re-export commonly used fixtures for convenience
# Note: Fixtures are auto-discovered by pytest from conftest.py,
# but we can import them here for type hints or programmatic access

__all__ = []  # No public API from this module


def _validate_test_environment():
    """Validate that test environment is properly configured.

    Checks:
    - Required test dependencies are available
    - Test directory structure is intact
    - Python version meets minimum requirements

    Raises:
        ImportError: If required test dependencies are missing
        EnvironmentError: If test environment is misconfigured
    """
    # Check Python version
    if _sys.version_info < (3, 11):
        raise EnvironmentError(
            f"Tests require Python 3.11 or higher. Current version: {_sys.version_info[0]}.{_sys.version_info[1]}"
        )

    # Verify pytest is available
    try:
        import pytest  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "pytest is required to run tests. Install it with: pip install pytest"
        ) from e

    # Check that test directory structure exists
    tests_dir = _Path(__file__).parent
    required_paths = {
        "helpers": tests_dir / "helpers",
    }

    missing_paths = []
    for name, path in required_paths.items():
        if not path.exists():
            missing_paths.append(f"{name} ({path})")

    if missing_paths:
        _warnings.warn(
            f"Test directory structure incomplete. Missing: {', '.join(missing_paths)}",
            UserWarning,
            stacklevel=2,
        )


def _initialize_test_package():
    """Initialize the test package with error handling.

    Performs initialization steps with proper error handling and reporting.
    """
    try:
        _validate_test_environment()
    except ImportError as e:
        # Critical error - cannot proceed without dependencies
        print(f"ERROR: Test initialization failed - {e}", file=_sys.stderr)
        raise
    except EnvironmentError as e:
        # Critical error - environment misconfigured
        print(f"ERROR: Test environment misconfigured - {e}", file=_sys.stderr)
        raise
    except Exception as e:
        # Unexpected error during initialization
        print(
            f"WARNING: Unexpected error during test initialization: {e}",
            file=_sys.stderr,
        )
        # Don't raise - allow tests to attempt running


# Run initialization when module is imported
_initialize_test_package()
